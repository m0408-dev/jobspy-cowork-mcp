"""
Free job-data sources (no proxy, no API key beyond public client-ids).
=======================================================================

Complements the JobSpy scrapers with official / public JSON APIs that work from a
datacenter IP. Tuned for the DACH market: German-language IT / support roles, 100 %
Homeoffice. Everything is free, key-less (or a public client-id), fetched concurrently
over httpx, and normalised into ONE schema.

Common job schema returned by every ``fetch_*`` coroutine::

    {source, title, company, location, is_remote, date_posted, salary, job_url, description}

Sources: arbeitsagentur, himalayas, remotive, remoteok, arbeitnow, jobicy, hackernews,
weworkremotely, themuse (themuse kept but OUT of the default set — weak for DACH).
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import html
import logging
import re
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

log = logging.getLogger("jobspy-mcp.sources")

# Default aggregated set — all sources on (recall-first; the AI classifies downstream).
API_SOURCES = [
    "arbeitsagentur", "himalayas", "remotive", "remoteok",
    "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse",
]
# Remote-first sources used by search_remote_jobs.
REMOTE_SOURCES = ["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse"]

SOURCE_INFO: dict[str, dict[str, str]] = {
    "arbeitsagentur": {"coverage": "Germany (largest official DB)", "remote": "arbeitszeit=ho + keyword", "auth": "free public client-id"},
    "himalayas":      {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none"},
    "remotive":       {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none (search ignored → server-filtered)"},
    "remoteok":       {"coverage": "remote tech", "remote": "remote-only", "auth": "none"},
    "arbeitnow":      {"coverage": "Germany + EU + remote (best DE-remote API)", "remote": "mixed", "auth": "none"},
    "jobicy":         {"coverage": "remote worldwide (geo=germany for DACH)", "remote": "remote-only", "auth": "none"},
    "hackernews":     {"coverage": "startups/tech 'Who is hiring'", "remote": "mixed", "auth": "none"},
    "weworkremotely": {"coverage": "remote (programming)", "remote": "remote-only", "auth": "none"},
    "themuse":        {"coverage": "global US-heavy (NOT in default set)", "remote": "mixed", "auth": "none"},
    "indeed":         {"coverage": "global via JobSpy", "remote": "filterable", "auth": "none (proxy in cloud)"},
    "linkedin":       {"coverage": "global via JobSpy", "remote": "filterable", "auth": "none (rate-limited)"},
}

_UA = "Mozilla/5.0 (compatible; jobspy-mcp/1.0; +https://github.com/m0408-dev/jobspy-cowork-mcp)"
_HEADERS = {"User-Agent": _UA, "Accept": "application/json"}
_TIMEOUT = httpx.Timeout(25.0, connect=10.0)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]*\n[ \t]*")

# Words that mark a remote / home-office role (DE + EN).
REMOTE_KW = re.compile(
    r"home\s?office|homeoffice|remote|mobiles?\s+arbeiten|telearbeit|fully\s+remote|100\s*%\s*remote|remote[-\s]?first",
    re.IGNORECASE,
)
# Titles that are clearly content, not job postings (BUG6). Kept minimal on purpose so we
# never drop a real job (e.g. 'Tour Guide', 'Studienberater') — only unambiguous non-jobs.
_NON_JOB = re.compile(
    r"\b(webinar|whitepaper|white\s?paper|e-?book|newsletter|blog(?:post|beitrag)?|podcast)\b",
    re.IGNORECASE,
)
# Location strings that clearly restrict to a NON-DACH / non-European region (BUG5).
_NON_DACH = re.compile(
    r"\b(usa|u\.s\.a?\.?|united\s+states|us\s+only|remote\s*[-(]?\s*us|canada|australia|new\s+zealand|"
    r"philippines|india|pakistan|bangladesh|brazil|brasil|argentina|mexico|colombia|latam|apac|"
    r"singapore|indonesia|nigeria|kenya|south\s+africa|uae|dubai|japan|china|"
    r"new\s+york|san\s+francisco|los\s+angeles|toronto|bangalore|bengaluru|manila|"
    r"são\s+paulo|sao\s+paulo|sydney|melbourne|austin|seattle|chicago|boston)\b",
    re.IGNORECASE,
)
_DACH_OK = re.compile(
    r"\b(german|germany|deutschland|austria|österreich|oesterreich|switzerland|schweiz|dach|"
    r"europe|european|eu|emea|cet|worldwide|anywhere|global|remote)\b",
    re.IGNORECASE,
)

# German IT / support synonym groups — expand a query token to related terms so e.g.
# "IT-Support" also matches Helpdesk / Service-Desk / Anwenderbetreuung (feature wish).
_SYNONYMS: dict[str, set[str]] = {
    "support": {"support", "helpdesk", "servicedesk", "anwenderbetreuung"},
    "helpdesk": {"helpdesk", "support", "servicedesk"},
    "servicedesk": {"servicedesk", "helpdesk", "support"},
    "systemadministrator": {"systemadministrator", "sysadmin", "administrator", "systemadmin"},
    "administrator": {"administrator", "systemadministrator", "sysadmin", "admin"},
    "sysadmin": {"sysadmin", "systemadministrator", "administrator"},
    "systemadministration": {"systemadministration", "systemadministrator", "sysadmin"},
    "fachinformatiker": {"fachinformatiker", "systemintegration"},
    "it": {"it", "edv", "ict"},
    "edv": {"edv", "it", "ict"},
}


def _strip_html(text: Any, limit: int = 1500) -> str | None:
    if not text:
        return None
    s = html.unescape(_TAG_RE.sub(" ", str(text)))
    s = _WS_RE.sub("\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    if len(s) > limit:
        s = s[:limit].rstrip() + " …[truncated — open job_url for full text]"
    return s or None


def _tokens(term: str) -> list[str]:
    """Significant query tokens: split on non-word chars, keep len>=2, lower-cased (DE chars kept)."""
    return [t for t in re.split(r"[^0-9a-zA-Zäöüß]+", (term or "").lower()) if len(t) >= 2]


def _hit(hay: str, tok: str) -> bool:
    """Whole-word (synonym-expanded) match of one query token against the haystack."""
    for e in _SYNONYMS.get(tok, {tok}):
        if re.search(rf"(?<![0-9a-zäöüß]){re.escape(e)}(?![0-9a-zäöüß])", hay):
            return True
    return False


def _match_any(job: dict[str, Any], term: str) -> bool:
    """Recall-first relevance gate: keep a job if ANY significant (synonym-expanded) query
    token matches as a whole word in title/company/location/description. Deliberately GENEROUS
    — an AI classifies the results downstream, so it is better to return too many than too few.
    Only jobs that match nothing at all (e.g. a pure-gibberish query) are dropped."""
    toks = [t for t in _tokens(term) if len(t) >= 3 and not t.isdigit()]
    if not toks:  # very short / 1-2 char queries → don't filter, let everything through
        return True
    hay = " ".join(str(job.get(k) or "") for k in ("title", "company", "location", "description")).lower()
    return any(_hit(hay, t) for t in toks)


def looks_like_job(job: dict[str, Any]) -> bool:
    """False for obvious articles/webinars/blog posts, not real postings (BUG6)."""
    title = job.get("title") or ""
    if not title.strip():
        return False
    return not _NON_JOB.search(title)


def looks_remote(text: str | None) -> bool:
    return bool(text and REMOTE_KW.search(text))


def dach_ok(location: str | None) -> bool:
    """True unless the location clearly restricts to a non-DACH/non-European region (BUG5).
    Unknown / worldwide / anywhere / europe are kept (accessible from DACH)."""
    loc = (location or "").strip().lower()
    if not loc:
        return True
    if _DACH_OK.search(loc):
        return True
    if _NON_DACH.search(loc):
        return False
    return True


def _parse_date(date_posted: Any) -> _dt.date | None:
    """Best-effort parse of the many date formats our sources emit (ISO, RFC-822 RSS pubDate,
    unix epoch). Returns None when it cannot parse — callers must treat None as 'unknown'."""
    if not date_posted:
        return None
    s = str(date_posted).strip()
    if not s:
        return None
    if s.isdigit() and len(s) >= 10:                       # unix epoch (e.g. arbeitnow created_at)
        try:
            return _dt.datetime.utcfromtimestamp(int(s[:10])).date()
        except (ValueError, OverflowError):
            return None
    try:
        return _dt.date.fromisoformat(s[:10])              # ISO date / datetime prefix
    except ValueError:
        pass
    try:
        parsed = parsedate_to_datetime(s)                  # RFC-822 (RSS pubDate)
        return parsed.date() if parsed else None
    except (TypeError, ValueError, IndexError):
        return None


def date_ordinal(job: dict[str, Any]) -> int:
    """Sort key for 'freshest first' (BUG1, recall-first): higher = newer. Undated / unparseable
    postings sort to the BOTTOM (0) instead of being dropped — we never cull by date, because
    AA's home-office pool for niche terms is legitimately old and a hard cutoff would zero it.
    The date stays visible in the payload so the downstream AI can deprioritise stale ads itself."""
    d = _parse_date(job.get("date_posted"))
    return d.toordinal() if d else 0


def _salary(lo: Any, hi: Any, cur: Any = None, period: str | None = None) -> str | None:
    lo = lo or None
    hi = hi or None
    if not lo and not hi:
        return None
    rng = f"{int(lo)}-{int(hi)}" if lo and hi else str(int(lo or hi))
    out = rng
    if cur:
        out += f" {cur}"
    if period:
        out += f" / {period}"
    return out


# --------------------------------------------------------------------------- #
# Source clients — each returns list[normalised job dict]
# --------------------------------------------------------------------------- #

_AA_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs"
_AA_HEADERS = {**_HEADERS, "X-API-Key": "jobboerse-jobsuche"}


def _aa_records(data: dict[str, Any], remote_only: bool, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for j in (data.get("stellenangebote") or [])[:limit]:
        ort = j.get("arbeitsort") or {}
        loc = ", ".join(p for p in (ort.get("ort"), ort.get("region"), ort.get("land")) if p) or "Deutschland"
        title = j.get("titel") or j.get("beruf")
        out.append({
            "source": "arbeitsagentur",
            "title": title,
            "company": j.get("arbeitgeber"),
            "location": loc,
            "is_remote": bool(remote_only or looks_remote(title)),
            "date_posted": j.get("aktuelleVeroeffentlichungsdatum") or j.get("eintrittsdatum"),
            "salary": None,
            "job_url": f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{j.get('refnr')}" if j.get("refnr") else None,
            "description": None,
        })
    return out


async def fetch_arbeitsagentur(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    """Bundesagentur für Arbeit. BUG1: for remote we set arbeitszeit=ho and DO NOT apply
    veroeffentlichtseit (the date filter zeroes the small home-office pool). We also union
    a broad query filtered by home-office/remote keywords in the title for extra recall."""
    base: dict[str, Any] = {"size": min(limit, 100), "page": 1}
    if location and location.lower() not in ("germany", "deutschland", "remote", "dach", ""):
        base["wo"] = location

    async def _query(params: dict[str, Any]) -> dict[str, Any]:
        r = await client.get(_AA_BASE, params=params, headers=_AA_HEADERS)
        r.raise_for_status()
        return r.json()

    results: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(recs: list[dict[str, Any]]) -> None:
        for rec in recs:
            key = (rec.get("job_url") or rec.get("title") or "").lower()
            if key and key not in seen:
                seen.add(key)
                results.append(rec)

    if remote_only:
        # 1) Home-office-tagged jobs (NO date filter — that was BUG1).
        data = await _query({**base, "was": term, "arbeitszeit": "ho"})
        if not data.get("stellenangebote") and len(_tokens(term)) > 1:
            data = await _query({**base, "was": _tokens(term)[0], "arbeitszeit": "ho"})
        _add(_aa_records(data, remote_only=True, limit=limit))
        # 2) Broad query, keep titles that mention home office / remote AND at least loosely
        #    match the search term (BUG2: without the term gate this pulled in ANY remote job,
        #    e.g. Handelsvertreter for "IT-Support"). The term gate is the same generous
        #    ANY-token/synonym match used everywhere else — recall-first, just on-topic.
        if len(results) < limit:
            broad = await _query({**base, "was": term, "size": 100})
            kw = [
                r for r in _aa_records(broad, remote_only=False, limit=100)
                if looks_remote(r.get("title")) and _match_any(r, term)
            ]
            for r in kw:
                r["is_remote"] = True
            _add(kw)
    else:
        params = {**base, "was": term}
        if days and days > 0:
            params["veroeffentlichtseit"] = min(days, 100)
        data = await _query(params)
        if not data.get("stellenangebote") and len(_tokens(term)) > 1:
            params["was"] = _tokens(term)[0]
            params.pop("veroeffentlichtseit", None)
            data = await _query(params)
        _add(_aa_records(data, remote_only=False, limit=limit))

    # Return generously — Arbeitsagentur's server-side `was` search is the filter; the AI
    # downstream classifies. (No extra client-side relevance filter: recall over precision.)
    return results[:limit]


async def fetch_himalayas(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get("https://himalayas.app/jobs/api/search", params={"q": term, "limit": 20}, headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("jobs", []):
        locs = j.get("locationRestrictions") or []
        job = {
            "source": "himalayas",
            "title": j.get("title"),
            "company": j.get("companyName"),
            "location": ", ".join(locs) if locs else "Remote",
            "is_remote": True,
            "date_posted": j.get("pubDate"),
            "salary": _salary(j.get("minSalary"), j.get("maxSalary"), j.get("currency"), j.get("salaryPeriod")),
            "job_url": j.get("applicationLink"),
            "description": _strip_html(j.get("description")),
        }
        if _match_any(job, term):        # BUG2: himalayas search is fuzzy → filter client-side
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_remotive(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    # BUG2: remotive's ?search= is ignored (always returns the same latest jobs) → filter client-side.
    r = await client.get("https://remotive.com/api/remote-jobs", params={"search": term, "limit": 100}, headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("jobs", []):
        job = {
            "source": "remotive",
            "title": j.get("title"),
            "company": j.get("company_name"),
            "location": j.get("candidate_required_location") or "Remote",
            "is_remote": True,
            "date_posted": j.get("publication_date"),
            "salary": j.get("salary") or None,
            "job_url": j.get("url"),
            "description": _strip_html(j.get("description")),
        }
        if _match_any(job, term):
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_remoteok(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    toks = _tokens(term)
    params = {"tags": toks[0]} if toks else {}
    r = await client.get("https://remoteok.com/api", params=params, headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json():
        if not isinstance(j, dict) or not j.get("position"):
            continue
        job = {
            "source": "remoteok",
            "title": j.get("position"),
            "company": j.get("company"),
            "location": j.get("location") or "Remote",
            "is_remote": True,
            "date_posted": j.get("date"),
            "salary": _salary(j.get("salary_min"), j.get("salary_max"), "USD", "year"),
            "job_url": j.get("url") or (f"https://remoteok.com/l/{j.get('id')}" if j.get("id") else None),
            "description": _strip_html(j.get("description")),
        }
        if _match_any(job, term):        # BUG2: verify against title/desc, not just the server tag
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_arbeitnow(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get("https://www.arbeitnow.com/api/job-board-api", headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("data", []):
        is_remote = bool(j.get("remote"))
        if remote_only and not is_remote:
            continue
        job = {
            "source": "arbeitnow",
            "title": j.get("title"),
            "company": j.get("company_name"),
            "location": j.get("location"),
            "is_remote": is_remote,
            "date_posted": j.get("created_at"),
            "salary": None,
            "job_url": j.get("url"),
            "description": _strip_html(j.get("description")),
        }
        if _match_any(job, term):
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_jobicy(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int, geo: str | None = None,
) -> list[dict[str, Any]]:
    # BUG3: do NOT pass the free-text term as ?tag= (invalid tags → 404). Fetch by count
    # (+ geo for DACH) and filter client-side. Any HTTP error → empty list, never a string.
    params: dict[str, Any] = {"count": 50}
    if geo:
        params["geo"] = geo
    try:
        r = await client.get("https://jobicy.com/api/v2/remote-jobs", params=params, headers=_HEADERS)
        r.raise_for_status()
        payload = r.json()
    except (httpx.HTTPError, ValueError):
        return []
    out: list[dict[str, Any]] = []
    for j in payload.get("jobs", []):
        job = {
            "source": "jobicy",
            "title": j.get("jobTitle"),
            "company": j.get("companyName"),
            "location": j.get("jobGeo") or "Remote",
            "is_remote": True,
            "date_posted": j.get("pubDate"),
            "salary": _salary(j.get("annualSalaryMin"), j.get("annualSalaryMax"), j.get("salaryCurrency"), "year"),
            "job_url": j.get("url"),
            "description": _strip_html(j.get("jobExcerpt") or j.get("jobDescription")),
        }
        if _match_any(job, term):
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_hackernews(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get(
        "https://hn.algolia.com/api/v1/search_by_date",
        params={"tags": "story,author_whoishiring", "hitsPerPage": 10}, headers=_HEADERS,
    )
    r.raise_for_status()
    hits = r.json().get("hits", [])
    thread = next((h for h in hits if "who is hiring" in (h.get("title") or "").lower()), None)
    if not thread:
        return []
    r2 = await client.get(f"https://hn.algolia.com/api/v1/items/{thread['objectID']}", headers=_HEADERS)
    r2.raise_for_status()
    out: list[dict[str, Any]] = []
    for c in r2.json().get("children", []):
        txt = c.get("text")
        if not txt:
            continue
        headline = _strip_html(re.split(r"<p>", txt, maxsplit=1)[0], 300) or ""
        parts = [p.strip() for p in headline.split("|") if p.strip()]
        is_remote = bool(REMOTE_KW.search(txt))
        if remote_only and not is_remote:
            continue
        m = re.search(r'href="(https?://[^"]+)"', txt)
        job = {
            "source": "hackernews",
            "title": (" | ".join(parts[:3]) if parts else headline)[:160] or "HN job post",
            "company": parts[0] if parts else None,
            "location": None,
            "is_remote": is_remote,
            "date_posted": c.get("created_at"),
            "salary": None,
            "job_url": (m.group(1) if m else f"https://news.ycombinator.com/item?id={c.get('id')}"),
            "description": _strip_html(txt),
        }
        if _match_any(job, term):
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_weworkremotely(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get("https://weworkremotely.com/categories/remote-programming-jobs.rss", headers=_HEADERS)
    r.raise_for_status()

    def _tag(block: str, name: str) -> str | None:
        m = re.search(rf"<{name}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{name}>", block, re.S)
        return html.unescape(m.group(1).strip()) if m else None

    out: list[dict[str, Any]] = []
    for block in re.findall(r"<item>(.*?)</item>", r.text, re.S):
        title = _tag(block, "title") or ""
        company, role = (title.split(":", 1) + [""])[:2] if ":" in title else ("", title)
        job = {
            "source": "weworkremotely",
            "title": (role or title).strip() or None,
            "company": company.strip() or None,
            "location": _tag(block, "region") or "Remote",
            "is_remote": True,
            "date_posted": _tag(block, "pubDate"),
            "salary": None,
            "job_url": _tag(block, "link"),
            "description": _strip_html(_tag(block, "description")),
        }
        if _match_any(job, term):
            out.append(job)
        if len(out) >= limit:
            break
    return out


async def fetch_themuse(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(3):
        if len(out) >= limit:
            break
        r = await client.get("https://www.themuse.com/api/public/jobs", params={"page": page}, headers=_HEADERS)
        r.raise_for_status()
        results = r.json().get("results", [])
        if not results:
            break
        for j in results:
            locs = [l.get("name") for l in (j.get("locations") or []) if l.get("name")]
            is_remote = any("remote" in (l or "").lower() for l in locs)
            if remote_only and not is_remote:
                continue
            job = {
                "source": "themuse",
                "title": j.get("name"),
                "company": (j.get("company") or {}).get("name"),
                "location": ", ".join(locs) or None,
                "is_remote": is_remote,
                "date_posted": j.get("publication_date"),
                "salary": None,
                "job_url": (j.get("refs") or {}).get("landing_page"),
                "description": _strip_html(j.get("contents")),
            }
            if _match_any(job, term):
                out.append(job)
            if len(out) >= limit:
                break
    return out


_FETCHERS = {
    "arbeitsagentur": fetch_arbeitsagentur,
    "himalayas": fetch_himalayas,
    "remotive": fetch_remotive,
    "remoteok": fetch_remoteok,
    "arbeitnow": fetch_arbeitnow,
    "jobicy": fetch_jobicy,
    "hackernews": fetch_hackernews,
    "weworkremotely": fetch_weworkremotely,
    "themuse": fetch_themuse,
}


# --------------------------------------------------------------------------- #
# Aggregator
# --------------------------------------------------------------------------- #

def _dedup_key(job: dict[str, Any]) -> tuple[str, str]:
    title = re.sub(r"\W+", " ", (job.get("title") or "").lower()).strip()[:70]
    company = re.sub(r"\W+", " ", (job.get("company") or "").lower()).strip()[:40]
    return (title, company)


async def fetch_sources(
    sources: list[str], term: str, location: str | None = None,
    remote_only: bool = False, limit_per_source: int = 15, days: int = 30,
    dach_only: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch named sources concurrently, apply cross-cutting filters (non-job + DACH),
    dedup, and return (jobs, fetched_per_source_meta). Meta counts are POST-filter, PRE-dedup."""
    sources = [s for s in sources if s in _FETCHERS]
    meta: dict[str, Any] = {}

    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        async def run(name: str) -> list[dict[str, Any]]:
            try:
                kwargs: dict[str, Any] = {}
                if name == "jobicy" and dach_only:
                    kwargs["geo"] = "germany"
                jobs = await _FETCHERS[name](client, term, location, remote_only, limit_per_source, days, **kwargs)
                jobs = [j for j in jobs if looks_like_job(j)]                        # BUG6
                if dach_only and name not in ("arbeitsagentur", "arbeitnow"):        # BUG5 (DE-native kept)
                    jobs = [j for j in jobs if dach_ok(j.get("location"))]
                meta[name] = len(jobs)
                return jobs
            except Exception as exc:  # noqa: BLE001 — one bad source must not sink the rest
                log.warning("source %s failed: %s", name, exc)
                meta[name] = 0
                return []

        batches = await asyncio.gather(*(run(s) for s in sources))

    seen: set[tuple[str, str]] = set()
    merged: list[dict[str, Any]] = []
    for batch in batches:
        for job in batch:
            if not job.get("title"):
                continue
            key = _dedup_key(job)
            if key in seen:
                continue
            seen.add(key)
            merged.append(job)
    return merged, meta
