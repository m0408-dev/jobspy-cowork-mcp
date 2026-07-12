"""
Free job-data sources (no proxy, no API key beyond public client-ids).
=======================================================================

Complements the JobSpy scrapers (Indeed/LinkedIn) with official / public JSON
APIs that work fine from a datacenter IP — the ones JobSpy can't reach without
residential proxies. Everything here is:

  * free forever (no paid tier touched),
  * key-less or uses a publicly documented client-id,
  * fetched over plain httpx (async, concurrent), and
  * normalised into ONE common schema so results merge cleanly.

Common job schema returned by every ``fetch_*`` coroutine::

    {
      "source":      str,        # "arbeitsagentur" | "himalayas" | ...
      "title":       str,
      "company":     str | None,
      "location":    str | None,
      "is_remote":   bool,
      "date_posted": str | None, # ISO-ish string when known
      "salary":      str | None, # human-readable, e.g. "50000-70000 EUR / year"
      "job_url":     str | None,
      "description": str | None, # plain text, HTML stripped + truncated
    }

Sources (all verified working 2026-07):
  * arbeitsagentur — Bundesagentur für Arbeit, the largest German job database
  * himalayas      — remote jobs, worldwide
  * remotive       — remote jobs
  * remoteok       — remote tech jobs
  * arbeitnow      — German + remote jobs
  * jobicy         — remote jobs
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
from typing import Any

import httpx

log = logging.getLogger("jobspy-mcp.sources")

# Names usable in the aggregated tools. "indeed"/"linkedin" are handled by JobSpy.
API_SOURCES = [
    "arbeitsagentur", "himalayas", "remotive", "remoteok", "arbeitnow",
    "jobicy", "hackernews", "weworkremotely", "themuse",
]

# Human-readable catalog, surfaced by the list_job_sources MCP tool so an agent can
# pick the right `sources=[...]` for a query. (JobSpy's indeed/linkedin listed too.)
SOURCE_INFO: dict[str, dict[str, str]] = {
    "arbeitsagentur": {"coverage": "Germany (largest official DB)", "remote": "filterable", "auth": "free public client-id"},
    "himalayas":      {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none"},
    "remotive":       {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none"},
    "remoteok":       {"coverage": "remote tech", "remote": "remote-only", "auth": "none"},
    "arbeitnow":      {"coverage": "Germany + EU + remote", "remote": "mixed", "auth": "none"},
    "jobicy":         {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none"},
    "hackernews":     {"coverage": "startups/tech via monthly 'Who is hiring'", "remote": "mixed", "auth": "none"},
    "weworkremotely": {"coverage": "remote (programming)", "remote": "remote-only", "auth": "none"},
    "themuse":        {"coverage": "global (US-heavy)", "remote": "mixed", "auth": "none"},
    "indeed":         {"coverage": "global aggregator (via JobSpy)", "remote": "filterable", "auth": "none (may need proxy in cloud)"},
    "linkedin":       {"coverage": "global (via JobSpy)", "remote": "filterable", "auth": "none (rate-limited)"},
}

# Which sources are remote-first (used by search_remote_jobs).
REMOTE_SOURCES = ["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse"]

_UA = "Mozilla/5.0 (compatible; jobspy-mcp/1.0; +https://github.com/m0408-dev/jobspy-cowork-mcp)"
_HEADERS = {"User-Agent": _UA, "Accept": "application/json"}
_TIMEOUT = httpx.Timeout(25.0, connect=10.0)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]*\n[ \t]*")


def _strip_html(text: Any, limit: int = 1500) -> str | None:
    """Turn an HTML (or plain) description into trimmed plain text."""
    if not text:
        return None
    s = html.unescape(_TAG_RE.sub(" ", str(text)))
    s = _WS_RE.sub("\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    if len(s) > limit:
        s = s[:limit].rstrip() + " …[truncated — open job_url for full text]"
    return s or None


def _tokens(term: str) -> list[str]:
    """Significant (len>2) lower-case tokens of a query."""
    return [t for t in term.lower().split() if len(t) > 2]


def _match_any(job: dict[str, Any], term: str) -> bool:
    """Keep a job if ANY significant query token appears in title+company+description.

    Feeds like RemoteOK / Arbeitnow have no (or coarse) server-side search, so we filter
    locally. ANY-token (OR) semantics are deliberately forgiving: 'python developer' should
    still surface a 'Senior Python Engineer', not just exact 'Python Developer' titles.
    """
    toks = _tokens(term)
    if not toks:
        return True
    hay = " ".join(str(job.get(k) or "") for k in ("title", "company", "description")).lower()
    return any(tok in hay for tok in toks)


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
# Individual source clients — each returns list[normalised job dict]
# --------------------------------------------------------------------------- #

async def fetch_arbeitsagentur(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    """Bundesagentur für Arbeit — official German federal job database (free, public client-id)."""
    params: dict[str, Any] = {"was": term, "size": min(limit, 100), "page": 1}
    if location and location.lower() not in ("germany", "deutschland", "remote", ""):
        params["wo"] = location
    if remote_only:
        params["arbeitszeit"] = "ho"          # ho = Homeoffice/telearbeit
    if days and days > 0:
        params["veroeffentlichtseit"] = min(days, 100)
    r = await client.get(
        "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs",
        params=params, headers={**_HEADERS, "X-API-Key": "jobboerse-jobsuche"},
    )
    r.raise_for_status()
    data = r.json()
    # English/phrase queries often miss the German DB (listings say "Python-Entwickler").
    # If a multi-word term yields nothing, retry with the primary skill token and widen the
    # window (the remote-only German pool is small, so a tight date filter zeroes it out).
    if not data.get("stellenangebote") and len(_tokens(term)) > 1:
        params["was"] = _tokens(term)[0]
        params.pop("veroeffentlichtseit", None)
        r = await client.get(
            "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs",
            params=params, headers={**_HEADERS, "X-API-Key": "jobboerse-jobsuche"},
        )
        r.raise_for_status()
        data = r.json()
    out: list[dict[str, Any]] = []
    for j in data.get("stellenangebote", [])[:limit]:
        ort = j.get("arbeitsort") or {}
        loc = ", ".join(p for p in (ort.get("ort"), ort.get("region"), ort.get("land")) if p) or None
        out.append({
            "source": "arbeitsagentur",
            "title": j.get("titel") or j.get("beruf"),
            "company": j.get("arbeitgeber"),
            "location": loc,
            "is_remote": remote_only,
            "date_posted": j.get("aktuelleVeroeffentlichungsdatum") or j.get("eintrittsdatum"),
            "salary": None,
            "job_url": f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{j.get('refnr')}"
                       if j.get("refnr") else None,
            "description": None,   # list endpoint has no description; job_url has the full posting
        })
    return out


async def fetch_himalayas(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get(
        "https://himalayas.app/jobs/api/search",
        params={"q": term, "limit": min(limit, 20)}, headers=_HEADERS,
    )
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("jobs", [])[:limit]:
        locs = j.get("locationRestrictions") or []
        out.append({
            "source": "himalayas",
            "title": j.get("title"),
            "company": j.get("companyName"),
            "location": ", ".join(locs) if locs else "Remote",
            "is_remote": True,
            "date_posted": j.get("pubDate"),
            "salary": _salary(j.get("minSalary"), j.get("maxSalary"),
                              j.get("currency"), j.get("salaryPeriod")),
            "job_url": j.get("applicationLink"),
            "description": _strip_html(j.get("description")),
        })
    return out


async def fetch_remotive(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    r = await client.get(
        "https://remotive.com/api/remote-jobs",
        params={"search": term, "limit": limit}, headers=_HEADERS,
    )
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("jobs", [])[:limit]:
        out.append({
            "source": "remotive",
            "title": j.get("title"),
            "company": j.get("company_name"),
            "location": j.get("candidate_required_location") or "Remote",
            "is_remote": True,
            "date_posted": j.get("publication_date"),
            "salary": j.get("salary") or None,
            "job_url": j.get("url"),
            "description": _strip_html(j.get("description")),
        })
    return out


async def fetch_remoteok(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    # RemoteOK supports a server-side ?tags= filter; use the primary skill token.
    toks = _tokens(term)
    params = {"tags": toks[0]} if toks else {}
    # Flat array; element 0 is a legal-notice object, not a job.
    r = await client.get("https://remoteok.com/api", params=params, headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json():
        if not isinstance(j, dict) or not j.get("position"):
            continue
        out.append({
            "source": "remoteok",
            "title": j.get("position"),
            "company": j.get("company"),
            "location": j.get("location") or "Remote",
            "is_remote": True,
            "date_posted": j.get("date"),
            "salary": _salary(j.get("salary_min"), j.get("salary_max"), "USD", "year"),
            "job_url": j.get("url") or (f"https://remoteok.com/l/{j.get('id')}" if j.get("id") else None),
            "description": _strip_html(j.get("description")),
        })
        if len(out) >= limit:
            break
    return out


async def fetch_arbeitnow(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    # Free feed has no server-side search — pull the latest batch and filter locally.
    r = await client.get("https://www.arbeitnow.com/api/job-board-api", headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("data", []):
        if remote_only and not j.get("remote"):
            continue
        job = {
            "source": "arbeitnow",
            "title": j.get("title"),
            "company": j.get("company_name"),
            "location": j.get("location"),
            "is_remote": bool(j.get("remote")),
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
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"count": min(limit, 50)}
    if term:
        params["tag"] = term
    r = await client.get("https://jobicy.com/api/v2/remote-jobs", params=params, headers=_HEADERS)
    r.raise_for_status()
    out: list[dict[str, Any]] = []
    for j in r.json().get("jobs", [])[:limit]:
        out.append({
            "source": "jobicy",
            "title": j.get("jobTitle"),
            "company": j.get("companyName"),
            "location": j.get("jobGeo") or "Remote",
            "is_remote": True,
            "date_posted": j.get("pubDate"),
            "salary": _salary(j.get("annualSalaryMin"), j.get("annualSalaryMax"),
                              j.get("salaryCurrency"), "year"),
            "job_url": j.get("url"),
            "description": _strip_html(j.get("jobExcerpt") or j.get("jobDescription")),
        })
    return out


async def fetch_hackernews(
    client: httpx.AsyncClient, term: str, location: str | None,
    remote_only: bool, limit: int, days: int,
) -> list[dict[str, Any]]:
    """HackerNews monthly 'Ask HN: Who is hiring?' thread — each top-level comment is a job.

    The thread is posted monthly by the 'whoishiring' bot; we find the newest one via the
    free Algolia API, then read its comments. Great for startup / remote tech roles.
    """
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
        # First paragraph is the "Company | Role | Location | REMOTE | $" headline line.
        headline = _strip_html(re.split(r"<p>", txt, maxsplit=1)[0], 300) or ""
        parts = [p.strip() for p in headline.split("|") if p.strip()]
        low = txt.lower()
        is_remote = "remote" in low
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
    """WeWorkRemotely programming RSS feed — all remote, parsed without an XML dep."""
    r = await client.get(
        "https://weworkremotely.com/categories/remote-programming-jobs.rss", headers=_HEADERS,
    )
    r.raise_for_status()

    def _tag(block: str, name: str) -> str | None:
        m = re.search(rf"<{name}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{name}>", block, re.S)
        return html.unescape(m.group(1).strip()) if m else None

    out: list[dict[str, Any]] = []
    for block in re.findall(r"<item>(.*?)</item>", r.text, re.S):
        title = _tag(block, "title") or ""          # "Company: Role"
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
    """The Muse public API (free, no key). No free-text search, so pull pages and filter locally."""
    out: list[dict[str, Any]] = []
    for page in range(3):
        if len(out) >= limit:
            break
        r = await client.get(
            "https://www.themuse.com/api/public/jobs",
            params={"page": page}, headers=_HEADERS,
        )
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
# Aggregator — run selected sources concurrently, dedup, return merged list
# --------------------------------------------------------------------------- #

def _dedup_key(job: dict[str, Any]) -> tuple[str, str]:
    title = re.sub(r"\W+", " ", (job.get("title") or "").lower()).strip()[:70]
    company = re.sub(r"\W+", " ", (job.get("company") or "").lower()).strip()[:40]
    return (title, company)


async def fetch_sources(
    sources: list[str], term: str, location: str | None = None,
    remote_only: bool = False, limit_per_source: int = 15, days: int = 30,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch every named source concurrently, dedup across them, return (jobs, per_source_meta)."""
    sources = [s for s in sources if s in _FETCHERS]
    meta: dict[str, Any] = {}

    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        async def run(name: str) -> list[dict[str, Any]]:
            try:
                jobs = await _FETCHERS[name](client, term, location, remote_only, limit_per_source, days)
                meta[name] = len(jobs)
                return jobs
            except Exception as exc:  # noqa: BLE001 — one bad source must not sink the rest
                log.warning("source %s failed: %s", name, exc)
                meta[name] = f"error: {exc}"
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
