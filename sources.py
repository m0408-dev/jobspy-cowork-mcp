"""
Free job-data sources (no proxy, no API key beyond public client-ids).
=======================================================================

V3: caller-selected sources, bounded pages, full stored texts and explicit errors.
Relevance scores only rank; market/language decisions belong to the caller.
All fetched records survive output paging. Upstream retrieval is finite and reported.

Common job schema returned by every ``fetch_*`` coroutine::

    {source, title, company, location, is_remote, date_posted, salary, job_url, description}

The aggregator then adds: relevance, remote_confidence, remote_signals.

Sources: arbeitsagentur, himalayas, remotive, remoteok, arbeitnow, jobicy,
hackernews, weworkremotely, themuse.
"""

from __future__ import annotations

import asyncio
import base64
import datetime as _dt
import html
import logging
import re
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from http_cache import CachedClient

log = logging.getLogger("jobspy-mcp.sources")

# Supported APIs, not the default selection (server.py selects a market explicitly).
API_SOURCES = [
    "arbeitsagentur", "himalayas", "remotive", "remoteok",
    "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse",
]
# Remote-first sources used by search_remote_jobs.
REMOTE_SOURCES = ["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse"]

SOURCE_INFO: dict[str, dict[str, str]] = {
    "arbeitsagentur": {"coverage": "Germany; endpoint may reject public-client access", "remote": "text hints only", "auth": "public client-id, may be blocked"},
    "himalayas":      {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none"},
    "remotive":       {"coverage": "remote worldwide", "remote": "remote-only", "auth": "none (server ignores ?search → we rank client-side)"},
    "remoteok":       {"coverage": "remote tech", "remote": "remote-only", "auth": "none"},
    "arbeitnow":      {"coverage": "Germany + EU + remote (best DE-remote API)", "remote": "mixed", "auth": "none"},
    "jobicy":         {"coverage": "remote worldwide (geo=germany for DACH)", "remote": "remote-only", "auth": "none"},
    "hackernews":     {"coverage": "startups/tech 'Who is hiring'", "remote": "mixed", "auth": "none"},
    "weworkremotely": {"coverage": "remote (programming)", "remote": "remote-only", "auth": "none"},
    "themuse":        {"coverage": "global, US-heavy", "remote": "mixed", "auth": "none"},
    "indeed":         {"coverage": "global via JobSpy", "remote": "filterable", "auth": "none (proxy in cloud)"},
    "linkedin":       {"coverage": "global via JobSpy", "remote": "filterable", "auth": "none (rate-limited)"},
}

_UA = "jobspy-mcp/3.0 (+https://github.com/m0408-dev/jobspy-cowork-mcp)"
_HEADERS = {"User-Agent": _UA, "Accept": "application/json"}
_TIMEOUT = httpx.Timeout(30.0, connect=10.0)

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t]*\n[ \t]*")

# Words that mark a remote / home-office role (DE + EN).
REMOTE_KW = re.compile(
    r"home\s?office|homeoffice|remote|mobiles?\s+arbeiten|telearbeit|fully\s+remote|100\s*%\s*remote|remote[-\s]?first",
    re.IGNORECASE,
)
# Titles that are clearly content, not job postings. Kept minimal on purpose so we never
# drop a real job (e.g. 'Tour Guide', 'Studienberater') — only unambiguous non-jobs.
_NON_JOB = re.compile(
    r"\b(webinar|whitepaper|white\s?paper|e-?book|newsletter|blog(?:post|beitrag)?|podcast)\b",
    re.IGNORECASE,
)
# Location strings that clearly restrict to a NON-DACH / non-European region.
# ONLY consulted when the caller explicitly opts in via dach_only=True.
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


# --------------------------------------------------------------------------- #
# Query expansion  —  the fix for German compound nouns
# --------------------------------------------------------------------------- #
#
# Measured against the live Arbeitsagentur API (München, 2026-07-30):
#     was=Nachtschicht ->  97 hits
#     was=Nachtdienst  ->  77 hits
#     was=Nachtwache   ->   6 hits
#     was=Nacht        -> 300 hits
#     overlap Nachtschicht/Nachtdienst = 3   (i.e. they are almost disjoint sets)
#     union of all three               = 352
# A single query therefore sees ~28 % of what exists. There is no server-side stemming
# for German compounds, so the only way to get them all is to FIRE SEVERAL QUERIES and
# union the results. That is what expand_terms() builds.
#
# The rule is generative, not a per-industry dictionary: a compound job word is
# <modifier><tail>, and tails come in families whose members are interchangeable
# (Nacht|schicht ~ Nacht|dienst ~ Nacht|wache). Split off a known tail, then re-attach
# every sibling tail from the same family.

_TAIL_FAMILIES: list[list[str]] = [
    ["schicht", "dienst", "wache", "arbeit", "einsatz", "betrieb"],
    ["kraft", "helfer", "helferin", "assistent", "assistenz", "mitarbeiter", "personal", "hilfe"],
    ["techniker", "monteur", "mechaniker", "elektroniker", "installateur", "handwerker", "meister"],
    ["berater", "betreuer", "begleiter", "coach"],
    ["leiter", "leitung", "manager"],
    ["fahrer", "kurier", "bote", "lieferant"],
    ["verkäufer", "verkaeufer", "berater", "kassierer"],
    ["entwickler", "programmierer", "architekt", "ingenieur"],
    ["administrator", "admin", "betreuer", "operator"],
    ["pfleger", "pflegerin", "pflege", "helfer"],
    ["reiniger", "reinigung", "reinigungskraft"],
    ["support", "service", "hilfe", "betreuung"],
]
# tail -> its family (first match wins)
_TAIL_INDEX: dict[str, list[str]] = {}
for _fam in _TAIL_FAMILIES:
    for _t in _fam:
        _TAIL_INDEX.setdefault(_t, _fam)
# longest tails first so 'reinigungskraft' beats 'kraft'
_TAILS_SORTED = sorted(_TAIL_INDEX, key=len, reverse=True)

# Flat synonym groups for terms that are NOT compounds (abbreviations, loan words).
_SYNONYMS: dict[str, list[str]] = {
    "support": ["helpdesk", "service desk", "anwenderbetreuung", "kundenbetreuung"],
    "helpdesk": ["support", "service desk", "servicedesk"],
    "servicedesk": ["service desk", "helpdesk", "support"],
    "systemadministrator": ["sysadmin", "systemadministration", "administrator"],
    "sysadmin": ["systemadministrator", "administrator"],
    "administrator": ["systemadministrator", "sysadmin"],
    "fachinformatiker": ["systemintegration", "anwendungsentwicklung"],
    "it": ["edv", "ict"],
    "edv": ["it", "ict"],
    "nachts": ["nacht", "nachtschicht", "nachtdienst"],
    "putzen": ["reinigung", "reinigungskraft"],
    "lager": ["lagerist", "lagerhelfer", "kommissionierer"],
    "wachmann": ["sicherheitsmitarbeiter", "objektschutz", "werkschutz"],
    "security": ["sicherheitsmitarbeiter", "objektschutz", "werkschutz"],
    "koch": ["küchenhilfe", "beikoch"],
    "pflege": ["pflegekraft", "pflegehelfer", "altenpflege"],
}

MAX_QUERY_VARIANTS = 12


def _tokens(term: str) -> list[str]:
    """Significant query tokens: split on non-word chars, keep len>=2, lower-cased (DE chars kept)."""
    return [t for t in re.split(r"[^0-9a-zA-Zäöüß]+", (term or "").lower()) if len(t) >= 2]


def expand_terms(
    term: str, extra_terms: list[str] | None = None, expand: bool = True,
    max_variants: int = MAX_QUERY_VARIANTS,
) -> list[str]:
    """Build the list of queries to actually fire, most specific first.

    Always includes the caller's own term(s) verbatim and never reorders them away from
    the front, so an explicit ``search_terms=[...]`` list is honoured exactly. With
    ``expand=True`` it additionally derives:

      * each individual token of a multi-word query  ("IT-Support" -> "it", "support")
      * compound splits + sibling tails             ("Nachtschicht" -> "nacht",
        "nachtdienst", "nachtwache", "nachtarbeit", ...)
      * flat synonyms for non-compound terms         ("helpdesk" -> "support", ...)

    Deduplicated, capped at ``max_variants`` so one call stays a handful of HTTP requests.
    """
    out: list[str] = []
    seen: set[str] = set()

    def add(t: str | None) -> None:
        t = (t or "").strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            out.append(t)

    add(term)
    for t in (extra_terms or []):
        add(t)
    if not expand:
        return out[:max_variants]

    base_tokens: list[str] = []
    for t in [term, *(extra_terms or [])]:
        base_tokens.extend(_tokens(t))

    # multi-token queries: also try each token alone (AA ANDs its terms, so a 2-word
    # query is strictly narrower than either word by itself).
    if len(base_tokens) > 1:
        for tok in base_tokens:
            if len(tok) >= 3:
                add(tok)

    for tok in base_tokens:
        if len(tok) < 6:
            continue
        for tail in _TAILS_SORTED:
            if tok.endswith(tail) and len(tok) - len(tail) >= 3:
                stem = tok[: -len(tail)]
                add(stem)                                   # Nachtschicht -> Nacht
                for sibling in _TAIL_INDEX[tail]:           # -> Nachtdienst, Nachtwache, ...
                    if sibling != tail:
                        add(stem + sibling)
                break                                       # one split per token is enough

    for tok in base_tokens:
        for syn in _SYNONYMS.get(tok, []):
            add(syn)

    return out[:max_variants]


# --------------------------------------------------------------------------- #
# Relevance — a SORT signal, never a filter
# --------------------------------------------------------------------------- #

def relevance(job: dict[str, Any], terms: list[str]) -> float:
    """0.0–1.0 hint at how well a posting matches the query. Used ONLY for ordering, so
    that when the size cap truncates, the weakest matches are what falls off the end.

    Field weights are what stops the 'It-excelsus GmbH matched IT-Support' class of
    false positive: a hit in the company name is worth 0.15, a hit in the title 1.0.
    Nothing is ever dropped for scoring low — a 0.0 job is still returned.
    """
    toks: list[str] = []
    for t in terms:
        toks.extend(tok for tok in _tokens(t) if len(tok) >= 3 and not tok.isdigit())
    toks = list(dict.fromkeys(toks))
    if not toks:
        return 1.0

    title = (job.get("title") or "").lower()
    desc = (job.get("description") or "").lower()
    company = (job.get("company") or "").lower()
    loc = (job.get("location") or "").lower()

    best = 0.0
    total = 0.0
    for tok in toks:
        if tok in title:
            score = 1.0
        elif tok in desc:
            score = 0.5
        elif tok in company or tok in loc:
            score = 0.15
        else:
            score = 0.0
        total += score
        best = max(best, score)
    # average across tokens, nudged by the single best hit so a strong title match on one
    # word of a long query still ranks above a weak scatter of company-name hits.
    return round(min(1.0, 0.7 * (total / len(toks)) + 0.3 * best), 3)


def _strip_html(text: Any, limit: int | None = None) -> str | None:
    if not text:
        return None
    s = _TAG_RE.sub(" ", html.unescape(html.unescape(str(text))))
    s = _WS_RE.sub("\n", s)
    s = re.sub(r"[ \t]{2,}", " ", s).strip()
    if limit is not None and len(s) > limit:
        s = s[:limit].rstrip() + " …[truncated — open job_url for full text]"
    return s or None


def looks_like_job(job: dict[str, Any]) -> bool:
    """False only for obvious articles/webinars/blog posts — not a relevance filter."""
    title = job.get("title") or ""
    if not title.strip():
        return False
    return not _NON_JOB.search(title)


def looks_remote(text: str | None) -> bool:
    return bool(text and REMOTE_KW.search(text))


def dach_ok(location: str | None) -> bool:
    """True unless the location clearly restricts to a non-DACH/non-European region.
    Only consulted when the caller sets dach_only=True. Unknown/worldwide/europe are kept."""
    loc = (location or "").strip().lower()
    if not loc:
        return True
    if _NON_DACH.search(loc):
        return False
    return True


def german_evidence(job: dict[str, Any]) -> str:
    """Language evidence only; a German location alone proves nothing about language."""
    text = f"{job.get('title') or ''} {job.get('description') or ''}"
    if re.search(r"german[- ]speaking|deutschsprach|deutschkenntnisse|fluent.{0,20}german|german.{0,20}(?:fluent|native|c1|c2)", text, re.I):
        return "explicit_signal"
    if len(re.findall(r"\b(?:und|wir|deine|dein|ihre|aufgaben|kenntnisse|bewerbung|erfahrung)\b", text, re.I)) >= 4:
        return "german_ad_not_requirement"
    return "unknown"


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
    """Sort key for 'freshest first': higher = newer. Undated / unparseable postings sort to
    the BOTTOM (0) rather than being dropped. We never cull by date: German ads stay open for
    months, and a 3-day cutoff was measurably throwing away live postings. The date stays in
    the payload so the calling AI can deprioritise stale ads itself."""
    d = _parse_date(job.get("date_posted"))
    return d.toordinal() if d else 0


# --------------------------------------------------------------------------- #
# Remote detection
# --------------------------------------------------------------------------- #

# Onsite / hybrid obligations that disqualify a TRUE 100%-remote role even when the post also
# says "remote" (remote-first with mandatory office days, relocation, occasional on-site, …).
_BUERO = r"b(?:ü|ue|u)ro"          # büro / buero / buro — ads are written with all three
_HYBRID_KW = re.compile(
    r"\b(hybrid|hybride[rsn]?|teil(?:weise|s)?\s*remote|teilremote|vor[-\s]?ort|on[-\s]?site|"
    rf"präsenz|praesenz|anwesenheit(?:spflicht)?|im\s+{_BUERO}|ins\s+{_BUERO}|"
    rf"\d\s*tage?\s*(?:pro\s*woche\s*)?(?:im\s*)?(?:{_BUERO}|office|vor\s*ort)|"
    r"relocation|umzug|gelegentlich\s+vor\s+ort|occasional(?:ly)?\s+on[-\s]?site)\b",
    re.IGNORECASE,
)
# Strong signals of a genuinely location-independent role.
_STRICT_KW = re.compile(
    r"(100\s*%?\s*(?:remote|home\s?office|homeoffice)|fully\s+remote|full[-\s]?remote|remote[-\s]?only|"
    r"vollst[äa]ndig\s+remote|komplett\s+remote|voll\s*remote|work\s+from\s+anywhere|ortsunabh[äa]ngig|"
    r"dauerhaft\s+(?:remote|home\s?office))",
    re.IGNORECASE,
)


def remote_confidence(job: dict[str, Any]) -> str:
    """Label the remote-ness of a posting **from its actual text**:

      'strict'  – explicitly 100% / fully remote / work-from-anywhere, no onsite signal
      'hybrid'  – states an onsite / Präsenz / relocation obligation -> not 100% remote
      'mixed'   – says both -> read it
      'likely'  – the text mentions home office / remote but not how much
      'no_signal' – there IS a description and it says nothing about remote work
      'unknown' – we have NO description to judge, so we make no claim

    The 'unknown' case is the important one: judging remoteness from a title alone
    produced nonsense like a 'Sandwichartist' rated remote-likely. If there is no text,
    this now says so instead of guessing. Fetch descriptions (fetch_details=True for
    Arbeitsagentur) to turn 'unknown' into a real verdict.
    """
    desc = (job.get("description") or "").strip()
    title = (job.get("title") or "")
    if not desc:
        # No body text. A title/location can still be explicit ("... 100% Remote").
        hay_t = f"{title} {job.get('location') or ''}"
        if _STRICT_KW.search(hay_t):
            return "unverified_title"
        if REMOTE_KW.search(hay_t):
            return "likely"
        return "unknown"

    hay = f"{title} {desc} {job.get('location') or ''}".lower()
    if re.search(r"(?:no|not|kein(?:e|en)?|nicht)\s+(?:fully\s+|100\s*%\s*)?(?:remote|home\s?office)", hay):
        return "negative_or_mixed"
    hybrid = bool(_HYBRID_KW.search(hay))
    strict = bool(_STRICT_KW.search(hay))
    if strict and hybrid:
        return "mixed"
    if hybrid:
        return "hybrid"
    if strict:
        return "strict"
    if REMOTE_KW.search(hay):
        return "likely"
    return "no_signal"


def remote_signals(job: dict[str, Any]) -> list[str]:
    """The actual phrases that drove remote_confidence, so the AI can audit the call
    instead of trusting a label. Empty list = nothing found in the text we have."""
    hay = " ".join(str(job.get(k) or "") for k in ("title", "description", "location"))
    found = []
    for rx in (_STRICT_KW, _HYBRID_KW, REMOTE_KW):
        for m in rx.finditer(hay):
            s = m.group(0).strip().lower()
            if s and s not in found:
                found.append(s)
            if len(found) >= 6:
                return found
    return found


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
# Source clients — each returns list[normalised job dict], UNFILTERED
# --------------------------------------------------------------------------- #

_AA_BASE = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs"
_AA_DETAIL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobdetails/"
_AA_HEADERS = {**_HEADERS, "X-API-Key": "jobboerse-jobsuche"}
_AA_PAGE_SIZE = 100
# Measured: page=75 (7 500 results deep) still returns data, page=90 returns 0, page=101 is a
# 400. Keep a safe ceiling on how deep one query walks.
_AA_MAX_PAGES = 75
# Detail fetches are cheap (measured ~107 req/s at concurrency 8) but we still bound them.
_AA_DETAIL_CONCURRENCY = 8


def _aa_records(data: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for j in (data.get("stellenangebote") or []):
        ort = j.get("arbeitsort") or {}
        loc = ", ".join(p for p in (ort.get("ort"), ort.get("region"), ort.get("land")) if p) or "Deutschland"
        title = j.get("titel") or j.get("beruf")
        out.append({
            "source": "arbeitsagentur",
            "title": title,
            "company": j.get("arbeitgeber"),
            "location": loc,
            "is_remote": looks_remote(title),
            "date_posted": j.get("aktuelleVeroeffentlichungsdatum"),
            "start_date": j.get("eintrittsdatum"),
            "salary": None,
            "job_url": f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{j.get('refnr')}" if j.get("refnr") else None,
            "description": None,
            "beruf": j.get("beruf"),
            "_refnr": j.get("refnr"),
        })
    return out


async def _aa_fetch_details(
    client: httpx.AsyncClient, jobs: list[dict[str, Any]], limit: int,
) -> int:
    """Fill in ``description`` from the per-posting detail endpoint.

    The list endpoint returns ``description: null`` for every hit, which is why remote
    detection used to be guesswork. The detail endpoint (refnr base64-encoded in the path)
    returns the full ad — measured ~3 200 chars average. Returns how many were enriched.
    """
    targets = [j for j in jobs if j.get("_refnr") and not j.get("description")][:limit]
    if not targets:
        return 0
    sem = asyncio.Semaphore(_AA_DETAIL_CONCURRENCY)
    filled = 0

    async def one(job: dict[str, Any]) -> None:
        nonlocal filled
        ref = job["_refnr"]
        async with sem:
            try:
                url = _AA_DETAIL + base64.b64encode(str(ref).encode()).decode()
                r = await client.get(url, headers=_AA_HEADERS)
                if r.status_code != 200:
                    return
                d = r.json()
            except (httpx.HTTPError, ValueError):
                return
        desc = d.get("stellenangebotsBeschreibung")
        if desc:
            job["description"] = _strip_html(desc)
            filled += 1
        if d.get("verguetungsangabe") and d["verguetungsangabe"] != "KEINE_ANGABEN":
            job["salary"] = str(d["verguetungsangabe"])
        if d.get("istArbeitnehmerUeberlassung"):
            job["zeitarbeit"] = True
        if d.get("arbeitszeitVollzeit") is not None:
            job["job_type"] = "Vollzeit" if d["arbeitszeitVollzeit"] else "Teilzeit"

    await asyncio.gather(*(one(j) for j in targets))
    return filled


async def fetch_arbeitsagentur(
    client: httpx.AsyncClient, terms: list[str], location: str | None,
    limit: int, days: int, remote_boost: bool = False, fetch_details: bool = True,
    max_pages: int = 5, source_offset: int = 0, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Bundesagentur für Arbeit — the only source that really covers the German market.

    RAW PULL, three deliberate changes over v1:

    1. **Deep pagination.** v1 fetched exactly one page of <=100. A term like 'IT-Support'
       has ~8 000 live ads nationwide, so one page saw 0.3 % of the market. We now walk
       pages until ``limit`` is satisfied or the result set ends, and report the true
       ``total_available`` so the caller knows what is behind the number it got.
    2. **Multiple queries, unioned.** German compounds are near-disjoint on this API
       (Nachtschicht 97 hits, Nachtdienst 77, overlap 3). Every expanded term is fired and
       the results merged, which is the only way to see all of them.
    3. **No date filter unless asked.** ``veroeffentlichtseit`` is sent only when the caller
       passes days > 0, because German ads stay open for months.

    ``remote_boost`` ADDS an ``arbeitszeit=ho`` leg — it never restricts the result set.
    Measured 2026-07-30: 'IT-Support' nationwide = 8 043 hits, the same search with
    arbeitszeit=ho = 0. Employers essentially never tick that box, so using it as the
    primary filter returns nothing (or, worse, a fuzzy fallback of unrelated ads).
    """
    base: dict[str, Any] = {"size": _AA_PAGE_SIZE}
    if location and location.lower() not in ("germany", "deutschland", "remote", "dach", ""):
        base["wo"] = location
    if days and days > 0:
        base["veroeffentlichtseit"] = min(days, 100)

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    totals: dict[str, int] = {}
    errors: list[str] = []
    next_pages: dict[str, int] = {}
    exhausted: set[str] = set()
    calls: dict[str, int] = {}

    def _add(recs: list[dict[str, Any]]) -> int:
        added = 0
        for rec in recs:
            key = str(rec.get("_refnr") or rec.get("job_url") or rec.get("title") or "").lower()
            if key and key not in seen:
                seen.add(key)
                results.append(rec)
                added += 1
        return added

    async def _page(params: dict[str, Any]) -> dict[str, Any]:
        r = await client.get(_AA_BASE, params=params, headers=_AA_HEADERS)
        r.raise_for_status()
        return r.json()

    async def _walk(query: dict[str, Any], want: int, label: str) -> None:
        """Page through one query until we have `want` records from it or it runs out."""
        page = next_pages.get(label, source_offset + 1)
        got_for_query = 0
        while got_for_query < want and calls.get(label, 0) < max_pages and label not in exhausted:
            calls[label] = calls.get(label, 0) + 1
            try:
                data = await _page({**query, "page": page})
            except (httpx.HTTPError, ValueError) as exc:
                errors.append(f"{label}: page {page}: {safe_error(exc)}")
                exhausted.add(label)
                return
            recs = _aa_records(data)
            if label not in totals:
                # keyed by label, not by `was`: the arbeitszeit=ho leg reuses the same term
                # and would otherwise overwrite the real total with its near-zero count.
                totals[label] = int(data.get("maxErgebnisse") or 0)
            if not recs:
                exhausted.add(label)
                return
            _add(recs)
            next_pages[label] = page + 1
            got_for_query += len(recs)
            if page * _AA_PAGE_SIZE >= (data.get("maxErgebnisse") or float("inf")):
                exhausted.add(label)
                return
            page += 1

    # Budget the pull ACROSS the variants instead of letting the first broad one eat it all.
    # 'Nachtschicht' expands to 7 queries; if 'nacht' (300 hits) consumed the whole limit,
    # 'nachtdienst' and 'nachtwache' would never run — which is the exact blind spot the
    # expansion exists to close. So every variant gets a guaranteed share first, then the
    # caller's own term walks deeper with whatever budget is left.
    share = max(_AA_PAGE_SIZE, -(-limit // max(1, len(terms))))
    await asyncio.gather(*(
        _walk({**base, "was": t}, share, str(t)) for t in terms
    ))
    if len(results) < limit and terms:
        await _walk({**base, "was": terms[0]}, limit - len(results), str(terms[0]))

    # Rank before capping: the variants are fetched in parallel, so insertion order is
    # arbitrary interleaving — cutting by it would drop good matches at random. Ranking here
    # also means we only pay for detail fetches on postings that survive the cap.
    for r in results:
        r["relevance"] = relevance(r, terms)
    results.sort(key=lambda j: (-j["relevance"], -date_ordinal(j)))
    scanned = len(results)
    # Retain all fetched records; the result store pages them without tail loss.

    enriched = 0
    if fetch_details and results:
        enriched = await _aa_fetch_details(client, results, limit=min(limit, len(results)))

    for r in results:
        r.pop("_refnr", None)

    meta = {
        "queries": [str(t) for t in terms],
        # Largest single-query pool among the queries fired. Reporting only the caller's own
        # term would understate it (the union of variants can exceed it), and summing would
        # double-count the overlap — so this is the honest "there is at least this much".
        "total_available": max(totals.values()) if totals else None,
        "total_for_your_term": totals.get(str(terms[0])) if terms else 0,
        "total_per_query": totals,
        "descriptions_fetched": enriched,
        "scanned_before_cap": scanned,
        "errors": errors,
        "pages_requested": calls,
        "next_source_offsets": {k: v-1 for k, v in next_pages.items() if k not in exhausted},
        "coverage": "partial_error" if errors else ("query_exhausted" if len(exhausted) == len(terms) else "budget_limited"),
        "description_requests": min(limit, len(results)) if fetch_details else 0,
        "description_missing_or_failed": max(0, min(limit, len(results))-enriched) if fetch_details else 0,
    }
    return results, meta


async def fetch_himalayas(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int,
    max_pages: int = 5, source_offset: int = 0, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors, next_offsets = [], {}
    for term in terms:
        for page in range(source_offset+1, source_offset+max_pages+1):
            try:
                r = await client.get("https://himalayas.app/jobs/api/search", params={"q": term, "page": page}, headers=_HEADERS)
                r.raise_for_status()
                data = r.json()
                batch = data.get("jobs", [])
            except (httpx.HTTPError, ValueError) as exc:
                errors.append(f"{term}: {safe_error(exc)}")
                break
            if not batch:
                next_offsets.pop(term, None)
                break
            added = 0
            for j in batch:
                url = j.get("applicationLink")
                identity = str(j.get("guid") or url or j.get("title"))
                if identity in seen:
                    continue
                seen.add(identity)
                added += 1
                out.append({"source": "himalayas", "title": j.get("title"), "company": j.get("companyName"),
                    "location": ", ".join(j.get("locationRestrictions") or []) or "Remote",
                    "is_remote": True, "date_posted": j.get("pubDate"), "job_type": j.get("employmentType"),
                    "salary": _salary(j.get("minSalary"), j.get("maxSalary"), j.get("currency"), j.get("salaryPeriod")),
                    "job_url": url, "source_url": j.get("guid"), "description": _strip_html(j.get("description"))})
            next_offsets[term] = page
            if not added:
                errors.append(f"{term}: repeated page; pagination unverified")
                break
            if len(out) >= limit:
                break
    return out, {"errors": errors, "next_source_offsets": next_offsets,
                 "coverage": "partial_error" if errors else "bounded_search"}


async def fetch_remotive(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # remotive's ?search= is ignored server-side (it always returns the latest jobs), so we
    # pull the whole feed and let relevance ranking + the caller sort it out.
    r = await client.get("https://remotive.com/api/remote-jobs",
                         params={"search": terms[0] if terms else "", "limit": 500}, headers=_HEADERS)
    r.raise_for_status()
    out = [{
        "source": "remotive",
        "title": j.get("title"),
        "company": j.get("company_name"),
        "location": j.get("candidate_required_location") or "Remote",
        "is_remote": True,
        "date_posted": j.get("publication_date"),
        "salary": j.get("salary") or None,
        "job_url": j.get("url"),
        "description": _strip_html(j.get("description")),
    } for j in r.json().get("jobs", [])]
    return out, {"scanned": len(out)}


async def fetch_remoteok(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    r = await client.get("https://remoteok.com/api", headers=_HEADERS)
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
    return out, {"scanned": len(out)}


async def fetch_arbeitnow(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int,
    max_pages: int = 5, source_offset: int = 0, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Arbeitnow — second-best DE coverage after Arbeitsagentur. Paginated for real recall."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors, next_offset = [], None
    for page in range(source_offset+1, source_offset+max_pages+1):
        try:
            r = await client.get("https://www.arbeitnow.com/api/job-board-api",
                                 params={"page": page}, headers=_HEADERS)
            r.raise_for_status()
            payload = r.json()
            data = payload.get("data", [])
        except (httpx.HTTPError, ValueError) as exc:
            errors.append(safe_error(exc))
            break
        if not data:
            break
        for j in data:
            url = j.get("url")
            if url and url in seen:
                continue
            seen.add(url or "")
            out.append({
                "source": "arbeitnow",
                "title": j.get("title"),
                "company": j.get("company_name"),
                "location": j.get("location"),
                "is_remote": bool(j.get("remote")),
                "date_posted": j.get("created_at"),
                "salary": None,
                "job_url": url,
                "description": _strip_html(j.get("description")),
            })
        next_offset = page if (payload.get("links") or {}).get("next") else None
        if next_offset is None or len(out) >= limit:
            break
    return out, {"scanned": len(out), "errors": errors, "next_source_offset": next_offset,
                 "coverage": "partial_error" if errors else ("budget_limited" if next_offset else "feed_exhausted")}


async def fetch_jobicy(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int,
    geo: str | None = None, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    # Do NOT pass the free-text term as ?tag= (invalid tags 404). Fetch by count (+ geo for
    # DACH) and rank client-side.
    params: dict[str, Any] = {"count": 100}
    if geo:
        params["geo"] = geo
    try:
        r = await client.get("https://jobicy.com/api/v2/remote-jobs", params=params, headers=_HEADERS)
        r.raise_for_status()
        payload = r.json()
    except (httpx.HTTPError, ValueError) as exc:
        return [], {"scanned": 0, "errors": [safe_error(exc)], "coverage": "error"}
    out = [{
        "source": "jobicy",
        "title": j.get("jobTitle"),
        "company": j.get("companyName"),
        "location": j.get("jobGeo") or "Remote",
        "is_remote": True,
        "date_posted": j.get("pubDate"),
        "salary": _salary(j.get("annualSalaryMin"), j.get("annualSalaryMax"), j.get("salaryCurrency"), "year"),
        "job_url": j.get("url"),
        "description": _strip_html(j.get("jobDescription") or j.get("jobExcerpt")),
    } for j in payload.get("jobs", [])]
    return out, {"scanned": len(out), "coverage": "latest_100_only", "exhaustive": False}


async def fetch_hackernews(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    r = await client.get(
        "https://hn.algolia.com/api/v1/search_by_date",
        params={"tags": "story,author_whoishiring", "hitsPerPage": 10}, headers=_HEADERS,
    )
    r.raise_for_status()
    hits = r.json().get("hits", [])
    thread = next((h for h in hits if "who is hiring" in (h.get("title") or "").lower()), None)
    if not thread:
        return [], {"scanned": 0}
    r2 = await client.get(f"https://hn.algolia.com/api/v1/items/{thread['objectID']}", headers=_HEADERS)
    r2.raise_for_status()
    out: list[dict[str, Any]] = []
    for c in r2.json().get("children", []):
        txt = c.get("text")
        if not txt:
            continue
        headline = _strip_html(re.split(r"<p>", txt, maxsplit=1)[0], 300) or ""
        parts = [p.strip() for p in headline.split("|") if p.strip()]
        m = re.search(r'href="(https?://[^"]+)"', txt)
        out.append({
            "source": "hackernews",
            "title": (" | ".join(parts[:3]) if parts else headline)[:160] or "HN job post",
            "company": parts[0] if parts else None,
            "location": None,
            "is_remote": bool(REMOTE_KW.search(txt)),
            "date_posted": c.get("created_at"),
            "salary": None,
            "job_url": (m.group(1) if m else f"https://news.ycombinator.com/item?id={c.get('id')}"),
            "description": _strip_html(txt),
        })
    return out, {"scanned": len(out)}


async def fetch_weworkremotely(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    feeds = [
        "https://weworkremotely.com/remote-jobs.rss",
    ]

    def _tag(block: str, name: str) -> str | None:
        m = re.search(rf"<{name}>(?:<!\[CDATA\[)?(.*?)(?:\]\]>)?</{name}>", block, re.S)
        return html.unescape(m.group(1).strip()) if m else None

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    errors = []
    for feed in feeds:
        try:
            r = await client.get(feed, headers=_HEADERS)
            r.raise_for_status()
        except httpx.HTTPError as exc:
            errors.append(safe_error(exc))
            continue
        for block in re.findall(r"<item>(.*?)</item>", r.text, re.S):
            title = _tag(block, "title") or ""
            link = _tag(block, "link")
            if link and link in seen:
                continue
            seen.add(link or "")
            company, role = (title.split(":", 1) + [""])[:2] if ":" in title else ("", title)
            out.append({
                "source": "weworkremotely",
                "title": (role or title).strip() or None,
                "company": company.strip() or None,
                "location": _tag(block, "region") or "Remote",
                "is_remote": True,
                "date_posted": _tag(block, "pubDate"),
                "salary": None,
                "job_url": link,
                "description": _strip_html(_tag(block, "description")),
            })
    return out, {"scanned": len(out), "errors": errors, "coverage": "rss_window", "exhaustive": False}


async def fetch_themuse(
    client: httpx.AsyncClient, terms: list[str], location: str | None, limit: int, days: int,
    max_pages: int = 5, source_offset: int = 0, **_: Any,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    out: list[dict[str, Any]] = []
    errors, next_offset = [], None
    for page in range(source_offset, source_offset+max_pages):
        try:
            r = await client.get("https://www.themuse.com/api/public/jobs",
                                 params={"page": page}, headers=_HEADERS)
            r.raise_for_status()
            payload = r.json()
            results = payload.get("results", [])
        except (httpx.HTTPError, ValueError) as exc:
            errors.append(safe_error(exc))
            break
        if not results:
            break
        for j in results:
            locs = [l.get("name") for l in (j.get("locations") or []) if l.get("name")]
            out.append({
                "source": "themuse",
                "title": j.get("name"),
                "company": (j.get("company") or {}).get("name"),
                "location": ", ".join(locs) or None,
                "is_remote": any("remote" in (l or "").lower() for l in locs),
                "date_posted": j.get("publication_date"),
                "salary": None,
                "job_url": (j.get("refs") or {}).get("landing_page"),
                "description": _strip_html(j.get("contents")),
            })
        next_offset = page+1 if page+1 < payload.get("page_count", page+2) else None
        if next_offset is None or len(out) >= limit:
            break
    return out, {"scanned": len(out), "errors": errors, "next_source_offset": next_offset,
                 "coverage": "partial_error" if errors else ("budget_limited" if next_offset else "query_exhausted")}


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

def _dedup_key(job: dict[str, Any]) -> tuple:
    """Prefer canonical identity; never merge different job IDs merely for equal titles."""
    url = job.get("job_url")
    if url:
        p = urlsplit(url)
        qs = [(k, v) for k, v in parse_qsl(p.query) if not k.lower().startswith("utm_") and k.lower() not in ("ref", "source", "trk")]
        return ("url", urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), urlencode(sorted(qs)), "")))
    return ("text", job.get("source"), *(str(job.get(k) or "").strip().lower() for k in ("title", "company", "location")))


def merge_jobs(jobs):
    merged, index, duplicates = [], {}, 0
    for job in jobs:
        key = _dedup_key(job)
        if key not in index:
            job = dict(job)
            job["sources"] = list(dict.fromkeys([*(job.get("sources") or []), job.get("source")]))
            merged.append(job)
            index[key] = job
            continue
        duplicates += 1
        old = index[key]
        old["sources"] = list(dict.fromkeys([*old["sources"], *(job.get("sources") or []), job.get("source")]))
        for k, v in job.items():
            if k == "description" and len(v or "") > len(old.get(k) or ""):
                old[k] = v
            elif old.get(k) is None and v is not None:
                old[k] = v
    return merged, duplicates


def safe_error(exc):
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return type(exc).__name__


async def fetch_sources(
    sources: list[str], term: str, location: str | None = None,
    remote_only: bool = False, limit_per_source: int = 100, days: int = 0,
    dach_only: bool = False, extra_terms: list[str] | None = None,
    expand_query: bool = False, fetch_details: bool = False,
    max_pages: int = 5, source_offset: int = 0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Fetch every named source concurrently and return (jobs, meta).

    Recall-first contract:
      * sources are queried as widely as their API allows (deep pagination, query expansion)
      * NOTHING is dropped for being off-topic — every job comes back with a ``relevance``
        score and the list is ordered by it, so a size-cap truncation costs the weakest
        matches first
      * per-source caps are reported in ``meta`` (scanned vs kept vs total_available), so
        the caller can always see when it is looking at a slice rather than the whole market

    ``remote_only`` is a BOOST, not a filter: it adds Arbeitsagentur's arbeitszeit=ho leg
    and sorts remote-looking jobs first. It never removes non-remote results — that
    checkbox is set by employers so rarely that filtering on it returns near-zero.
    """
    if any(s not in _FETCHERS for s in sources):
        raise ValueError("Unsupported source; see list_job_sources")
    if not term.strip() or len(term) > 200 or len(extra_terms or []) > 11 or any(not t.strip() or len(t) > 200 for t in extra_terms or []):
        raise ValueError("Queries must be 1-200 characters, with at most 11 extra terms")
    sources = list(dict.fromkeys(sources))
    terms = expand_terms(term, extra_terms, expand=expand_query)
    meta: dict[str, Any] = {"queries_used": terms, "per_source": {}, "remote_boost": remote_only}

    async with CachedClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        async def run(name: str) -> list[dict[str, Any]]:
            try:
                kwargs: dict[str, Any] = {"max_pages": max_pages, "source_offset": source_offset}
                if name == "jobicy" and dach_only:
                    kwargs["geo"] = "germany"
                if name == "arbeitsagentur":
                    kwargs["remote_boost"] = remote_only
                    kwargs["fetch_details"] = fetch_details
                jobs, src_meta = await _FETCHERS[name](
                    client, terms, location, limit_per_source, days, **kwargs
                )
                scanned = len(jobs)
                jobs = [j for j in jobs if j.get("title")]
                if dach_only and name not in ("arbeitsagentur", "arbeitnow"):
                    jobs = [j for j in jobs if dach_ok(j.get("location"))]
                for j in jobs:
                    j["relevance"] = relevance(j, terms)
                # Rank inside the source, THEN cap — so a per-source cap keeps the best
                # matches rather than whatever the API happened to list first.
                jobs.sort(key=lambda j: (-j["relevance"], -date_ordinal(j)))
                kept = jobs  # Every fetched row is retained in the snapshot, not dropped by response size.
                for j in kept:
                    j["remote_confidence"] = remote_confidence(j)
                    sig = remote_signals(j)
                    if sig:
                        j["remote_signals"] = sig
                entry = {"scanned": scanned, "kept": len(kept), "coverage": "source_window", "exhaustive": False}
                entry.update(src_meta)
                entry["status"] = ("partial_error" if kept else "error") if entry.get("errors") else ("results_received" if kept else "empty_unverified")
                if scanned > len(kept):
                    entry["explicit_filter_removed"] = scanned - len(kept)
                meta["per_source"][name] = entry
                return kept
            except Exception as exc:  # noqa: BLE001 — one bad source must not sink the rest
                log.warning("source %s failed: %s", name, safe_error(exc))
                meta["per_source"][name] = {"scanned": 0, "kept": 0, "errors": [safe_error(exc)], "coverage": "error"}
                return []

        batches = await asyncio.gather(*(run(s) for s in sources))

    merged, duplicates = merge_jobs([j for batch in batches for j in batch if j.get("title")])
    for job in merged:
        d = _parse_date(job.get("date_posted"))
        if d:
            job["date_posted"] = d.isoformat()
    meta["duplicates_removed"] = duplicates
    meta["total_available"] = None  # No adapter knows the unique whole-market total.
    meta["source_offset_unit"] = "pages for paginated APIs; unsupported for window feeds"
    meta["date_filter"] = "AA upstream only; other source dates returned unfiltered" if days else "none"
    return merged, meta
