"""
JobSpy Cowork MCP Server
========================
A Model Context Protocol server that scrapes real job postings from Indeed, LinkedIn,
Glassdoor, Google, ZipRecruiter, Bayt, Naukri & BDJobs via the JobSpy library, exposed
as a single `search_jobs` tool.

The same file runs two ways (chosen with the MCP_TRANSPORT env var):
  * stdio  -> local testing, Claude Desktop, Claude Code
  * http   -> public Streamable-HTTP endpoint for Claude Cowork custom connectors

Everything runs in-process: JobSpy is a Python library and we call it directly. There is
no shell and no subprocess, so the command-injection surface of the original Node/Docker
wrapper does not exist here. All configuration is via environment variables (see
.env.example).
"""

from __future__ import annotations

import asyncio
import datetime
import json
import logging
import math
import os
import threading
import time
from collections import defaultdict
from typing import Annotated, Any, Literal

import pandas as pd
from pydantic import Field

from fastmcp import FastMCP
from fastmcp.server.auth import StaticTokenVerifier
from starlette.requests import Request
from starlette.responses import PlainTextResponse

try:
    from jobspy import scrape_jobs
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "python-jobspy is not installed. Run `uv sync` or `pip install -r requirements.txt`."
    ) from exc

# Free, key-less/public-client-id JSON APIs that work from a datacenter IP
# (Arbeitsagentur, Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy).
from sources import (  # noqa: E402
    API_SOURCES, REMOTE_SOURCES, SOURCE_INFO, fetch_sources, _dedup_key,
    looks_like_job, looks_remote, date_ordinal, remote_confidence,
)

ApiSource = Literal[
    "arbeitsagentur", "himalayas", "remotive", "remoteok", "arbeitnow",
    "jobicy", "hackernews", "weworkremotely", "themuse",
]


# --------------------------------------------------------------------------- #
# Configuration (all env-driven)
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
)
log = logging.getLogger("jobspy-mcp")

TRANSPORT = os.getenv("MCP_TRANSPORT", "http").lower()          # "http" | "stdio"
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))                           # Render/Railway inject $PORT
HTTP_PATH = os.getenv("MCP_HTTP_PATH", "/mcp/")                 # set to a secret path for Cowork
AUTH_TOKEN = os.getenv("MCP_AUTH_TOKEN", "").strip()            # optional bearer token

# Comma-separated residential proxies, JobSpy format:  user:pass@host:port
_env_proxies = os.getenv("JOBSPY_PROXIES", "").strip()
DEFAULT_PROXIES: list[str] | None = (
    [p.strip() for p in _env_proxies.split(",") if p.strip()] if _env_proxies else None
)

# Claude tool results are capped around 150k characters — stay comfortably under it.
MAX_RESULT_CHARS = int(os.getenv("MAX_RESULT_CHARS", "140000"))
MAX_DESC_CHARS = int(os.getenv("MAX_DESC_CHARS", "1800"))

# --- Abuse / resource protection (matters when the endpoint is public) -----
# Per-IP request budget (token bucket over a 60s window) enforced by ASGI middleware.
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "40"))
# Cap concurrent JobSpy scrapes — they are heavy AND share ONE datacenter IP, so
# unbounded parallelism is what gets that IP rate-limited/banned by Indeed/LinkedIn.
# threading (not asyncio) so it also bounds the sync search_jobs tool, which FastMCP
# runs in a worker thread. Non-blocking acquire → fail fast instead of piling up.
JOBSPY_CONCURRENCY = int(os.getenv("JOBSPY_CONCURRENCY", "1"))
_JOBSPY_LOCK = threading.BoundedSemaphore(max(1, JOBSPY_CONCURRENCY))


def _run_scrape(**kwargs: Any):
    """Run JobSpy under the concurrency cap; fail fast if the slot is busy."""
    if not _JOBSPY_LOCK.acquire(timeout=2):
        raise RuntimeError(
            "Scraper busy (too many concurrent Indeed/LinkedIn requests). "
            "Retry shortly, or use the API sources via search_all_jobs(include_jobspy=false)."
        )
    try:
        return scrape_jobs(**kwargs)
    finally:
        _JOBSPY_LOCK.release()

SITES = ["indeed", "linkedin", "glassdoor", "google", "zip_recruiter", "bayt", "naukri", "bdjobs"]
SiteName = Literal["indeed", "linkedin", "glassdoor", "google", "zip_recruiter", "bayt", "naukri", "bdjobs"]

# Columns we surface from JobSpy's DataFrame (lower-cased). Everything else is dropped.
KEEP_COLUMNS = [
    "site", "title", "company", "location", "job_type", "date_posted",
    "salary_source", "interval", "min_amount", "max_amount", "currency",
    "is_remote", "job_level", "job_url", "job_url_direct", "company_url",
    "description",
]


# --------------------------------------------------------------------------- #
# MCP server
# --------------------------------------------------------------------------- #

auth = (
    StaticTokenVerifier(tokens={AUTH_TOKEN: {"sub": "owner", "client_id": "cowork"}})
    if AUTH_TOKEN
    else None
)

mcp = FastMCP(
    name="JobSpy Job Search",
    instructions=(
        "Job-posting search tuned for DACH (Germany/Austria/Switzerland), German-language IT/support "
        "roles and 100% Homeoffice. RECALL-FIRST: these tools cast a wide net and return generously — "
        "YOU are the classifier. Expect some off-topic hits and rank/drop them yourself; do NOT assume "
        "'no results' unless a tool truly returns count 0. Prefer `search_all_jobs` — it queries "
        "Arbeitsagentur (official DE DB), the remote APIs (Himalayas, Remotive, RemoteOK, Arbeitnow, "
        "Jobicy, HackerNews, WeWorkRemotely, The Muse) and optionally Indeed+LinkedIn in parallel, "
        "deduped. Use German search terms for German roles ('IT-Support', 'Systemadministrator', "
        "'Helpdesk', 'Application Support', '1st/2nd Level Support', 'Fachinformatiker'); synonyms are "
        "matched automatically. `remote_only=true` returns Home-Office roles. Optional `dach_only` "
        "(default OFF) pre-drops jobs restricted to non-European regions if you want less noise. "
        "IMPORTANT for a strict 100%-remote hunt: `is_remote` only means the SOURCE calls it remote — "
        "many are remote-first with mandatory office days. Use the per-job `remote_confidence` field "
        "instead: 'strict' = explicitly 100%/fully remote; 'likely' = remote-tagged, no onsite wording "
        "(confirm in the posting); 'hybrid' = has an onsite/Präsenz/relocation obligation → drop for a "
        "100%-remote filter; 'mixed' = says both, read it. Postings can also be expired — check "
        "date_posted (results are sorted newest-first). `search_remote_jobs` = remote APIs only; "
        "`search_jobs` = direct JobSpy. Call `list_job_sources` to pick `sources=[...]`. "
        "Result: {count, returned, fetched_per_source, jobs:[...]}."
    ),
    auth=auth,
)


@mcp.custom_route("/health", methods=["GET"])
async def health(_request: Request) -> PlainTextResponse:
    """Plain health check for uptime monitors and platform probes (Render, Railway, Fly)."""
    return PlainTextResponse("ok")


def _clean_scalar(value: Any) -> Any:
    """Convert a pandas/NumPy scalar into something json.dumps can handle (NaN/NaT -> None)."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if isinstance(value, (datetime.date, datetime.datetime)):  # date_posted etc.
        return value.isoformat()
    try:
        na = pd.isna(value)
        if isinstance(na, bool) and na:
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):  # numpy scalar -> native python
        try:
            return value.item()
        except Exception:  # noqa: BLE001
            return str(value)
    return value


def _dataframe_to_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Turn a JobSpy DataFrame into a list of clean, json-safe dicts with lower-cased keys."""
    records: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        records.append({str(col).lower(): _clean_scalar(val) for col, val in row.items()})
    return records


@mcp.tool
def search_jobs(
    search_term: Annotated[
        str, Field(description="Keywords to search for, e.g. 'python backend developer'.")
    ],
    location: Annotated[
        str, Field(description="City / region, e.g. 'Berlin, Germany'. Use 'remote' for remote-only.")
    ] = "Germany",
    site_name: Annotated[
        list[SiteName],
        Field(description="Which job boards to search. indeed = most reliable; linkedin = richest but rate-limits."),
    ] = ["indeed", "linkedin", "google"],
    results_wanted: Annotated[
        int, Field(ge=1, le=100, description="Number of results to fetch per site.")
    ] = 20,
    hours_old: Annotated[
        int, Field(ge=0, description="Only jobs posted within the last N hours. 0 = no time filter. 168 = last week.")
    ] = 168,
    job_type: Annotated[
        Literal["fulltime", "parttime", "internship", "contract"] | None,
        Field(description="Filter by employment type."),
    ] = None,
    is_remote: Annotated[bool, Field(description="Only return remote positions.")] = False,
    distance: Annotated[int, Field(ge=0, description="Search radius in miles around the location.")] = 50,
    country_indeed: Annotated[
        str, Field(description="Country for Indeed & Glassdoor, e.g. 'germany', 'usa', 'uk'.")
    ] = "germany",
    google_search_term: Annotated[
        str | None,
        Field(description="Full Google-style query for the Google Jobs site (overrides the auto one)."),
    ] = None,
    linkedin_fetch_description: Annotated[
        bool, Field(description="Fetch each LinkedIn job's full description. More detail, much slower.")
    ] = False,
    include_description: Annotated[
        bool, Field(description="Include (truncated) job descriptions in the result.")
    ] = True,
    offset: Annotated[int, Field(ge=0, description="Skip the first N results (pagination).")] = 0,
    proxies: Annotated[
        list[str] | None,
        Field(description="Override residential proxies (user:pass@host:port). Falls back to JOBSPY_PROXIES."),
    ] = None,
) -> str:
    """Search multiple job boards at once and return structured listings as JSON.

    Returns a JSON object: {count, sites_searched, jobs: [...]}. Each job has title, company,
    location, salary range, remote flag, the application url (job_url / job_url_direct) and a
    truncated description. Follow job_url for the full posting.
    """
    sites = list(site_name) or ["indeed"]
    used_proxies = proxies or DEFAULT_PROXIES
    log.info(
        "search_jobs term=%r location=%r sites=%s results=%d proxies=%s",
        search_term, location, sites, results_wanted, bool(used_proxies),
    )

    try:
        df = _run_scrape(
            site_name=sites,
            search_term=search_term,
            google_search_term=google_search_term or f"{search_term} jobs near {location}",
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old if hours_old and hours_old > 0 else None,
            job_type=job_type,
            is_remote=is_remote,
            distance=distance,
            country_indeed=country_indeed,
            linkedin_fetch_description=linkedin_fetch_description,
            offset=offset,
            proxies=used_proxies,
            verbose=1,
        )
    except Exception as exc:  # noqa: BLE001 - surface any scrape error to the client
        log.exception("scrape_jobs failed")
        return json.dumps(
            {
                "count": 0,
                "jobs": [],
                "error": f"Job search failed: {exc}",
                "hint": "If this is a cloud/Cowork deployment, the site likely blocked the "
                        "datacenter IP. Configure residential proxies via JOBSPY_PROXIES.",
            },
            ensure_ascii=False,
        )

    if df is None or len(df) == 0:
        return json.dumps(
            {
                "count": 0,
                "jobs": [],
                "message": "No jobs found. Try broader keywords, more sites, a larger radius, "
                           "or (in the cloud) check that proxies are configured.",
            },
            ensure_ascii=False,
        )

    records = _dataframe_to_records(df)

    jobs: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for rec in records:
        job = {key: rec.get(key) for key in KEEP_COLUMNS if key in rec}
        # BUG6: drop obvious non-jobs (blog/webinar/article) that JobSpy sometimes returns.
        if not looks_like_job(job):
            continue
        # BUG4: JobSpy's is_remote filter is unreliable → enforce it ourselves. Keep a job only
        # if it is flagged remote OR the title/description says home office / remote, and then
        # normalise the flag to True so the returned is_remote is consistent with the filter.
        if is_remote:
            if not (
                job.get("is_remote") is True
                or looks_remote(job.get("title"))
                or looks_remote(job.get("description"))
            ):
                continue
            job["is_remote"] = True
        # BUG4b: JobSpy returns the same posting several times (Indeed/LinkedIn emit one row per
        # URL locale variant). Dedup by normalised title+company so a job appears once.
        dk = _dedup_key(job)
        if dk in seen_keys:
            continue
        seen_keys.add(dk)
        job["remote_confidence"] = remote_confidence(job)   # 100%-remote hint (strict/likely/hybrid/mixed)
        if not include_description:
            job.pop("description", None)
        elif job.get("description"):
            desc = str(job["description"])
            if len(desc) > MAX_DESC_CHARS:
                job["description"] = desc[:MAX_DESC_CHARS] + " …[truncated — open job_url for full text]"
        jobs.append(job)

    def render(items: list[dict[str, Any]], truncated: bool) -> str:
        payload: dict[str, Any] = {"count": len(items), "sites_searched": sites, "jobs": items}
        if truncated:
            payload["note"] = (
                "Result was truncated to fit the size limit. Lower results_wanted or set "
                "include_description=false for full coverage."
            )
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    text = render(jobs, truncated=False)
    while len(text) > MAX_RESULT_CHARS and len(jobs) > 1:
        jobs = jobs[: max(1, len(jobs) * 3 // 4)]  # drop ~25% and retry
        text = render(jobs, truncated=True)

    log.info("search_jobs returning %d jobs (%d chars)", len(jobs), len(text))
    return text


# --------------------------------------------------------------------------- #
# Aggregated tools over the free JSON APIs (+ optional JobSpy)
# --------------------------------------------------------------------------- #

def _jobspy_to_common(rec: dict[str, Any]) -> dict[str, Any]:
    """Normalise one JobSpy record into the shared cross-source schema."""
    lo, hi, cur = rec.get("min_amount"), rec.get("max_amount"), rec.get("currency") or ""
    salary = None
    if lo or hi:
        salary = (f"{int(lo)}-{int(hi)}" if lo and hi else str(int(lo or hi))) + (f" {cur}" if cur else "")
    desc = rec.get("description")
    job = {
        "source": rec.get("site") or "jobspy",
        "title": rec.get("title"),
        "company": rec.get("company"),
        "location": rec.get("location"),
        "is_remote": bool(rec.get("is_remote")),
        "date_posted": str(rec["date_posted"]) if rec.get("date_posted") else None,
        "salary": salary,
        "job_url": rec.get("job_url_direct") or rec.get("job_url"),
        "description": (str(desc)[:MAX_DESC_CHARS] + " …[truncated]") if desc and len(str(desc)) > MAX_DESC_CHARS
                       else (str(desc) if desc else None),
    }
    job["remote_confidence"] = remote_confidence(job)
    return job


# In "concise" mode we drop the heavy/optional fields so many more jobs fit under the
# size cap — the agent can re-query a shortlist with response_format="detailed".
_CONCISE_DROP = ("description", "salary", "date_posted")


def _render_aggregate(
    jobs: list[dict[str, Any]], meta: dict[str, Any],
    response_format: str = "concise", extra: dict[str, Any] | None = None,
) -> str:
    """JSON-encode aggregated results, shrinking the list if it blows the size cap."""
    if response_format == "concise":
        jobs = [{k: v for k, v in j.items() if k not in _CONCISE_DROP} for j in jobs]

    def build(items: list[dict[str, Any]], truncated: bool) -> str:
        # BUG8: `count` == number of jobs actually returned. `fetched_per_source` is the
        # PRE-dedup per-source count (fetched), clearly separated from the returned total.
        payload: dict[str, Any] = {
            "count": len(items),
            "returned": len(items),
            "fetched_per_source": meta,
            "response_format": response_format,
        }
        if extra:
            payload.update(extra)
        payload["jobs"] = items
        if truncated:
            payload["note"] = (
                "Result truncated to fit the size limit. Use response_format='concise', "
                "fewer sources, or a lower results_per_source to see everything."
            )
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    text = build(jobs, truncated=False)
    while len(text) > MAX_RESULT_CHARS and len(jobs) > 1:
        jobs = jobs[: max(1, len(jobs) * 3 // 4)]
        text = build(jobs, truncated=True)
    return text


@mcp.tool
def list_job_sources() -> str:
    """List every job source this server can search, with coverage and how to select it.

    Use this to decide the `sources=[...]` argument for search_all_jobs / search_remote_jobs.
    Returns {api_sources, remote_sources, jobspy_sources, catalog}. Cheap, no network call.
    """
    return json.dumps({
        "total_sources": len(SOURCE_INFO),
        "api_sources": API_SOURCES,
        "remote_sources": REMOTE_SOURCES,
        "jobspy_sources": ["indeed", "linkedin", "glassdoor", "google", "zip_recruiter", "bayt", "naukri", "bdjobs"],
        "catalog": SOURCE_INFO,
        "recommended": "search_all_jobs for the widest net; search_german_jobs for the DE market; "
                       "search_remote_jobs for remote-only.",
    }, ensure_ascii=False, indent=2)


@mcp.tool
async def search_all_jobs(
    search_term: Annotated[
        str, Field(description="Keywords, e.g. 'python backend developer' or 'devops engineer'.")
    ],
    location: Annotated[
        str, Field(description="City/region/country, e.g. 'Berlin, Germany' or 'Germany'. Used by Arbeitsagentur + Indeed/LinkedIn.")
    ] = "Germany",
    remote_only: Annotated[bool, Field(description="Only remote positions.")] = False,
    results_per_source: Annotated[
        int, Field(ge=1, le=50, description="How many results to pull from each source before dedup. Higher = more recall.")
    ] = 25,
    days_old: Annotated[
        int, Field(ge=0, le=100, description="Only jobs newer than N days. 0 = no filter.")
    ] = 30,
    include_jobspy: Annotated[
        bool, Field(description="Also scrape Indeed + LinkedIn (richer but slower ~30-60s, and shares one IP so it's rate-capped). Default off; enable for a deeper sweep.")
    ] = False,
    sources: Annotated[
        list[ApiSource] | None,
        Field(description="Restrict which free APIs to hit. Default = all nine. See list_job_sources."),
    ] = None,
    response_format: Annotated[
        Literal["concise", "detailed"],
        Field(description="concise = title/company/location/url (fits many more jobs — best for a first sweep); detailed = also description/salary/date."),
    ] = "concise",
    dach_only: Annotated[
        bool, Field(description="Optional DACH pre-filter (default OFF — return broadly, you classify). Set true to drop jobs explicitly restricted to non-European regions (US/AU/IN/PH/BR only)."),
    ] = False,
) -> str:
    """MOST POWERFUL search — the best default for a real job hunt (tuned for DACH).

    Recall-first by design: it casts a wide net and returns generously — YOU (the calling AI)
    classify / rank / drop the irrelevant ones. Better too many results than too few.

    Queries in parallel: Germany's official federal job database (Arbeitsagentur), the remote-job
    APIs (Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy, HackerNews, WeWorkRemotely) and,
    unless disabled, Indeed + LinkedIn via JobSpy. Results are deduplicated across every source
    and returned as: {count, returned, fetched_per_source, jobs:[...]}.
    """
    api_sources = list(sources) if sources else list(API_SOURCES)
    log.info("search_all_jobs term=%r loc=%r remote=%s jobspy=%s dach=%s",
             search_term, location, remote_only, include_jobspy, dach_only)

    jobs, meta = await fetch_sources(
        api_sources, search_term, location, remote_only, results_per_source, days_old,
        dach_only=dach_only,
    )

    if include_jobspy:
        try:
            df = await asyncio.to_thread(
                _run_scrape,
                site_name=["indeed", "linkedin"],
                search_term=search_term,
                location=location,
                results_wanted=results_per_source,
                hours_old=(days_old * 24) if days_old and days_old > 0 else None,
                is_remote=remote_only,
                country_indeed="germany",
                proxies=DEFAULT_PROXIES,
                verbose=0,
            )
            if df is not None and len(df):
                seen = {_dedup_key(j) for j in jobs}
                added = 0
                for rec in _dataframe_to_records(df):
                    job = _jobspy_to_common(rec)
                    if not job["title"]:
                        continue
                    key = _dedup_key(job)
                    if key in seen:
                        continue
                    seen.add(key)
                    jobs.append(job)
                    added += 1
                meta["indeed+linkedin"] = added
        except Exception as exc:  # noqa: BLE001
            log.warning("jobspy leg failed: %s", exc)
            meta["indeed+linkedin"] = f"error: {exc}"

    # Surface remote roles first, then freshest first (BUG1: stale ads sink instead of being
    # dropped — the date stays visible so the calling AI can deprioritise old postings).
    jobs.sort(key=lambda j: (not j.get("is_remote"), -date_ordinal(j)))
    return _render_aggregate(jobs, meta, response_format=response_format)


@mcp.tool
async def search_german_jobs(
    search_term: Annotated[str, Field(description="Keywords, e.g. 'Softwareentwickler' or 'data engineer'.")],
    location: Annotated[
        str, Field(description="City/region, e.g. 'Berlin' or 'München'. Leave as 'Germany' for nationwide.")
    ] = "Germany",
    remote_only: Annotated[bool, Field(description="Only Homeoffice/remote roles (Arbeitsagentur 'ho' filter).")] = False,
    results_wanted: Annotated[int, Field(ge=1, le=100, description="Number of results.")] = 25,
    days_old: Annotated[int, Field(ge=0, le=100, description="Only jobs newer than N days. 0 = no filter.")] = 30,
) -> str:
    """Search the official German federal job database (Bundesagentur für Arbeit / Arbeitsagentur).

    This is the largest German job database and works without proxies. Returns
    {count, jobs:[...]} — follow each job_url for the full posting (the list API has no
    description field). Great for the German market, incl. a native remote filter.
    """
    jobs, meta = await fetch_sources(
        ["arbeitsagentur"], search_term, location, remote_only, results_wanted, days_old
    )
    jobs.sort(key=lambda j: (not j.get("is_remote"), -date_ordinal(j)))
    return _render_aggregate(jobs, meta, response_format="detailed")


@mcp.tool
async def search_remote_jobs(
    search_term: Annotated[str, Field(description="Keywords, e.g. 'react developer' or 'sre'.")],
    results_per_source: Annotated[int, Field(ge=1, le=50, description="Results per API before dedup. Higher = more recall.")] = 25,
    sources: Annotated[
        list[Literal["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy", "hackernews", "weworkremotely", "themuse"]] | None,
        Field(description="Which remote APIs to hit. Default = all eight remote sources."),
    ] = None,
    response_format: Annotated[
        Literal["concise", "detailed"],
        Field(description="concise = compact (more jobs fit); detailed = with descriptions/salary."),
    ] = "concise",
    dach_only: Annotated[
        bool, Field(description="Optional DACH pre-filter (default OFF — return broadly, you classify). Set true to drop jobs restricted to non-European regions."),
    ] = False,
) -> str:
    """Aggregate remote jobs across Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy,
    HackerNews 'Who is hiring' and WeWorkRemotely (The Muse only if explicitly requested).

    Fast (no scraping), free, deduplicated. Returns {count, returned, fetched_per_source, jobs:[...]}.
    """
    api_sources = list(sources) if sources else list(REMOTE_SOURCES)
    jobs, meta = await fetch_sources(
        api_sources, search_term, location=None, remote_only=True,
        limit_per_source=results_per_source, days=0, dach_only=dach_only,
    )
    jobs.sort(key=lambda j: (not j.get("is_remote"), -date_ordinal(j)))
    return _render_aggregate(jobs, meta, response_format=response_format)


class _RateLimit:
    """Pure-ASGI per-IP rate limiter. SSE-safe: it decides before the app runs and
    never wraps the response stream. Reads the real client IP from X-Forwarded-For
    (set by the Caddy reverse proxy). In-memory sliding window — fine for one instance.
    """

    def __init__(self, inner: Any, per_min: int) -> None:
        self.inner = inner
        self.per_min = per_min
        self.hits: dict[str, list[float]] = defaultdict(list)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or self.per_min <= 0:
            return await self.inner(scope, receive, send)
        if scope.get("path", "").rstrip("/").endswith("/health"):
            return await self.inner(scope, receive, send)

        headers = dict(scope.get("headers") or [])
        xff = headers.get(b"x-forwarded-for", b"").decode()
        ip = (xff.split(",")[0].strip() if xff else "") or (scope.get("client") or ["?"])[0]

        now = time.monotonic()
        cutoff = now - 60.0
        window = self.hits[ip]
        drop = 0
        while drop < len(window) and window[drop] < cutoff:
            drop += 1
        if drop:
            del window[:drop]

        if len(window) >= self.per_min:
            body = json.dumps({
                "error": "rate_limit_exceeded",
                "limit_per_min": self.per_min,
                "hint": "Slow down. For heavy/continuous use, self-host your own free instance "
                        "(see the repo README) so you're not sharing one datacenter IP.",
            }).encode()
            await send({"type": "http.response.start", "status": 429,
                        "headers": [(b"content-type", b"application/json"), (b"retry-after", b"30")]})
            await send({"type": "http.response.body", "body": body})
            return

        window.append(now)
        if len(self.hits) > 4096:  # opportunistic cleanup so the map can't grow unbounded
            for k in [k for k, v in self.hits.items() if not v or v[-1] < cutoff]:
                self.hits.pop(k, None)
        return await self.inner(scope, receive, send)


# ASGI app (rate-limited) so production hosts can run `uvicorn server:app`.
app = _RateLimit(mcp.http_app(path=HTTP_PATH), RATE_LIMIT_PER_MIN)


def main() -> None:
    if TRANSPORT == "stdio":
        log.info("Starting JobSpy MCP over stdio (local mode)")
        mcp.run(transport="stdio")
    else:
        import uvicorn
        log.info(
            "Starting JobSpy MCP (hardened) at http://%s:%d%s  [rate=%d/min/IP, jobspy_concurrency=%d]",
            HOST, PORT, HTTP_PATH, RATE_LIMIT_PER_MIN, JOBSPY_CONCURRENCY,
        )
        uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()
