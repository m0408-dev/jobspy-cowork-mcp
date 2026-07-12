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
from sources import API_SOURCES, fetch_sources, _dedup_key  # noqa: E402

ApiSource = Literal["arbeitsagentur", "himalayas", "remotive", "remoteok", "arbeitnow", "jobicy"]


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
        "Real job-posting search across 8 free sources. Preferred tool: `search_all_jobs` — it "
        "queries Germany's official federal job database (Arbeitsagentur), five remote-job APIs "
        "(Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy) AND Indeed+LinkedIn in parallel, then "
        "dedups into one merged, normalised list. Use `search_german_jobs` for the German market "
        "only, `search_remote_jobs` for remote-only aggregation, and `search_jobs` for direct "
        "control over the JobSpy scrapers (Indeed/LinkedIn work from the cloud; Google/Glassdoor/"
        "ZipRecruiter need residential proxies via JOBSPY_PROXIES). Every result has title, company, "
        "location, remote flag, salary when known, an application url and a description."
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
        df = scrape_jobs(
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
    for rec in records:
        job = {key: rec.get(key) for key in KEEP_COLUMNS if key in rec}
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
    return {
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


def _render_aggregate(jobs: list[dict[str, Any]], meta: dict[str, Any], extra: dict[str, Any] | None = None) -> str:
    """JSON-encode aggregated results, shrinking the list if it blows the size cap."""
    def build(items: list[dict[str, Any]], truncated: bool) -> str:
        payload: dict[str, Any] = {"count": len(items), "sources": meta}
        if extra:
            payload.update(extra)
        payload["jobs"] = items
        if truncated:
            payload["note"] = "Result truncated to fit the size limit — lower results_per_source."
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)

    text = build(jobs, truncated=False)
    while len(text) > MAX_RESULT_CHARS and len(jobs) > 1:
        jobs = jobs[: max(1, len(jobs) * 3 // 4)]
        text = build(jobs, truncated=True)
    return text


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
        int, Field(ge=1, le=50, description="How many results to pull from each source before dedup.")
    ] = 15,
    days_old: Annotated[
        int, Field(ge=0, le=100, description="Only jobs newer than N days. 0 = no filter.")
    ] = 30,
    include_jobspy: Annotated[
        bool, Field(description="Also scrape Indeed + LinkedIn (richer but slower ~30-60s). Set false for a fast API-only search.")
    ] = True,
    sources: Annotated[
        list[ApiSource] | None,
        Field(description="Restrict which free APIs to hit. Default = all six."),
    ] = None,
) -> str:
    """MOST POWERFUL search — the best default for a real job hunt.

    Queries in parallel: Germany's official federal job database (Arbeitsagentur — the largest
    German job DB), five remote-job APIs (Himalayas, Remotive, RemoteOK, Arbeitnow, Jobicy) and,
    unless disabled, Indeed + LinkedIn via JobSpy. Results are deduplicated across every source
    and returned as one normalised list: {count, sources:{per-source counts}, jobs:[...]}.
    """
    api_sources = list(sources) if sources else list(API_SOURCES)
    log.info("search_all_jobs term=%r loc=%r remote=%s jobspy=%s", search_term, location, remote_only, include_jobspy)

    jobs, meta = await fetch_sources(
        api_sources, search_term, location, remote_only, results_per_source, days_old
    )

    if include_jobspy:
        try:
            df = await asyncio.to_thread(
                scrape_jobs,
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

    # Remote-first ordering when remote_only, else keep source order but surface remote first.
    jobs.sort(key=lambda j: (not j.get("is_remote"),))
    return _render_aggregate(jobs, meta, {"total_before_dedup_note": "counts in `sources` are post-fetch, pre-merge"})


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
    return _render_aggregate(jobs, meta)


@mcp.tool
async def search_remote_jobs(
    search_term: Annotated[str, Field(description="Keywords, e.g. 'react developer' or 'sre'.")],
    results_per_source: Annotated[int, Field(ge=1, le=50, description="Results per API before dedup.")] = 20,
    sources: Annotated[
        list[Literal["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy"]] | None,
        Field(description="Which remote APIs to hit. Default = all five."),
    ] = None,
) -> str:
    """Aggregate remote-only jobs across Himalayas, Remotive, RemoteOK, Arbeitnow and Jobicy.

    Fast (no scraping), free, deduplicated. Returns {count, sources, jobs:[...]}.
    """
    api_sources = list(sources) if sources else ["himalayas", "remotive", "remoteok", "arbeitnow", "jobicy"]
    jobs, meta = await fetch_sources(
        api_sources, search_term, location=None, remote_only=True,
        limit_per_source=results_per_source, days=0,
    )
    return _render_aggregate(jobs, meta)


# ASGI app so production hosts can run `uvicorn server:app` if they prefer.
app = mcp.http_app(path=HTTP_PATH)


def main() -> None:
    if TRANSPORT == "stdio":
        log.info("Starting JobSpy MCP over stdio (local mode)")
        mcp.run(transport="stdio")
    else:
        log.info("Starting JobSpy MCP over Streamable HTTP at http://%s:%d%s", HOST, PORT, HTTP_PATH)
        mcp.run(transport="http", host=HOST, port=PORT, path=HTTP_PATH)


if __name__ == "__main__":
    main()
