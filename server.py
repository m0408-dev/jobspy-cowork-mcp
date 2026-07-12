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
        "Search real job postings across Indeed, LinkedIn, Glassdoor, Google, ZipRecruiter, "
        "Bayt, Naukri and BDJobs with the `search_jobs` tool. It returns structured listings "
        "(title, company, location, salary, url, description) that you can review, compare and "
        "apply to. If a search comes back empty from a cloud host, the site most likely blocked "
        "the datacenter IP — residential proxies (JOBSPY_PROXIES) fix that."
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
