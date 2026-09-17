"""Explicit employer adapters and on-demand discovery; never crawl arbitrary/internal URLs."""
import re
import xml.etree.ElementTree as ET
from typing import Annotated, Literal
from pydantic import Field
from http_cache import CachedClient
from sources import _strip_html, safe_error, relevance
from results import encode


def parse_xml(text):
    if len(text) > 8_000_000 or re.search(r"<!\s*(DOCTYPE|ENTITY)", text, re.I):
        raise ValueError("Unsafe or oversized XML")
    return ET.fromstring(text)


async def employer_jobs(provider, employer, region="eu"):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{0,79}", employer):
        raise ValueError("Expected employer slug, not URL")
    urls = {"greenhouse": f"https://boards-api.greenhouse.io/v1/boards/{employer}/jobs?content=true",
            "lever": f"https://api{'.eu' if region == 'eu' else ''}.lever.co/v0/postings/{employer}?mode=json",
            "personio": f"https://{employer}.jobs.personio.de/xml?language=de"}
    async with CachedClient(timeout=30, follow_redirects=False) as client:
        r = await client.get(urls[provider])
        r.raise_for_status()
    out = []
    if provider == "personio":
        for node in parse_xml(r.text).findall(".//position"):
            job_id = node.findtext("id")
            if not job_id or not job_id.isdigit():
                continue
            desc = "\n".join(" ".join(d.itertext()) for d in node.findall(".//jobDescription"))
            out.append({"title": node.findtext("name"), "company": employer, "location": node.findtext("office"),
                "job_type": node.findtext("employmentType"), "description": _strip_html(desc),
                "job_url": f"https://{employer}.jobs.personio.de/job/{job_id}?language=de"})
    elif provider == "greenhouse":
        for j in r.json().get("jobs", []):
            out.append({"title": j.get("title"), "company": employer, "location": (j.get("location") or {}).get("name"),
                "date_updated": j.get("updated_at"), "description": _strip_html(j.get("content")), "job_url": j.get("absolute_url")})
    else:
        for j in r.json():
            categories = j.get("categories") or {}
            desc = (j.get("descriptionPlain") or "") + "\n" + "\n".join(
                _strip_html(x.get("content")) or "" for x in j.get("lists", [])) + "\n" + (j.get("additionalPlain") or "")
            out.append({"title": j.get("text"), "company": employer, "location": categories.get("location"),
                "job_type": categories.get("commitment"), "description": desc,
                "job_url": j.get("hostedUrl"), "application_url": j.get("applyUrl"), "salary": j.get("salaryRange")})
    for job in out:
        job.update(source=provider, is_remote=None)
    return out


def register_tools(mcp, snapshot, read):
    @mcp.tool(annotations={**read, "title": "Read employer job board"})
    async def search_employer_jobs(provider: Literal["greenhouse", "lever", "personio"],
        employer: str, search_term: str = "", region: Literal["eu", "global"] = "eu") -> str:
        """Read an explicitly selected employer's public ATS by slug. No application submission/form verification. Lever region selects API host."""
        try:
            jobs = await employer_jobs(provider, employer, region)
        except Exception as exc:
            return encode({"error": safe_error(exc), "provider": provider, "employer": employer, "verified": False})
        for job in jobs:
            job["relevance"] = relevance(job, [search_term])
        return snapshot(jobs, {"employer": employer, "provider": provider, "coverage": "public_board_response", "application_form_checked": False})

    @mcp.tool(annotations={**read, "title": "Discover additional job sources"})
    async def discover_job_sources(query: Annotated[str, Field(min_length=1, max_length=200)],
        market: Literal["germany", "international"] = "germany",
        limit: Annotated[int, Field(ge=1, le=20)] = 10) -> str:
        """On-demand web discovery via Bing RSS for new boards/employers. Returns unverified links, not scraped jobs; may be blocked."""
        suffix = " Stellenangebote Jobbörse Deutschland" if market == "germany" else " German speaking jobs careers"
        try:
            async with CachedClient(timeout=20, follow_redirects=False) as client:
                r = await client.get("https://www.bing.com/search", params={"q": query+suffix, "format": "rss"})
                r.raise_for_status()
            root = parse_xml(r.text)
            links = [{"title": n.findtext("title"), "url": n.findtext("link")} for n in root.findall(".//item")]
            links = [j for j in links if (j["url"] or "").startswith("https://")]
            return encode({"market": market, "verified": False, "links": links[:limit], "coverage": "search_engine_sample"})
        except Exception as exc:
            return encode({"error": safe_error(exc), "links": [], "coverage": "unavailable; use browser search"})
