"""Data-backed source planning and auditable, paginated coverage. No network I/O."""
import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit, urlencode

CATALOG = json.loads(Path(__file__).with_name("source_catalog.json").read_text(encoding="utf-8"))
ACCESS = json.loads(Path(__file__).with_name("source_access.json").read_text(encoding="utf-8"))
MARKETS = {"germany", "international", "worldwide"}

def segments(row):
    return set(row["markets"])

def select_sources(market):
    if market not in MARKETS:
        raise ValueError("Unknown market")
    return [r for r in CATALOG["sources"] if r.get("research_status") != "alias"
            and (market == "worldwide" or market in segments(r))]

def source_tasks(meta):
    """All selected catalog rows x explicit queries, including unverified candidates."""
    if not meta.get("catalog_scope"):
        return []
    market = meta["market"]
    location = meta.get("search_location", "")
    tasks = []
    for row in select_sources(market):
        url = row["url"]
        if market != "germany":
            url = {"indeed":"https://www.indeed.com/", "glassdoor":"https://www.glassdoor.com/"}.get(row["id"], url)
        domain = urlsplit(url).hostname
        for term in meta["queries_used"]:
            tasks.append({"id": str(len(tasks)), "source": row["id"], "source_name": row["name"],
                "url": url, "indexed_search_url": "https://www.google.com/search?" + urlencode({"q":f"site:{domain} {term} {location} jobs"}),
                "query":term, "market":market, "location":location,
                "reason":"full_catalog_browser_check", "research_status":row["research_status"],
                "filters":{"remote_requested":bool(meta.get("remote_boost")), "days_old":meta.get("days_old",0)},
                "status":"pending"})
            hint = ACCESS["observations"].get(url)
            if hint:
                tasks[-1]["historical_access_hint"] = {"observed_on": ACCESS["checked_on"], "observation":hint,
                    "current_search_verified":False}
    # Give every board its first query before working through synonyms on one board.
    # Preserve all source/query pairs while making short browser batches broad.
    order = {term:i for i,term in enumerate(meta["queries_used"])}
    tasks.sort(key=lambda t: order[t["query"]])
    for i, task in enumerate(tasks):
        task["id"] = str(i)
    return tasks

def coverage(meta):
    tasks = meta.get("_browser_tasks", [])
    rows = meta.get("_catalog_sources", select_sources(meta["market"]) if meta.get("catalog_scope") else [])
    direct = meta.get("per_source", {})
    entries = []
    for row in rows:
        related = [t for t in tasks if t["source"] == row["id"]]
        finished = sum(t["status"] in ("checked", "checked_no_results") for t in related)
        attempted = row["id"] in direct or any(t.get("checked_at") for t in related)
        entries.append({"id":row["id"], "name":row["name"], "url":row["url"],
            "status":"documented_scope_checked" if related and finished == len(related) else ("attempted_pending_browser" if attempted else "not_attempted"),
            "direct":direct.get(row["id"]), "browser_pending":len(related)-finished,
            "browser_outcomes":dict(Counter(t["status"] for t in related)),
            "browser_issues":dict(Counter(t.get("issue","unknown") for t in related if t.get("checked_at"))),
            "last_browser_attempt_at":max((t.get("checked_at",0) for t in related), default=0) or None,
            "direct_finished_at":meta.get("direct_attempts_finished_at") if row["id"] in direct else None})
    counts = Counter(e["status"] for e in entries)
    return {"scope":meta.get("catalog_scope", "narrow"), "market":meta.get("market"),
        "selected_sources":len(entries), "status_counts":dict(counts),
        "all_selected_sources_attempted":bool(entries) and not counts["not_attempted"],
        "all_documented_scopes_checked":bool(entries) and counts["documented_scope_checked"] == len(entries),
        "exhaustive":False, "entries":entries}
