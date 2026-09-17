"""Opt-in network smoke test; reports coverage only, not private endpoint config."""
import asyncio
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from server import search_all_jobs, STORE
from discovery import employer_jobs

async def main():
    for market, sources, query in [("germany", ["arbeitsagentur","arbeitnow"], "IT Support"),
                                    ("international", ["himalayas"], "German support")]:
        result = json.loads(await search_all_jobs(query, market=market, sources=sources, max_pages=1, results_per_source=20))
        print(json.dumps({"market": market, "count":result["total_fetched"],"chars":len(json.dumps(result)),
                          "coverage":result["coverage"]["per_source"]},ensure_ascii=False))
        assert result["count"] <= 30
        if result["jobs"]:
            STORE.details(result["result_id"], [result["jobs"][0]["id"]])
    jobs = await employer_jobs("greenhouse", "canonical", "global")
    print(json.dumps({"employer":"canonical", "greenhouse_jobs":len(jobs)}))
    assert jobs and jobs[0].get("job_url")

asyncio.run(main())
