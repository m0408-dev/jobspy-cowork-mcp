import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock
from fastmcp import Client
import server
from results import ResultStore
from browser_handoff import make_tasks, summary
from sources import _dedup_key


class HandoffTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=ResultStore(str(Path(self.tmp.name)/"test.db"))
        # Registered tools retain the global store object; use its isolated path.
        self.old_path=server.STORE.path
        server.STORE.path=self.store.path
        self.client=Client(server.mcp)
        await self.client.__aenter__()

    async def asyncTearDown(self):
        await self.client.__aexit__(None,None,None)
        server.STORE.path=self.old_path
        self.tmp.cleanup()

    async def call(self,name,**args):
        r=await self.client.call_tool(name,args)
        return json.loads(r.content[0].text)

    def snapshot(self):
        meta={"queries_used":["IT Support"],"per_source":{"arbeitsagentur":{"error":"HTTP 403"}}}
        return json.loads(server._snapshot([],meta))["result_id"]

    async def test_failure_creates_concrete_task(self):
        rid=self.snapshot()
        d=await self.call("get_browser_tasks",result_id=rid)
        self.assertEqual(d["summary"]["pending"],1)
        self.assertIn("arbeitsagentur.de/jobsuche/suche?",d["tasks"][0]["url"])

    async def test_import_and_retry_do_not_duplicate(self):
        rid=self.snapshot()
        args=dict(result_id=rid,task_id="0",outcome="partial",browser="test fixture",
            visited_urls=["https://example.com/jobs/1"],evidence="One listing read; further result pages were not inspected.",
            jobs=[dict(title="Support",company="Example",job_url="https://example.com/jobs/1",
                description="Support position with network troubleshooting and documentation.")])
        await self.call("record_browser_check",**args)
        await self.call("record_browser_check",**args)
        page=await self.call("get_result_page",result_id=rid)
        self.assertEqual(page["total_fetched"],1)
        self.assertEqual(page["coverage"]["browser_handoff"]["pending"],1)
        self.assertFalse(page["jobs"][0]["application_form_checked"])
        self.assertNotIn("_browser_tasks",page["coverage"])

    async def test_block_never_completes_task(self):
        rid=self.snapshot()
        d=await self.call("record_browser_check",result_id=rid,task_id="0",outcome="blocked",
            browser="test fixture",visited_urls=["https://example.com"],evidence="Browser displayed a login wall; no listing could be read.")
        self.assertEqual(d["browser_handoff"]["pending"],1)
        t=await self.call("get_browser_tasks",result_id=rid)
        self.assertIn("another available browser",t["tasks"][0]["next_action"])

    async def test_unvisited_job_rejected(self):
        rid=self.snapshot()
        with self.assertRaises(Exception):
            await self.call("record_browser_check",result_id=rid,task_id="0",outcome="checked",browser="fixture",
                inspection_stage="listing",issue="none",
                visited_urls=["https://example.com/search"],evidence="A search page was read, but not the individual listing.",
                jobs=[dict(title="Support",company="Example",job_url="https://example.com/jobs/1",
                    description="A sufficiently long description but not actually visited.")])
        self.assertEqual(len(server.STORE.load(rid)["jobs"]),0)

    async def test_missing_details_queue_browser(self):
        rid=server.STORE.save([dict(title="Support",job_url="https://example.com/jobs/1")],{})
        d=await self.call("get_job_details",result_id=rid,job_ids=["0"])
        self.assertEqual(d["browser_handoff"]["pending"],1)

    async def test_broad_search_always_independent_sweep(self):
        with patch("server.fetch_sources",AsyncMock(return_value=([],{"per_source":{},"queries_used":["support"]}))), patch("server._jobspy_batch",AsyncMock(return_value=([],{}))):
            d=await self.call("search_all_jobs",search_term="support")
        tasks=await self.call("get_browser_tasks",result_id=d["result_id"],page_size=10)
        all_tasks=server.STORE.load(d["result_id"])["meta"]["_browser_tasks"]
        self.assertTrue({"linkedin","xing","indeed","stepstone","glassdoor","monster"}.issubset({t["source"] for t in all_tasks}))
        self.assertGreater(len(all_tasks),50)

    def test_international_not_silently_germany(self):
        tasks=make_tasks({"market":"international","browser_sweep":True,"queries_used":["German support"]})
        self.assertTrue(all(t["location"]=="" and "Deutschland" not in t["url"] for t in tasks))

    def test_metadata_cannot_exceed_response_budget(self):
        rid=self.store.save([dict(title="Support")],{"large":"x"*40000})
        self.assertLess(len(self.store.page(rid,max_chars=8000)),8000)

    def test_api_and_browser_aa_identity(self):
        a={"job_url":"https://www.arbeitsagentur.de/jobsuche/jobdetail/10000-123-S"}
        b={"job_url":"https://www.arbeitsagentur.de/jobsuche/suche?was=IT&id=10000-123-S"}
        self.assertEqual(_dedup_key(a),_dedup_key(b))

    async def test_checked_scope_closes_pending_not_global_coverage(self):
        rid=self.snapshot()
        result=await self.call("record_browser_check",result_id=rid,task_id="0",outcome="checked_no_results",
            inspection_stage="search_results",issue="none",
            browser="test fixture",visited_urls=["https://example.com/search"],
            evidence="Search page checked; it explicitly reports zero matches for the requested query.")
        self.assertEqual(result["browser_handoff"]["pending"],0)
        self.assertFalse(result["browser_handoff"]["exhaustive"])

    async def test_application_claim_requires_visited_form(self):
        rid=self.snapshot()
        with self.assertRaises(Exception):
            await self.call("record_browser_check",result_id=rid,task_id="0",outcome="checked",browser="fixture",
                inspection_stage="listing",issue="none",
                visited_urls=["https://example.com/jobs/1"],evidence="Listing read but the application form was not inspected.",
                jobs=[dict(title="Support",company="Example",job_url="https://example.com/jobs/1",
                    description="A sufficiently long job description with responsibilities.",
                    application_url="https://example.com/apply",application_requirements="CV")])

    async def test_homepage_and_unknown_never_complete(self):
        for stage in ("homepage","unknown"):
            with self.assertRaises(Exception):
                await self.call("record_browser_check",result_id=self.snapshot(),task_id="0",outcome="checked",
                    browser="fixture",inspection_stage=stage,issue="none",visited_urls=["https://example.com/"],
                    evidence="Only the entry page was loaded; no query was performed.")

    async def test_each_unresolved_issue_rejects_completion(self):
        for issue in ("cookie_banner","wrong_url","dns_error","tls_error","network_error","render_incomplete",
                      "bot_protection","login_required","eligibility_required","unknown"):
            with self.assertRaises(Exception):
                await self.call("record_browser_check",result_id=self.snapshot(),task_id="0",outcome="checked",
                    browser="fixture",inspection_stage="search_results",issue=issue,visited_urls=["https://example.com/"],
                    evidence="Search could not be verified because an access problem remains.")

    async def test_unattempted_queue_does_not_retry_block(self):
        rid=self.snapshot()
        await self.call("record_browser_check",result_id=rid,task_id="0",outcome="blocked",issue="tls_error",
            browser="fixture",visited_urls=["https://example.com/"],evidence="The browser reported a certificate mismatch; not bypassed.")
        self.assertEqual((await self.call("get_browser_tasks",result_id=rid,unattempted_only=True))["tasks"],[])
        pending=await self.call("get_browser_tasks",result_id=rid,pending_only=True)
        self.assertEqual(pending["tasks"][0]["issue"],"tls_error")
        self.assertEqual(pending["summary"]["pending"],1)

    async def test_browser_recovers_api_failure(self):
        rid=self.snapshot()
        result=await self.call("record_browser_check",result_id=rid,task_id="0",outcome="checked",
            inspection_stage="search_results",issue="api_access_denied",browser="fixture",
            visited_urls=["https://www.arbeitsagentur.de/jobsuche/suche?was=IT"],
            evidence="API denied the request but the browser displayed results for the requested search scope.")
        self.assertEqual(result["browser_handoff"]["pending"],0)

    def test_broad_failure_has_query_link(self):
        tasks=make_tasks({"market":"germany","catalog_scope":"all_selected_market_sources",
            "queries_used":["Koch"],"per_source":{"arbeitsagentur":{"error":"HTTP 403"}}})
        aa=next(t for t in tasks if t["source"]=="arbeitsagentur")
        self.assertIn("was=Koch",aa["direct_search_url"])
        self.assertEqual(aa["reason"],"upstream_failed_or_ambiguous")

    def test_historical_hints_are_not_live_completion(self):
        tasks=make_tasks({"market":"worldwide","catalog_scope":"all_selected_market_sources","queries_used":["Koch"]})
        hints=[t for t in tasks if "historical_access_hint" in t]
        self.assertEqual(len(hints),47)
        self.assertTrue(all(t["status"]=="pending" and not t["historical_access_hint"]["current_search_verified"] for t in hints))
        self.assertTrue(any(t["url"]=="https://jobs.golem.de/" for t in tasks))

if __name__=="__main__": unittest.main()
