import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock
import httpx
import pandas as pd
from fastmcp import Client
import sources
import server
from results import ResultStore
from http_cache import CachedClient, _cache
from discovery import employer_jobs, parse_xml


class ResultsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = ResultStore(str(Path(self.tmp.name)/"test.sqlite"))
    def tearDown(self):
        self.tmp.cleanup()
    def test_all_1837_results_recoverable(self):
        jobs = [{"title": f"Job {i}", "job_url": f"https://example.org/{i}", "description": "full text "*1000} for i in range(1837)]
        rid = self.store.save(jobs, {})
        offset, ids = 0, []
        while offset is not None:
            raw = self.store.page(rid, offset, 100, max_chars=8000)
            self.assertLessEqual(len(raw), 8000)
            page = json.loads(raw)
            ids += [j["id"] for j in page["jobs"]]
            offset = page["next_offset"]
        self.assertEqual(len(set(ids)), 1837)
        self.assertEqual(len(json.loads(self.store.details(rid, ["0"], 0, 20000))["jobs"][0]["description"]), 10000)
    def test_persistent_across_instance(self):
        rid = self.store.save([{"title": "saved"}], {})
        self.assertEqual(ResultStore(self.store.path).load(rid)["jobs"][0]["title"], "saved")
    def test_expiry_and_unknown(self):
        rid = self.store.save([], {})
        with patch("results.time.time", return_value=10**12):
            with self.assertRaises(ValueError): self.store.load(rid)
        with self.assertRaises(ValueError): self.store.load("not-a-token")
    def test_oversize_not_silently_dropped(self):
        self.store.max_bytes = 20
        with self.assertRaises(ValueError): self.store.save([{"description": "long"*100}], {})
    def test_huge_record_progress(self):
        rid = self.store.save([{"title": "x"*10000}], {})
        page = json.loads(self.store.page(rid, max_chars=8000))
        self.assertEqual(page["count"], 1)
        self.assertIsNone(page["next_offset"])
    def test_update_preserves_id_and_details(self):
        rid = self.store.save([{"title":"Support","description":None}],{})
        self.store.update_jobs(rid,{"0":{"description":"new description"}})
        self.assertEqual(self.store.load(rid)["jobs"][0]["id"],"0")
        self.assertEqual(json.loads(self.store.details(rid,["0"]))["jobs"][0]["description"],"new description")


class SignalsTests(unittest.TestCase):
    def test_long_description_remote_contradiction(self):
        desc = sources._strip_html("100% remote " + "x"*5000 + " mandatory office attendance, hybrid")
        self.assertGreater(len(desc), 5000)
        self.assertEqual(sources.remote_confidence({"description": desc}), "mixed")
    def test_no_remote_not_positive(self):
        self.assertEqual(sources.remote_confidence({"description": "No remote work allowed"}), "negative_or_mixed")
    def test_title_not_verified(self):
        self.assertEqual(sources.remote_confidence({"title": "100% remote"}), "unverified_title")
    def test_url_identity_preserves_distinct_ids(self):
        a = {"title": "Support", "company": "Acme", "location": "Berlin", "job_url": "https://x.test/jobs?id=1"}
        b = {**a, "job_url": "https://x.test/jobs?id=2"}
        self.assertEqual(len(sources.merge_jobs([a,b])[0]), 2)
    def test_merge_preserves_best_text_and_sources(self):
        a = {"job_url": "https://x.test/1?utm_source=x", "source": "indeed", "description": "short"}
        b = {"job_url": "https://x.test/1", "source": "linkedin", "description": "longer description"}
        jobs, n = sources.merge_jobs([a,b])
        self.assertEqual(n, 1)
        self.assertEqual(jobs[0]["description"], b["description"])
        self.assertEqual(jobs[0]["sources"], ["indeed", "linkedin"])
    def test_salary_period_and_origin(self):
        salary = server._jobspy_to_common({"min_amount":16,"currency":"EUR","interval":"hourly","salary_source":"direct_data"})["salary"]
        self.assertEqual(salary["period"], "hourly")
        self.assertEqual(salary["source"], "direct_data")
    def test_german_location_not_language(self):
        self.assertEqual(sources.german_evidence({"location":"Berlin"}), "unknown")
        self.assertEqual(sources.german_evidence({"title":"German-speaking support"}), "explicit_signal")
    def test_reject_xml_entities(self):
        with self.assertRaises(ValueError): parse_xml('<!DOCTYPE x [<!ENTITY y "z">]><x>&y;</x>')


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_aa_http_error_visible(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(403))) as c:
            jobs, meta = await sources.fetch_arbeitsagentur(c,["support"],None,10,0,fetch_details=False)
        self.assertEqual(jobs, [])
        self.assertIn("HTTP 403", str(meta["errors"]))
    async def test_aa_continuation_does_not_restart_page_one(self):
        pages = []
        def reply(r):
            p = int(r.url.params["page"]); pages.append(p)
            return httpx.Response(200, json={"maxErgebnisse":500,"stellenangebote":[{"titel":"Support","refnr":str(i)} for i in range((p-1)*100,p*100)]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as c:
            jobs, meta = await sources.fetch_arbeitsagentur(c,["a","b"],None,250,0,fetch_details=False,max_pages=4)
        self.assertEqual(pages.count(1), 2) # once per query, never refetched for top-up
        self.assertGreaterEqual(len(jobs), 250)
    async def test_himalayas_all_explicit_terms_and_pages(self):
        calls=[]
        def reply(r):
            calls.append((r.url.params["q"],r.url.params["page"]))
            return httpx.Response(200,json={"jobs":[{"title":"Support","guid":str(r.url),"applicationLink":str(r.url)}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as c:
            jobs, meta = await sources.fetch_himalayas(c,["a","b","c","d"],None,100,0,max_pages=2)
        self.assertEqual(len(calls),8)
        self.assertIn(("d","2"),calls)
    async def test_cache_avoids_second_request(self):
        _cache.clear(); calls=[]
        def reply(r): calls.append(r); return httpx.Response(200,json={"data":[]})
        async with CachedClient(transport=httpx.MockTransport(reply)) as c:
            await c.get("https://test.example/api")
            await c.get("https://test.example/api")
        self.assertEqual(len(calls),1)
    async def test_blocked_http_is_cached_without_hiding_status(self):
        _cache.clear(); calls=[]
        def reply(r): calls.append(r); return httpx.Response(403)
        async with CachedClient(transport=httpx.MockTransport(reply)) as c:
            self.assertEqual((await c.get("https://test.example/blocked")).status_code,403)
            self.assertEqual((await c.get("https://test.example/blocked")).status_code,403)
        self.assertEqual(len(calls),1)
    async def test_defaults_no_international_calls(self):
        mock = AsyncMock(return_value=([],{"queries_used":["support"],"per_source":{}}))
        with patch("server.fetch_sources",mock), patch("server._snapshot",return_value="ok"), patch("server._jobspy_batch",AsyncMock(return_value=([],{}))) as boards:
            await server.search_all_jobs("support")
        self.assertEqual(mock.call_args.args[0],["arbeitsagentur","arbeitnow"])
        self.assertFalse(mock.call_args.kwargs["expand_query"])
        self.assertFalse(mock.call_args.kwargs["fetch_details"])
        self.assertEqual(boards.call_args.args[3],["indeed","linkedin","glassdoor","google"])
    async def test_international_is_separate(self):
        mock = AsyncMock(return_value=([],{"queries_used":["German support"],"per_source":{}}))
        with patch("server.fetch_sources",mock), patch("server._snapshot",return_value="ok"):
            await server.search_all_jobs("German support",market="international")
        self.assertNotIn("arbeitsagentur",mock.call_args.args[0])
    async def test_international_jobspy_requires_country(self):
        with self.assertRaises(ValueError):
            await server.search_all_jobs("support",market="international",include_jobspy=True)
    async def test_indeed_conflicting_parameters(self):
        with patch("server._run_scrape",return_value=pd.DataFrame()) as mock:
            jobs, meta = await server._jobspy_batch(["support"],"Berlin","germany",["indeed"],5,48,True,False)
        self.assertIsNone(mock.call_args.kwargs["hours_old"])
        self.assertEqual(meta["indeed"]["status"],"empty_or_blocked")
    async def test_ats_rejects_arbitrary_url(self):
        with self.assertRaises(ValueError): await employer_jobs("personio","127.0.0.1/admin")
    async def test_protocol_tools_and_call(self):
        async with Client(server.mcp) as client:
            tools = await client.list_tools()
            self.assertEqual(len(tools),12)
            self.assertTrue(all(t.annotations.readOnlyHint for t in tools if t.name!='record_browser_check'))
            self.assertFalse(next(t for t in tools if t.name=='record_browser_check').annotations.readOnlyHint)
            result = await client.call_tool("list_job_sources",{})
            self.assertFalse(result.is_error)

if __name__ == "__main__": unittest.main()
