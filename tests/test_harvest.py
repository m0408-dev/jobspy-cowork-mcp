import asyncio
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from fastmcp import Client
from harvest_store import HarvestStore
from harvest_engine import HarvestEngine,plan
from public_crawler import structured_jobs,xml_content,employer_from_url,public_url

JOB={'source':'arbeitnow','title':'IT Support','company':'Example','job_url':'https://example.com/jobs/1','description':'Technical customer service in Berlin'}

class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=HarvestStore(Path(self.tmp.name)/'jobs.sqlite')
        self.config={'queries':['IT Support'],'market':'germany','sources':['arbeitnow'],'employers':[],
                     'location':'Germany','country':'germany','page_limit':1}
        self.rid=self.store.create(self.config,plan(self.config))
    def tearDown(self):self.tmp.cleanup()
    def commit(self,more=False):
        task=self.store.claim();payload={**task['payload'],'cursor':1}
        self.store.finish(task,[JOB],payload,more=more)
        return task
    def test_page_limit_resume_and_dedup(self):
        self.commit(True)
        self.assertEqual(self.store.status(self.rid)['tasks'],{'limited':1})
        self.store.resume(self.rid,2)
        with self.store.connect() as db:db.execute('UPDATE tasks SET due=0')
        task=self.store.claim();self.assertEqual(task['payload']['cursor'],1)
        self.store.finish(task,[JOB],task['payload'])
        self.assertEqual(self.store.status(self.rid)['stored_jobs'],1)
        self.assertEqual(self.store.status(self.rid)['status'],'finished')
    def test_crash_lease_recovery_and_stale_commit(self):
        old=self.store.claim(lease_seconds=-1)
        reopened=HarvestStore(self.store.path);new=reopened.claim()
        self.assertEqual(old['id'],new['id'])
        self.assertFalse(reopened.finish(old,[JOB],old['payload']))
        self.assertTrue(reopened.finish(new,[JOB],new['payload']))
    def test_transient_retry_and_no_false_empty_success(self):
        task=self.store.claim()
        self.store.finish(task,[],task['payload'],error='HTTP 503')
        self.assertEqual(self.store.status(self.rid)['tasks'],{'queued':1})
        self.assertIsNone(self.store.claim())
        with self.store.connect() as db:db.execute('UPDATE tasks SET due=0')
        task=self.store.claim();self.store.finish(task,[],task['payload'])
        self.assertEqual(self.store.status(self.rid)['tasks'],{'needs_recovery':1})
    def test_partial_error_keeps_jobs(self):
        task=self.store.claim();self.store.finish(task,[JOB],task['payload'],error='HTTP 403')
        self.assertEqual(self.store.status(self.rid)['stored_jobs'],1)
        self.assertEqual(self.store.status(self.rid)['tasks'],{'needs_recovery':1})
    def test_query_and_detail_and_persistence(self):
        self.commit()
        reopened=HarvestStore(self.store.path)
        self.assertEqual(reopened.find(self.rid)['matching_jobs'],1)
        self.assertEqual(reopened.find(self.rid,'customer Berlin')['matching_jobs'],1)
        self.assertEqual(reopened.find(self.rid,'accountant')['matching_jobs'],0)
        self.assertEqual(reopened.find(self.rid,'')['matching_jobs'],1)
        self.assertEqual(reopened.find(self.rid,'" OR *')['matching_jobs'],0)
        job_id=reopened.find(self.rid)['jobs'][0]['id']
        self.assertEqual(reopened.read(self.rid,job_id,0,9)['next_text_offset'],9)
        with self.assertRaises(ValueError):reopened.read('unknown',job_id)
    def test_unfiltered_planning_does_not_multiply_feeds(self):
        config={**self.config,'sources':['arbeitsagentur','arbeitnow'],'queries':['IT Support','helpdesk']}
        self.assertEqual(len(plan(config)),3)
        config['queries']=[]
        self.assertEqual(len(plan(config)),2)
    def test_duplicate_start_reuses_active_run(self):
        self.assertEqual(self.store.create(self.config,plan(self.config)),self.rid)
    def test_pause_invalidates_lease_and_resumes(self):
        task=self.store.claim();self.store.pause(self.rid)
        self.assertEqual(self.store.status(self.rid)['status'],'paused')
        self.assertIsNone(self.store.claim())
        self.assertFalse(self.store.finish(task,[JOB],task['payload']))
        self.store.resume(self.rid)
        self.assertIsNotNone(self.store.claim())
    def test_repeated_pages_terminate(self):
        self.commit(True);self.store.resume(self.rid,5)
        with self.store.connect() as db:db.execute('UPDATE tasks SET due=0')
        task=self.store.claim();self.store.finish(task,[JOB],task['payload'],more=True)
        self.assertEqual(self.store.status(self.rid)['tasks'],{'needs_recovery':1})
    def test_recovery_import_is_idempotent(self):
        task=self.store.claim();self.store.finish(task,[],task['payload'],error='HTTP 403')
        for _ in range(2):self.store.record(self.rid,task['id'],[JOB],'checked',{'text':'Observed listing'})
        self.assertEqual(self.store.status(self.rid)['stored_jobs'],1)
    def test_sources_isolate_work_and_board_memory(self):
        config={**self.config,'sources':['indeed','linkedin','arbeitnow']}
        self.store.create(config,plan(config))
        claimed=[self.store.claim() for _ in range(4)]
        self.assertEqual(sum(t is not None and t['kind']=='board' for t in claimed),1)

class ParserTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_urls_rejected(self):
        for url in ['http://127.0.0.1/jobs','http://169.254.169.254/','http://[::1]/','file:///etc/passwd','https://user:pass@example.com']:
            with self.assertRaises(ValueError):await public_url(url)
    async def test_structured_graph_and_xml(self):
        html='<script type="application/ld+json">'+json.dumps({'@graph':[{'@type':'JobPosting','title':'Nurse','description':'<p>Care work</p>','hiringOrganization':{'name':'Hospital'},'url':'/jobs/2'}]})+'</script>'
        jobs,_=structured_jobs(html,'https://example.com/jobs','test')
        self.assertEqual(jobs[0]['job_url'],'https://example.com/jobs/2')
        self.assertEqual(jobs[0]['description'],'Care work')
        jobs,links=xml_content('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://example.com/jobs/3</loc></url></urlset>','https://example.com','test')
        self.assertEqual(len(links),1)
        self.assertEqual(employer_from_url('https://jobs.lever.co/example/abc'),('lever','example','global'))

class EngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_queryless_aa_omits_empty_parameter(self):
        import httpx
        from sources import fetch_arbeitsagentur
        requests=[]
        def handle(request):
            requests.append(request)
            return httpx.Response(200,json={'ergebnisliste':[],'maxErgebnisse':0})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            jobs,meta=await fetch_arbeitsagentur(client,[''],'Germany',100,0,max_pages=1,fetch_details=False)
        self.assertNotIn('was',requests[0].url.params)
        self.assertEqual(meta['coverage'],'query_exhausted')

    async def test_automatic_fallback_before_ai_handoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=HarvestStore(Path(tmp)/'jobs.sqlite')
            config={'queries':['x'],'market':'germany','sources':['arbeitsagentur'],'employers':[],
                     'location':'Germany','country':'germany','page_limit':5}
            rid=store.create(config,plan(config))
            async def fetch(task):return [],task['payload'],False,'HTTP 403',False
            engine=HarvestEngine(store,fetch)
            await engine.tick()
            self.assertEqual(store.status(rid)['tasks'],{'queued':1})
            rows,_=store.task_page(rid)
            self.assertEqual(json.loads(rows[0]['payload'])['fallback'],'public')

    async def test_mcp_contract_fast_return_compact_schema(self):
        import v4_server as server
        with tempfile.TemporaryDirectory() as tmp:
            store=HarvestStore(Path(tmp)/'jobs.sqlite')
            async def fetch(task):
                await asyncio.sleep(.1)
                return [JOB],task['payload'],False,None,True
            engine=HarvestEngine(store,fetch)
            with patch.object(server,'STORE',store),patch.object(server,'ENGINE',engine):
                async with Client(server.mcp) as client:
                    tools=await client.list_tools()
                    self.assertEqual(len(tools),9)
                    self.assertLess(len(json.dumps([t.model_dump() for t in tools])),20000)
                    before=time.monotonic()
                    result=await client.call_tool('start_job_search',{'queries':[],'sources':['arbeitnow']})
                    self.assertLess(time.monotonic()-before,1)
                    data=result.structured_content
                    self.assertIn('search_id',data)
                    for _ in range(30):
                        await asyncio.sleep(.05)
                        if store.status(data['search_id'])['status']=='finished':break
                    page=await client.call_tool('find_jobs',{'search_id':data['search_id']})
                    self.assertEqual(page.structured_content['matching_jobs'],1)
                    self.assertLess(len(json.dumps(page.structured_content)),2000)
                    self.assertNotIn('description',page.structured_content['jobs'][0])
