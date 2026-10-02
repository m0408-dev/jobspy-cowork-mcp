import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
import pandas as pd
from fastmcp import Client
import server
from linkedin_adapter import detail_url, parse_detail, enrich_jobs
from results import ResultStore


class DetailsTests(unittest.IsolatedAsyncioTestCase):
    def jobs(self, count=3):
        return [dict(source='linkedin', title='Example', job_url=f'https://www.linkedin.com/jobs/view/{i}') for i in range(count)]

    def test_parser_and_url_allowlist(self):
        self.assertEqual(parse_detail('<div class="show-more-less-html__markup"><p>Hallo</p><p>Welt</p></div>')['description'], 'Hallo\nWelt')
        self.assertEqual(parse_detail('Please sign in')['detail_status'], 'unavailable_or_blocked')
        for url in ['http://www.linkedin.com/jobs/view/1', 'https://www.linkedin.com.evil.test/jobs/view/1',
                    'https://user@www.linkedin.com/jobs/view/1', 'https://127.0.0.1/jobs/view/1',
                    'https://www.linkedin.com/jobs/view/../secret']:
            self.assertIsNone(detail_url(url))

    async def test_rate_limit_keeps_all_cards_and_defers_rest(self):
        calls=[]
        def reply(req):
            calls.append(str(req.url))
            return httpx.Response(429)
        jobs=self.jobs()
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            counts=await enrich_jobs(client,jobs)
        self.assertEqual(len(calls),1)
        self.assertEqual(len(jobs),3)
        self.assertEqual(counts,{'blocked_or_error':1,'deferred':2})

    async def test_timeout_retains_cards(self):
        async def reply(req):
            await asyncio.sleep(.1)
            return httpx.Response(200)
        jobs=self.jobs()
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            await enrich_jobs(client,jobs,budget=.01)
        self.assertEqual(jobs[0]['detail_error'],'TimeoutError')
        self.assertEqual(jobs[1]['detail_status'],'deferred')

    async def test_search_flag_enriches_after_worker_returns(self):
        def scrape(**kw):
            self.assertFalse(kw['linkedin_fetch_description'])
            return pd.DataFrame([dict(site='linkedin',title='Example',job_url='https://www.linkedin.com/jobs/view/1')])
        async def enrich(client,jobs,budget=12):
            jobs[0].update(description='German documentation role',detail_status='loaded')
            return {'loaded':1}
        with patch('server._run_scrape',side_effect=scrape), patch('server.enrich_linkedin',side_effect=enrich) as enrich_mock:
            jobs,meta=await server._jobspy_batch(['x'],'Germany','germany',['linkedin'],1,0,True,True)
            self.assertEqual(jobs[0]['description'],'German documentation role')
            self.assertEqual(meta['linkedin']['detail_counts'],{'loaded':1})
            await server._jobspy_batch(['x'],'Germany','germany',['linkedin'],1,0,True,False)
            self.assertEqual(enrich_mock.call_count,1)

    async def test_shortlist_via_mcp_persists_body_without_refetch(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ResultStore(str(Path(tmp)/'snapshot.db'))
            jobs=self.jobs(1)
            jobs[0].update(detail_status='blocked_or_error',detail_error='HTTP 429')
            rid=store.save(jobs,{})
            original=server.STORE.path
            server.STORE.path=store.path
            calls=[]
            async def get(client,url,**kwargs):
                calls.append(url)
                return httpx.Response(200,text='<div class="show-more-less-html__markup">Vollständig remote. Deutsch.</div>')
            try:
                with patch('http_cache.CachedClient.get',new=get):
                    async with Client(server.mcp) as c:
                        for _ in range(2):
                            r=await c.call_tool('get_job_details',dict(result_id=rid,job_ids=['0'],fetch_missing=True))
                            body=json.loads(r.content[0].text)
                            self.assertEqual(body['jobs'][0]['detail_status'],'loaded')
                            self.assertIsNone(body['jobs'][0].get('detail_error'))
                            self.assertIn('Deutsch',body['jobs'][0]['description'])
                self.assertEqual(len(calls),1)
                self.assertEqual(json.loads(store.page(rid))['missing_descriptions'],0)
            finally:
                server.STORE.path=original
