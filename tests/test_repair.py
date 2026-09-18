"""Regression cases from the September 18 coverage audit."""
import datetime
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import httpx
import pandas as pd
import server
import sources
from browser_handoff import make_tasks
from catalog import source_tasks
from results import ResultStore, encode


class RepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_snapshot_work_off_event_loop(self):
        loop_thread=threading.get_ident()
        seen=[]
        def snapshot(*args):
            seen.append(threading.get_ident())
            return 'ok'
        api=AsyncMock(return_value=([],{'queries_used':['a'],'per_source':{}}))
        with patch('server.fetch_sources',api),patch('server._snapshot',snapshot),patch('server._jobspy_batch',AsyncMock(return_value=([],{}))):
            await server.search_all_jobs('a',include_jobspy=False)
            await server.search_german_jobs('a')
            await server.search_jobs('a')
        self.assertEqual(len(seen),3)
        self.assertTrue(all(t!=loop_thread for t in seen))

    async def test_jobspy_depth_and_breadth(self):
        calls = []
        def scrape(**kw):
            calls.append((kw['search_term'], kw['offset'], kw['results_wanted']))
            return pd.DataFrame([{'title':'Role', 'job_url':f"https://jobs.example/{kw['search_term']}/{i}"}
                for i in range(kw['offset'], kw['offset']+kw['results_wanted'])])
        with patch('server._run_scrape', side_effect=scrape):
            jobs, meta = await server._jobspy_batch(['a','b'],'Germany','germany',['indeed'],250,0,True,False,max_pages=5)
        self.assertEqual(len(jobs),500)
        self.assertEqual(calls,[('a',0,100),('b',0,100),('a',100,100),('b',100,100),('a',200,50),('b',200,50)])
        self.assertEqual(meta['indeed']['next_source_offsets'],{'a':250,'b':250})

    async def test_jobspy_repeated_page_stops_with_cursor(self):
        frame=pd.DataFrame([{'title':'Role','job_url':f'https://jobs.example/{i}'} for i in range(100)])
        with patch('server._run_scrape',return_value=frame) as mock:
            jobs,meta=await server._jobspy_batch(['a'],'Germany','germany',['indeed'],1000,0,False,False,max_pages=10)
        self.assertEqual(mock.call_count,2)
        self.assertEqual(len(jobs),100)
        self.assertEqual(meta['indeed']['query_states']['a']['status'],'repeated_page')
        self.assertEqual(meta['indeed']['next_source_offsets']['a'],100)

    async def test_failed_source_records_unattempted_queries(self):
        with patch('server._run_scrape',side_effect=RuntimeError('fixture')) as mock:
            _,meta=await server._jobspy_batch(['a','b'],'Germany','germany',['linkedin'],100,0,False,False,max_pages=2)
        self.assertEqual(mock.call_count,1)
        self.assertEqual(meta['linkedin']['query_states']['b']['status'],'source_error_deferred')
        self.assertEqual(meta['linkedin']['next_source_offsets']['b'],0)

    async def test_broad_forwards_remote_limit_and_depth(self):
        api=AsyncMock(return_value=([],{'queries_used':['Retail'],'per_source':{}}))
        boards=AsyncMock(return_value=([],{}))
        with patch('server.fetch_sources',api),patch('server._jobspy_batch',boards),patch('server._snapshot',return_value='ok'):
            await server.search_all_jobs('Retail',remote_only=True,results_per_source=500,max_pages=6)
        self.assertEqual(boards.call_args.args[4],500)
        self.assertTrue(boards.call_args.args[6])
        self.assertEqual(boards.call_args.kwargs['max_pages'],6)

    async def test_himalayas_overlap_between_queries_is_not_repetition(self):
        calls=[]
        def reply(req):
            query=req.url.params['q']; page=int(req.url.params['page'])
            calls.append((query,page))
            identity='shared' if page==1 else query+'new' if page==2 else None
            return httpx.Response(200,json={'jobs':[] if identity is None else [{'title':identity,'guid':identity}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as c:
            jobs,meta=await sources.fetch_himalayas(c,['a','b'],None,100,3,max_pages=3)
        self.assertEqual(len(jobs),3)
        self.assertIn(('b',2),calls)
        self.assertEqual(meta['errors'],[])

    async def test_himalayas_real_repeat_detected(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,json={'jobs':[{'title':'x','guid':'1'}]}))) as c:
            jobs,meta=await sources.fetch_himalayas(c,['a'],None,100,0,max_pages=5)
        self.assertEqual(len(jobs),1)
        self.assertIn('repeated page',str(meta['errors']))

    def test_aa_v6_mapping_keeps_homeoffice_unverified(self):
        jobs=sources._aa_records({'ergebnisliste':[{'stellenangebotsTitel':'Role','firma':'Company',
            'referenznummer':'123','stellenlokationen':[{'adresse':{'ort':'Berlin'}}],
            'datumErsteVeroeffentlichung':'2026-09-15','arbeitszeitVollzeit':True,
            'homeofficemoeglich':True,'homeofficetyp':'NACH_VEREINBARUNG'}]})
        self.assertEqual(jobs[0]['company'],'Company')
        self.assertTrue(jobs[0]['job_url'].endswith('/123'))
        self.assertEqual(jobs[0]['job_type'],'fulltime')
        self.assertEqual(jobs[0]['location'],'Berlin')
        self.assertNotEqual(sources.remote_confidence(jobs[0]),'strict')
        self.assertTrue(sources._AA_BASE.endswith('/v6/jobs'))

    def test_aa_unknown_schema_is_not_zero_jobs(self):
        with self.assertRaises(ValueError): sources._aa_records({'unexpected':[]})

    def test_browser_breadth_first_all_pairs_retained(self):
        tasks=source_tasks({'market':'germany','catalog_scope':'all','queries_used':['a','b']})
        self.assertEqual(len(tasks),144)
        self.assertEqual(len({t['source'] for t in tasks[:72]}),72)
        self.assertEqual(len({(t['source'],t['query']) for t in tasks}),144)

    def test_browser_links_carry_requested_filters(self):
        tasks=make_tasks({'market':'germany','catalog_scope':'all','queries_used':['Operations'],
                         'search_location':'Germany','remote_boost':True,'days_old':3})
        link=next(t['url'] for t in tasks if t['source']=='linkedin')
        self.assertIn('f_WT=2',link)
        self.assertIn('f_TPR=r259200',link)
        self.assertIn('fromage=3',next(t['url'] for t in tasks if t['source']=='indeed'))

    def test_unicode_separators_roundtrip(self):
        value={'title':'Ä\u2028B\u2029C\u0085D'}
        raw=encode(value)
        self.assertEqual(len(raw.splitlines()),1)
        self.assertEqual(json.loads(raw),value)

    def test_later_pages_keep_incomplete_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ResultStore(str(Path(tmp)/'s.db'))
            meta={'market':'germany','catalog_scope':'all','queries_used':['a']}
            meta['_browser_tasks']=make_tasks(meta)
            rid=store.save([{'title':'a'},{'title':'b'}],meta)
            page=json.loads(store.page(rid,offset=1,page_size=1))
        self.assertFalse(page['search_status']['complete'])
        self.assertEqual(page['search_status']['sources_not_attempted'],72)
        self.assertEqual(page['search_status']['browser_pending'],72)

    def test_recency_labels_keep_older_unknown_and_future(self):
        today=datetime.datetime.now(datetime.timezone.utc).date()
        jobs=[{'title':'old','date_posted':str(today-datetime.timedelta(days=10))},
              {'title':'new','date_posted':str(today)}, {'title':'unknown'},
              {'title':'future','date_posted':str(today+datetime.timedelta(days=1))}]
        with tempfile.TemporaryDirectory() as tmp:
            store=ResultStore(str(Path(tmp)/'s.db'))
            with patch('server.STORE',store):
                result=json.loads(server._snapshot(jobs,{'days_old':3}))
        self.assertEqual(result['total_fetched'],4)
        self.assertEqual(set(result['coverage']['recency_counts']),
            {'older_than_requested','within_requested_window','unknown','future_date_unverified'})

if __name__=='__main__': unittest.main()
