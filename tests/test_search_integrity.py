import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock
import httpx
from bs4 import BeautifulSoup
from content_kind import classify
from harvest_store import HarvestStore
from harvest_engine import plan
from public_crawler import search_cards, discover_search, crawl_page
from search_routes import search_url
from sources import fetch_hackernews


class IntegrityTests(unittest.TestCase):
    def test_non_jobs_are_separate_and_raw_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=HarvestStore(Path(tmp)/'db');rid=s.create({'queries':[]},[])
            jobs=[{'title':'Location: Dubrovnik, Croatia','description':'Remote: Yes\nWilling to relocate: yes','job_url':'https://example.com/1'},
                  {'title':'Jobstash.xyz is a job aggregator with 17000+ vacancies','job_url':'https://example.com/2'},
                  {'title':'Accountant','description':'Work on our job aggregator. We are hiring an accountant.','job_url':'https://example.com/3'},
                  {'title':'Nurse','job_url':'https://example.com/4'}]
            with s.connect() as db:s._save_jobs(db,rid,jobs)
            self.assertEqual(s.status(rid)['stored_jobs'],4)
            self.assertEqual(s.find(rid)['matching_jobs'],2)
            self.assertEqual(s.find(rid,record_view='all')['matching_jobs'],4)
            self.assertEqual(s.find(rid,record_view='non_jobs')['matching_jobs'],2)
            self.assertEqual(s.find(rid,record_view='missing_descriptions')['matching_jobs'],1)
            self.assertEqual(HarvestStore(s.path).find(rid)['matching_jobs'],2)

    def test_routes_are_query_specific_and_generic(self):
        config={'market':'germany','queries':['Pflege','Buchhaltung'],'location':'Germany','country':'germany','page_limit':30,'sources':['stepstone','xing'],'employers':[]}
        tasks=plan(config)
        self.assertEqual(len(tasks),4)
        self.assertTrue(all(t['query'] and t['payload']['search_url'] for t in tasks))
        self.assertTrue(all(not t['payload']['discovery_only'] for t in tasks))
        self.assertIn('q=A%26B',search_url('https://www.adzuna.de/','A&B'))
        self.assertIsNone(search_url('https://example.com/','Nurse'))

    def test_cards_no_navigation_or_fabricated_text(self):
        soup=BeautifulSoup('<a href="/jobs/berlin-nurse-12345678"><h3>Nurse</h3></a><a href="/jobs/berlin-nurse-12345678">Mehr</a><a href="/jobs/nurse">All jobs</a>','html.parser')
        cards=search_cards(soup,'https://www.xing.com/jobs/nurse','xing')
        self.assertEqual(len(cards),1)
        self.assertEqual(cards[0]['title'],'Nurse')
        self.assertIsNone(cards[0]['description'])

    def test_safe_observed_forms_only(self):
        soup=BeautifulSoup('<form action="/search"><input type="search" name="q"></form><form method="post"><input type="search" name="q"></form><form action="https://other.test"><input name="q"></form>','html.parser')
        urls=discover_search(soup,'https://example.com',['Nurse','Mechanic'])
        self.assertEqual(urls,['https://example.com/search?q=Nurse','https://example.com/search?q=Mechanic'])

    def test_xing_overlay_anchor(self):
        soup=BeautifulSoup('<article><a href="/jobs/nurse-12345678" aria-label="Nurse"></a><h2>Nurse</h2><p class="item__Company">Hospital</p></article>','html.parser')
        cards=search_cards(soup,'https://www.xing.com/jobs/nurse','xing')
        self.assertEqual(cards[0]['title'],'Nurse')
        self.assertEqual(cards[0]['company'],'Hospital')

    def test_rollback_writer_changes_are_reclassified_on_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=HarvestStore(Path(tmp)/'db');rid=s.create({'queries':[]},[])
            with s.connect() as db:
                s._save_jobs(db,rid,[{'title':'Nurse','job_url':'https://example.com/1'}])
                db.execute("UPDATE jobs SET body=?,last_seen=last_seen+1",(json.dumps({'title':'Jobstash is a job aggregator with many vacancies','job_url':'https://example.com/1'}),))
            self.assertEqual(HarvestStore(s.path).find(rid,query='')['matching_jobs'],0)

    def test_required_terms_do_not_restrict_other_sectors(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=HarvestStore(Path(tmp)/'db');rid=s.create({'queries':[]},[])
            with s.connect() as db:s._save_jobs(db,rid,[{'title':'Remote nurse German','job_url':'https://example.com/1'}, {'title':'Remote engineer English','job_url':'https://example.com/2'}])
            self.assertEqual(s.find(rid,'remote',required_terms=['German'])['matching_jobs'],1)
            self.assertEqual(s.find(rid,'remote')['matching_jobs'],2)
            self.assertEqual(s.find(rid,'remote',required_terms=['" OR *'])['matching_jobs'],0)


class AsyncIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_adapter_negative_fixtures(self):
        def reply(r):
            if 'search_by_date' in r.url.path:return httpx.Response(200,json={'hits':[{'title':'Who is hiring?','objectID':'1'}]})
            return httpx.Response(200,json={'children':[{'id':2,'text':'Location: Dubrovnik<p>Willing to relocate: yes'},{'id':3,'text':'Jobstash.xyz is a job aggregator with 17000+ vacancies'},{'id':4,'text':'Example | Nurse | Remote<p>We are hiring nurses'}]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as c:
            jobs,_=await fetch_hackernews(c,[],None,100,0)
        self.assertEqual([classify(j)[0] for j in jobs],['job_seeker','directory_promotion','unclassified'])

    async def test_discovery_cannot_report_homepage_as_completed_search(self):
        policy=type('Policy',(),{'can_fetch':lambda *a:True,'crawl_delay':lambda *a:0,'site_maps':lambda *a:[]})()
        with patch('public_crawler.robot_policy',AsyncMock(return_value=policy)),patch('public_crawler.download',AsyncMock(return_value=('<html><a href="/jobs">Jobs</a></html>','https://example.com'))),patch('public_crawler.asyncio.sleep',AsyncMock()):
            jobs,payload,more=await crawl_page('fixture',{'url':'https://example.com','discovery_only':True,'queries':['Nurse']})
        self.assertFalse(more)
        self.assertEqual(payload['coverage'],'search_adapter_missing')
        self.assertEqual(payload['failed_urls'][0]['error'],'search_adapter_missing')

    async def test_search_preserves_page_and_detail_links(self):
        policy=type('Policy',(),{'can_fetch':lambda *a:True,'crawl_delay':lambda *a:0,'site_maps':lambda *a:[]})()
        html='<a href="/jobs/berlin-nurse-12345678">Nurse</a><a rel="next" href="/jobs/nurse?page=2">Next</a><a href="/jobs/accountant">Accountants</a>'
        with patch('public_crawler.robot_policy',AsyncMock(return_value=policy)),patch('public_crawler.download',AsyncMock(return_value=(html,'https://www.xing.com/jobs/nurse'))),patch('public_crawler.asyncio.sleep',AsyncMock()):
            jobs,payload,more=await crawl_page('xing',{'url':'https://www.xing.com/jobs','search_url':'https://www.xing.com/jobs/nurse','queries':['nurse']})
        self.assertTrue(more)
        self.assertEqual(len(jobs),1)
        self.assertEqual(len(payload['frontier']),2)
        self.assertFalse(any('accountant' in u for u in payload['frontier']))

if __name__=='__main__':unittest.main()
