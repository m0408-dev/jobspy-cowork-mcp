import asyncio
import os
from pathlib import Path
import sqlite3
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch, AsyncMock
from datetime import datetime, timezone
import httpx
from fastmcp import Client
from access import Credentials
from harvest_store import HarvestStore
from quality import evidence
from discovery import employer_page

class QualityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=HarvestStore(Path(self.tmp.name)/'db')
    def tearDown(self):self.tmp.cleanup()
    def test_v4_database_migration_preserves_owner_and_ids(self):
        path=Path(self.tmp.name)/'old.sqlite'
        db=sqlite3.connect(path)
        db.execute('CREATE TABLE runs(id TEXT PRIMARY KEY,created REAL NOT NULL,config TEXT NOT NULL)')
        db.execute('INSERT INTO runs VALUES (?,?,?)',('existing',0,'{"queries":[]}'))
        db.commit();db.close()
        migrated=HarvestStore(path)
        self.assertEqual(migrated.run('existing')['owner'],'owner')
        migrated.authorize('existing','owner')
        with self.assertRaises(ValueError):migrated.authorize('existing','alice')

    def test_cross_sector_recall_and_strict_opt_in(self):
        cases=[('IT Support remote','Support specialist'),('Pflege Teilzeit','Pflegefachkraft'),
               ('Finance remote','Finance analyst'),('Elektriker Außendienst','Elektriker'),
               ('Marketing Homeoffice','Marketing Manager'),('Logistik Schicht','Logistik Mitarbeiter')]
        for query,title in cases:
            rid=self.store.create({'queries':[query]},[])
            with self.store.connect() as db:self.store._save_jobs(db,rid,[{'title':title,'job_url':'https://example.com/'+str(len(query))}])
            self.assertEqual(self.store.find(rid)['matching_jobs'],1,query)
            self.assertEqual(self.store.find(rid,match_mode='all_words')['matching_jobs'],0,query)
            self.assertIn('remote_evidence',self.store.find(rid)['jobs'][0])
    def test_owner_isolation_and_updated_description(self):
        a=self.store.create({'queries':[]},[],owner='alice')
        b=self.store.create({'queries':[]},[],owner='bob')
        job={'title':'Nurse','company':'Hospital','job_url':'https://example.com/1','description':'old long description'}
        with self.store.connect() as db:
            self.store._save_jobs(db,a,[job])
            self.store._save_jobs(db,b,[{**job,'description':'private recovery'}])
            self.store._save_jobs(db,a,[{**job,'description':'new'}])
        aid=self.store.find(a)['jobs'][0]['id'];bid=self.store.find(b)['jobs'][0]['id']
        self.assertNotEqual(aid,bid)
        self.assertEqual(self.store.read(a,aid)['description'],'new')
        self.assertEqual(self.store.read(b,bid)['description'],'private recovery')
        with self.assertRaises(ValueError):self.store.authorize(a,'bob')
        with self.assertRaises(ValueError):self.store.read(b,aid)
    def test_evidence_never_claims_live_availability(self):
        e=evidence({'title':'Accountant','is_remote':True,'valid_through':'2020-01-01','date_posted':'invalid'})
        self.assertEqual(e['expiry_evidence'],'expired')
        self.assertEqual(e['remote_evidence'],'source_declared_remote')
        self.assertEqual(e['availability'],'not_reverified')
        self.assertIsNone(e['posted_age_days'])
    def test_duplicate_hints_keep_distinct_vacancies(self):
        rid=self.store.create({'queries':[]},[])
        with self.store.connect() as db:self.store._save_jobs(db,rid,[{'title':'Nurse','company':'Hospital','job_url':f'https://example.com/{i}'} for i in range(2)])
        cards=self.store.find(rid)['jobs']
        self.assertEqual(len(cards),2)
        self.assertEqual(cards[0]['possible_duplicate_group'],cards[1]['possible_duplicate_group'])
    def test_active_reuse_scoped_and_quota(self):
        task={'source':'fixture','kind':'api','payload':{}}
        a=self.store.create({'queries':[]},[task],owner='alice')
        self.assertEqual(a,self.store.create({'queries':[]},[task],owner='alice'))
        self.assertNotEqual(a,self.store.create({'queries':[]},[task],owner='bob'))
        paused=self.store.create({'queries':['paused']},[task],owner='alice')
        self.store.pause(paused)
        with patch.dict(os.environ,MAX_ACTIVE_SEARCHES_PER_USER='1'):
            with self.assertRaises(ValueError):self.store.resume(paused)
            with self.assertRaises(ValueError):self.store.create({'queries':['new']},[task],owner='alice')

class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_ashby_visibility_and_remote_fields(self):
        response=httpx.Response(200,json={'jobs':[
            {'title':'Finance','jobUrl':'https://jobs.ashbyhq.com/x/1','isListed':True,'isRemote':True,'descriptionPlain':'Finance work'},
            {'title':'Unlisted','jobUrl':'https://jobs.ashbyhq.com/x/2','isListed':False}]},request=httpx.Request('GET','https://example.com'))
        with patch('discovery.CachedClient') as cls:
            cls.return_value.__aenter__.return_value.get=AsyncMock(return_value=response)
            jobs,cursor=await employer_page('ashby','Example')
            self.assertEqual(len(jobs),1)
            self.assertTrue(jobs[0]['is_remote']);self.assertIsNone(cursor)
            response._content=b'{}'
            with self.assertRaises(ValueError):await employer_page('ashby','Example')

    async def test_smartrecruiters_cursor_and_schema(self):
        response=httpx.Response(200,json={'totalFound':2,'content':[{'id':'1','name':'Nurse','location':{'remote':False}}]},request=httpx.Request('GET','https://example.com'))
        with patch('discovery.CachedClient') as cls:
            client=cls.return_value.__aenter__.return_value
            client.get=AsyncMock(return_value=response)
            jobs,cursor=await employer_page('smartrecruiters','Example')
            self.assertEqual(cursor,1);self.assertFalse(jobs[0]['is_remote'])
            jobs,cursor=await employer_page('smartrecruiters','Example',cursor=1)
            self.assertIsNone(cursor)
            response._content=b'{}'
            with self.assertRaises(ValueError):await employer_page('smartrecruiters','Example')

class HttpIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_users_real_http_revocation_and_private_route(self):
        with tempfile.TemporaryDirectory() as tmp:
            auth=Credentials(Path(tmp)/'access.sqlite')
            alice=auth.issue('alice');bob=auth.issue('bob')
            store=HarvestStore(Path(tmp)/'harvest.sqlite')
            rid=store.create({'queries':[]},[],owner='alice')
            with store.connect() as db:store._save_jobs(db,rid,[{'title':'Private fixture','job_url':'https://example.com/private'}])
            jid=store.find(rid)['jobs'][0]['id']
            with socket.socket() as sock:
                sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
            env={**os.environ,'HARVEST_DB':store.path,'AUTH_DB':auth.path,'PORT':str(port),'HOST':'127.0.0.1','MCP_HTTP_PATH':'/private-test-owner/','MCP_AUTH_TOKEN':'','RATE_LIMIT_PER_MIN':'1000'}
            proc=subprocess.Popen([sys.executable,'v4_server.py'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            url=f'http://127.0.0.1:{port}'
            try:
                async with httpx.AsyncClient() as http:
                    for _ in range(150):
                        try:
                            if (await http.get(url+'/health')).status_code==200:break
                        except httpx.TransportError:pass
                        await asyncio.sleep(.1)
                    else:self.fail('HTTP startup failed')
                    self.assertEqual((await http.post(url+'/mcp/',json={})).status_code,401)
                async with Client(url+'/mcp/',auth=alice) as a, Client(url+'/mcp/',auth=bob) as b:
                    self.assertEqual((await a.call_tool('find_jobs',{'search_id':rid})).structured_content['matching_jobs'],1)
                    requests=[('find_jobs',{}),('get_search_status',{}),('read_job',{'job_id':jid}),('pause_search',{}),('resume_search',{}),('get_recovery_tasks',{}),('record_recovery',{'task_id':'x','outcome':'blocked','evidence':'This public page was inaccessible during the inspection.','visited_urls':['https://example.com']})]
                    for name,extra in requests:
                        result=await b.call_tool(name,{'search_id':rid,**extra},raise_on_error=False)
                        self.assertTrue(result.is_error,name)
                auth.revoke('alice')
                async with httpx.AsyncClient() as http:
                    self.assertEqual((await http.post(url+'/mcp/',json={},headers={'Authorization':'Bearer '+alice})).status_code,401)
                async with Client(url+'/private-test-owner/') as owner:
                    self.assertEqual(len(await owner.list_tools()),9)
                    self.assertTrue((await owner.call_tool('find_jobs',{'search_id':rid},raise_on_error=False)).is_error)
            finally:
                proc.terminate();proc.wait(timeout=15)
