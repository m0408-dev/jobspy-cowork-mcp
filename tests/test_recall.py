import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import httpx
import pandas as pd
import server
import sources
from linkedin_adapter import parse_page
from results import ResultStore

class RecallTests(unittest.IsolatedAsyncioTestCase):
    async def test_aa_bad_second_page_retains_first_and_other_query(self):
        def reply(req):
            term, page = req.url.params['was'], int(req.url.params['page'])
            if term == 'a' and page == 2:
                return httpx.Response(200, json={'unexpected': []})
            return httpx.Response(200, json={'maxErgebnisse': 300, 'ergebnisliste': [
                {'stellenangebotsTitel':'Role', 'referenznummer':f'{term}-{page}-{i}'} for i in range(100)]})
        async with httpx.AsyncClient(transport=httpx.MockTransport(reply)) as client:
            jobs, meta = await sources.fetch_arbeitsagentur(client,['a','b'],None,400,0,fetch_details=False,max_pages=2)
        self.assertEqual(len(jobs),300)
        self.assertEqual(meta['failed_queries'],['a'])
        self.assertEqual(meta['next_source_offsets']['a'],1)
        self.assertEqual(meta['coverage'],'partial_error')

    def test_aa_explicit_empty_but_not_unknown(self):
        self.assertEqual(sources._aa_records({'maxErgebnisse':0}),[])
        with self.assertRaises(ValueError): sources._aa_records({'maxErgebnisse':30})

    def test_linkedin_exact_cursor_counts_raw_cards_not_parsed_total(self):
        html=''.join(f'<div class="base-search-card"><a class="base-card__full-link" href="https://www.linkedin.com/jobs/view/role-{i}"></a><h3 class="base-search-card__title">Role {i}</h3><h4 class="base-search-card__subtitle">Company</h4></div>' for i in range(25))
        page = parse_page(html,25)
        self.assertEqual(len(page['jobs']),25)
        self.assertEqual(page['upstream']['next_offset'],50)
        self.assertEqual(parse_page(html,50)['upstream']['next_offset'],75)
        self.assertEqual(parse_page('Sign in',0)['upstream']['status'],'empty_or_blocked')

    async def test_linkedin_short_public_pages_continue_without_skips(self):
        offsets=[]
        def scrape(**kw):
            offset=kw['offset']; offsets.append(offset)
            frame=pd.DataFrame([{'site':'linkedin','title':'Role','job_url':f'https://example.com/{i}'} for i in range(offset,offset+10)])
            frame.attrs['upstream']={'next_offset':offset+10,'status':'page_received','errors':[]}
            return frame
        with patch('server._run_scrape',side_effect=scrape):
            jobs, meta=await server._jobspy_batch(['a'],'Germany','germany',['linkedin'],100,24,True,False,max_pages=4)
        self.assertEqual(offsets,[0,10,20,30])
        self.assertEqual(len(jobs),40)
        self.assertEqual(meta['linkedin']['next_source_offsets']['a'],40)

    def test_compact_paging_preserves_errors_and_exact_next_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ResultStore(str(Path(tmp)/'x.db'))
            rid=store.save([{'title':'x'*1000,'job_url':f'https://x.test/{i}'} for i in range(140)],
                {'per_source':{'linkedin':{'scanned':140,'errors':['Timeout'],
                'query_states':{'a':{'status':'time_budget_deferred'}}}},'large':'x'*9000})
            offset, ids=0,[]
            while offset is not None:
                raw=store.page(rid,offset,100,max_chars=8000)
                self.assertLessEqual(len(raw),8000)
                page=json.loads(raw)
                self.assertEqual(page['retrieval_summary']['linkedin']['deferred_queries'],1)
                ids.extend(j['id'] for j in page['jobs'])
                offset=page['next_offset']
                if offset is not None:
                    self.assertEqual(page['next_page_call']['arguments']['offset'],offset)
            self.assertEqual(len(set(ids)),140)
