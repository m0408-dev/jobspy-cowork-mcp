import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock
from fastmcp import Client
import server
from catalog import select_sources, source_tasks, coverage, CATALOG

class CatalogTests(unittest.IsolatedAsyncioTestCase):
    def test_market_partitions_and_alias(self):
        de={r['id'] for r in select_sources('germany')}
        international={r['id'] for r in select_sources('international')}
        world={r['id'] for r in select_sources('worldwide')}
        self.assertEqual(world,de|international)
        self.assertEqual(len(world),len(CATALOG['sources'])-1)
        self.assertNotIn('bayt',de)
        self.assertNotIn('arbeitsagentur',international)
        self.assertIn('bayt',world)
        self.assertTrue({'linkedin','indeed'}.issubset(de&international))

    def test_every_query_every_source_and_no_false_attempt(self):
        meta={'market':'worldwide','catalog_scope':'all_selected_market_sources','queries_used':['Koch','看護師']}
        meta['_browser_tasks']=source_tasks(meta)
        self.assertEqual(len(meta['_browser_tasks']),286)
        self.assertEqual(len({t['id'] for t in meta['_browser_tasks']}),286)
        report=coverage(meta)
        self.assertEqual(report['status_counts']['not_attempted'],143)
        self.assertFalse(report['all_selected_sources_attempted'])
        self.assertFalse(report['exhaustive'])

    async def test_protocol_full_queue_pagination_and_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            old=server.STORE.path
            from results import ResultStore
            store=ResultStore(str(Path(tmp)/'test.sqlite'))
            server.STORE.path=store.path
            try:
                async with Client(server.mcp) as client:
                    async def call(name,**kw):
                        return json.loads((await client.call_tool(name,kw)).content[0].text)
                    with patch('server.fetch_sources',AsyncMock(return_value=([],{'queries_used':['Koch'],'per_source':{}}))):
                        result=await call('search_all_jobs',search_term='Koch',market='worldwide',include_jobspy=False)
                    rid=result['result_id']
                    ids=[]; offset=0
                    while offset is not None:
                        page=await call('get_browser_tasks',result_id=rid,offset=offset,page_size=10)
                        ids.extend(t['id'] for t in page['tasks'])
                        offset=page['next_offset']
                    self.assertEqual(len(ids),143)
                    self.assertEqual(len(set(ids)),143)
                    await call('record_browser_check',result_id=rid,task_id='0',outcome='checked_no_results',
                        browser='test fixture',visited_urls=['https://www.arbeitsagentur.de/jobsuche/'],
                        evidence='Test fixture: queried the source and inspected the empty result page.')
                    pending=await call('get_browser_tasks',result_id=rid,pending_only=True)
                    self.assertNotIn('0',[t['id'] for t in pending['tasks']])
                    audit=await call('get_search_coverage',result_id=rid)
                    self.assertEqual(audit['status_counts']['documented_scope_checked'],1)
                    self.assertEqual(audit['status_counts']['not_attempted'],142)
                    self.assertFalse(audit['all_documented_scopes_checked'])
                    # Still stored, not just a process-local iterator.
                    self.assertEqual(len(store.load(rid)['meta']['_browser_tasks']),143)
            finally:
                server.STORE.path=old

    def test_catalog_pages_and_size(self):
        rows=[]; offset=0
        while offset is not None:
            raw=server.list_job_sources(market='worldwide',offset=offset)
            self.assertLess(len(raw),12000)
            page=json.loads(raw)
            rows.extend(page['sources_page']); offset=page['next_offset']
        self.assertEqual(len(rows),143)

    def test_partial_remains_open(self):
        meta={'market':'germany','catalog_scope':'all_selected_market_sources','queries_used':['Retail']}
        meta['_browser_tasks']=source_tasks(meta)
        meta['_browser_tasks'][0].update(status='blocked',checked_at=1)
        report=coverage(meta)
        self.assertEqual(report['status_counts']['attempted_pending_browser'],1)
        self.assertFalse(report['all_documented_scopes_checked'])
