import json
import tempfile
import unittest
from pathlib import Path
from results import ResultStore
from sources import remote_confidence


class ViewTests(unittest.TestCase):
    def test_optional_views_preserve_ids_and_all_cards(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = ResultStore(str(Path(tmp)/'test.db'))
            rid = store.save([{'title':'QA' if i % 2 else 'Office',
                               'remote_confidence':'unknown', 'job_url':f'https://example.com/{i}'}
                              for i in range(20)], {})
            first = json.loads(store.page(rid, page_size=3, text_query='QA', remote_labels=['unknown']))
            self.assertEqual(first['total_fetched'],20)
            self.assertEqual(first['total_in_view'],10)
            self.assertEqual([j['id'] for j in first['jobs']],['1','3','5'])
            self.assertEqual(first['next_page_call']['arguments']['text_query'],'QA')
            second = json.loads(store.page(rid,offset=first['next_offset'],page_size=3,text_query='QA',remote_labels=['unknown']))
            self.assertEqual([j['id'] for j in second['jobs']],['7','9','11'])
            self.assertEqual(json.loads(store.page(rid))['count'],20)

    def test_empty_view_is_not_empty_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ResultStore(str(Path(tmp)/'test.db'))
            rid=store.save([{'title':'Office','description':'Datenpflege in Teilzeit'}],{})
            page=json.loads(store.page(rid,text_query='QA'))
            self.assertEqual(page['total_in_view'],0)
            self.assertEqual(page['total_fetched'],1)
            self.assertIsNone(page['next_offset'])
            self.assertEqual(json.loads(store.page(rid,text_query='QA|teilzeit'))['count'],1)

    def test_remote_attendance_negation(self):
        self.assertEqual(remote_confidence({'description':'100% Remote ohne Präsenzpflicht.'}),'strict')
        self.assertEqual(remote_confidence({'description':'100% Remote mit Präsenzpflicht.'}),'mixed')
        self.assertEqual(remote_confidence({'description':'Fully remote, no mandatory office attendance.'}),'strict')

    def test_bounded_remote_days_are_not_full_remote(self):
        self.assertEqual(remote_confidence({'description':'Remote work: work remotely for up to 90 days per year.'}),'hybrid')
        self.assertEqual(remote_confidence({'description':'Homeoffice bis zu 90 Tage pro Jahr möglich.'}),'hybrid')

    def test_uncertainty_retained(self):
        self.assertEqual(remote_confidence({'title':'Office Assistant'}),'unknown')
        self.assertEqual(remote_confidence({'description':'Mobiles Arbeiten möglich.'}),'likely')
