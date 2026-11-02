"""Regression fixtures for dated feeds and bounded full-text retrieval."""
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
import httpx
import ssl

from harvest_store import HarvestStore
from quality import evidence
from sources import safe_error


class QueryRegressions(unittest.TestCase):
    def test_quality_migration_pages_without_losing_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'db'
            store = HarvestStore(path)
            run = store.create({'queries': []}, [])
            with store.connect() as db:
                store._save_jobs(db, run, [
                    {'title': 'Nurse', 'description': 'Clinical work',
                     'job_url': f'https://example.com/migration/{i}'} for i in range(305)])
                db.execute('DELETE FROM job_quality')
            reopened = HarvestStore(path)
            self.assertEqual(reopened.find(run)['matching_jobs'], 305)
            self.assertEqual(reopened.status(run)['missing_descriptions'], 0)

    def test_certificate_failure_is_actionable_without_leaking_exception(self):
        exc = httpx.ConnectError('secret URL')
        exc.__cause__ = ssl.SSLCertVerificationError('CERTIFICATE_VERIFY_FAILED secret URL')
        self.assertEqual(safe_error(exc), 'tls_certificate_verification_failed')
        with tempfile.TemporaryDirectory() as tmp:
            store = HarvestStore(Path(tmp) / 'db')
            run = store.create({'queries': []}, [{'source': 'fixture', 'kind': 'api', 'payload': {}}])
            task = store.claim()
            store.finish(task, [], task['payload'], error=safe_error(exc))
            self.assertEqual(store.status(run)['tasks'], {'needs_recovery': 1})

    def test_epoch_dates_seconds_and_milliseconds(self):
        now = datetime(2026, 11, 2, tzinfo=timezone.utc)
        timestamp = datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp()
        for value in (timestamp, str(int(timestamp)), timestamp * 1000):
            with self.subTest(value=value):
                self.assertEqual(evidence({'date_posted': value}, now)['posted_age_days'], 1)
        for value in (None, True, 'invalid', float('inf'), '99999999999999999999'):
            with self.subTest(value=value):
                self.assertIsNone(evidence({'date_posted': value}, now)['posted_age_days'])

    def test_fts_query_has_bounded_sql_work_and_preserves_all_matches(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = HarvestStore(Path(tmp) / 'db')
            run = store.create({'queries': ['remote'], 'market': 'germany'}, [])
            with store.connect() as db:
                store._save_jobs(db, run, [
                    {'title': 'Remote nurse', 'job_url': f'https://example.com/{i}',
                     'description': 'German remote work'} for i in range(1500)])
            # SQL VM steps make this independent of CPU speed. The former join
            # scanned the complete FTS match set once per record.
            from contextlib import contextmanager
            original = store.connect
            ticks = [0]
            statements = []
            @contextmanager
            def bounded():
                with original() as db:
                    def progress():
                        ticks[0] += 1
                        return int(ticks[0] > 1000)
                    db.set_progress_handler(progress, 1000)
                    db.set_trace_callback(statements.append)
                    yield db
            store.connect = bounded
            result = store.find(run, 'remote', limit=10)
            self.assertEqual(result['matching_jobs'], 1500)
            self.assertEqual(len(result['jobs']), 10)
            self.assertEqual(result['next_offset'], 10)
            self.assertEqual(result['collection_market'], 'germany')
            self.assertIn('not job eligibility', result['geography_scope'])
            self.assertLess(ticks[0], 1000)
            matching = [sql for sql in statements if 'job_text MATCH' in sql]
            self.assertTrue(matching)
            self.assertTrue(all('job_text.rowid=x.fts_rowid' in sql for sql in matching))


if __name__ == '__main__':
    unittest.main()
