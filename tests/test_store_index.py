import sqlite3
import tempfile
import unittest
from pathlib import Path
from harvest_store import HarvestStore


class TextLocatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'store.sqlite'
        self.store=HarvestStore(self.path)
        self.run=self.store.create({'queries':[]},[])

    def tearDown(self):
        self.tmp.cleanup()

    def save(self, jobs):
        with self.store.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self.store._save_jobs(db,self.run,jobs)

    def test_indexed_updates_preserve_one_search_document(self):
        job={'title':'Administrator','job_url':'https://example.org/jobs/1','description':'oldunique'}
        self.save([job])
        self.save([{**job,'description':'newunique'}])
        self.assertEqual(self.store.find(self.run,'newunique')['matching_jobs'],1)
        self.assertEqual(self.store.find(self.run,'oldunique')['matching_jobs'],0)
        with self.store.connect() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM job_text').fetchone()[0],1)
            plan=db.execute('EXPLAIN QUERY PLAN SELECT fts_rowid FROM job_text_rows WHERE id=?',('x',)).fetchone()[3]
            self.assertIn('SEARCH',plan)
            plan=db.execute('EXPLAIN QUERY PLAN DELETE FROM job_text WHERE rowid=?',(1,)).fetchone()[3]
            self.assertIn('=',plan)

    def test_legacy_rowids_and_rollback_changes_are_reconciled(self):
        job={'title':'Administrator','job_url':'https://example.org/jobs/1','description':'before'}
        self.save([job])
        with self.store.connect() as db:
            row=db.execute('SELECT * FROM job_text').fetchone()
            db.execute('DELETE FROM job_text')
            db.execute('INSERT INTO job_text(rowid,id,title,company,location,description) VALUES (?,?,?,?,?,?)',(999,*tuple(row)))
            db.execute('DROP TABLE job_text_rows')
        reopened=HarvestStore(self.path)
        with reopened.connect() as db:
            self.assertEqual(db.execute('SELECT fts_rowid FROM job_text_rows').fetchone()[0],999)
        self.save([{**job,'description':'after'}])
        self.assertEqual(reopened.find(self.run,'after')['matching_jobs'],1)
        self.assertEqual(reopened.status(self.run)['stored_jobs'],1)

    def test_atomic_rollback_includes_locator_and_text(self):
        with self.assertRaises(RuntimeError):
            with self.store.connect() as db:
                db.execute('BEGIN IMMEDIATE')
                self.store._save_jobs(db,self.run,[{'title':'Test','job_url':'https://example.org/jobs/2'}])
                raise RuntimeError('interrupted transaction')
        with self.store.connect() as db:
            for table in ['jobs','job_text','job_text_rows','run_jobs']:
                self.assertEqual(db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)
