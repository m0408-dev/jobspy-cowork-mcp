"""Durable page queue and searchable job records; no model calls or snapshot TTL."""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import time

from sources import _dedup_key


def dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str, separators=(',', ':'))


class HarvestStore:
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, created REAL NOT NULL, config TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL, source TEXT NOT NULL,
                    query TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued', pages INTEGER NOT NULL DEFAULT 0,
                    received INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
                    due REAL NOT NULL DEFAULT 0, lease TEXT, leased_until REAL,
                    error TEXT, evidence TEXT, updated REAL NOT NULL);
                CREATE INDEX IF NOT EXISTS tasks_run ON tasks(run_id,status);
                CREATE INDEX IF NOT EXISTS tasks_due ON tasks(status,due);
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, body TEXT NOT NULL, first_seen REAL NOT NULL,
                    last_seen REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS run_jobs (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL,
                    job_id TEXT NOT NULL, UNIQUE(run_id,job_id));
                CREATE VIRTUAL TABLE IF NOT EXISTS job_text USING fts5(
                    id UNINDEXED, title, company, location, description, tokenize='unicode61');
                CREATE TABLE IF NOT EXISTS receipts (
                    task_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    PRIMARY KEY(task_id,fingerprint));
            ''')
            if 'owner' not in {r[1] for r in db.execute('PRAGMA table_info(runs)')}:
                db.execute("ALTER TABLE runs ADD COLUMN owner TEXT NOT NULL DEFAULT 'owner'")

    def authorize(self, run_id, owner):
        if self.run(run_id)['owner'] != owner:
            raise ValueError('Unknown search_id')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def create(self, config, tasks, owner='owner'):
        run_id = secrets.token_urlsafe(24)
        now = time.time()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Safe retries of a start call reuse a still-active equivalent run.
            previous=db.execute('''SELECT r.id FROM runs r WHERE r.config=? AND r.owner=? AND EXISTS
                (SELECT 1 FROM tasks t WHERE t.run_id=r.id AND t.status IN ('queued','running'))
                ORDER BY r.created DESC LIMIT 1''',(dumps(config),owner)).fetchone()
            if previous:return previous['id']
            active=db.execute("SELECT COUNT(DISTINCT r.id) FROM runs r JOIN tasks t ON t.run_id=r.id WHERE r.owner=? AND t.status IN ('queued','running')",(owner,)).fetchone()[0]
            if active >= int(os.getenv('MAX_ACTIVE_SEARCHES_PER_USER','5')):
                raise ValueError('Active search quota reached; pause an existing search first')
            db.execute('INSERT INTO runs(id,created,config,owner) VALUES (?,?,?,?)', (run_id, now, dumps(config), owner))
            for task in tasks:
                db.execute('''INSERT INTO tasks(id,run_id,source,query,kind,payload,updated)
                    VALUES (?,?,?,?,?,?,?)''', (secrets.token_hex(12), run_id, task['source'],
                    task.get('query', ''), task['kind'], dumps(task['payload']), now))
        return run_id

    def run(self, run_id):
        with self.connect() as db:
            row = db.execute('SELECT * FROM runs WHERE id=?', (run_id,)).fetchone()
        if row is None:
            raise ValueError('Unknown search_id')
        return {**dict(row), 'config': json.loads(row['config'])}

    def claim(self, lease_seconds=180):
        now = time.time()
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # A killed process cannot strand work. Previously saved pages remain intact.
            db.execute("UPDATE tasks SET status='queued',lease=NULL WHERE status='running' AND leased_until<?", (now,))
            row = db.execute('''SELECT t.* FROM tasks t WHERE t.status='queued' AND t.due<=?
                AND NOT EXISTS(SELECT 1 FROM tasks busy WHERE busy.source=t.source AND busy.status='running')
                AND NOT EXISTS(SELECT 1 FROM tasks busy WHERE t.kind='board' AND busy.kind='board' AND busy.status='running')
                ORDER BY t.pages,t.updated LIMIT 1''', (now,)).fetchone()
            if row is None:
                return None
            token = secrets.token_hex(16)
            db.execute("UPDATE tasks SET status='running',lease=?,leased_until=?,updated=? WHERE id=?",
                       (token, now+lease_seconds, now, row['id']))
            return {**dict(row), 'payload': json.loads(row['payload']), 'lease': token}

    def _save_jobs(self, db, run_id, jobs):
        now = time.time()
        owner = db.execute('SELECT owner FROM runs WHERE id=?',(run_id,)).fetchone()[0]
        for item in jobs:
            if not isinstance(item.get('title'),str) or not item['title'].strip() or not isinstance(item.get('job_url'),str) or not item['job_url']:
                continue
            job_id = hashlib.sha256(((owner+'\0' if owner!='owner' else '')+dumps(_dedup_key(item))).encode()).hexdigest()[:32]
            old = db.execute('SELECT body FROM jobs WHERE id=?', (job_id,)).fetchone()
            previous = json.loads(old['body']) if old else {}
            body = {**previous, **{k:v for k,v in item.items() if v is not None and not k.startswith('_')}}
            if not item.get('description') and previous.get('description'):
                body['description'] = previous['description']
            body['sources'] = sorted(set(previous.get('sources', []) + item.get('sources', []) + [item.get('source', 'unknown')]))
            body['id'] = job_id
            db.execute('''INSERT INTO jobs VALUES (?,?,?,?) ON CONFLICT(id) DO UPDATE
                SET body=excluded.body,last_seen=excluded.last_seen''', (job_id,dumps(body),now,now))
            db.execute('INSERT OR IGNORE INTO run_jobs(run_id,job_id) VALUES (?,?)', (run_id,job_id))
            db.execute('DELETE FROM job_text WHERE id=?', (job_id,))
            db.execute('INSERT INTO job_text VALUES (?,?,?,?,?)',
                       (job_id, *(str(body.get(k) or '') for k in ('title','company','location','description'))))

    def finish(self, task, jobs, payload, *, more=False, error=None, verified_end=False):
        """Atomically commit records AND cursor. A retry cannot lose or duplicate a page."""
        now = time.time()
        fingerprint = hashlib.sha256(dumps(sorted(dumps(_dedup_key(j)) for j in jobs)).encode()).hexdigest() if jobs else None
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM tasks WHERE id=? AND lease=? AND status=\'running\'', (task['id'],task['lease'])).fetchone()
            if row is None:
                return False
            self._save_jobs(db, task['run_id'], jobs)
            repeated = fingerprint and db.execute('SELECT 1 FROM receipts WHERE task_id=? AND fingerprint=?', (task['id'],fingerprint)).fetchone()
            if fingerprint:
                db.execute('INSERT OR IGNORE INTO receipts VALUES (?,?)', (task['id'],fingerprint))
            pages = row['pages'] + (0 if error else 1)
            attempts = row['attempts'] + 1 if error else 0
            status, due = 'done', now + 1
            if error:
                # Hard barriers go to recovery immediately; transient failures retry automatically.
                hard = any(s in error.lower() for s in ('403','401','999','captcha','robots_denied','unsupported','unverified','schema'))
                status = 'needs_recovery' if hard or attempts >= 3 else 'queued'
                due = now + (60 if '429' in error else min(60, 3 ** attempts))
            elif repeated and more:
                status, error = 'needs_recovery', 'repeated_page: upstream cursor did not advance'
            elif more:
                status = 'limited' if pages >= payload.get('page_limit', 30) else 'queued'
            elif not jobs and row['received'] == 0 and not verified_end:
                status, error = 'needs_recovery', 'empty_unverified'
            db.execute('''UPDATE tasks SET payload=?,status=?,pages=?,received=received+?,attempts=?,
                due=?,lease=NULL,leased_until=NULL,error=?,updated=? WHERE id=?''',
                (dumps(payload),status,pages,len(jobs),attempts,due,error,now,task['id']))
        return True

    def status(self, run_id):
        run = self.run(run_id)
        with self.connect() as db:
            counts = {r['status']:r['n'] for r in db.execute('SELECT status,COUNT(*) n FROM tasks WHERE run_id=? GROUP BY status',(run_id,))}
            jobs = db.execute('SELECT COUNT(*) FROM run_jobs WHERE run_id=?',(run_id,)).fetchone()[0]
            sources = db.execute('SELECT COUNT(DISTINCT source) FROM tasks WHERE run_id=?',(run_id,)).fetchone()[0]
            updated = db.execute('SELECT MAX(updated) FROM tasks WHERE run_id=?',(run_id,)).fetchone()[0]
        active = counts.get('queued',0)+counts.get('running',0)
        return {'search_id':run_id,'status':'running' if active else 'paused' if counts.get('paused') else 'needs_attention' if counts.get('needs_recovery') or counts.get('limited') else 'finished',
                'stored_jobs':jobs,'sources':sources,'tasks':counts,'updated_at':updated,
                'exhaustive':False,'poll_after_seconds':10 if active else None,
                'next_action':'find_jobs for available results; get_search_status after poll_after_seconds' if active else 'resume_search to continue paused work' if counts.get('paused') else
                    'get_recovery_tasks for gaps; resume_search for page limits' if counts.get('needs_recovery') or counts.get('limited') else 'find_jobs',
                'scope':'Requested sources and accessible windows; not the entire market.'}

    def task_page(self, run_id, offset=0, limit=10, recovery=False):
        self.run(run_id)
        clause = " AND status IN ('needs_recovery','limited')" if recovery else ''
        with self.connect() as db:
            rows = db.execute('SELECT * FROM tasks WHERE run_id=?'+clause+' ORDER BY id LIMIT ? OFFSET ?', (run_id,limit+1,offset)).fetchall()
        return rows[:limit], offset+limit if len(rows)>limit else None

    def find(self, run_id, query=None, offset=0, limit=10, match_mode="broad"):
        run = self.run(run_id)
        terms = run['config']['queries'] if query is None else ([query] if query else [])
        # Literal words, OR across caller variants. No injected FTS operators.
        if match_mode not in ('broad','all_words'):raise ValueError('Unknown match_mode')
        joiner = ' OR ' if match_mode=='broad' else ' AND '
        fts = ' OR '.join('('+joiner.join('"'+w.replace('"','""')+'"'+('*' if match_mode=='broad' else '') for w in t.split())+')' for t in terms if t.strip())
        args = [run_id]
        base = ' FROM jobs j JOIN run_jobs r ON r.job_id=j.id'
        if fts:
            base += ' JOIN job_text ON job_text.id=j.id WHERE r.run_id=? AND job_text MATCH ?'
            args.append(fts)
            order = ' ORDER BY bm25(job_text,0,5,2,1,1),r.seq'
        else:
            base += ' WHERE r.run_id=?'
            order = ' ORDER BY r.seq'
        with self.connect() as db:
            total = db.execute('SELECT COUNT(*)'+base, args).fetchone()[0]
            rows = db.execute('SELECT j.body,j.first_seen,j.last_seen'+base+order+' LIMIT ? OFFSET ?',[*args,limit,offset]).fetchall()
        jobs, size = [], 0
        for row in rows:
            body=json.loads(row['body'])
            card={k:body.get(k) for k in ('id','title','company','location','date_posted','job_url','sources','evidence_origin') if body.get(k) is not None}
            card={k:(v[:1200] if isinstance(v,str) else v) for k,v in card.items()}
            from quality import evidence
            card.update(evidence(body))
            card.update(description_available=bool(body.get('description')),last_seen=row['last_seen'])
            cost=len(dumps(card))
            if jobs and size+cost>10000:
                break
            jobs.append(card); size+=cost
        end=offset+len(jobs)
        return {'search_id':run_id,'matching_jobs':total,'jobs':jobs,'next_offset':end if end<total else None,
                'filter':terms,'match_mode':match_mode,'search_status':self.status(run_id)['status'],
                'note':'Searches collected text. Missing descriptions may hide matches. query="" returns all collected cards.'}

    def read(self, run_id, job_id, offset=0, chars=5000):
        self.run(run_id)
        with self.connect() as db:
            row=db.execute('SELECT j.body,j.first_seen,j.last_seen FROM jobs j JOIN run_jobs r ON r.job_id=j.id WHERE r.run_id=? AND j.id=?',(run_id,job_id)).fetchone()
        if not row:
            raise ValueError('Unknown job_id in this search')
        body=json.loads(row['body']); description=body.pop('description',None) or ''
        from quality import evidence
        return {**body,**evidence({**body,'description':description}),'description':description[offset:offset+chars],'description_chars':len(description),
                'next_text_offset':offset+chars if offset+chars<len(description) else None,
                'first_seen':row['first_seen'],'last_seen':row['last_seen']}

    def enrich(self, run_id, job):
        self.read(run_id,job['id'])
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            self._save_jobs(db,run_id,[job])

    def resume(self, run_id, extra_pages=30, retry_failed=False):
        run=self.run(run_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            active=db.execute("SELECT COUNT(DISTINCT r.id) FROM runs r JOIN tasks t ON t.run_id=r.id WHERE r.owner=? AND r.id!=? AND t.status IN ('queued','running')",(run['owner'],run_id)).fetchone()[0]
            if active >= int(os.getenv('MAX_ACTIVE_SEARCHES_PER_USER','5')):
                raise ValueError('Active search quota reached; pause an existing search first')
            statuses=['limited','paused']+(['needs_recovery'] if retry_failed else [])
            for row in db.execute('SELECT * FROM tasks WHERE run_id=?',(run_id,)).fetchall():
                if row['status'] not in statuses:
                    continue
                payload=json.loads(row['payload']);payload['page_limit']=row['pages']+extra_pages
                db.execute("UPDATE tasks SET status='queued',payload=?,attempts=0,due=0,error=NULL WHERE id=?",(dumps(payload),row['id']))
        return self.status(run_id)

    def pause(self,run_id):
        self.run(run_id)
        with self.connect() as db:
            db.execute("UPDATE tasks SET status='paused',lease=NULL,leased_until=NULL WHERE run_id=? AND status IN ('queued','running')",(run_id,))
        return self.status(run_id)

    def record(self, run_id, task_id, jobs, outcome, evidence):
        self.run(run_id)
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM tasks WHERE id=? AND run_id=?',(task_id,run_id)).fetchone()
            if not row or row['status'] not in ('needs_recovery','limited','checked','partial','blocked'):
                raise ValueError('Task is not available for recovery')
            self._save_jobs(db,run_id,[{**j,'source':row['source'],'evidence_origin':'client_reported'} for j in jobs])
            status='checked' if outcome=='checked' else 'needs_recovery'
            db.execute('UPDATE tasks SET status=?,evidence=?,updated=? WHERE id=?',(status,dumps(evidence),time.time(),task_id))
        return self.status(run_id)
