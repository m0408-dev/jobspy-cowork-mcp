"""Read-only production snapshot, temporary write benchmark; no production edits."""
import json
import sqlite3
import tempfile
import time
from pathlib import Path
from harvest_store import HarvestStore

with tempfile.TemporaryDirectory() as tmp:
    target=Path(tmp)/'bench.sqlite'
    # /prod must contain a completed SQLite backup, never the live WAL database.
    source=sqlite3.connect('file:/prod/harvest.sqlite?mode=ro&immutable=1',uri=True,timeout=30)
    dest=sqlite3.connect(target)
    source.backup(dest); source.close(); dest.close()
    started=time.monotonic(); store=HarvestStore(target)
    migration=time.monotonic()-started
    with store.connect() as db:
        before=db.execute('SELECT count(*) FROM jobs').fetchone()[0]
        text_before=db.execute('SELECT count(*) FROM job_text').fetchone()[0]
    run=store.create({'queries':[]},[])
    batch=[{'title':'Benchmark','company':'Benchmark','job_url':f'https://example.org/benchmark/{i}',
            'description':'isolated benchmark searchable'} for i in range(100)]
    timings=[]
    for _ in range(2):
        started=time.monotonic()
        with store.connect() as db:
            db.execute('BEGIN IMMEDIATE');store._save_jobs(db,run,batch)
        timings.append(round(time.monotonic()-started,3))
    with store.connect() as db:
        assert db.execute('SELECT count(*) FROM jobs').fetchone()[0]==before+100
        assert db.execute('SELECT count(*) FROM job_text').fetchone()[0]==text_before+100
    assert store.find(run,'isolated')['matching_jobs']==100
    print(json.dumps({'existing_jobs':before,'migration_seconds':round(migration,3),'insert_100_seconds':timings[0],
                     'update_100_seconds':timings[1],'search_verified':True}))
