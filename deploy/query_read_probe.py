"""Read-only live query diagnostics: emits counts/timing, never job text."""
import json
import sqlite3
import time

db = sqlite3.connect('file:/app/data/harvest.sqlite?mode=ro', uri=True)
run = db.execute('SELECT run_id FROM run_jobs ORDER BY seq DESC LIMIT 1').fetchone()[0]
queries = {
    'ranked_planner_order': 'SELECT j.id FROM jobs j JOIN run_jobs r ON r.job_id=j.id JOIN job_quality q ON q.id=j.id JOIN job_text_rows x ON x.id=j.id JOIN job_text ON job_text.rowid=x.fts_rowid WHERE r.run_id=? AND job_text MATCH ? ORDER BY bm25(job_text,0,5,2,1,1),r.seq LIMIT 3',
    'ranked_fts_outer': 'SELECT j.id FROM job_text CROSS JOIN job_text_rows x ON x.fts_rowid=job_text.rowid CROSS JOIN run_jobs r ON r.job_id=x.id CROSS JOIN jobs j ON j.id=x.id CROSS JOIN job_quality q ON q.id=j.id WHERE r.run_id=? AND job_text MATCH ? ORDER BY bm25(job_text,0,5,2,1,1),r.seq LIMIT 3',
}
for name, query in queries.items():
    started = time.monotonic()
    db.set_progress_handler(lambda: int(time.monotonic() - started > 5), 10000)
    try:
        count = len(db.execute(query, (run, 'remote')).fetchall())
        print(json.dumps({'query': name, 'count': count, 'seconds': round(time.monotonic()-started, 3)}))
    except sqlite3.OperationalError as exc:
        print(json.dumps({'query': name, 'seconds': round(time.monotonic()-started, 3), 'error': str(exc)}))
db.close()
