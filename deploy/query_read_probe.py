"""Read-only live query diagnostics: emits counts/timing, never job text."""
import json
import sqlite3
import time

db = sqlite3.connect('file:/app/data/harvest.sqlite?mode=ro', uri=True)
run = db.execute('SELECT run_id FROM run_jobs ORDER BY seq DESC LIMIT 1').fetchone()[0]
queries = {
    'legacy': 'SELECT COUNT(*) FROM jobs j JOIN run_jobs r ON r.job_id=j.id JOIN job_text ON job_text.id=j.id WHERE r.run_id=? AND job_text MATCH ?',
    'rowid': 'SELECT COUNT(*) FROM job_text JOIN job_text_rows x ON x.fts_rowid=job_text.rowid JOIN run_jobs r ON r.job_id=x.id WHERE r.run_id=? AND job_text MATCH ?',
}
for name, query in queries.items():
    started = time.monotonic()
    db.set_progress_handler(lambda: int(time.monotonic() - started > 5), 10000)
    try:
        count = db.execute(query, (run, 'remote')).fetchone()[0]
        print(json.dumps({'query': name, 'count': count, 'seconds': round(time.monotonic()-started, 3)}))
    except sqlite3.OperationalError as exc:
        print(json.dumps({'query': name, 'seconds': round(time.monotonic()-started, 3), 'error': str(exc)}))
db.close()
