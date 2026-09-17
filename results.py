"""Bounded persistent snapshots. Paging/details never repeat upstream searches."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
import secrets
import sqlite3
import tempfile
import time
from pathlib import Path


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


class ResultStore:
    def __init__(self, path=None, ttl=None, max_bytes=None):
        self.path = path or os.getenv("RESULT_DB", str(Path(tempfile.gettempdir()) / "jobspy-results.sqlite"))
        self.ttl = ttl or int(os.getenv("RESULT_TTL_SECONDS", "21600"))
        self.max_bytes = max_bytes or int(os.getenv("RESULT_CACHE_BYTES", "67108864"))
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("CREATE TABLE IF NOT EXISTS snapshots (id TEXT PRIMARY KEY, created REAL, body TEXT)")

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=15)
        try:
            with db:
                yield db
        finally:
            db.close()

    def save(self, jobs, meta):
        items = []
        for i, job in enumerate(jobs):
            items.append({**job, "id": str(i)})
        body = encode({"jobs": items, "meta": meta})
        size = len(body.encode())
        if size > self.max_bytes:
            raise ValueError("Search exceeds snapshot storage budget; narrow the search. No partial snapshot saved.")
        result_id = secrets.token_urlsafe(24)
        now = time.time()
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("DELETE FROM snapshots WHERE created < ?", (now - self.ttl,))
            used = db.execute("SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM snapshots").fetchone()[0]
            # Never evict an unexpired result silently.
            if used + size > self.max_bytes:
                raise ValueError("Snapshot cache is full; retry after expiry or increase RESULT_CACHE_BYTES.")
            db.execute("INSERT INTO snapshots VALUES (?,?,?)", (result_id, now, body))
        return result_id

    def load(self, result_id):
        with self.connect() as db:
            row = db.execute("SELECT created,body FROM snapshots WHERE id=?", (result_id,)).fetchone()
        if not row or row[0] < time.time() - self.ttl:
            raise ValueError("Unknown or expired result_id; run a new search.")
        data = json.loads(row[1])
        data["expires_in_seconds"] = max(0, int(row[0]+self.ttl-time.time()))
        return data

    def update_jobs(self, result_id, updates):
        def apply(data):
            for job in data["jobs"]:
                if job["id"] in updates:
                    job.update(updates[job["id"]])
        self.mutate(result_id, apply)

    def mutate(self, result_id, operation):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT created,body FROM snapshots WHERE id=?", (result_id,)).fetchone()
            if not row or row[0] < time.time()-self.ttl:
                raise ValueError("Unknown or expired result_id")
            data = json.loads(row[1])
            operation(data)
            body = encode(data)
            used = db.execute("SELECT COALESCE(SUM(length(CAST(body AS BLOB))),0) FROM snapshots").fetchone()[0]
            if used - len(row[1].encode()) + len(body.encode()) > self.max_bytes:
                raise ValueError("Snapshot cache full; cannot store enrichment")
            db.execute("UPDATE snapshots SET body=? WHERE id=?", (body, result_id))

    def page(self, result_id, offset=0, page_size=30, detailed=False, max_chars=24000):
        if offset < 0 or not 1 <= page_size <= 100:
            raise ValueError("Invalid page offset/size")
        data = self.load(result_id)
        jobs = data["jobs"]
        if data["meta"].get("catalog_scope"):
            from catalog import coverage
            data["meta"]["catalog_coverage"] = {k:v for k,v in coverage(data["meta"]).items() if k != "entries"}
        payload = {"result_id": result_id, "total_fetched": len(jobs), "offset": offset,
                   "expires_in_seconds": data["expires_in_seconds"], "jobs": []}
        if offset == 0:
            payload["coverage"] = {k:v for k,v in data["meta"].items() if not k.startswith("_")}
            if len(encode(payload["coverage"])) > max_chars // 3:
                payload["coverage"] = {"metadata_truncated":True,
                    "catalog_coverage":data["meta"].get("catalog_coverage"),
                    "browser_handoff":data["meta"].get("browser_handoff"),"exhaustive":False}
        for job in jobs[offset:offset + page_size]:
            item = {k: v for k, v in job.items() if v is not None and not k.startswith("_") and k != "description"}
            if detailed and job.get("description"):
                item["description"] = job["description"][:4000]
                item["description_chars"] = len(job["description"])
                item["description_truncated"] = len(job["description"]) > 4000
            if len(encode(payload)) + len(encode(item)) > max_chars - 300:
                if payload["jobs"]:
                    break
                # A single huge field cannot stall pagination.
                item = {k: job.get(k) for k in ("id", "title", "company", "job_url")}
                item = {k: v[:1500] if isinstance(v, str) else v for k, v in item.items()}
                item["oversize_record"] = True
            payload["jobs"].append(item)
        end = offset + len(payload["jobs"])
        payload.update(count=len(payload["jobs"]), next_offset=end if end < len(jobs) else None)
        return encode(payload)

    def details(self, result_id, ids, text_offset=0, text_chars=6000):
        data = self.load(result_id)
        indexed = {j["id"]: j for j in data["jobs"]}
        out = []
        for job_id in ids:
            if job_id not in indexed:
                raise ValueError(f"Unknown job id: {job_id}")
            job = indexed[job_id]
            desc = job.get("description") or ""
            out.append({**{k: v for k, v in job.items() if k != "description" and not k.startswith("_")},
                        "description": desc[text_offset:text_offset + text_chars],
                        "description_chars": len(desc),
                        "next_text_offset": text_offset + text_chars if len(desc) > text_offset + text_chars else None})
        return encode({"result_id": result_id, "jobs": out,
            "browser_handoff":data["meta"].get("browser_handoff")})
