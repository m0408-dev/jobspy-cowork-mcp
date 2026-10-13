"""Opt-in live collector acceptance, with isolated durable storage."""
import asyncio
import json
import os
from pathlib import Path
import sys
import tempfile
import time
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

async def main():
    from harvest_store import HarvestStore
    from harvest_engine import HarvestEngine,plan
    from catalog import select_sources
    with tempfile.TemporaryDirectory() as tmp:
        store=HarvestStore(Path(tmp)/'acceptance.sqlite')
        config={'queries':['IT Support'],'market':'germany',
            'sources':['arbeitsagentur','arbeitnow','linkedin','stepstone','xing'],
            'employers':[{'provider':'greenhouse','slug':'canonical','region':'global'}],
            'location':'Germany','country':'germany','page_limit':2}
        rid=store.create(config,plan(config));engine=HarvestEngine(store)
        await engine.start();start=time.monotonic()
        try:
            while time.monotonic()-start<210:
                await asyncio.sleep(5)
                status=store.status(rid)
                if status['status']!='running':break
            rows,_=store.task_page(rid,limit=20)
            print(json.dumps({'elapsed_seconds':round(time.monotonic()-start),'status':store.status(rid),
                'sources':[{k:r[k] for k in ('source','status','pages','received','error')} for r in rows],
                'matching_jobs':store.find(rid)['matching_jobs'],
                'card_response_chars':len(json.dumps(store.find(rid))),
                'sample_titles':[j['title'] for j in store.find(rid)['jobs'][:3]]},ensure_ascii=False),flush=True)
            assert store.status(rid)['stored_jobs']>0,'No jobs retrieved; investigate source errors'
        finally:await engine.stop()

asyncio.run(main())
