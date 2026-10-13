"""Opt-in public structured-crawler acceptance, no JobSpy/API sources."""
import asyncio,json,sys,tempfile,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from harvest_store import HarvestStore
from harvest_engine import HarvestEngine,plan

async def main():
    with tempfile.TemporaryDirectory() as tmp:
        store=HarvestStore(Path(tmp)/'test.sqlite')
        config={'queries':[],'market':'germany','sources':['source_21','source_37','source_32','source_85'],
                'location':'Germany','country':'germany','page_limit':8,'employers':[]}
        rid=store.create(config,plan(config));engine=HarvestEngine(store);await engine.start()
        started=time.monotonic()
        try:
            while time.monotonic()-started<180:
                await asyncio.sleep(5)
                if store.status(rid)['status']!='running':break
            rows,_=store.task_page(rid)
            print(json.dumps({'status':store.status(rid),'sources':[{k:r[k] for k in ('source','pages','received','status','error')} for r in rows],
                              'sample':store.find(rid,limit=3)},ensure_ascii=False))
        finally:await engine.stop()

asyncio.run(main())
