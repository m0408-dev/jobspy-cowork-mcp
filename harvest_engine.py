"""Source-isolated workers with durable cursors, retries and public-site fallbacks."""
import asyncio
import logging
import os
import time
from urllib.parse import urlsplit

from catalog import select_sources
from discovery import employer_jobs
from http_cache import CachedClient
from public_crawler import crawl_page
from sources import _FETCHERS, safe_error

log=logging.getLogger(__name__)
BOARDS={'indeed','linkedin','glassdoor','google','zip_recruiter','bayt','naukri','bdjobs'}
QUERY_APIS={'arbeitsagentur','himalayas'}


def plan(config):
    rows=select_sources(config['market'])
    if config.get('sources') is not None:
        selected=set(config['sources']);known={r['id'] for r in rows}
        if selected-known:raise ValueError('Unknown source for selected market: '+', '.join(sorted(selected-known)))
        rows=[r for r in rows if r['id'] in selected]
    tasks=[]
    for row in rows:
        source=row['id']
        kind='api' if source in _FETCHERS else 'board' if source in BOARDS else 'public'
        # Public feeds/crawls enumerate once, regardless of how many query variants were supplied.
        terms=(config['queries'] or ['']) if source in QUERY_APIS or kind=='board' else ['']
        for query in terms:
            tasks.append({'source':source,'query':query,'kind':kind,'payload':{
                'url':row['url'],'cursor':0,'page_limit':config['page_limit'],
                'location':config['location'],'country':config['country'],'market':config['market']}})
    for employer in config.get('employers',[]):
        provider,slug=employer['provider'],employer['slug']
        url={'greenhouse':f'https://boards.greenhouse.io/{slug}',
             'lever':f'https://jobs.{"eu." if employer["region"]=="eu" else ""}lever.co/{slug}',
             'personio':f'https://{slug}.jobs.personio.de'}[provider]
        tasks.append({'source':f'{provider}:{slug}','query':'','kind':'employer',
                      'payload':{**employer,'url':url,'page_limit':1}})
    if not tasks:raise ValueError('Select at least one source or employer')
    return tasks


async def fetch_task(task):
    source,query,payload=task['source'],task['query'],dict(task['payload'])
    kind=task['kind']
    if payload.get('fallback')=='public' or kind=='public':
        try:
            jobs,payload,more=await crawl_page(source,payload)
            # A successfully read sibling page cannot erase inaccessible frontier pages.
            error='unverified_public_frontier_gaps' if not more and (payload.get('failed_urls') or payload.get('frontier_truncated')) else None
            return jobs,payload,more,error,False
        except Exception as exc:
            frontier=list(payload.get('frontier') or [payload['url']])
            failed=frontier.pop(0)
            # Continue independent URLs from this site when one page is unavailable.
            if frontier:
                message=str(exc)
                reason=message if message.startswith(('robots_denied','unsupported_')) and len(message)<100 else safe_error(exc)
                payload['failed_urls']=(payload.get('failed_urls',[])+[{'url':failed,'error':reason}])[-10:]
                payload['frontier']=frontier
                payload['visited']=payload.get('visited',[])+[failed]
                return [],payload,True,None,False
            raise
    if kind=='employer':
        jobs=await employer_jobs(payload['provider'],payload['slug'],payload['region'])
        return jobs,payload,False,None,True
    cursor=payload.get('cursor',0)
    if kind=='api':
        async with CachedClient(timeout=12,follow_redirects=True) as client:
            jobs,meta=await _FETCHERS[source](client,[query],payload.get('location'),100,0,
                max_pages=1,source_offset=cursor,fetch_details=False)
        next_cursor=meta.get('next_source_offsets',{}).get(query,meta.get('next_source_offset'))
        errors=meta.get('errors',[])
        # A failed page retries its own cursor, never restarts a successful previous page.
        if next_cursor is not None:payload['cursor']=next_cursor
        error='; '.join(errors)[:600] if errors else None
        paged=source in QUERY_APIS or source in ('arbeitnow','themuse')
        payload['coverage']=meta.get('coverage','feed_window')
        verified=meta.get('coverage') in ('query_exhausted','feed_exhausted')
        return jobs,payload,paged and next_cursor is not None,error,verified
    # Existing isolated subprocess adapter prevents one blocked library from hanging the worker.
    from server import _jobspy_batch
    jobs,meta=await _jobspy_batch([query],payload.get('location') or '',payload.get('country') or 'germany',
        [source],100,0,False,False,offset=cursor,max_pages=1)
    entry=meta[source];state=entry['query_states'][query]
    payload['cursor']=state['next_offset']
    payload['coverage']='board_search_window'
    errors=entry.get('errors',[])
    if errors:return jobs,payload,True,'; '.join(errors),False
    if not jobs:return jobs,payload,False,'empty_unverified',False
    return jobs,payload,state['status'] not in ('window_end',),None,False


class HarvestEngine:
    def __init__(self,store,fetcher=fetch_task,workers=None):
        self.store=store;self.fetcher=fetcher
        self.workers=max(1,min(8,workers or int(os.getenv('HARVEST_WORKERS','4'))))
        self.tasks=[]

    async def start(self):
        if not self.tasks:
            self.tasks=[asyncio.create_task(self.worker(),name=f'collector-{i}') for i in range(self.workers)]

    async def stop(self):
        for task in self.tasks:task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True)
        self.tasks=[]

    async def tick(self):
        task=await asyncio.to_thread(self.store.claim)
        if not task:return False
        jobs=[];payload=dict(task['payload']);more=False;error=None;verified=False
        try:
            jobs,payload,more,error,verified=await asyncio.wait_for(self.fetcher(task),timeout=75)
        except asyncio.CancelledError:
            # Lease expiry resumes interrupted work; already committed pages survive.
            raise
        except Exception as exc:
            # Preserve our bounded diagnostic codes, never arbitrary exception/URL text.
            message=str(exc)
            error=message if isinstance(exc,ValueError) and message in (
                'robots_denied','unsupported_public_url','unsupported_nonpublic_host',
                'unsupported_response_size','unsupported_redirect_chain',
                'unsupported_crawl_delay_requires_scheduling','unsupported_xml_entities') else safe_error(exc)
        # The deterministic public route runs BEFORE handing a hard API failure to a host AI.
        # Its failure remains a gap and cannot turn the original failure into confirmed zero.
        if error and task['kind'] in ('api','board') and not payload.get('fallback') and (
            task['attempts']>=2 or any(x in error.lower() for x in ('403','401','999','unverified','schema'))):
            payload.update(fallback='public',upstream_error=error,frontier=[payload['url']],visited=[])
            error=None;more=True
        await asyncio.to_thread(self.store.finish,task,jobs,payload,more=more,error=error,verified_end=verified)
        return True

    async def worker(self):
        while True:
            try:
                if not await self.tick():await asyncio.sleep(.5)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception('Collector worker error; persisted lease will recover')
                await asyncio.sleep(2)
