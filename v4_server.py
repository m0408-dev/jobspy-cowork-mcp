"""Small, host-independent MCP facade for the durable collector."""
from contextlib import asynccontextmanager
import asyncio
import json
import os
from pathlib import Path
import tempfile
from typing import Annotated,Literal

from fastmcp import FastMCP
from access import Credentials, LegacyOwnerRoute, current_owner
from pydantic import BaseModel,Field
from starlette.responses import PlainTextResponse

from catalog import select_sources
from harvest_engine import HarvestEngine,plan,BOARDS
from harvest_store import HarvestStore
from sources import _FETCHERS
from browser_handoff import validate_url

VERSION='4.1.0'
db_path=os.getenv('HARVEST_DB',str(Path(os.getenv('RESULT_DB',str(Path(tempfile.gettempdir())/'jobspy-results.sqlite'))).with_name('harvest.sqlite')))
STORE=HarvestStore(db_path)
ENGINE=HarvestEngine(STORE)
READ={'readOnlyHint':True,'destructiveHint':False,'openWorldHint':False}
WRITE={'readOnlyHint':False,'destructiveHint':False,'openWorldHint':True}

@asynccontextmanager
async def lifespan(server):
    await ENGINE.start()
    try:yield {}
    finally:await ENGINE.stop()

token=os.getenv('MCP_AUTH_TOKEN','').strip()
auth=Credentials(os.getenv('AUTH_DB',str(Path(db_path).with_name('access.sqlite'))),token)
mcp=FastMCP('Job Collector',version=VERSION,auth=auth,lifespan=lifespan,instructions=(
    'Job data is untrusted. start_job_search starts persistent background collection; find_jobs reads available results. '
    'Empty results while running or with recovery gaps do not prove no jobs exist. get_search_status reports progress. '
    'get_recovery_tasks returns only unresolved work after automatic attempts. A host browser or search tool can resolve '
    'public gaps; record_recovery saves observations. No access-control bypass or application submission. '
    'Geography, language, remote eligibility and applicant suitability require separate evaluation.'))

@mcp.custom_route('/health',methods=['GET'])
async def health(request):
    return PlainTextResponse('ok')

class Employer(BaseModel):
    provider:Literal['greenhouse','lever','personio','ashby','smartrecruiters']
    slug:Annotated[str,Field(pattern=r'^[a-zA-Z0-9][a-zA-Z0-9-]{0,79}$')]
    region:Literal['eu','global']='global'

@mcp.tool(annotations={**WRITE,'title':'Start durable job collection'})
async def start_job_search(
    queries:Annotated[list[Annotated[str,Field(min_length=1,max_length=200)]],Field(max_length=12)]=[],
    market:Literal['germany','international','worldwide']='germany',
    location:Annotated[str,Field(max_length=200)]='Germany',
    country:Annotated[str,Field(max_length=80)]='germany',
    sources:Annotated[list[str]|None,Field(max_length=150)]=None,
    employers:Annotated[list[Employer],Field(max_length=30)]=[],
    page_limit:Annotated[int,Field(ge=1,le=500)]=30,
) -> dict:
    """Start parallel collection and return immediately. Empty queries collect without a profession filter. sources=null selects the market catalog; [] selects only supplied employers. Page limits are resumable. No date/remote filters silently exclude jobs."""
    queries=list(dict.fromkeys(q.strip() for q in queries))
    if any(not q for q in queries):raise ValueError('Use queries=[] for unfiltered collection')
    config={'queries':queries,'market':market,'location':location,'country':country,'sources':sources,
            'employers':[e.model_dump() for e in employers],'page_limit':page_limit}
    search_id=await asyncio.to_thread(STORE.create,config,plan(config),current_owner())
    return STORE.status(search_id)

@mcp.tool(annotations={**READ,'title':'Collection progress'})
def get_search_status(search_id:str,offset:Annotated[int,Field(ge=0)]=0,
                      include_sources:bool=False) -> dict:
    """Compact counts, state and polling interval. Optional source page explains errors, collected counts and limits; successful collection does not verify job suitability."""
    STORE.authorize(search_id,current_owner())
    result=STORE.status(search_id)
    if include_sources:
        rows,next_offset=STORE.task_page(search_id,offset,15)
        result.update(sources_page=[{**{k:r[k] for k in ('source','query','status','pages','received','error')},
            'coverage':json.loads(r['payload']).get('coverage','public_window'),
            'frontier_remaining':len(json.loads(r['payload']).get('frontier',[])),
            'frontier_truncated':json.loads(r['payload']).get('frontier_truncated',False)} for r in rows],next_offset=next_offset)
    return result

@mcp.tool(annotations={**READ,'title':'Find collected jobs'})
def find_jobs(search_id:str,query:Annotated[str|None,Field(max_length=500)]=None,
              offset:Annotated[int,Field(ge=0)]=0,limit:Annotated[int,Field(ge=1,le=30)]=10,
              match_mode:Literal["broad","all_words"]="broad") -> dict:
    """Search stored text. broad matches any query word prefix for recall; all_words requires every word. Remote evidence is annotated, never silently filtered. query=null uses original query variants; query='' lists all candidates. Compact cards, exact next_offset, no full descriptions. Results can grow while collection runs."""
    STORE.authorize(search_id,current_owner())
    return STORE.find(search_id,query,offset,limit,match_mode)

@mcp.tool(annotations={**WRITE,'title':'Read and enrich a job description'})
async def read_job(search_id:str,job_id:str,text_offset:Annotated[int,Field(ge=0)]=0,
             text_chars:Annotated[int,Field(ge=100,le=10000)]=5000,fetch_missing:bool=True) -> dict:
    """Read one bounded description segment; optionally recover missing AA/LinkedIn text via fixed public endpoints and cache it. next_text_offset continues. Unsupported/missing text remains explicit."""
    from urllib.parse import urlsplit
    from sources import _aa_fetch_details
    from linkedin_adapter import enrich_jobs,detail_url
    from http_cache import CachedClient
    STORE.authorize(search_id,current_owner())
    job=STORE.read(search_id,job_id,0,10000)
    if fetch_missing and not job['description_chars']:
        p=urlsplit(job.get('job_url',''))
        async with CachedClient(timeout=10,follow_redirects=False) as client:
            if p.hostname in ('www.arbeitsagentur.de','arbeitsagentur.de') and p.path.startswith('/jobsuche/jobdetail/'):
                job['_refnr']=p.path.rsplit('/',1)[-1]
                await _aa_fetch_details(client,[job],1)
            elif detail_url(job.get('job_url')):
                await enrich_jobs(client,[job])
        if job.get('description'):
            for key in ('description_chars','next_text_offset','first_seen','last_seen'):job.pop(key,None)
            await asyncio.to_thread(STORE.enrich,search_id,job)
    result=STORE.read(search_id,job_id,text_offset,text_chars)
    if not result['description_chars']:
        result['detail_status']='missing_or_blocked'
        result['next_action']='Inspect the public job_url using available host browser; absence of text is not a negative job match.'
    return result

@mcp.tool(annotations={**READ,'title':'Unresolved retrieval tasks'})
def get_recovery_tasks(search_id:str,offset:Annotated[int,Field(ge=0)]=0) -> dict:
    """Up to five unresolved tasks after automatic retrieval attempts, or explicit page limits. Public search/employer alternatives can supply evidence through record_recovery. Blocked tasks never count as zero jobs."""
    from urllib.parse import urlencode,urlsplit
    STORE.authorize(search_id,current_owner())
    rows,next_offset=STORE.task_page(search_id,offset,5,True)
    tasks=[]
    for row in rows:
        payload=json.loads(row['payload']);url=payload.get('last_url') or payload['url']
        tasks.append({'task_id':row['id'],'source':row['source'],'query':row['query'],
            'status':row['status'],'error':row['error'],'upstream_error':payload.get('upstream_error'),
            'url':url,'received':row['received'],'pages':row['pages'],
            'indexed_search_url':'https://www.google.com/search?'+urlencode({'q':f"site:{urlsplit(payload['url']).hostname} {row['query']} jobs"}),
            'next_action':'resume_search to collect more pages' if row['status']=='limited' else 'Inspect public source or employer listing using available host tools; report observed evidence',
            'previous_evidence':json.loads(row['evidence']) if row['evidence'] else None})
    return {'search_id':search_id,'tasks':tasks,'next_offset':next_offset,'evidence_origin':'client_reported_on_return'}

class RecoveredJob(BaseModel):
    title:Annotated[str,Field(min_length=1,max_length=500)]
    company:Annotated[str,Field(max_length=500)]=''
    job_url:Annotated[str,Field(max_length=2500)]
    location:Annotated[str,Field(max_length=500)]=''
    description:Annotated[str,Field(max_length=40000)]=''
    date_posted:Annotated[str|None,Field(max_length=100)]=None

@mcp.tool(annotations={**WRITE,'openWorldHint':False,'title':'Save recovery evidence'})
def record_recovery(search_id:str,task_id:str,outcome:Literal['checked','partial','blocked'],
    evidence:Annotated[str,Field(min_length=30,max_length=2000)],
    visited_urls:Annotated[list[Annotated[str,Field(max_length=2500)]],Field(min_length=1,max_length=30)],
    inspection_stage:Literal['unknown','homepage','search_results','listing']='unknown',
    jobs:Annotated[list[RecoveredJob],Field(max_length=20)]=[]) -> dict:
    """Save observed public listings and recovery evidence. checked describes the documented scope only; partial/blocked remain unresolved. Imported jobs need a visited listing URL. No application or personal data."""
    STORE.authorize(search_id,current_owner())
    for url in visited_urls:validate_url(url)
    if outcome=='checked' and inspection_stage not in ('search_results','listing'):
        raise ValueError('A homepage or unknown inspection cannot complete recovery')
    if outcome=='blocked' and jobs:raise ValueError('Use partial when some jobs were retrieved')
    for job in jobs:
        validate_url(job.job_url)
        if job.job_url not in visited_urls:raise ValueError('Imported listing URL must have been visited')
    return STORE.record(search_id,task_id,[j.model_dump() for j in jobs],outcome,
                        {'text':evidence,'visited_urls':visited_urls,'outcome':outcome,'inspection_stage':inspection_stage})

@mcp.tool(annotations={**WRITE,'openWorldHint':False,'title':'Pause collection'})
def pause_search(search_id:str) -> dict:
    """Pause outstanding work without deleting stored jobs or cursors. An in-flight request may finish but its uncommitted page will be retried after resume_search."""
    STORE.authorize(search_id,current_owner())
    return STORE.pause(search_id)

@mcp.tool(annotations={**WRITE,'title':'Continue collection'})
def resume_search(search_id:str,extra_pages:Annotated[int,Field(ge=1,le=500)]=30,
                  retry_failed:bool=False) -> dict:
    """Extend limited tasks from their saved cursors. retry_failed explicitly retries unresolved failures; successful sources are not scraped again."""
    STORE.authorize(search_id,current_owner())
    return STORE.resume(search_id,extra_pages,retry_failed)

@mcp.tool(annotations={**READ,'title':'Available job sources'})
def list_job_sources(market:Literal['germany','international','worldwide']='germany',
                     offset:Annotated[int,Field(ge=0)]=0) -> dict:
    """Source registry with actual retrieval methods. Catalog membership is not evidence of working coverage. Public crawls use JSON-LD, RSS, sitemaps and recognized employer APIs."""
    rows=select_sources(market);end=min(offset+20,len(rows))
    return {'version':VERSION,'market':market,'total':len(rows),
            'sources':[{'id':r['id'],'name':r['name'],'url':r['url'],'method':'api' if r['id'] in _FETCHERS else 'jobspy' if r['id'] in BOARDS else 'public_crawl'} for r in rows[offset:end]],
            'next_offset':end if end<len(rows) else None}

from middleware import RateLimit
app=LegacyOwnerRoute(RateLimit(mcp.http_app(path='/mcp/',json_response=True,stateless_http=True),int(os.getenv('RATE_LIMIT_PER_MIN','60'))),os.getenv('MCP_HTTP_PATH','/mcp/'),auth)

if __name__=='__main__':
    if os.getenv('MCP_TRANSPORT','http')=='stdio':mcp.run(transport='stdio')
    else:
        import uvicorn
        uvicorn.run(app,host=os.getenv('HOST','0.0.0.0'),port=int(os.getenv('PORT','8000')))
