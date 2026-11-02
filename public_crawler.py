"""Bounded public HTML/RSS/sitemap collection, without rendering or model tokens.

Only catalog seeds and links on their own hosts (or recognized employer ATS) enter
the frontier. DNS and every redirect are checked; no login/challenge bypass.
"""
import asyncio
import ipaddress
import json
import re
import socket
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
import httpx
from discovery import employer_jobs
from sources import _strip_html
from search_routes import listing_url

AGENT='JobCollector/4.0 (+https://github.com/m0408-dev/jobspy-cowork-mcp)'
JOB_LINK=re.compile(r'job|career|karriere|stellen|vacanc|position|arbeiten|recruit',re.I)
_robots={}


def employer_from_url(url):
    p=urlsplit(url);host=p.hostname or '';parts=p.path.strip('/').split('/')
    if host in ('boards.greenhouse.io','job-boards.greenhouse.io') and parts[0]:
        return ('greenhouse',parts[0],'global')
    if host in ('jobs.lever.co','jobs.eu.lever.co') and parts[0]:
        return ('lever',parts[0],'eu' if '.eu.' in host else 'global')
    if host == 'jobs.ashbyhq.com' and parts[0]:
        return ('ashby',parts[0],'global')
    if host.endswith(('.jobs.personio.de','.jobs.personio.com')):
        return ('personio',host.split('.')[0],'eu')
    return None


async def public_url(url):
    p=urlsplit(url)
    if p.scheme not in ('http','https') or not p.hostname or p.username or p.password or p.port not in (None,80,443):
        raise ValueError('unsupported_public_url')
    addresses=await asyncio.get_running_loop().getaddrinfo(p.hostname,p.port or (443 if p.scheme=='https' else 80),type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
        raise ValueError('unsupported_nonpublic_host')
    return urlunsplit((p.scheme,p.netloc,p.path or '/',p.query,''))


async def download(client,url):
    for _ in range(4):
        url=await public_url(url)
        async with client.stream('GET',url,follow_redirects=False) as response:
            if response.is_redirect:
                url=urljoin(url,response.headers.get('location',''))
                continue
            response.raise_for_status()
            parts=[];size=0
            async for part in response.aiter_bytes():
                size+=len(part)
                if size>4_000_000:
                    raise ValueError('unsupported_response_size')
                parts.append(part)
            return b''.join(parts).decode('utf-8',errors='replace'),str(response.url)
    raise ValueError('unsupported_redirect_chain')


async def robot_policy(client,url):
    import time
    p=urlsplit(url);origin=f'{p.scheme}://{p.netloc}'
    cached=_robots.get(origin)
    if cached and cached[0]>time.monotonic():
        return cached[1]
    parser=RobotFileParser();parser.set_url(origin+'/robots.txt')
    try:
        text,_=await download(client,origin+'/robots.txt')
        parser.parse(text.splitlines())
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code==404:
            parser.parse([])
        else:
            raise
    if len(_robots)>512:
        _robots.clear()
    _robots[origin]=(time.monotonic()+3600,parser)
    return parser


def structured_jobs(text,url,source):
    soup=BeautifulSoup(text,'html.parser')
    jobs=[]
    def walk(node):
        if isinstance(node,list):
            for child in node: walk(child)
        elif isinstance(node,dict):
            types=node.get('@type',[])
            if isinstance(types,str):types=[types]
            if 'JobPosting' in types:
                org=node.get('hiringOrganization') or {}
                locations=node.get('jobLocation') or []
                if isinstance(locations,dict):locations=[locations]
                place=[]
                for location in locations:
                    address=location.get('address',{}) if isinstance(location,dict) else {}
                    if isinstance(address,str):place.append(address)
                    else: place.append(', '.join(str(address[k]) for k in ('addressLocality','addressRegion','addressCountry') if address.get(k)))
                job_url=node.get('url') or url
                if isinstance(job_url,str) and node.get('title'):
                    jobs.append({'title':node['title'],'company':org.get('name') if isinstance(org,dict) else str(org),
                        'location':'; '.join(place),'description':_strip_html(node.get('description')),
                        'job_url':urljoin(url,job_url),'date_posted':node.get('datePosted'),
                        'valid_through':node.get('validThrough'),'job_type':node.get('employmentType'),
                        'is_remote':node.get('jobLocationType')=='TELECOMMUTE',
                        'source':source,'evidence_origin':'public_jobposting_jsonld'})
            for key in ('@graph','itemListElement','item','mainEntity'):
                if key in node:walk(node[key])
    for script in soup.find_all('script',type='application/ld+json'):
        try:walk(json.loads(script.string or script.get_text()))
        except (ValueError,TypeError,RecursionError):continue
    return jobs,soup


def search_cards(soup, url, source):
    """Known listing links only; snippets are not fabricated full descriptions."""
    out=[];seen=set()
    for a in soup.select('a[href]'):
        link=urljoin(url,a['href']).split('#',1)[0]
        if not listing_url(link) or link in seen:continue
        heading=a.find(['h2','h3','h4'])
        title=(heading or a).get_text(' ',strip=True) or a.get('aria-label','').strip()
        card=a.find_parent('article')
        if not title and card:
            heading=card.find(['h2','h3','h4'])
            if heading:title=heading.get_text(' ',strip=True)
        if not title or len(title)>300 or title.lower() in ('mehr','details','job ansehen','ansehen'):continue
        seen.add(link)
        company=card.select_one('[class*="Company"]') if card else None
        out.append({'source':source,'title':title,'job_url':link,
                    'company':company.get_text(' ',strip=True) if company else None,
                    'evidence_origin':'board_listing_card','description':None})
    return out


def discover_search(soup, url, queries):
    """Only observed same-origin GET forms; never submit POST/login forms."""
    links=[]
    for form in soup.select('form'):
        if form.get('method','get').lower()!='get':continue
        action=urljoin(url,form.get('action') or url)
        if urlsplit(action).netloc!=urlsplit(url).netloc:continue
        fields=form.select('input[name]')
        search=next((f for f in fields if f.get('type')=='search' or f.get('name','').lower() in ('q','query','keywords','search','searchterm','what')),None)
        if search is None:continue
        if any(f.get('type')=='password' for f in fields):continue
        from urllib.parse import urlencode,parse_qsl,urlunsplit
        p=urlsplit(action)
        base=dict(parse_qsl(p.query))
        for q in queries or ['']:
            params={**base,search['name']:q}
            links.append(urlunsplit((p.scheme,p.netloc,p.path,urlencode(params),'')))
    return list(dict.fromkeys(links))


def xml_content(text,url,source):
    if re.search(r'<!\s*(DOCTYPE|ENTITY)',text,re.I):
        raise ValueError('unsupported_xml_entities')
    root=ET.fromstring(text)
    tag=lambda node:node.tag.rsplit('}',1)[-1]
    links=[];jobs=[]
    if tag(root) in ('sitemapindex','urlset'):
        for node in root.iter():
            if tag(node)=='loc' and node.text:links.append(node.text.strip())
        return jobs,links
    for item in root.iter():
        if tag(item) not in ('item','entry'):continue
        fields={tag(n):''.join(n.itertext()).strip() for n in item}
        link=fields.get('link') or next((n.get('href') for n in item if tag(n)=='link'),None)
        if not fields.get('title') or not link:continue
        path=urlsplit(urljoin(url,link)).path
        # A site's blog/news RSS is not a jobs feed merely because the site has careers.
        if not JOB_LINK.search(path) or re.search(r'/(blog|news|articles?|ratgeber)(/|$)',path,re.I):continue
        jobs.append({'title':fields['title'],'job_url':urljoin(url,link),'source':source,
                     'description':_strip_html(fields.get('description') or fields.get('content') or fields.get('summary')),
                     'date_posted':fields.get('pubDate') or fields.get('published'),
                     'evidence_origin':'public_feed_candidate'})
    return jobs,links


async def crawl_page(source,payload):
    """One durable frontier step. Index pages are never treated as zero-job proof."""
    payload=dict(payload)
    frontier=list(payload.get('frontier') or [payload.get('search_url') or payload['url']])
    visited=list(payload.get('visited',[]))
    target=frontier.pop(0)
    known=set(visited+frontier+[target])
    visited.append(target)
    jobs=[];links=[]
    ats=employer_from_url(target)
    if ats:
        jobs=await employer_jobs(*ats)
    else:
        async with httpx.AsyncClient(timeout=12,headers={'User-Agent':AGENT},trust_env=False) as client:
            policy=await robot_policy(client,target)
            if not policy.can_fetch(AGENT,target):
                raise ValueError('robots_denied')
            delay=policy.crawl_delay(AGENT) or 1
            if delay>20:
                raise ValueError('unsupported_crawl_delay_requires_scheduling')
            await asyncio.sleep(max(1,delay))
            text,final=await download(client,target)
            if re.search(r'<(?:\w+:)?(?:urlset|sitemapindex|rss|feed)[\s>]',text[:1500],re.I):
                jobs,links=await asyncio.to_thread(xml_content,text,final,source)
            else:
                jobs,soup=await asyncio.to_thread(structured_jobs,text,final,source)
                cards=search_cards(soup,final,source)
                known_jobs={j['job_url'] for j in jobs}
                jobs.extend(j for j in cards if j['job_url'] not in known_jobs)
                search_links=discover_search(soup,final,payload.get('queries',[])) if payload.get('discovery_only') else []
                links=[urljoin(final,a['href']) for a in soup.select('a[href]')
                       if JOB_LINK.search(a['href']) or employer_from_url(urljoin(final,a['href']))]
                if payload.get('discovery_only'):
                    if not search_links:
                        payload.update(last_url=final,coverage='search_adapter_missing',frontier=[],visited=visited)
                        # Preserve any real listings found, but never count a homepage as searched.
                        payload['failed_urls']=[{'url':final,'error':'search_adapter_missing'}]
                        return jobs,payload,False
                    links=search_links
                    payload.update(discovery_only=False,coverage='discovered_search_window')
                elif payload.get('search_url') or payload.get('coverage')=='discovered_search_window':
                    # Follow actual listings and pagination, not related occupations/location SEO trees.
                    links=[urljoin(final,a['href']) for a in soup.select('a[href]') if
                           listing_url(urljoin(final,a['href'])) or employer_from_url(urljoin(final,a['href'])) or
                           'next' in a.get('rel',[]) or a.get_text(strip=True).isdigit() or
                           re.search(r'next|nächste|weiter',a.get('aria-label','')+' '+a.get_text(' ',strip=True),re.I)]
                    if not jobs and not links:
                        payload['failed_urls']=(payload.get('failed_urls',[])+[{'url':final,'error':'search_or_detail_unparsed'}])[-10:]
                links.extend(urljoin(final,a['href']) for a in soup.select('link[rel="alternate"][href]')
                             if 'rss' in a.get('type','') or 'atom' in a.get('type',''))
                if len(visited)==1 and not payload.get('search_url') and not payload.get('queries'):
                    links.extend(policy.site_maps() or [urljoin(final,'/sitemap.xml')])
    host=(urlsplit(payload['url']).hostname or '').removeprefix('www.')
    # Bounded crawl frontier; truncation is explicitly visible, never reported as exhaustive.
    for link in links:
        p=urlsplit(link)
        if p.scheme not in ('http','https') or link in known:continue
        if (p.hostname or '').removeprefix('www.')!=host and not employer_from_url(link):continue
        if len(frontier)>=1000:
            payload['frontier_truncated']=True;break
        frontier.append(link);known.add(link)
    # Listing-like URLs before general indexes. Stable ordering avoids losing the tail.
    frontier.sort(key=lambda u:(0 if employer_from_url(u) else 1 if JOB_LINK.search(urlsplit(u).path) else 2))
    payload.update(frontier=frontier,visited=visited,last_url=target)
    for job in jobs:job['source']=source
    return jobs,payload,bool(frontier)
