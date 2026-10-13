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

AGENT='JobCollector/4.0 (+https://github.com/m0408-dev/jobspy-cowork-mcp)'
JOB_LINK=re.compile(r'job|career|karriere|stellen|vacanc|position|arbeiten|recruit',re.I)
_robots={}


def employer_from_url(url):
    p=urlsplit(url);host=p.hostname or '';parts=p.path.strip('/').split('/')
    if host in ('boards.greenhouse.io','job-boards.greenhouse.io') and parts[0]:
        return ('greenhouse',parts[0],'global')
    if host in ('jobs.lever.co','jobs.eu.lever.co') and parts[0]:
        return ('lever',parts[0],'eu' if '.eu.' in host else 'global')
    if host.endswith('.jobs.personio.de'):
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
        jobs.append({'title':fields['title'],'job_url':urljoin(url,link),'source':source,
                     'description':_strip_html(fields.get('description') or fields.get('content') or fields.get('summary')),
                     'date_posted':fields.get('pubDate') or fields.get('published'),
                     'evidence_origin':'public_feed_candidate'})
    return jobs,links


async def crawl_page(source,payload):
    """One durable frontier step. Index pages are never treated as zero-job proof."""
    payload=dict(payload)
    frontier=list(payload.get('frontier') or [payload['url']])
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
                jobs,links=xml_content(text,final,source)
            else:
                jobs,soup=structured_jobs(text,final,source)
                links=[urljoin(final,a['href']) for a in soup.select('a[href]')
                       if JOB_LINK.search(a['href']) or employer_from_url(urljoin(final,a['href']))]
                links.extend(urljoin(final,a['href']) for a in soup.select('link[rel="alternate"][href]')
                             if 'rss' in a.get('type','') or 'atom' in a.get('type',''))
                if len(visited)==1:
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
