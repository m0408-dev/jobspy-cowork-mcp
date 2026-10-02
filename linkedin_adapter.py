"""One public LinkedIn result page per call, with an exact upstream cursor.

No login automation or challenge bypass. Empty/challenge pages remain unverified.
Keeping page boundaries outside python-jobspy avoids its cumulative offset bug
and saves successful pages before a later request can time out.
"""
import re
import asyncio
import time
from urllib.parse import urlsplit
import httpx
from bs4 import BeautifulSoup

URL = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search'


def detail_url(job_url):
    """Only canonical public LinkedIn IDs; never fetch arbitrary snapshot URLs."""
    url = urlsplit(job_url or '')
    match = re.fullmatch(r'/jobs/view/(\d+)/?', url.path)
    if url.scheme != 'https' or url.netloc != 'www.linkedin.com' or not match:
        return None
    return 'https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/' + match[1]


def parse_detail(html):
    soup = BeautifulSoup(html, 'html.parser')
    body = soup.select_one('.show-more-less-html__markup')
    if not body or not body.get_text(' ', strip=True):
        return {'detail_status': 'unavailable_or_blocked'}
    return {'description': body.get_text('\n', strip=True), 'detail_status': 'loaded'}


async def enrich_jobs(client, jobs, budget=12):
    """Bounded optional enrichment after cards are retained by the parent.

    Failures never discard cards. A rate limit/login wall stops further requests.
    Remaining jobs stay explicitly deferred for shortlist/browser recovery.
    """
    deadline = time.monotonic() + max(0, budget)
    stopped = False
    for job in jobs:
        if job.get('description'):
            continue
        url = detail_url(job.get('job_url'))
        if not url:
            job['detail_status'] = 'unsupported_url'
            continue
        remaining = deadline - time.monotonic()
        if stopped or remaining <= 0:
            job['detail_status'] = 'deferred'
            continue
        try:
            response = await asyncio.wait_for(client.get(url), timeout=min(4, remaining))
            if response.status_code != 200:
                job['detail_status'] = 'blocked_or_error'
                job['detail_error'] = 'HTTP ' + str(response.status_code)
                stopped = response.status_code in (301, 302, 303, 307, 308, 401, 403, 429, 999)
            else:
                job.update(parse_detail(response.text))
                stopped = job['detail_status'] == 'unavailable_or_blocked'
        except (httpx.HTTPError, asyncio.TimeoutError) as exc:
            job['detail_status'] = 'blocked_or_error'
            job['detail_error'] = type(exc).__name__
            stopped = True
    return {status: sum(j.get('detail_status') == status for j in jobs)
            for status in sorted({j.get('detail_status') for j in jobs if j.get('detail_status')})}

def parse_page(html, offset):
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('div.base-search-card')
    jobs, errors = [], []
    for card in cards:
        link = card.select_one('a.base-card__full-link')
        title = card.select_one('h3.base-search-card__title') or card.select_one('span.sr-only')
        company = card.select_one('h4.base-search-card__subtitle')
        location = card.select_one('.job-search-card__location')
        date = card.select_one('time[datetime]')
        href = link.get('href', '') if link else ''
        match = re.search(r'/jobs/view/(?:[^/?]*-)?(\d+)(?:[/?]|$)', href)
        if not title or not match:
            errors.append('UnparsedCard')
            continue
        jobs.append({'site': 'linkedin', 'title': title.get_text(' ', strip=True),
            'company': company.get_text(' ', strip=True) if company else None,
            'location': location.get_text(' ', strip=True) if location else None,
            'date_posted': date.get('datetime') if date else None,
            'job_url': 'https://www.linkedin.com/jobs/view/'+match.group(1)})
    return {'jobs': jobs, 'upstream': {'next_offset': offset+len(cards),
        'cards_received': len(cards), 'status': 'page_received' if cards else 'empty_or_blocked',
        'errors': errors, 'exhaustive': False}}

def fetch_page(arguments):
    offset = arguments.get('offset', 0)
    params = {'keywords': arguments['search_term'], 'location': arguments.get('location', ''),
              'start': offset, 'distance': arguments.get('distance', 50)}
    if arguments.get('is_remote'): params['f_WT'] = '2'
    if arguments.get('hours_old'): params['f_TPR'] = 'r'+str(arguments['hours_old']*3600)
    job_type = {'fulltime':'F', 'parttime':'P', 'contract':'C', 'internship':'I'}.get(arguments.get('job_type'))
    if job_type: params['f_JT'] = job_type
    # Public read-only endpoint; no automatic retries on access/rate-limit failures.
    proxies = arguments.get('proxies') or []
    proxy = proxies[0] if isinstance(proxies, list) and proxies else proxies or None
    with httpx.Client(timeout=15, follow_redirects=False, proxy=proxy) as client:
        response = client.get(URL, params=params)
    if response.status_code != 200:
        return {'jobs': [], 'upstream': {'next_offset': offset, 'cards_received': 0,
            'status': 'blocked_or_error', 'errors': [f'HTTP {response.status_code}'], 'exhaustive': False}}
    return parse_page(response.text, offset)
