"""One public LinkedIn result page per call, with an exact upstream cursor.

No login automation or challenge bypass. Empty/challenge pages remain unverified.
Keeping page boundaries outside python-jobspy avoids its cumulative offset bug
and saves successful pages before a later request can time out.
"""
import re
import httpx
from bs4 import BeautifulSoup

URL = 'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search'

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
