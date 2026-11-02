"""Query routing and site-specific listing URLs, independent of profession.

Unconfigured sites get search-form discovery, not a claim of working search.
These routes describe public UI paths; robots/access checks remain mandatory.
"""
import re
from urllib.parse import quote, urlencode, urlsplit


def search_url(url, query, location=''):
    host = (urlsplit(url).hostname or '').removeprefix('www.')
    slug = quote(re.sub(r'\s+', '-', query.strip()), safe='')
    if host in ('stepstone.de', 'stepstone.at'):
        return f'https://www.{host}/jobs/{slug}' if query else url
    if host == 'xing.com':
        return f'https://www.xing.com/jobs/{slug}' if query else url
    if host == 'adzuna.de':
        return 'https://www.adzuna.de/search?' + urlencode({'q': query, 'w': location})
    return None


def listing_url(url):
    p = urlsplit(url)
    host = (p.hostname or '').removeprefix('www.')
    if host.startswith('stepstone.'):
        return bool(re.search(r'/stellenangebote--.+--\d+.*\.html$', p.path))
    if host == 'xing.com':
        return bool(re.search(r'/jobs/.+-\d{6,}$', p.path))
    if host == 'adzuna.de':
        return bool(re.search(r'/details/\d+', p.path))
    return False


def indexed_url(url, query, location=''):
    host = urlsplit(url).hostname or ''
    return 'https://www.google.com/search?' + urlencode({'q': f'site:{host} {query} {location} jobs'.strip()})
