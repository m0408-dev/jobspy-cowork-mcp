# Durable collection, compact MCP (v4)

## Findings verified before the rebuild

At revision 9306d4b, all 88 existing tests passed. A real single-page call on both
the development machine and Oracle returned 100 Arbeitsagentur cards and 300
Arbeitnow cards for the broad IT Support experiment. The AA response reported
7,056 results for its query. The Canonical Greenhouse feed returned 311 jobs.
These are observations from this investigation, not promises of current counts
or a diagnosis of an unavailable historical Monday run.

The previous aggregate call waited for every source before saving a snapshot.
Four board budgets could add minutes before any result reached a host. A client
timeout could therefore hide successful retrieval. 72 Germany catalog entries
also produced 216 host-browser tasks for three queries, whether the automatic
source had succeeded or not. Results expired after six hours. Existing tests
verified those contracts rather than durable collection.

## Research and decisions

* [MCP tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools):
  typed input, structured result objects, distinct read/write annotations, concise
  descriptions. The default v4 facade has nine tools and returns compact cards.
* [MCP sampling](https://modelcontextprotocol.io/specification/2025-06-18/client/sampling):
  client capability, not a universally available remote browser or autonomous
  agent. Recovery is an explicit result contract, with a reporting tool. The server
  does not pretend to call a browser hosted in another MCP server.
* [FastMCP tasks](https://gofastmcp.com/servers/tasks): protocol tasks need client
  support. This implementation uses ordinary start/status/result tools and a
  durable SQLite queue so older MCP clients can also operate it. Native protocol
  task support is not claimed. Existing pinned FastMCP is retained.
* [Scrapy job persistence](https://docs.scrapy.org/en/latest/topics/jobs.html):
  persist progress and queued requests. Here cursors, frontier, page receipts and
  fetched rows are committed together; expired leases recover interrupted work.
* [Scrapy throttling](https://docs.scrapy.org/en/latest/topics/autothrottle.html):
  bounded concurrency and backoff instead of aggressive retries. Four workers,
  one active task per source, one memory-heavy JobSpy subprocess at a time on the
  existing 1 GiB Oracle instance. This is a small custom collector, not Scrapy.
* [JobPosting data](https://developers.google.com/search/docs/appearance/structured-data/job-posting):
  parse structured public listings locally rather than sending HTML to a model.
  Public RSS/Atom and sitemap links supplement fixed API adapters.
* [JobSpy](https://github.com/speedyapply/JobSpy),
  [Greenhouse](https://docs.greenhouse.io/job-board.html),
  [Lever](https://github.com/lever/postings-api): reuse source adapters and fetch
  public employer listings directly when recognized. Library/site limitations
  still apply. The Claude connector documentation export was unavailable during
  research; no claim of directory certification or Claude-specific acceptance.

## Runtime

`start_job_search` commits a run and work items and returns without waiting for
upstreams. Workers request one page/frontier step. Each completed page saves jobs
and its next cursor in one transaction. A process restart preserves committed
work and retries abandoned leases after 180 seconds. No six-hour expiry applies
to v4 data. SQLite WAL supports readers during collection; the Docker data volume
holds `harvest.sqlite` separately from legacy `results.sqlite`.

Full-feed/public-site tasks run once per source, not once per synonym. Query APIs
and boards keep separate cursors per caller query. Empty queries request unfiltered
collection where the source allows it; this is not a guarantee that a search-only
board exposes its entire database. Configured page limits become `limited` with
the cursor intact. `resume_search` adds pages without repeating completed sources.

Transient failures get bounded retries. Hard API failures or repeated errors first
switch to public structured retrieval. Remaining failures become `needs_recovery`,
with the original error, URL and public search alternative. The host can contribute
observed listings in batches. Successful sources do not generate obligatory browser
checks. Recovered data is explicitly client-reported, not server-attested.

No model is called during collection, extraction, deduplication or full-text search.
Cards omit descriptions. `read_job` retrieves a bounded text segment and can enrich
missing AA/LinkedIn text through fixed endpoints. SQL FTS indexes the retained text;
an empty keyword match does not establish that missing descriptions lack that job.

## Honest limits

* Public structured crawling is not a custom parser for all catalog sites. Sites
  requiring rendering, search forms, login, disallowed robots routes, or inaccessible
  data stay explicit gaps. Sitemaps/frontiers and pages have visible resource limits.
* No CAPTCHA solving, login bypass, TLS bypass or proxy rotation to evade barriers.
  Public alternative sources are separate evidence, not proof of board coverage.
* Full-text matching is lexical, not a paid embedding or model service. Callers can
  provide up to 12 variants; `query=""` returns all collected candidates.
* Search pages are a live view while the run grows; stable job IDs allow deduplication.
  First/last seen are collection times, not proof of posting date or application status.
* The existing owner deployment is not a multi-tenant SaaS: no per-user identity,
  billing, OAuth onboarding or private applicant-data storage was added.
* Retention is operator-managed. Monitor disk use; no invisible time-based deletion.
* Legacy `server.py` remains runnable for v3 integrations. The default container
  serves the smaller v4 contract; reconnect clients to refresh tool schemas.
