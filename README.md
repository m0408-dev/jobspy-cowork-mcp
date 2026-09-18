# JobSpy MCP v3

Generic job-search MCP (FastMCP, Streamable HTTP or stdio), independent of any CV or profession. Compact output,
explicit market selection, persistent pagination and source-level coverage reports.
No paid API or model call is required by this server.

## Search contract

- `search_all_jobs(market="germany")` defaults to six direct sources: Arbeitsagentur, Arbeitnow,
  Indeed, LinkedIn, Glassdoor and Google. Independent browser checks are additional, not counted
  as already scraped sources. No US-heavy feeds are added automatically.
- `market="international"` explicitly selects the remote API group instead. It does not
  add Germany's federal database. `sources=[...]` is an explicit override for either mode.
- Every broad search also creates a persistent task for **every non-alias source and query**
  in its selected catalog market. `market="worldwide"` includes all 143 non-alias entries;
  the 144th entry is an alias, not another board. Research candidates remain explicitly unverified.
  API overrides do not silently shrink this browser scope. Use the narrow tools for targeted calls.
- Working language is distinct from geography and chosen by the caller, never forced to German.
  `german_evidence` is optional evidence metadata labelling explicit language signals, German ad text,
  or unknown; it does not promise German is the working language or discard uncertain postings.
- All occupations are allowed. No CV, salary, seniority, phone-share or profession filter is hardcoded.
- `include_jobspy=None` automatically enables the four Germany boards when `sources` is omitted.
  An explicit `sources` list restricts API selection and disables automatic JobSpy unless
  `include_jobspy=True` or `jobspy_sites=[...]` is supplied. `include_jobspy=False` always disables it.
  This keeps precise calls inexpensive while broad calls are actually broad.
- Only caller-supplied `search_term` / `search_terms` are queried, with case-insensitive deduplication.
  The old profession synonym/compound dictionaries have been removed. `expand_query=True` raises
  an explicit migration error; semantic alternatives belong to the AI, not the connector.
  `fetch_details` stays opt-in. Profession, qualification, salary and suitability are not server policy.
- `remote_only` on API searches is a ranking boost, **not** a strict filter and no longer adds redundant
  checkbox queries. Direct `search_jobs(is_remote=True)` uses the upstream filter, without rewriting source flags.
- Text-based remote labels are heuristics, not verification. Missing bodies, negations and mixed signals are visible.
  Application portals and residence/office obligations still require live verification.

## Tools

| Tool | Purpose |
|---|---|
| `search_all_jobs` | Broad six-source Germany default; explicit overrides and separate international mode |
| `search_german_jobs` | Federal German database only |
| `search_remote_jobs` | Same broad defaults with remote ranking, international opt-in |
| `search_jobs` | Direct selection of eight JobSpy boards; explicit location/country |
| `get_result_page` | Next compact/detailed page from a saved result |
| `get_job_details` | Stored texts for up to three IDs; optional missing AA details only |
| `list_job_sources` | Capabilities/defaults/coverage gaps, without network calls |
| `search_employer_jobs` | Explicit Greenhouse, Lever or Personio employer board |
| `discover_job_sources` | Optional Bing RSS discovery of additional board/employer links |
| `get_browser_tasks` | Paginated source/query tasks; optional pending-only view |
| `record_browser_check` | Persist observed browser evidence and import jobs without duplicates |
| `get_search_coverage` | Paginated audit of every selected source and its outstanding work |

### Token-efficient workflow

1. `search_all_jobs(search_term="IT Support", search_terms=["Anwenderbetreuung"], max_pages=5)`.
2. A result contains `result_id`, `total_fetched`, compact `jobs` and `next_offset`.
3. Use `get_result_page(result_id, offset=next_offset)` to continue **without re-scraping**.
4. Fetch shortlisted texts by ID with `get_job_details`. Long texts have `next_text_offset`.
   `fetch_missing=true` requests AA bodies only for those selected jobs; unsupported missing texts
   require browser inspection. No hidden bulk enrichment.
5. International searches are separate calls, e.g. `market="international", search_term="German support"`.
   International JobSpy additionally requires explicit `location` and `country_indeed`.
6. Execute `get_browser_tasks` pages with the host browser and record observations. With
   `pending_only=true`, restart at offset zero after updates (stable task IDs, shrinking view).
7. `get_search_coverage` separates not-attempted, attempted-but-pending, and checked documented scopes.
   Queuing is not execution. Blocked and partial checks stay open. No browser means an incomplete run.

Compact JSON omits descriptions and null fields; default page size is 30 with a 24k-character budget.
**Fetched jobs are never removed to fit a response.** Every fetched row is saved first. Detailed pages may
show excerpts; stored full descriptions remain available through the detail tool. Snapshot/storage failures
raise explicit errors rather than pretending a partial list is complete.

### Upstream pagination and coverage

Result pagination (`next_offset`) and upstream pagination (`source_offset`) are different:

- Saved `next_offset` is a row offset in a persistent snapshot; browser feedback can append/enrich jobs.
- API `source_offset` is a **page offset**. Arbeitsagentur/Himalayas report per-query
  `next_source_offsets`; continue a specific query with query expansion disabled and that offset.
  Arbeitnow/The Muse report a single `next_source_offset`.
- Direct JobSpy's `offset` is a **job offset**; aggregate `source_offset` also acts as the JobSpy job offset.
  For deep continuation use source-specific calls to avoid mixing units.
- `max_pages` bounds requests per query (or feed) per invocation. `results_per_source` is a retrieval
  target, not an output cutoff. Whole upstream pages may exceed it; fetched extras are retained.
- Remotive/RemoteOK/Jobicy/HN/WWR expose finite feed windows, not a complete market. They cannot
  be deep-paginated via `source_offset`. Date filtering is currently upstream AA/JobSpy only;
  other sources return dates for caller-side evaluation.
- Partial failures retain successful pages. HTTP errors are reported without leaking credentials.
  JobSpy can swallow upstream errors, so empty output is labelled `empty_or_blocked`, not "no jobs exist".
- `total_available` globally is **null**. Individual AA totals describe query pools with overlap,
  never the unique whole-market total.

## Honest coverage limits

Expanded source research is maintained in [SOURCE_CATALOG.md](SOURCE_CATALOG.md)
and machine-readable [source_catalog.json](source_catalog.json). The catalog now drives
runtime browser planning; market membership is data, not profession-specific code.
Existing adapters fetch automatically. Additional sources use host-browser tasks with
entry URLs, exact queries, filters and indexed-search fallbacks. Tasks are saved with the
snapshot and survive process restarts until the snapshot TTL (six hours by default).
The source scope is frozen per snapshot so later catalog additions cannot rewrite an old audit.
Neither a readable homepage nor a queued task is counted as a verified search or working scraper.

Nine public APIs, eight JobSpy adapters, three employer ATS types. XING, StepStone, Monster,
blocked listings and application flows still require an independent browser. Discovery is a search-engine
sample and returns **unverified links**, not evidence that a board or application has been checked.
There is no promise to scrape every site, bypass restrictions, or find every job.

## Run / test

Python 3.11 is used in Docker. Dependencies are pinned in `uv.lock` and exported `requirements.txt`.

```sh
uv sync --frozen
uv run python -m unittest discover -s tests -v
uv run python tests/live_smoke.py  # optional real external requests
MCP_TRANSPORT=stdio uv run python server.py
docker build -t jobspy-mcp:v3 .
```

Configuration: `MCP_TRANSPORT`, `HOST`, `PORT`, `MCP_HTTP_PATH`, `MCP_AUTH_TOKEN`, `JOBSPY_PROXIES`,
`JOBSPY_CONCURRENCY`, `RATE_LIMIT_PER_MIN`, `LOG_LEVEL`, `MAX_RESULT_CHARS` (8k–60k; default 24k),
`RESULT_DB`, `RESULT_TTL_SECONDS` (default 21600), `RESULT_CACHE_BYTES` (default 64 MiB).
Successful public HTTP responses are cached for 15 minutes; HTTP failures for one minute; cache capped at 32 MiB.
Each JobSpy board/query runs in an isolated child process with a hard deadline
(`JOBSPY_TIMEOUT_SECONDS`, default 40, range 5–120). A timed-out worker is killed and
reported as a source error with browser handoff; it cannot hold a scraper thread indefinitely.
No model/API subscription costs are generated by the server itself. It is designed for a single owner;
snapshot IDs are opaque bearer capabilities, not a multi-tenant access-control boundary.

For HTTP, bind Docker to loopback behind TLS. Preserve the existing secret path/auth configuration;
do not publish it in Git or logs. Persist `/app/data` in a dedicated volume owned by uid 10001.
Production updates require a tested immutable image, candidate health/protocol checks and retained rollback container.
`deploy/release.py` stages/tests/cuts over the existing `jobspy` container while retaining the previous container.
Use `--rollback <backup-name>` to restore it. Deployment requires separately authorized administrator access;
the restricted operational account is intentionally not a Docker administrator.

## v2 migration

Tool names remain; output now has `result_id/next_offset` instead of discarded tails. Default markets and
expansion/enrichment defaults changed intentionally. Remote searches no longer silently fan out internationally.
Client tool schemas may need refresh/reconnection. Direct proxy overrides were removed from tool arguments;
operators configure proxies server-side. Existing callers should consume the v3 schema instead of old assumptions.

## References

- https://modelcontextprotocol.io/specification/2025-06-18/server/tools
- https://github.com/speedyapply/JobSpy (board/filter limitations)
- https://himalayas.app/api (search uses page-based pagination)
- https://developers.greenhouse.io/job-board.html
- https://github.com/lever/postings-api
# Browser handoff (v3.1)

An API error or ambiguous empty result now creates a persisted browser task with
the query, market, location and a concrete navigation/search URL. Broad
`search_all_jobs` calls also create independent checks for LinkedIn, XING, Indeed,
StepStone, Glassdoor and Monster. These are a minimum sweep, not an exhaustive
registry of the world's job boards. Additional failed adapters, ATS requests,
discovery requests and missing shortlisted descriptions also create tasks.

The host assistant reads `get_browser_tasks`, uses its available browser, reads
individual listings and their Apply flow, then sends actual observations and jobs
through `record_browser_check`. Browser jobs join the existing snapshot with stable
IDs and URL-based deduplication. No application is submitted. The feedback tool
is correctly marked as a non-destructive write, not a read-only tool.

`partial`, `blocked` and `login_required` remain pending. The host is instructed to
try another available browser, indexed search and public employer pages, without
bypassing access controls. Evidence is explicitly **client-reported**; the server
cannot itself operate or attest another MCP server's browser. A host without a
browser must report that missing capability, not claim a check succeeded.
`checked` means the documented browser-check scope, never exhaustive market coverage.

Task pages and feedback are bounded; search replies carry only compact handoff
counts. Germany stays the default, international adapters remain explicit opt-in.
# Access recovery (v3.4)

Direct API failure never proves that the public website is inaccessible. Broad tasks retain a query-specific fallback URL and dated browser observations from `source_access.json`. Historical observations never complete a current task.

Host workflow: call `get_browser_tasks(unattempted_only=true)` at offset 0, execute the returned tasks in the host's available browser, and record each attempt. Reject optional cookie banners, wait for actual results, and inspect the requested query. Then audit remaining tasks with `pending_only=true` and `get_search_coverage`. Filtered queues change after writes, so restart offset 0. Stop retrying an unchanged hard barrier; report it and use official employer/indexed alternatives without pretending the original board was checked.

`record_browser_check` requires `inspection_stage=search_results|listing|application` and `issue=none|api_access_denied` for completed checks. Homepages, loading states and unresolved browser problems must remain partial/blocked. Issues distinguish cookie, URL, DNS, TLS, network, rendering, bot, login and eligibility problems. Do not bypass security warnings or confirm user eligibility. A browser is controlled by the host, not by the remote MCP; if unavailable, explicitly report incomplete coverage.

## Search correctness repair (v3.5)

- Arbeitsagentur search uses the currently working `/pc/v6/jobs` endpoint and maps
  `ergebnisliste`, reference numbers, companies, multiple locations, publication dates,
  full-time and raw home-office policy. Home-office availability is not full-remote proof.
  Unknown response schemas raise an error, never a false empty success.
- Broad JobSpy searches now page beyond 100 records. `results_per_source` is the
  target per board/query, `max_pages` is the page ceiling per query, and
  `JOBSPY_SITE_BUDGET_SECONDS` (default 60, clamped 5–180) bounds total time per board.
  Queries run breadth-first. Per-query statuses/cursors expose time limits, deferred
  queries and repeated pages. Resume using `search_jobs` with one site, term and offset.
  Partial/empty windows are never a claim of market exhaustion.
- `remote_only` is forwarded to JobSpy's upstream remote filter. Other feeds retain
  ranking semantics. No generic scraper can attest 100% remote from a board badge.
  Indeed cannot combine its upstream date and remote filters; that conflict is recorded.
  Snapshots label known old, recent, future and unknown dates without discarding them.
- Himalayas repetition checks are per query; overlap between synonyms no longer
  prevents reaching later pages. Requests with a recency preference sort by recent.
- Every saved page carries compact incomplete-search status and unresolved source
  counts. Browser tasks are breadth-first across the entire selected catalog; no
  source/query pair is removed. LinkedIn/Indeed entry links carry requested filters.
  A queue is not browser execution: the host must perform and record those checks.
- HTTP uses JSON responses supported by Streamable HTTP. Unicode line separators
  are escaped for compatibility with simplistic clients. Correct SSE clients must
  split on protocol line endings, not Python `str.splitlines()` (which also splits
  inside valid JSON strings). A diagnostic-client bug caused the September 18 page
  parse failure; it was not loss of server-stored results.

Known external limits remain: upstream login requirements, bot/rate limits, finite
feed windows and client browser availability. Version 3.5 does not add scraping
adapters for all catalog entries or make protected boards publicly accessible.
The caller must report incomplete coverage while browser tasks remain unresolved.

Protocol and source references: [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports),
[AA community-maintained schema](https://github.com/bundesAPI/jobsuche-api),
[Himalayas API](https://himalayas.app/api), [JobSpy limits](https://github.com/speedyapply/JobSpy).
