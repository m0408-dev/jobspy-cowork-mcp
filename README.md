# JobSpy MCP v3

Personal job-search MCP (FastMCP, Streamable HTTP or stdio). Compact output,
explicit market selection, persistent pagination and source-level coverage reports.
No paid API or model call is required by this server.

## Search contract

- `market="germany"` is the default: Arbeitsagentur + Arbeitnow, without US-heavy feeds.
- `market="international"` explicitly selects the remote API group instead. It does not
  add Germany's federal database. `sources=[...]` is an explicit override for either mode.
- Working language is distinct from geography. Use German-language keywords / "German speaking"
  for international searches. `german_evidence` labels explicit language signals, German ad text,
  or unknown; it does not promise German is the working language or discard uncertain postings.
- All occupations are allowed. No CV, salary, seniority, phone-share or profession filter is hardcoded.
- `expand_query`, `include_jobspy` and `fetch_details` default to **false**. More upstream work is opt-in.
  Explicit `search_terms` are unioned. Optional expansion uses documented German word families in sources.py.
- `remote_only` on API searches is a ranking boost, **not** a strict filter and no longer adds redundant
  checkbox queries. Direct `search_jobs(is_remote=True)` uses the upstream filter, without rewriting source flags.
- Text-based remote labels are heuristics, not verification. Missing bodies, negations and mixed signals are visible.
  Application portals and residence/office obligations still require live verification.

## Tools

| Tool | Purpose |
|---|---|
| `search_all_jobs` | Market-scoped API search, optional explicit JobSpy boards |
| `search_german_jobs` | Federal German database only |
| `search_remote_jobs` | Remote-oriented search, Germany default, international opt-in |
| `search_jobs` | Direct selection of eight JobSpy boards; explicit location/country |
| `get_result_page` | Next compact/detailed page from a saved result |
| `get_job_details` | Stored texts for up to three IDs; optional missing AA details only |
| `list_job_sources` | Capabilities/defaults/coverage gaps, without network calls |
| `search_employer_jobs` | Explicit Greenhouse, Lever or Personio employer board |
| `discover_job_sources` | Optional Bing RSS discovery of additional board/employer links |

### Token-efficient workflow

1. `search_all_jobs(search_term="IT Support", search_terms=["Anwenderbetreuung"], max_pages=5)`.
2. A result contains `result_id`, `total_fetched`, compact `jobs` and `next_offset`.
3. Use `get_result_page(result_id, offset=next_offset)` to continue **without re-scraping**.
4. Fetch shortlisted texts by ID with `get_job_details`. Long texts have `next_text_offset`.
   `fetch_missing=true` requests AA bodies only for those selected jobs; unsupported missing texts
   require browser inspection. No hidden bulk enrichment.
5. International searches are separate calls, e.g. `market="international", search_term="German support"`.
   International JobSpy additionally requires explicit `location` and `country_indeed`.

Compact JSON omits descriptions and null fields; default page size is 30 with a 24k-character budget.
**Fetched jobs are never removed to fit a response.** Every fetched row is saved first. Detailed pages may
show excerpts; stored full descriptions remain available through the detail tool. Snapshot/storage failures
raise explicit errors rather than pretending a partial list is complete.

### Upstream pagination and coverage

Result pagination (`next_offset`) and upstream pagination (`source_offset`) are different:

- Saved `next_offset` is a row offset in an immutable search snapshot.
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
