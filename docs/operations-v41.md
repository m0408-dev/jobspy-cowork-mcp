# v4.1 operations and verification

## Access

HTTP uses FastMCP TokenVerifier and stateless Streamable HTTP, with a fresh token
check on every request. Opaque keys are generated with 384 bits of entropy;
only SHA-256 digests are stored in access.sqlite. Tools derive user identity from
verified request context, never from a caller-supplied user parameter. Search IDs
are not authorization. Local stdio/in-process clients run as operator.

For a self-hosted operator, set MCP_AUTH_TOKEN to a strong secret and configure
the client Authorization bearer header. To issue a separate user credential on
the deployed container (administrator only):

```sh
docker exec jobspy python access.py issue alice --output /app/data/alice.key
docker exec jobspy python access.py revoke alice
```

Transfer the private key file securely to that user and remove the plaintext file
after delivery. Never put credentials in issue descriptions, logs or source code.
AUTH_DB defaults beside HARVEST_DB; the management CLI defaults to /app/data/access.sqlite.
For local installations set AUTH_DB explicitly for both server and CLI.
Reissuing the same subject means the same user, not a new isolated identity.
Revocation disables all issued keys for that subject immediately. User data stays
available for backup; revocation is not data deletion.

MCP_HTTP_PATH retains the existing private owner route for upgrade compatibility.
That URL itself grants owner access, without requiring a bearer header. Do not
share it with other users. Public /mcp/ always requires a valid key. The default
/mcp/ path never enables anonymous access. MAX_ACTIVE_SEARCHES_PER_USER defaults
to five and applies to both start and resume. Global source concurrency protects
upstream services, and existing per-IP request limits remain enabled.

This is controlled bearer-key onboarding, not an OAuth authorization server or
public signup service. Clients must support bearer headers (or local stdio).
Clients requiring OAuth login need an external identity-provider integration.
FastMCP explicitly documents this distinction:
https://gofastmcp.com/servers/auth/token-verification

## Search semantics and data quality

Broad mode ORs literal query word prefixes, including German compound beginnings;
all_words requires exact words. Variants are caller-provided. This is lexical
retrieval, not a guarantee of semantic suitability. Missing descriptions and
uncollected sources can still hide matches. query="" exposes every collected card.
Cards include remote_evidence, source_remote_flag, posted_age_days,
expiry_evidence and availability=not_reverified. Remote labels are evidence,
not a promise of geographic eligibility. No profession/remote/date hard filter
runs during collection. Possible_duplicate_group is a hint only; separate URLs
remain separate vacancies. New nonempty descriptions replace older versions even
when shorter; missing descriptions do not destroy previously collected text.
Search result order can change while collection/enrichment is running.

Ashby excludes postings explicitly marked isListed=false, as required by its
public API visibility semantics. SmartRecruiters persists offsets and refuses to
report exhaustion for malformed or prematurely empty responses. Its list API has
no description; this absence remains explicit rather than generating text.
References:
https://developers.ashbyhq.com/docs/public-job-posting-api
https://developers.smartrecruiters.com/docs/endpoints

## Verification and limits

The regression corpus covers IT, care, finance, electrical trades, marketing and
logistics. It checks specified lexical candidates are retained, not real-world
recall across the entire internet. HTTP tests use two credentials and attempt all
seven search-specific tools against another user's search; reads and mutations
are denied. Tests also verify anonymous rejection, immediate revocation, the
private owner route, per-user reuse/quota, update freshness and duplicate hints.

Live acceptance on 2026-10-14: Ashby/Ashby returned 63 listed jobs, all with text;
SmartRecruiters/BoschGroup returned two 100-job pages without overlapping URLs.
These are adapter checks, not claims about jobs suitable for a particular person.
Run python live_quality.py to repeat this optional network check. Counts change.

The release helper validates the candidate with the real MCP HTTP protocol,
backs up SQLite with its backup API (including WAL), retains the previous
container, checks production health/protocol and rolls back a failed switch.
Backups are /app/data/harvest.sqlite.backup-<timestamp>. The schema migration only
adds owner with default owner, preserving prior searches and IDs. Container
rollback keeps the migrated database; restoring a backup is a separate admin
operation requiring stopped writers and preservation of the current database.
Back up access.sqlite with SQLite backup as well to preserve issued identities.
The local volume is persistent, not an off-host disaster recovery service.

No scraper can guarantee all vacancies, bypass unavailable upstream access, or
make a host perform recovery autonomously. Status distinguishes source failures,
page limits and verified completion. Review get_search_status(include_sources=true)
and recovery tasks rather than interpreting zero matching cards as zero jobs.
