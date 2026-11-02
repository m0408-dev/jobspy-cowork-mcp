# 4.2.0: search integrity and retrieval diagnostics

This release includes the previously uncommitted query-routing/content-integrity changes plus the November 2 read-path corrections. It does not introduce any applicant, profession, salary or seniority exclusion.

- StepStone, XING and Adzuna public routes carry the caller's query; other public pages attempt observed same-origin GET search forms. Homepage access is not reported as a completed search. Unknown/blocked routes remain explicit host-browser recovery tasks.
- Listing cards and pagination are collected without inventing descriptions. Raw non-job records remain accessible separately. Missing-description records can be enumerated. Literal required terms are opt-in.
- Full-text reads use the indexed ID-to-rowid locator instead of joining on the FTS UNINDEXED text ID. A bounded read-only production probe interrupted the former COUNT after 5.3 seconds; the locator variant completed with 1,370 matches in 2.5 seconds under live load. This is a diagnostic comparison, not a universal latency promise.
- Unix seconds/milliseconds now contribute posting-age evidence. Raw dates remain unchanged.
- Certificate verification failures have a bounded, non-sensitive reason code and enter recovery immediately instead of repeating ineffective retries. TLS validation remains enabled.
- The response explicitly distinguishes the selected source market from each job's location/residency eligibility. International feed records are not silently deleted.
- Async description/start tools offload SQLite work; synchronous tools already run in FastMCP's worker pool.

Validation covers query routes, missing descriptions, owner isolation, page retention, negative non-job fixtures, timestamp formats, certificate errors and the indexed read path. Live deploy requires the existing candidate/backup/rollback helper and MCP protocol probe.

Remaining limitations: source access controls, page budgets, unsupported site layouts, employer eligibility and salary provenance need explicit review. A stored vacancy is not a verified application recommendation; salary estimates must not be treated as an employer budget. A recovery task is not an executed browser visit. No claim of complete market coverage.
