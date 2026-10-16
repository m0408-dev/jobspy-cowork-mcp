# 4.1.1: bounded text-index updates

Production diagnosis found 66,758 FTS documents. Looking up one ID in the
UNINDEXED FTS id column took 1.711 seconds and EXPLAIN confirmed a full scan.
Every collected job performed that scan inside a write transaction, which can
exceed the 30-second writer timeout during normal page batches.

The store now keeps an indexed id-to-FTS-rowid locator and addresses FTS writes
by rowid. A single startup scan reconstructs locators without replacing the
existing FTS index or altering job IDs, owner isolation, saved tasks or cursors.
Reconstruction also handles rowid changes made by an older rollback release.
Locator updates share the same atomic transaction as job/page writes.

tests/test_store_index.py covers indexed access, updates, legacy rowids,
rollback reconstruction, search freshness and transaction rollback. The optional
tests/bench_store.py benchmarks against a temporary backup of a read-only mounted
production volume; it never writes to the source database.

This fixes a verified scaling bottleneck, not every possible SQLite contention
scenario or upstream board failure. Source failures and limits remain explicit.
