"""
Dealership inventory scanner (HTTP-only network replay; a browser only in discovery behind
SCANNER_ALLOW_BROWSER): CLI, VDP enrichment, DB upsert, post-scan pipeline.

Subpackages / modules:

- ``cli`` — manifest-driven scan entrypoint (also runnable as repo-root ``scanner.py``).
- ``vdp`` — HTTP detail-page pass (prefetch) and VDP JSON recipes.
- ``database`` — ``inventory.db`` upsert and scanner-adjacent DB helpers.
- ``post_pipeline`` — repair, listing parse, vision passes, enrichment orchestration.
- ``listing_gap_fill`` — optional gap-fill after scan.
- ``inventory_reconcile`` — soft-unlist rows missing from a dealer scrape.
- ``scrapers`` — inventory JSON merge, Next.js helpers, intercept URL gating.
- ``utils`` — VDP HTML / gallery / price parsing helpers (shared with listing gap fill).

Scanner DB upserts live in ``backend.scanner.database``.
"""
