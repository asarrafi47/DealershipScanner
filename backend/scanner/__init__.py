"""
Dealership inventory scanner (Playwright): CLI, VDP enrichment, DB upsert, post-scan pipeline.

Subpackages / modules:

- ``cli`` — manifest-driven scan entrypoint (also runnable as repo-root ``scanner.py``).
- ``vdp`` — VDP page enrichment (gallery, EP, price hints).
- ``bmw_enhancer`` — BMW-specific timing/selectors.
- ``database`` — ``inventory.db`` upsert and scanner-adjacent DB helpers.
- ``post_pipeline`` — repair, listing parse, vision passes, enrichment orchestration.
- ``listing_gap_fill`` — optional gap-fill after scan.
- ``inventory_reconcile`` — soft-unlist rows missing from a dealer scrape.
- ``scrapers`` — inventory JSON merge, Next.js helpers, intercept URL gating.
- ``utils`` — VDP HTML / gallery / price parsing helpers (shared with listing gap fill).

Scanner DB upserts live in ``backend.scanner.database``.
"""
