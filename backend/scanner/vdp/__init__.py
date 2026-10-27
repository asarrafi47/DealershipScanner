"""
VDP (vehicle detail page) helpers for HTTP-only scans.

- prefetch       — per-car detail-page HTML pass + DB carry-forward (scan time)
- vdp_recipes    — per-dealer VDP JSON recipe capture/replay
- extract        — capture analysis / EP fragments used by vdp_recipes
- html_recovery  — gallery/description recovery from listing HTML (post-scan)
- spec_fetch     — single-URL spec fetch (enrichment)
- queue          — per-row gap predicates (spec gap, HTTPS gallery count)
- config         — env knobs (VDP concurrency)

Import names from the submodule that defines them; the package re-exports nothing.
"""
