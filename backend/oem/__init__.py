"""OEM reference data.

Sub-packages:
  backend.oem.vehicle_reference — BMW vehicle spec / reference SQLite DB, EPA web-service
  client, CSV export (CLI: ``python -m backend.oem.vehicle_reference.cli``).

The BMW dealer-locator intake (``oem.intake``) and the crawl4ai / Playwright scrapers
(``oem.scraper``) were deleted on 2026-10-01 (docs/monolith_audit_2026_10_01/enrich.md #12).
The read side of the intake SQLite store that pgvector still embeds lives in
backend/vector/bmw_store.py.
"""
