"""
Post-scan repair, enrichment, and window sticker pipeline.

- pipeline       — main run_post_scan orchestrator (repair, vision, enrichment)
- job            — standalone CLI for deferred post-scan runs
- gap_fill       — fill missing fields from listing HTML (+ optional DuckDuckGo)
- window_sticker — OEM Monroney PDF fetch and parse by VIN WMI
"""
