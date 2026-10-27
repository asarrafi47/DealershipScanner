# N32 Shims and dead functions (17A) - RESUME

Branch phase/N32, merged into integ/draft at 16003e806 (head 0d3a68286). Draft only.

| Unit | Head | Status |
|---|---|---|
| P17A.1 | cf19e12ad | DONE |
| P17A.2 | 02bdb0fc3 | DONE |
| P17A.3, P17A.9 | e999ca885 | DONE |
| P17A.5, P17A.6 | e06cce919 | DONE |
| P17A.7, P17A.10, P17A.11 | 1b00dc4fb | DONE |
| P17A.8 | 0d3a68286 | DONE |
| P17A.4 | - | blocked on N19 (Wave C) |
| N32.owner-1 | - | owner N1.7 archive, not code |

## Deferred checks
- P17A.4 after N19 merge (N19 is in integ/draft now; re-check dependency and run Wave C)
- Full suite not run; only touched test files (330 + 139 + 52 passed in merge)
- run.py import not checked (needs INVENTORY_DATABASE_URL)

## Owner asks
- N1.7 archive and notice before merging P17A.3 to main
- Confirm with attribution owner the golden edit `A.match is match` (test_attribution_golden_20261001)
- Env cleanup: SCANNER_WARMUP_PHASE_TIMEOUT_SEC, SCANNER_VDP_PHASE_TIMEOUT_SEC, ANTHROPIC_VISION_MAX_RETRIES, SCANNER_FAILURE_HAR=0 lines in deploy/car-scanner/cronjob.yaml and scripts/cleanroom/run.env.template
- Four singleton dead-function candidates for a later sweep: inventory_reconcile._reconcile_enabled, gap_fill._merge_provenance, ownership.tier_rank, image_text.has_content
