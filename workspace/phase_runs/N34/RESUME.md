# N34 Module splits and the knob registry (17C) - RESUME

Branch phase/N34, merged into integ/draft at b386980a3 (head 61f9aaa9e). Draft only.

| Unit | Head | Status |
|---|---|---|
| P17C.1, P17C.2 | d730ff130 | DONE |
| P17C.3 | 9f28319ce | DONE |
| P17C.4 | e939bd5e8 | DONE |
| P17C.5 | 0c88ee3e0 | DONE, pg tier not run |
| P17C.6 | 61f9aaa9e | DONE |
| P17C.7a, P17C.7b, P17C.8, P17C.9 | - | skipped / blocked |

## Deferred checks
- Re-run test_brochure_extract_surface.py::test_ladder_stores_does_not_load_pdfplumber on integ/draft (failed in merge check; P17C.3 now merged)
- Socket-count check over the ~20 upsert tests for the vPIC offline stub
- PG-tier comments_db tests (52 skipped, no TEST_PG_ADMIN_DSN)
- 2 order-dependent test_import_epa_master_append dry-run failures (RealDictionaryWriteBlocked) in wide runs; pass alone; not checked on a clean baseline
- P17C.7a needs owner Railway/mini variable export (P0A.1); P17C.8 after P17C.7a; P17C.9 at the release step

## Owner asks
- Export Railway/mini env variables; decide retirement of browser_era knobs (SCANNER_VDP_EP_MAX, SCANNER_VDP_PRICE_MAX, SCANNER_VDP_DOWNLOAD_IMAGES)
