# N13 Recipe store authority (Phase 7) - RESUME

Branch phase/N13 (head 5161fb802), merged into integ/draft at c4a8bf165. Draft only.

| Unit | Head | Status |
|---|---|---|
| P7.1, P14A.9 | 632aa2c64, 31e608054 | DONE (V034) |
| P7.2 | 426baf3ee | DONE |
| P7.3 | 43a5aafd4 | DONE |
| P7.4, P7.9a | cc084f6f9 | DONE |
| P7.5 | 96d179a5e | DONE |
| P7.6 | b16f2daf6 | DONE |
| P7.7, P7.8 | 5161fb802 | DONE |
| SQLite ladder conversion | - | DEFERRED (six legacy test files keep it) |

## Deferred checks
- PG tier for test_recipe_store_cas_pg on a TEST_PG_ADMIN_DSN server.
- reconcile_recipe_store --compare-cache on real data.
- Full conversion of the six legacy SQLite test files.

## Owner asks
- Apply V034 to target DBs.
- Decide when to finish the SQLite-ladder conversion.
