# N22 EPA catalog code foundation (10A) - RESUME

Branch phase/N22, merged into integ/draft at fa2c19a8f (head da42873ad). Draft only.

| Unit | Head | Status |
|---|---|---|
| P10A.1, P10A.2 | f9b2a9e62 | DONE |
| P10A.3a | e1b029b16 | DONE |
| P10A.7 | 940b9d88f | DONE |
| P10A.4, P10A.6 | b051be686 | DONE |
| P10A.5 | d4995a331 | DONE code |
| P10A.8 | da42873ad | DONE code |

## Deferred checks
- P10A.3b apply V036 locally/prod (owner, quiet window, SHARE lock on cars)
- P10A.5b real-data patch/verify on pinned vehicles.csv.zip (D-EPA1)
- PG tests not run in merge: P10A.8 apply/restore round trip
- test_no_sqlite fails on 2 entries: link_cars_to_catalog.py qmark count (P10A.7), test_backfill_epa_master_provenance.py db_path_literal (P10A.6); regenerate allowlist or fix
- Run the provenance backfill on the real DB BEFORE P10D.8 deletes DICTIONARY/

## Owner asks
- Update plan registry and N22 exit gate text: V032 -> V036, "migrate --target 31" -> target 36
- D-EPA1, D-ET2, D-ET3, D-ET5, D-ER2; approve pinned zip
- Forced-induction ambiguity follows spec literally (multi-source key = ambiguous): confirm
