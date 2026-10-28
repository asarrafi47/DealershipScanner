# N35 Repo hygiene (18A) - RESUME

Branch phase/N35, merged into integ/draft at 7becf45df (heads 0a18429ca / f5d9f380f / d83a0e53a). Draft only.

| Unit | Head | Status |
|---|---|---|
| P18A.1a, P18A.2 | 0a18429ca | DONE |
| P18A.3, P18A.4 | f5d9f380f | DONE |
| P18A.8 | d83a0e53a | DONE (ruff not fully green) |
| P18A.1b | - | owner decision |
| P18A.5-P18A.7 | - | need N14 |
| P18A.9-P18A.11, P18A.X | - | serial, later |

## Deferred checks
- Copy local scanner counter into workspace/state once
- git status clean after a local dealer_pipeline run
- Fix 3 pre-existing ruff errors (inventory_recovery.py:116, test_runner_pause_safe.py:126, test_gallery_harvest.py:111)
- Full offline suite not run

## Owner asks
- Decide drop vs discovery for the dealer ids before dealers_28173_50mi.json / backend/dealers.json deletion
