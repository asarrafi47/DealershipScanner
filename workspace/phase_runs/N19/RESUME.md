# N19 One recipe validation rule (12B) and under-collection rescans - RESUME

Branch phase/N19, merged into integ/draft at d4bac2b5d (head 0c1d46914). Draft only.

| Unit | Head | Status |
|---|---|---|
| P12B.1 | 0ee10f00b | DONE |
| P12B.2 | 0ee10f00b | DONE |
| P12B.3 | 87191620a | DONE |
| P12B.4 | c457f0d24 | DONE |
| P12B.5 | f63fd0ca9 | DONE |
| P12B.6 | c8d66f88d | DONE |
| P12B.7a | 0c1d46914 | DONE code |
| P12B.8 | 0c1d46914 | MOSTLY |

## Deferred checks
- P12B.7b rejudge data run, local and prod (owner approval)
- P12B.9 canary/deploy (prod-owner)
- P12B.10 fleet rescans (owner-run)
- P0B.1 per-dealer union diff and the "only listed verdicts flip" comparison (needs P0B.1 offline data)
- PG tier skipped (TEST_PG_ADMIN_DSN unset in merge run): test_recipe_replace_archive, test_rejudge_live_recipes
- workspace/dealer_logs/_learning/platform_playbook.md entry (MAIN only)

## Owner asks
- Approve the P12B.7 re-judge run and the --apply id list (retirement owner)
- Approve the P0B.1 union diff review
- Behavior notes to confirm: Team Velocity pagination=none validates as one shot; promote_from_ledger now walks the full live set to the page cap; a duplicate request rejects the whole set (allow_shared=True for confirmed group feeds)
- Draft commit ids written into OWNER_DECISIONS and audit README must be updated if phase/N19 is rebased
