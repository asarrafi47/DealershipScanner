# N10 Scan controls and pre-fleet tooling - RESUME

Branch phase/N10 (head d25ea634d), merged into integ/draft at 3fd48b1ba, no conflicts. Draft only.

| Unit | Head | Status |
|---|---|---|
| P11A.1, P11A.4 | b8b875639 | DONE |
| P6A.1, P6A.2 | 8102f5329 | DONE |
| N10.8 | 7e5cd453b | DONE |
| P5B.5, P5B.6 | e18fe87aa | DONE (apply not run) |
| P6A.5, N10.7 | d25ea634d | DONE (runs deferred) |
| P11A.2 | - | SKIPPED (needs N9 on main) |
| P11A.3 | - | SKIPPED (needs N8 release, N9 on main, P11A.2) |
| P6A.3' | - | SKIPPED (needs N8.6 V030 merged) |
| P6A.6 | - | SKIPPED (recipes.py owned by N9; apply needs owner OK) |
| P6A.4, P6A.7 | - | SKIPPED (Wave 4, serial) |

## Open after merge
- Merge test run: 46 passed, 16 skipped (PG tier). Tests of the other modified sources (scan_lock, cli, orchestrator, runner, fleet_scan, retirement_log) were not run separately on integ/draft.
- P11A.2 must make the pipeline retry on exit 75; run.py still works because it scrapes the log line.
- A lock probe briefly holds the flock, so a concurrent acquirer can lose spuriously and get 75.
- Old-style PID-file scanners are invisible to the new probe (mixed rollout only).
- Plan said listing_retirements.path; V032 has writer/created_at. WRITERS gained restore and restore_rollback.
- Mixed disown+reconcile same-second batches without provenance are listed as excluded:mixed_disown for hand review.
- Replay in audit verify can refresh recipe-file field_coverage (same as audit_recipe_coverage).

## Deferred checks
- P5B.6 real replay and VDP fetch; rollback diff 0 on a real copy.
- N10.7 on real DBs: ownership_snapshot.sh on cars (09-28) and cars_cleanroom, then ownership_diff --census; expect the RCA split (3,941+59 / 1,344+26 / 931+289 / 3,048).
- pull_railway_logs.sh: confirm `railway ssh ... tar` syntax; P6A.5 prod run needs a post-run hold (N12 timing).
- scan_leases and audit PG tiers beyond the local run.

## Owner asks
- Review candidates.csv before any audit_retirements --apply (P5B.6).
- OK for recipe-hygiene local apply counts (P6A.6).
- Read-only prod access for the N10.7 census.
- D-IF5, D-IF10, D-SR2 stand as accepted.
