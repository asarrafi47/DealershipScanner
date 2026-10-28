# N25 EPA data repairs and prod rollout (10D) - RESUME

Branch phase/N25, merged into integ/draft at 63e85b616 (head 68d02156a). Draft only.

| Unit | Head | Status |
|---|---|---|
| P10D.1-code, P10D.2-code | 3c1cb5641 | DONE on fixtures |
| P10D.7 | 5144e6ec3 | DONE (V038), pg tests passed on throwaway DBs |
| P10D.8-code | 68d02156a | DONE code half |
| P10D.9 | f015e9c06 | DONE docs |
| P10D.5-code | - | skipped, shipped in N24 (P10C.9) |
| P10D.3-P10D.6, release | - | not in this chunk |

## Deferred checks
- Real-data patch of 12,126 EPA and 10,614 Complete_Options CSVs with pinned vehicles.csv + sha256; spot checks (Integra, RDX, A6, XC90 T8, C230, Jetta); manifest.json comparison
- V038: merge only after P10D.6 (every migrate --apply and prod boot applies pending files); VALIDATE migration is V039 (N26.8) after a read-only prod check of 0 dangling links; rollback = drop both FKs before pg_restore of epa_master
- DICTIONARY/ deletion, .dockerignore:88 / .railwayignore:98 removal, final git grep, build_epa_master_pg --dry-run (after P10D.6 and prod provenance backfill)
- Run scripts/audit_root_dictionary.py --report workspace/phase_runs/N25/dictionary_audit.json (255 Complete_Options files with root-unique rows vs plan's 13)
- _learning notes (post-merge MAIN)
- Merge-time pg-tier tests not run (no TEST_PG_ADMIN_DSN)

## Owner asks
- Provide pinned vehicles.csv; approve data patch apply
- Read the audit JSON, then OK the DICTIONARY/ deletion (D-ER5)
- Approve V038 timing relative to P10D.6
