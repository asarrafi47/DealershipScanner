# N17 Precedence close-out and prod heal (Phase 8C) - RESUME

Branch phase/N17 (head 598b2f8ef), merged into integ/draft at 758ba8a50. Draft only.

| Unit | Head | Status |
|---|---|---|
| P8C.1a | 598b2f8ef | DONE |
| P8C.5a | 598b2f8ef | DONE (pg tests not run) |
| P8C.4 | 7d42de72d | DONE |
| P8C.1b | - | SKIPPED (needs N16 apply helper) |
| P8C.2 | - | SKIPPED (needs N15 decode_vins refresh API) |
| P8C.3 | - | SKIPPED (after N16 merged) |
| P8C.5b, P8C.6, P8C.7 | - | SKIPPED (owner-gated data/prod ops) |

## Deferred checks
- 3 PG tests in test_relabel_spec_provenance.py (dry-run vs applied counts, backup table, guarded skip): TEST_PG_ADMIN_DSN unset, not run.
- Add the errors_index.md line (feed reverted vPIC/sticker values on rescan, fixed by P8B.1) in workspace/dealer_logs/_learning after merge to MAIN.

## Owner asks
- Pick the prod window for dump, heal, relabel, relink, verify.
- OK for the local relabel apply (P8C.5b) and heal (P8C.6); relabel_spec_provenance needs --apply --owner-ok.
