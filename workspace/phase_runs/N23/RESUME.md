# N23 EPA builder, provenance backfill, legacy re-resolve (10B) - RESUME

Branch phase/N23, merged into integ/draft at a40ade275 (head 135f0a059). Draft only.

| Unit | Head | Status |
|---|---|---|
| P10B.1 | 8242664a6 | DONE code |
| P10B.2 | c5c210c21 | DONE code |
| P10B.3, P10B.4-script | 4b63ea5b5 | DONE code/scripts |
| P10B.6 | beb8c1a40 | DONE code (V037) |
| P10B.5, P10B.8-script | 239cc0691 | DONE code/tool |
| P10B.7 | 135f0a059 | DONE code, baselines pending |
| P10B.4 | - | MAIN data run, not started |
| P10B.8 | - | MAIN data run, not started |

## Deferred checks
- P10B.4: V036 applied locally, quiescent shards (`--check-quiescent`), backup, provenance backfill, owner OK on counts (RUNBOOK_P10B4.md)
- Real-data builder dry run (P10B.1) and any `--apply` (P10B.2): only after P10B.4
- PG-tier tests not run in merge (no TEST_PG_ADMIN_DSN): P10B.3 provenance/re-run, P10B.7 seeded defects
- P10B.5 run waits for N25 10D steps plus a `--years` import and builder `--apply`
- P10B.7: record baselines in data_quality_invariants_baseline.json and remove ids from CATALOG_INVARIANT_IDS exemption, before the next nightly (else NO_BASELINE fails closed)
- P10B.8 sample (>=98% by VIN decode)

## Owner asks
- Decide one label: plan `dictionary_legacy_root` vs backfill `legacy` (re-resolve autodetects; cheapest fix renames SRC_LEGACY)
- Plan registry assigns N23 no migration number; V037 was taken
- OK on P10B.4 counts, re-resolve review, local apply
