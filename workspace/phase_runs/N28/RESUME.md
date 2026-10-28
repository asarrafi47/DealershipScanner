# N28 Legacy DDL deletion, backup drops, scratch DB retirement - RESUME

Branch phase/N28, merged into integ/draft at 35f3867e4 (heads db2f2f94d / a0dc2e749). Draft only.

| Unit | Head | Status |
|---|---|---|
| P13B.4a | db2f2f94d | DONE |
| N28.5a | a0dc2e749 | DONE |
| P13B.1-P13B.3 | - | blocked on N27 |
| P13B.4b/c | - | owner-gated |
| N28.5b | - | owner op |

## Deferred checks
- Real dumps of backup tables (use PG18 binaries against prod)
- pg roundtrip test passed on local throwaway DB only
- V034 number vs N30

## Owner asks
- OK the backup-table dumps and the drop migration
- Per-DB OK flags and cars_cleanroom decision for scratch DB retirement
- Decide on plan snapshot tables (--include-plan-snapshots)
