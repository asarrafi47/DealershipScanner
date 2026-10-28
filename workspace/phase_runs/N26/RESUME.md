# N26 Scanner runtime remainder and upsert cost (11A rest) - RESUME

Branch phase/N26, merged into integ/draft at 1ff7624d6 (heads fdb9d6872 / 3e75db0b7). Draft only.

| Unit | Head | Status |
|---|---|---|
| P11A.6 | c3d1f6ac9 | DONE |
| P11A.7 | 8e4b9893f, fdb9d6872 | DONE |
| P11A.8 | 3e75db0b7 | DONE |
| P11B.1-P11B.4 | - | not in chunk (measurement-gated) |
| N26.8a/b | - | blocked on V038 constraint names / owner prod migration |

## Deferred checks
- pg-tier upsert golden (needs TEST_PG_ADMIN_DSN; same timing masking applies)
- Non-test merged files covered only through tests
- P11B.1 measurement on 3 real dealers

## Owner asks
- Decide whether P11B.1 measurement runs
- Prod 0-dangling-link check and apply for N26.8
