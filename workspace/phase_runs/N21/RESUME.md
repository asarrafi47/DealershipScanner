# N21 Incomplete-listings remainder and dealer-discovery batch - RESUME

Branch phase/N21, merged into integ/draft at d81f4f900 (head da76fc3a5). Draft only.

| Unit | Head | Status |
|---|---|---|
| N21.1 | 1341bb41f | DONE code |
| N21.2, N21.4a | d4df1df7a | DONE code |
| N21.3, N21.9 | ca2366fc1 | DONE |
| N21.5a, N21.7 | 2f8eea7e6 | DONE |
| N21.6 | 50626745f | DONE code |
| N21.8 | 4ba5997ed | DONE code |
| N21.10 | da76fc3a5 | DONE code |

## Deferred checks
- N21.4b, N21.5b, N21.11, N21.12 (blocked: live scan, owner fetches, N11.10 findings, prod read)
- Live checks: 100-car price sample (N21.1), condition re-check (N21.3), 200-row text sample (N21.9), photo-less drop >=30% (N21.6), I/O/Q 1,251-row re-measure (N21.2), Gardena incomplete rate (N21.4a)
- Drop of six recovery_* columns: separate DDL unit
- test_dealer_run_golden_20261001: 6 failures at HEAD, golden needs re-record
- PG tier skipped in merge run; merge deleted two modules and no repo-wide grep for consumers was run
- sqlite_allowlist regenerated, records earlier-chunk qmark debt

## Owner asks
- D-N18, D-N17b, D-N31, D-N33 accepted; confirm rows with MSRP-only feeds now get price NULL until VDP fills it
- Run owner hand-VDP checks for kingfordsales, koonstoyotaarlington, raypearman, centurybmwhuntsville
- Confirm SINGLE_TRIM_MODELS and non-road exemption sets (Corvette up to MY2004 is a guess)
- Confirm isFinalPrice pre-offer shape with a live capture
- Approve cosmos registry-id backfill --apply after backup and dry-run diff
- Last writer owns a shared I/O/Q id (dealer_id may flip between scans): confirm
