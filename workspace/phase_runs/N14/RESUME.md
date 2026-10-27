# N14 Home-IP lane (Phase 15A) and D-IF6 desk review - RESUME

Branch phase/N14 (head d5ba08cea), merged into integ/draft at 28e50131e. Draft only.

| Unit | Head | Status |
|---|---|---|
| N14.1 | c294657e6 | DONE (desk review doc; D-IF6 drafted) |
| P15A.3 | c294657e6 | DONE (D-IF3 a, hint cleanup) |
| P15A.1 | 7bb5e0a04 | DONE |
| P15A.2 | 7832f38d1 | DONE |
| P15A.4 | d5ba08cea | DONE |
| P15A.5 | - | SKIPPED (needs P11A.3 from N10) |
| P15A.6a-c | - | SKIPPED (owner/prod ops) |

## Deferred checks
- open_code_dealers SQL and alarm queries against a real Postgres.
- Real fleet_scan launch from the home lane, active-row count, launchctl load.
- Re-run test_fleet_lanes, test_fleet_roster_rule, test_fleet_scan_post_steps, window, safety, resume after N10 lands (shared fleet_scan.py regions).
- enrich_from_dictionary yield (only unmeasured N14.1 row).

## Owner asks
- Log the D-IF6 answer from docs/NIGHTLY_CAPABILITIES_2026_10.md.
- At P15A.6: touch .prod_lane_clone in the prod-lane clone, create ~/.config/sarraficars/home_lane.env (mode 600, outside repo).
- Land N10 (P11A.3) so P15A.5 can proceed.
