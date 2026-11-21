# DealershipScanner data repository

Private companion of the public DealershipScanner code repository. Layout and writers are specified in
`docs/scan_loop/BUNDLE.md` section 3 of the code repository; the short form:

| Path | Branch | Writer |
|---|---|---|
| `config/loop.json` | main | owner (budgets, kill switch) |
| `workspace/dealer_logs/` | main | Railway (scan_runs, scan_instructions, platform candidates) and sessions (summary, location, notes) |
| `mirror/` | main | Railway |
| `requests/rq-*.json`, `proposals/`, `findings/` | main | cloud sessions |
| `requests/*.status.json`, `fix_cycles/` | main | Railway |
| `runs/` , `evidence/` | runs | Railway |

`.gitattributes` makes the append-only dealer logs merge by union, so Railway and a session can both
append without conflicts. Nothing secret belongs here, and this repository is never made public.

Created from `docs/scan_loop/data_repo_template/` in the code repository. Setup steps for the owner are in
`docs/scan_loop/OWNER_SETUP.md` there.
