# Home-IP lane (P15A.2)

Scans the dealers listed in `deploy/railway/home_lane_dealers.txt` (they answer only from a
residential IP) and writes to the prod database. Railway excludes the same ids
(`excluded_home_lane`, P15A.1). Owner-run on the MBP; nothing here runs by itself.

## Setup (owner, P15A.6)

1. Make a dedicated clone at the release tag, for example `~/sarraficars-prod-lane`, and
   `touch .prod_lane_clone` in it. The clone has its own `workspace/` (recipe cache, dealer
   logs) so local and prod state never mix. The runner refuses any tree without the marker.
2. Create `~/.config/sarraficars/home_lane.env` with mode 0600, outside the repo:
   `HOME_LANE_DATABASE_URL=postgresql://...` (the prod DSN). Group- or world-readable files
   are refused.
3. `deploy/home_lane/run_home_lane.sh --dry-run` prints the target host and the roster.
4. One supervised `run_home_lane.sh`, then `install_launchd.sh --load` (02:00 local).

## What the runner does

- Refuses localhost, 127.*, ::1 and port 15432 unless `HOME_LANE_ALLOW_LOCAL=1`.
- Runs under `PROJECT_DOTENV_DISABLE=1`; `SCANNER_EGRESS_TAG`, `SCANNER_HTTP_PROXY` and
  `SCANNER_ALLOW_BROWSER` are exported empty so `.env` cannot refill them.
- `SCAN_LANE=home`, `SCAN_POST_STEPS=0`, `SCAN_SHARDS=${HOME_LANE_SHARDS:-2}`,
  `SCAN_PIPELINE_ARGS` defaults to `--no-discover` (no browser on an unattended run).
- Logs to `workspace/scanlogs/home_lane_<date>.log`.
