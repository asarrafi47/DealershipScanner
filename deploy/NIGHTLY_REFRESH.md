# Nightly browser-free HTTP inventory refresh

Keeps the car inventory fresh every night **without launching Chrome/Playwright**.
Everything runs over plain HTTP by replaying previously-captured API recipes.

## What it does

`deploy/nightly_http_refresh.sh` runs four steps, in order, from the repo root.
Every step is **best-effort**: a failing step is logged and the run continues.

| # | Command | Purpose |
|---|---------|---------|
| 1 | `scanner.py --manifest workspace/manifest_92694_25mi_full.json --delta` | HTTP delta refresh — replays captured API recipes, upserts fresh prices/inventory, marks sold cars inactive. |
| 2 | `backend/scripts/harvest_carscommerce.py` | Bulk field fill (colors/specs) from the shared CarsCommerce group API. Fills empty columns only; never touches price. |
| 3 | `backend/scripts/heal_from_recipes.py` | Fill remaining gaps from other captured network recipes. |
| 4 | `backend/scripts/rebuild_listings_index.py` | Recompute the incomplete-listings index. |

The wrapper:

- **Never spawns a browser.** `--delta` is HTTP-only by design; the script also
  snapshots chrome/chromium process counts around step 1 and logs a warning if
  any net-new browser appears (a bug if no other Playwright scan is running).
- **Never overlaps.** A PID lock at `workspace/scanlogs/.nightly_http_refresh.lock`
  makes a second invocation exit immediately while one is running; a stale lock
  from a dead PID is reclaimed.
- **Is idempotent** and safe to run by hand any time.
- **Logs** every line, timestamped, to `workspace/scanlogs/nightly_<YYYYMMDD>.log`,
  and reports the incomplete-listings count before vs. after.

## Run it manually

```bash
/Users/asarrafi/Projects/DealershipScanner/deploy/nightly_http_refresh.sh
# watch the log:
tail -f /Users/asarrafi/Projects/DealershipScanner/workspace/scanlogs/nightly_$(date +%Y%m%d).log
```

## Install the nightly schedule (launchd)

Runs every night at **03:30 local time**. One-time install:

```bash
cp /Users/asarrafi/Projects/DealershipScanner/deploy/com.sarraficars.nightly-http-refresh.plist \
   ~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist
launchctl load ~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist
```

Verify it is registered:

```bash
launchctl list | grep nightly-http-refresh
```

Trigger a run on demand (without waiting for 03:30):

```bash
launchctl start com.sarraficars.nightly-http-refresh
```

## Disable / uninstall

```bash
launchctl unload ~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist
rm ~/Library/LaunchAgents/com.sarraficars.nightly-http-refresh.plist   # optional: remove entirely
```

## Notes

- Uses system `python3` (Homebrew `python@3.14`), which the pipeline was verified
  against. If you prefer the repo `.venv`, change `PYTHON=` in
  `nightly_http_refresh.sh` to `.venv/bin/python3`.
- Environment (DB creds, API keys) is loaded from `.env` at the repo root, and
  `PYTHONPATH` is set to the repo root, exactly as for interactive runs.
- launchd starts jobs with a minimal `PATH`; the plist sets a Homebrew-friendly
  `PATH` so `python3`, `psql`, and `pgrep` resolve. Adjust if your Homebrew or
  Python live elsewhere.
- If the Mac is asleep at 03:30, launchd runs the job at the next wake.
