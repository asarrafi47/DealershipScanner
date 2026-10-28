# DealershipScanner — Resume Here

A pointer page. It holds no state of its own, so it cannot go stale the way the old one did. Read the linked documents.

**Version:** `cat VERSION`. Release history: [`CHANGELOG.md`](CHANGELOG.md).
**Branches:** `main` moves only by fast-forward to a tagged release; work happens on short `phase/<id>` branches ([`docs/RELEASING.md`](docs/RELEASING.md)). Every push carries a new `VERSION` (`scripts/bump_version.sh patch|minor|major`).
**Production:** Railway. Code reaches it only through `deploy/railway/deploy_web.sh` and `deploy/railway/deploy_scanner_nightly.sh` from a release tag; no Railway service builds from GitHub. Cron is controlled per [`docs/RAILWAY_SCANNING.md`](docs/RAILWAY_SCANNING.md).

## Where to look

| Need | Document |
|------|----------|
| What is next, in order, and what is done | [`docs/EXECUTION_PLAN_2026_09.md`](docs/EXECUTION_PLAN_2026_09.md) (includes the retired master-todo rows) |
| Open work by phase and unit | [`docs/NEXT_PHASES_PLAN_2026_10.md`](docs/NEXT_PHASES_PLAN_2026_10.md), unit specs in [`docs/REMEDIATION_PLAN_2026_10.md`](docs/REMEDIATION_PLAN_2026_10.md) |
| Owner decisions | [`docs/remediation/OWNER_DECISIONS_LOG.md`](docs/remediation/OWNER_DECISIONS_LOG.md) |
| Scanning a dealership (the per-dealer loop and logs) | [`docs/NETWORK_SCAN_PROCESS.md`](docs/NETWORK_SCAN_PROCESS.md) |
| Releasing and deploying | [`docs/RELEASING.md`](docs/RELEASING.md) |
| Session rules | [`CLAUDE.md`](CLAUDE.md) |

## Run commands

```bash
cd /Users/asarrafi/Projects/DealershipScanner
source .venv/bin/activate
python run.py                              # dev web app on http://localhost:5001 (override with PORT=...)
curl -sf http://localhost:5001/api/ready   # failure = app not up or a dependency (DB) unavailable
python -m pytest backend/tests -q -p no:cacheprovider
```

`run.py` is the Werkzeug dev server and refuses to start when `FLASK_ENV=production`; production serves through gunicorn (`scripts/docker-entrypoint-web.sh`). The inventory database is Postgres (`INVENTORY_DATABASE_URL`); `./deploy/up.sh` starts a local Postgres plus web in Docker.

`.env` in the repo root is loaded and must never be committed.
