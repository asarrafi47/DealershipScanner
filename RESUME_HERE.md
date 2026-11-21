# DealershipScanner — Resume Here

A pointer page. It holds no status table, so it cannot go stale the way the old one did
(the 2026-07 snapshot it replaced named a branch, a hosting setup and a test baseline
that no longer existed). Current state lives in the files linked below.

**Version:** `cat VERSION`. Release notes: [`CHANGELOG.md`](CHANGELOG.md).

## Read first

| Need | File |
|------|------|
| Session rules (scan loop, logs, VERSION bump, secrets) | [`CLAUDE.md`](CLAUDE.md) |
| Branches, releases, tags, how code reaches Railway | [`docs/RELEASING.md`](docs/RELEASING.md) |
| Scanning, discovering or fixing a dealership | [`docs/NETWORK_SCAN_PROCESS.md`](docs/NETWORK_SCAN_PROCESS.md) |
| Running a Claude Code cloud session on this repo | [`docs/CLOUD.md`](docs/CLOUD.md) |
| Phased plan and what is open | [`docs/EXECUTION_PLAN_2026_09.md`](docs/EXECUTION_PLAN_2026_09.md), [`docs/REMEDIATION_PLAN_2026_10.md`](docs/REMEDIATION_PLAN_2026_10.md) |
| Decisions the owner has made | [`docs/remediation/OWNER_DECISIONS_LOG.md`](docs/remediation/OWNER_DECISIONS_LOG.md) |
| Railway scanner and deploy | [`docs/RAILWAY_SCANNING.md`](docs/RAILWAY_SCANNING.md), [`deploy/railway/`](deploy/railway/) |

`master-todo.md` is a 2026-07 snapshot and is being folded into the execution plan; do not
start from it.

## Where things run

Railway scans; Claude Code cloud sessions fix (`docs/scan_loop/README.md`). Work on a
`claude/<topic>` branch cut from `dev`, PR into `dev`, environment `dealership-code`. In a project
thread run `bash scripts/cloud/setup.sh` first (`docs/CLOUD.md` section 12).

## Production

Production runs on Railway. Code reaches it only through `deploy/railway/deploy_web.sh` and
`deploy/railway/deploy_scanner_nightly.sh`, from a release tag; no Railway service builds from
GitHub (`docs/RELEASING.md`). `run.py` is dev-only and must never serve production traffic.

## Run it locally

```bash
git config core.hooksPath scripts/git-hooks   # once per clone: the pre-push VERSION rule
python run.py                                 # dev web app on http://localhost:5001 (PORT=… to change)
curl -sf http://localhost:5001/api/ready      # failure = app not up or a dependency (DB) unavailable
./deploy/up.sh                                # Docker stack: Postgres (pgvector) + web, on :18000
```

`run.py` starts a local LLM unless `LOCAL_LLM_AUTOSTART=0`. `.env` in the repo root is loaded and
is never committed; secrets are never written into docs.

## Tests

```bash
python -m pytest backend/tests -q -p no:cacheprovider
```

The CI tiers, the Postgres tier and the cloud-VM variants are in `docs/CLOUD.md` section 5.

## Standing constraints

- Behavior preservation: no route URL, endpoint name, function signature or import path may break;
  the existing tests enforce it.
- Never commit `.env`, API keys or `*.db` files.
- Production needs `FLASK_ENV=production` plus `SECRET_KEY` and `ADMIN_PASSWORD`
  (`docs/SECURITY_MASTER_TODO.md`).
- Every push carries a new `VERSION` (`scripts/bump_version.sh patch|minor|major`).
