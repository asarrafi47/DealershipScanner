# Owner setup: the scan loop, step by step

Everything a cloud session cannot do for you: accounts, secrets and Railway settings. Do the steps in
order; each has a check. Nothing here puts a secret in either repository. Background: `README.md`
(the loop) and `BUNDLE.md` (what travels where).

## 1. Create the private data repository (GitHub)

1. github.com, New repository, **Private**, name `dealership-scanner-data`, no template, no README.
2. Seed it from this repository's template (any machine with git and your GitHub login):

   ```bash
   git clone https://github.com/asarrafi47/DealershipScanner.git /tmp/ds && cd /tmp/ds
   git switch dev
   cp -a docs/scan_loop/data_repo_template/. /tmp/ds-data-seed/      # includes the dot files
   cd /tmp/ds-data-seed && git init -b main && git add -A && git commit -m "init from template"
   git remote add origin git@github.com:asarrafi47/dealership-scanner-data.git && git push -u origin main
   ```

   Check: the repository shows `.gitattributes`, `.gitignore`, `CLAUDE.md`, `README.md`, `config/loop.json`,
   `.claude/settings.json`. The `runs` branch appears after the first Railway publish.
3. Install the Claude GitHub App on that repository too (github.com/apps/claude/installations/new, only
   select repositories: `dealership-scanner-data`).

## 2. Create the publisher token (GitHub)

A **fine-grained personal access token**, resource owner you, **only** `dealership-scanner-data`,
permission *Contents: Read and write*, nothing else, expiry 90 days (set a reminder to rotate it).
Keep it out of chat, files and screenshots; it goes only into Railway in step 3.

## 3. Railway variables (service `scanner-nightly`)

Set them in the dashboard, with deploys staged (`docs/RELEASING.md`, Railway source policy), and never while
a fleet is running:

| Variable | Value |
|---|---|
| `LOOP_DATA_REPO` | `https://github.com/asarrafi47/dealership-scanner-data.git` |
| `LOOP_DATA_TOKEN` | the token of step 2 |
| `LOOP_TARGET` | `prod` |
| `SCAN_FLEET_WINDOW_UTC` | `08:55-09:40` (the run window; the 09:00 UTC cron falls inside it) |
| `SCAN_FLEET` | leave at `0` until step 5 |

## 4. Release and deploy the scanner

The publisher and the safety patch ride in one release (`docs/RELEASING.md`): merge the loop PRs into `dev`,
cut the tag, then `deploy/railway/deploy_scanner_nightly.sh` from that tag. First do a `--dry-run`. Do not
restart, roll back or redeploy the 2026-09-29 deployment: it carries its own `SCAN_FLEET=1`.

## 5. Turn the cron on

1. Railway service `scanner-nightly`, Settings, Cron schedule: `0 9 * * *` (UTC), restart policy Never.
2. Set `SCAN_FLEET=1` **only after** a supervised first run: with `SCAN_DEALERS=<2 dealer ids>` set and
   `SCAN_FLEET` unset, start one deployment by hand and check that the data repository gets a `runs/` bundle
   and a `logs:` commit. Then clear `SCAN_DEALERS`, set `SCAN_FLEET=1`, and watch three nights.
3. Production scans start with reconcile (retirement of vanished listings) off; leave it off until the gates
   pass twice (`README.md` section 5).

## 6. The analysis environment (claude.ai)

At claude.ai/code, New environment: name `dealership-analysis`, network **Limited** with package managers
allowed, **no** environment variables, only the repository `dealership-scanner-data` attached, setup script:

```bash
#!/bin/bash
# DealershipScanner analysis environment (docs/scan_loop/OWNER_SETUP.md). Never fails the session.
DATA="$(git rev-parse --show-toplevel 2>/dev/null || true)"
git clone --quiet --depth 1 --branch dev https://github.com/asarrafi47/DealershipScanner.git /opt/ds-code \
  && bash /opt/ds-code/scripts/cloud/ops_hydrate.sh "${DATA:-/opt/ds-data}" /opt/ds-code \
  || echo "ops hydrate incomplete: run bash /opt/ds-code/scripts/cloud/ops_hydrate.sh <data clone> by hand"
exit 0
```

Check: a session in that environment runs `python -m backend.scripts.ops_gates --help` from `/opt/ds-code`, and
`git -C /opt/ds-code push` is refused (anonymous clone).

## What stays manual

Rotating the token, releases and deploys, the kill switch (`enabled: false` in `config/loop.json` of the data
repository), and deleting branches (the cloud proxy refuses branch deletions).
