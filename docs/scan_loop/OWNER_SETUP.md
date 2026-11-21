# Owner setup: the scan loop, step by step

Everything a cloud session cannot do for you: Railway settings, tokens and the claude.ai environment. Do the
steps in order; each has a check. Nothing here puts a secret in the repository, which is public.
Background: `README.md` (the loop) and `BUNDLE.md` (what travels where).

**Where the data lives (decided 2026-11-21):** in the Railway database, table `loop_files` (migration V032), not
in git. Railway writes dealer logs, run bundles and the recipe mirror into it after every scan; cloud sessions
read it and submit requests, proposals and their own notes through the token-guarded `/api/loop` endpoints on the
web service. The store has per-file and total size caps (256 MB) and keeps the newest 30 runs. Where the older
docs say "data repository", read "the store"; `main`/`runs` branches become path prefixes (`runs/`, `mirror/`...).

## 1. Release and deploy

The change rides in one release (`docs/RELEASING.md`): cut the tag from `dev`, apply migration V032 on the
production database (`backend/scripts/migrate.py`, as for every migration), deploy the **web** service and then the
**scanner** from that tag with the deploy scripts (`--dry-run` first). Do not redeploy the 2026-09-29 scanner
deployment: it carries its own `SCAN_FLEET=1`.

## 2. Create two random tokens (any password manager or `openssl rand -hex 32`)

One **read** token and one **submit** token, each at least 24 characters, different from each other. Keep them out
of chat, files and screenshots; they go only into Railway (step 3) and the claude.ai environment (step 6).

## 3. Railway variables

| Service | Variable | Value |
|---|---|---|
| web | `LOOP_READ_TOKEN` | the read token |
| web | `LOOP_SUBMIT_TOKEN` | the submit token |
| `scanner-nightly` | `LOOP_STORE` | `db` |
| `scanner-nightly` | `LOOP_TARGET` | `prod` |
| `scanner-nightly` | `SCAN_FLEET_WINDOW_UTC` | `08:55-09:40` (the run window; the 09:00 UTC cron falls inside it) |
| `scanner-nightly` | `SCAN_FLEET` | leave at `0` until step 5 |

Without the web tokens `/api/loop` answers 404, so nothing is exposed until you set them. The scanner already has
the database URL. Stage the deploys and never change variables while a fleet is running.

Check: `curl -s -o /dev/null -w '%{http_code}\n' https://<web host>/api/loop/list` prints 401 (not 404).

## 4. The first supervised run

With `SCAN_DEALERS=<2 dealer ids>` set and `SCAN_FLEET` unset, start one deployment of `scanner-nightly` by hand.
Check: `curl -s -H "Authorization: Bearer $READ" "https://<web host>/api/loop/list?prefix=runs/"` lists a
`runs/fleet_...` folder, and `workspace/dealer_logs/` has the two dealers' logs.

## 5. Turn the cron on

1. Railway service `scanner-nightly`, Settings, Cron schedule: `0 9 * * *` (UTC), restart policy Never.
2. Clear `SCAN_DEALERS`, set `SCAN_FLEET=1`, and watch three nights.
3. Production scans start with reconcile (retirement of vanished listings) off; leave it off until the gates
   pass twice (`README.md` section 5).
4. The request tick: a second Railway cron on the same image and variables, command
   `python -m backend.scripts.ops_tick`, schedule `*/30 * * * *`. It seeds `config/loop.json` into the store on its
   first run (budgets, kill switch; `apply` left out of `kinds`).

`apply` writes the production recipe store. Add `"apply"` to `kinds` only after `probe`, `validate` and `rescan`
have run cleanly for a few days. Every apply checks the base hash, validates, writes a rollback file under
`<volume>/loop/rollback/`, saves, runs a canary rescan and restores the old set if the canary fails or fewer than
90% of rows are stamped. One apply per dealer per UTC day.

## 6. The analysis environment (claude.ai)

At claude.ai/code, New environment: name `dealership-analysis`, network **Limited** with package managers allowed
**and your web service's host added** (the API), environment variables `LOOP_API_URL=https://<web host>`,
`LOOP_READ_TOKEN` and `LOOP_SUBMIT_TOKEN`, no repository attached, setup script:

```bash
#!/bin/bash
# DealershipScanner analysis environment (docs/scan_loop/OWNER_SETUP.md). Never fails the session.
git clone --quiet --depth 1 --single-branch --branch dev https://github.com/asarrafi47/DealershipScanner.git /opt/ds-code \
  && bash /opt/ds-code/scripts/cloud/ops_hydrate.sh /opt/ds-data /opt/ds-code \
  || echo "ops hydrate incomplete: run bash /opt/ds-code/scripts/cloud/ops_hydrate.sh /opt/ds-data /opt/ds-code by hand"
exit 0
```

The submit token can only write new request and proposal files, a session's own `summary.md`/`location.md` notes
and findings, all schema-checked and scrubbed; it cannot write recipes, the mirror, bundles, statuses or config.
If you prefer a read-only environment, leave `LOOP_SUBMIT_TOKEN` out: sessions can analyse but not file requests.

Check: a session in that environment runs `python -m backend.scripts.ops_gates --help` from `/opt/ds-code`, and
`bash /opt/ds-code/scripts/cloud/persist.sh` reports "nothing to send".

## What stays manual

Rotating the tokens (set new values in Railway and the environment together), releases and deploys, the kill
switch (`"enabled": false` in the stored `config/loop.json`), and deleting branches (the cloud proxy refuses
branch deletions). The API's bearer tokens are all there is between the public internet and the store's contents,
which include recipe `auth_headers`: treat the read token like a password.
