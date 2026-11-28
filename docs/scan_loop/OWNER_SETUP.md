# Owner setup: the scan loop, step by step

Everything a cloud session cannot do for you: Railway settings, tokens and the claude.ai environment. Do the
steps in order; each has a check. Nothing here puts a secret in the repository, which is public.
Background: `README.md` (the loop) and `BUNDLE.md` (what travels where).

**Where the data lives (decided 2026-11-21):** in the Railway database, table `loop_files` (migration V032), not
in git. Railway writes dealer logs, run bundles and the recipe mirror into it after every scan; cloud sessions
read it and submit requests, proposals and their own notes through the token-guarded `/api/loop` endpoints on the
web service. The store has per-file and total size caps (256 MB) and keeps the newest 30 runs. Where the older
docs say "data repository", read "the store"; `main`/`runs` branches become path prefixes (`runs/`, `mirror/`...).

## 1. Release and deploy (all in GitHub Actions, no Mac)

One-time setup, in the browser:

1. Railway, project `dealership-scanner`, **Settings > Tokens**: create a **project token** for the
   `production` environment. Copy it once.
2. GitHub, `asarrafi47/DealershipScanner`, **Settings > Secrets and variables > Actions > New repository
   secret**: name `RAILWAY_TOKEN`, value the token.
3. Same page, a second secret `BACKUP_PASSPHRASE`: any long random string (it encrypts the pre-migration dump,
   because artifacts of a public repository are downloadable by anyone). Keep a copy in your password manager;
   without it the dump cannot be opened.
4. A **deploy key** for the `release` action, secret `RELEASE_DEPLOY_KEY`. Why: the `protect-main` ruleset
   (deletion, non-fast-forward and update on `main`) lists one bypass actor, **DeployKey**, and a personal
   repository cannot add the GitHub Actions app to it, so the default `GITHUB_TOKEN` cannot move `main`. The
   `release` job checks out over SSH with this key and pushes `main` and the tag with it; no other job sees it.
   1. On any machine, create an ed25519 key pair with no passphrase, in a temporary folder:
      `ssh-keygen -t ed25519 -N "" -C release-deploy-key -f ./release_deploy_key`
      (it writes `release_deploy_key` and `release_deploy_key.pub`).
   2. GitHub, `asarrafi47/DealershipScanner`, **Settings > Deploy keys > Add deploy key**: title
      `release`, key the contents of `release_deploy_key.pub`, tick **Allow write access**.
   3. **Settings > Secrets and variables > Actions > New repository secret**: name `RELEASE_DEPLOY_KEY`,
      value the whole contents of `release_deploy_key` (the private half, including the
      `-----BEGIN`/`-----END` lines).
   4. Delete both local files (`rm release_deploy_key release_deploy_key.pub`). Nothing else needs the key;
      if it is lost, delete the deploy key and repeat these steps.

   Check: run the workflow with action `check`. The step "Release deploy key authenticates (read-only)" prints
   `deploy key authenticates as asarrafi47/DealershipScanner` (while the secret is unset it only warns). It
   cannot see write access, so step 2's checkbox matters: without it `release` fails at the push.

Then run **Actions > Release and deploy > Run workflow** (`.github/workflows/release.yml`):

| action | what it does |
|---|---|
| `check` | read-only: deploy key authenticates (`ssh -T`), release guard, migrate dry run against prod |
| `backup` | encrypted `pg_dump` of prod, kept 90 days as a workflow artifact |
| `migrate` | `migrate --apply --target <N>` against prod (run `backup` first) |
| `release` | fast-forward `main` to `<ref>` and push the annotated tag `v<VERSION>`, over SSH with the deploy key |
| `deploy` | deploy web, check `/api/health` and that `/api/loop/list` is 401, then scanner-nightly |
| `scan` | supervised scan: `SCAN_DEALERS=<ids>`, `SCAN_FLEET=0`, no cron |
| `full` | `backup`, `migrate`, `release`, `deploy` in order |

Web boot also applies pending migrations (`MIGRATE_ON_BOOT=1`); the explicit `migrate` step runs after the backup
and before the new web code boots. Do not redeploy the 2026-09-29 scanner deployment: it carries its own
`SCAN_FLEET=1`.

## 2. The two loop tokens

One **read** token and one **submit** token, each at least 24 characters, different from each other. Easiest: in
Railway set both web variables to `${{ secret(32) }}` so Railway generates them; reveal them later only to paste
into the claude.ai environment (step 6). Keep them out of chat, files and screenshots.

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

Run the `scan` action of the workflow with two dealer ids (it sets `SCAN_DEALERS`, `SCAN_FLEET=0`, and starts one
deployment of `scanner-nightly`).
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
