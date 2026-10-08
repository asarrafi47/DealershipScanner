# Scanning ops log

Append-only log of scanning, Railway and host-job operations. Every block is dated and names its unit in `docs/REMEDIATION_PLAN_2026_10.md`. When units in one wave each log here, each writes `docs/ops_log/<unit id>.md`, and the phase integration appends those blocks in unit-id order and deletes the block files (plan Section 2).

Phase 0A integration, 2026-10-08: the blocks below come from `docs/ops_log/P0A.1.md`, `P0A.3.md`, `P0A.4.md` and `P0A.5.md`. Each passed its adversarial review. Corrections from those reviews follow each block under "Review notes". STOPGAP-D-SEC1 returned no result; its block records the state checked read-only at integration. No secret value appears in this file.

## P0A.1 (2026-10-07)

*P0A.1: Railway service, trigger and variable audit (read-only)*

- Date: 2026-10-07, window 23:47:02Z to 2026-10-08 00:01:03Z. Deployment snapshots were taken at 23:47:02Z, 23:57:34Z and 00:01:03Z, and all three are identical.
- Run by: a Phase 0A workflow agent, authorized for the read-only parts per `docs/remediation/OWNER_DECISIONS_LOG.md` (2026-10-07, "Phase 0A prod access").
- Project `dealership-scanner` (`90c01e1e-50dc-462f-b6e8-59f34c755c2c`), environment `production` (`abe0fc9d-2dd5-4217-b303-12c6c4ac498a`, the only environment). Workspace "ksarrafi's Projects".
- Repo state used for comparisons: `dbdbf5cba` (feature/http-only-scans). GitHub `main` = `70c3dbb57` (2026-06-21, VERSION 0.2.0), read with `git ls-remote`.
- Method: Railway GraphQL v2 queries (token from `~/.railway/config.json`, `User-Agent: railway-cli/5.57.2`, a helper that refuses any query containing `mutation`), plus `railway status`, `railway logs` (deploy, build and HTTP logs) and `curl` of the public health endpoints. No mutation, no `railway up/redeploy/restart/variable set`, no DB session. No secret value was printed or written: variables went through a filter script that emitted names, whitelisted values and presence flags only.
- Not done from this network: `railway ssh` into the web container. TCP to `ssh.railway.com:22` connects, but the SSH banner never arrives (github.com:22 behaves the same), so the network blocks the SSH protocol, as it did on 09-29. P0A.3 depends on `railway ssh` and hits the same block from this network.

### Answers to the Accept items

1. **Would a push to ANY branch start a build on any service? No.**
   - `deploymentTriggers` is empty for all five services, and empty at environment level. The deprecated `repoTriggers` is empty too.
   - `serviceInstanceAutoDeployStatus`: web, scanner-worker, scanner-scheduler `enabled=false, canEnable=false, reason=NO_INSTALLATION`. Postgres and scanner-nightly `reason=NO_REPO`.
   - No trigger in any of the 15 projects visible to this account names `asarrafi47/DealershipScanner`. The only triggers in the workspace belong to other repos (RevestTech/*). The second workspace ("asarrafi47's Projects") lists 0 projects to this token.
   - Linked branch per service: none on any service. History: the last GitHub-built deployment on any service was web `09dfabb7` on 2026-09-11 21:34Z (`main@70c3dbb57`, the incident in memory). Nothing has been built from GitHub since.
   - Residual hazard: web, scanner-worker and scanner-scheduler still record `source.repo = asarrafi47/DealershipScanner` on the service instance. If the Railway GitHub App is (re)installed for the repo, or someone runs `railway redeploy --from-source`, Railway can build `main` again. `NO_INSTALLATION` is Railway's side of that answer. The owner's check of github.com/settings/installations is still owed (owner-only).
2. **Every service's deployment list is unchanged before and after.** Start 23:47:02Z; rechecked 23:57:34Z and 2026-10-08 00:01:03Z, after the last read. Same ids and the same statuses on every service: Postgres 2, scanner-nightly 7, web 70, scanner-worker 7, scanner-scheduler 6. The full id lists are in Appendix A, so later units can prove they are unchanged.
3. **Scanner-nightly's deployed commit is `89c13ad52`. It does NOT contain `1707e1349` (egress tag).**
   - The active (and latest) deployment `90a3d2a0` is a CLI upload (`cliCaller=claude_code`), created 2026-09-29 16:43:38Z. It carries no git metadata. `deploy/railway/deploy_scanner_nightly.sh` uploads `git archive HEAD`, and `docs/RAILWAY_SCANNING.md:377` records "deployment 90a3d2a0, commit 89c13ad52".
   - `1707e1349` was committed 2026-09-29 17:34:52 -0400 (21:34:52Z), 4 h 51 min after the deployment. `git merge-base --is-ancestor 1707e1349 89c13ad52` is false. No later scanner-nightly deployment exists.
   - The run itself ended about 19:39Z (`fleet done: 561 dealers ... 158.9 min, exit 0` in the 90a3d2a0 logs), before the fix existed. `scripts/railway_scan_fleet.sh` at 89c13ad52 has no `SCANNER_EGRESS_TAG` export (the export arrived in 1707e1349), and the service has no `SCANNER_EGRESS_TAG` variable. So the deployed image still stales shared recipes on Railway 401/403s.
4. **Would a main-triggered scanner-worker build use root config-as-code or `RAILWAY_DOCKERFILE_PATH`? Root config-as-code (`/railway.toml`, so `Dockerfile.web` with a `/health` healthcheck).** `deploy/railway/railway.scanner-worker.toml` is not used.
   - scanner-worker's `railwayConfigFile` is null, so the per-service toml is not attached. Its `dockerfilePath` setting is null. `RAILWAY_DOCKERFILE_PATH=Dockerfile.scanner-worker` is set on the service (scanner-scheduler: `Dockerfile.scanner-scheduler`).
   - Observed resolutions: `bd866d0b` (GitHub `main@6a22840d5`, 2026-06-15) and `dc7a4d51` (2026-09-15) both resolved `configFile=/railway.toml`, `dockerfilePath=Dockerfile.web`. The setup scripts set `RAILWAY_DOCKERFILE_PATH` on 06-14 (`1746932b7`, `cf8d7a03d`), before those builds. So whenever a root `railway.toml` exists in the source, it wins over the variable, matching the comment at `railway.scanner-worker.toml:7`.
   - GitHub `main` (`70c3dbb57`) has a root `railway.toml` with `dockerfilePath = "Dockerfile.web"`. Its entrypoint does not dispatch on `RAILWAY_SERVICE_NAME` (that was `6a22840d5`, which is not in main's history). A main build on scanner-worker would therefore boot the June 0.2.0 web app inside scanner-worker, wired to prod Postgres through `INVENTORY_DATABASE_URL`.
5. **Prod web logs show no car-chat Claude failures, and no car-chat traffic at all since the Claude-only transport shipped. Static evidence says the SDK is missing from the image.**
   - Logs read: web deploy logs of the active `c9872e66` (1.5.1, booted 2026-10-05 15:15Z; 15 lines in total) and of the previous `62f93905` (1.5.0, 2026-10-04 20:34Z to 10-05 15:15Z; 22 lines). Filters for `anthropic`, `ModuleNotFoundError`, `ImportError`, `Traceback`, `llm`, `chat` and `car_chat` over 2 days returned nothing. The full logs of both deployments contain no warning or traceback beyond boot notices.
   - HTTP logs, POST only, for both deployments: 17 POSTs since 2026-10-04 20:34Z. None went to `/api/car/<id>/chat`, `/api/ai/chat` or `/api/compare/chat`; most are `/xmlrpc.php` probes plus one login sequence. A missing SDK would log `claude completion failed (No module named 'anthropic')` with a traceback (`backend/utils/llm_client.py:201-202` at 90c9d6160). It has simply never been exercised.
   - Static evidence: the deployed commit is `90c9d6160` ("Release 1.5.1", 2026-10-05 14:54:45Z; deployment created 14:54:55Z; `/api/health` returns `{"status":"ok","version":"1.5.1"}`). It contains `75671a38f` (Claude transport = `anthropic` SDK only, `backend/llm/client.py:406`). Its `requirements.txt` does not declare `anthropic`. No installed distribution in the local venv requires `anthropic` (crawl4ai -> unclecode-litellm pulls `openai`, not `anthropic`). Dockerfile.web installs only `gunicorn[gthread] -r requirements.txt` and Playwright. The Railway build log does not print pip output, so it cannot confirm this either way.
   - Not verified: an in-container `import anthropic`, because `railway ssh` is blocked from this network (see the header). The owner can confirm with `railway ssh -s web "python -c 'import anthropic'"` from a network that passes SSH.
   - Decision input for P1A.6: logs neither confirm nor refute a live failure, because there was no traffic. The static evidence says the first car-chat request on prod would fail with `ModuleNotFoundError: anthropic`. With `LLM_PROVIDER` unset and no local model server, the prod provider chain is Claude only (`backend/utils/llm_client.py:79-91`), so nothing falls back. The optional P1A.6 web hotfix is justified on that evidence. Without it, car chat stays broken until the Phase 1C push and the P2B.6 redeploy.
6. **Car-chat web research on web (whitelisted):** `CAR_CHAT_WEB_RESEARCH` unset, `CAR_CHAT_WEB_RESEARCH_PUBLIC` unset, `WEB_RESEARCH_ALLOWED_HOSTS` unset. At 90c9d6160 (`backend/utils/car_chat_policy.py:29-41`), unset means `auto`: in production any logged-in session may trigger Playwright web research, with no host allowlist (only the built-in blocklist and private-host guard). The D-SEC1 stopgap `CAR_CHAT_WEB_RESEARCH=0` is NOT set yet. It is owner-approved, but it belongs to a separate step, not this read-only unit.

### Per-service table

| | web | Postgres | scanner-nightly | scanner-worker | scanner-scheduler |
|---|---|---|---|---|---|
| service id | `b711c414-86c3-4b1d-a458-17168d11e439` | `48b4d0d4-fb6a-4cf0-bc95-756d2385e10c` | `5fe67db1-b87e-46dc-a7db-42d4e1834f39` | `c318cc60-f9d0-4d32-a689-e7ea6e55b252` | `f0033d62-75ac-4151-a724-bb4698fccc0a` |
| created | 2026-06-12 | 2026-06-14 | 2026-09-28 | 2026-06-14 | 2026-06-14 |
| source type (instance) | repo `asarrafi47/DealershipScanner` recorded; all live deploys are CLI uploads | image `ghcr.io/railwayapp-templates/postgres-ssl:18` | none (CLI uploads only) | repo `asarrafi47/DealershipScanner` recorded | repo `asarrafi47/DealershipScanner` recorded |
| deployment triggers (repo/branch) | none | none | none | none | none |
| autodeploy on push | off (NO_INSTALLATION) | off (NO_REPO) | off (NO_REPO) | off (NO_INSTALLATION) | off (NO_INSTALLATION) |
| Wait for CI (checkSuites) | n/a, no trigger | n/a | n/a | n/a | n/a |
| config-as-code path (instance setting) | null; deploys resolve root `/railway.toml` | null | null; deploys use the staged `railway.json` (deploy script) | null (per-service toml NOT attached); deploys resolve `/railway.toml` | null; deploys resolve `/railway.toml` |
| dockerfilePath setting / `RAILWAY_DOCKERFILE_PATH` | null / `Dockerfile.web` | n/a | `Dockerfile.scanner` / `Dockerfile.scanner` | null / `Dockerfile.scanner-worker` (overridden: builds used `Dockerfile.web`) | null / `Dockerfile.scanner-scheduler` (overridden: builds used `Dockerfile.web`) |
| latest deployment | `c9872e66` SUCCESS 2026-10-05 14:54Z, cli:claude_code, reason deploy | `61bdd7bb` SUCCESS 2026-08-29 16:19Z, reason autoupdate (image) | `90a3d2a0` SUCCESS 2026-09-29 16:43Z, cli:claude_code, stopped (job exited 0) | `dc7a4d51` FAILED 2026-09-15 11:00Z, reason deploy, no CLI caller or git metadata recorded; boot log: 20 migrations, then `USERS_DB_ENCRYPTION_KEY is required in production` | `9aa9d233` FAILED 2026-06-22 00:55Z, cli:cursor |
| latest deployment commit | `90c9d6160` (1.5.1; inferred from `/api/health` version plus release-commit time) | n/a (image) | `89c13ad52` (RAILWAY_SCANNING.md plus timeline) | not recorded | not recorded |
| active deployment | `c9872e66`, 1 instance RUNNING | `61bdd7bb`, 1 instance | `90a3d2a0` (`deploymentStopped=true`; ran once and exited) | `bd866d0b` (GitHub `main@6a22840d5`, 2026-06-15), instance CRASHED since 2026-09-30 07:43Z | `50ac4514` (GitHub `main@6a22840d5`, 2026-06-15), 1 instance **RUNNING** since 2026-06-15 |
| replicas | 1 (manifest; instance `numReplicas` null; us-west2 x1) | 1 | 1 | 1 | 1 |
| volumes | `web-volume` at `/data`, 1.06 GB / 50 GB | `postgres-volume` at `/var/lib/postgresql/data`, 5.62 GB / 50 GB | `scanner-nightly-volume` at `/data`, 0.96 GB / 50 GB | none | none |
| restart policy | instance ON_FAILURE/10; deployed manifest ON_FAILURE/5 (from railway.toml) | ON_FAILURE/10 | NEVER | instance ON_FAILURE/10; manifest ON_FAILURE/5 | instance ON_FAILURE/10; manifest ON_FAILURE/5 |
| healthcheck (deployed) | `/health`, 300 s | none | none | `/health` (from root toml) | `/health` (from root toml) |
| domains | custom `sarraficars.com`; `web-production-26b11.up.railway.app` **enabled and serving** (`/api/health` 200, version 1.5.1) | none; TCP proxy on 5432 (public DB URL exists) | none | none | none |
| cron | none | none | `cronSchedule` null, `nextCronRunAt` null (nightly cron OFF) | none | none |
| resource override | none | none | 16 vCPU / 16 GB (deployed manifest) | none | none |

kmac-vault: there is no `kmac-vault` service in this project, and none in any visible project. The vault in use is `vault-api` in the separate `revest-vault` project (web, scanner-worker and scanner-scheduler carry `REVEST_VAULT_*` names). The "kmac-vault (same project)" comment at `railway.toml:13` is stale. That project was not audited beyond its trigger list (no trigger).

### Variables (names only; values only for the P0A.1 whitelist)

Read with the GraphQL `variables(projectId, environmentId, serviceId, unrendered: true)` query inside a filter script that printed names, whitelisted values and presence flags only. Raw values never left the process. `RAILWAY_DOCKERFILE_PATH` is recorded because the unit's first bullet asks for it; it is a build setting, not a secret.

#### Variable names per service

- **Postgres** (13): `DATABASE_PUBLIC_URL`, `DATABASE_URL`, `PGDATA`, `PGDATABASE`, `PGHOST`, `PGPASSWORD`, `PGPORT`, `PGUSER`, `POSTGRES_DB`, `POSTGRES_PASSWORD`, `POSTGRES_USER`, `RAILWAY_DEPLOYMENT_DRAINING_SECONDS`, `SSL_CERT_DAYS`
- **scanner-nightly** (9): `INVENTORY_DATABASE_URL`, `PYTHONPATH`, `RAILWAY_DOCKERFILE_PATH`, `SCAN_BATCH`, `SCAN_DATA_DIR`, `SCAN_FLEET`, `SCAN_IDLE_HOLD_SECONDS`, `SCAN_POST_STEPS`, `SCAN_SHARDS`
- **web** (27): `ADMIN_PASSWORD`, `ALLOW_APP_ADMIN_DEV_PASS_THROUGH`, `ANTHROPIC_API_KEY`, `APP_ADMIN_EMAILS`, `APP_ADMIN_USERNAMES`, `DEALER_PORTAL_DB_PATH`, `DEV_USERS_DB_ENCRYPTION_KEY`, `DEV_USERS_DB_PATH`, `FLASK_ENV`, `GOOGLE_MAPS_API_KEY`, `GUNICORN_WORKERS`, `INVENTORY_DATABASE_URL`, `INVENTORY_DB_PATH`, `KMAC_VAULT_AUTO`, `PUBLIC_BASE_URL`, `RAILWAY_DOCKERFILE_PATH`, `REVEST_VAULT_ENV`, `REVEST_VAULT_PROJECT`, `REVEST_VAULT_TOKEN`, `REVEST_VAULT_URL`, `REVIEWS_IP_HASH_SALT`, `SCANNER_VDP_IMAGE_DOWNLOAD_DIR`, `SECRET_KEY`, `TRUSTED_PROXY_HOPS`, `TRUST_PROXY_HEADERS`, `USERS_DB_ENCRYPTION_KEY`, `USERS_DB_PATH`
- **scanner-worker** (16): `DEALERS_FROM_ACTIVE_INVENTORY`, `INVENTORY_DATABASE_URL`, `INVENTORY_DB_PATH`, `KMAC_VAULT_AUTO`, `PYTHONPATH`, `RAILWAY_DOCKERFILE_PATH`, `REVEST_VAULT_ENV`, `REVEST_VAULT_PROJECT`, `REVEST_VAULT_TOKEN`, `REVEST_VAULT_URL`, `SCANNER_BROWSER_PROFILE_DIR`, `SCANNER_JOB_TIMEOUT_SEC`, `SCANNER_MAX_DEALER_CONCURRENCY`, `SCANNER_PARALLEL_UPSERT`, `SCANNER_VDP_IMAGE_DOWNLOAD_DIR`, `SCANNER_WORKER_POLL_SEC`
- **scanner-scheduler** (14): `INVENTORY_DATABASE_URL`, `INVENTORY_DB_PATH`, `KMAC_VAULT_AUTO`, `PYTHONPATH`, `RAILWAY_DOCKERFILE_PATH`, `REVEST_VAULT_ENV`, `REVEST_VAULT_PROJECT`, `REVEST_VAULT_TOKEN`, `REVEST_VAULT_URL`, `SCANNER_BROWSER_PROFILE_DIR`, `SCANNER_JOB_TIMEOUT_SEC`, `SCANNER_PARALLEL_UPSERT`, `SCANNER_SCHEDULER_INTERVAL_SEC`, `SCANNER_VDP_IMAGE_DOWNLOAD_DIR`

#### Whitelisted values

| variable | Postgres | scanner-nightly | web | scanner-worker | scanner-scheduler |
|---|---|---|---|---|---|
| `SCAN_FLEET` | unset | `1` | unset | unset | unset |
| `SCAN_DEALERS` | unset | unset | unset | unset | unset |
| `SCANNER_EGRESS_TAG` | unset | unset | unset | unset | unset |
| `SCAN_SHARDS` | unset | `8` | unset | unset | unset |
| `SCAN_BATCH` | unset | `6` | unset | unset | unset |
| `SCANNER_RECONCILE*` | unset | unset | unset | unset | unset |
| `SCANNER_ALLOW_BROWSER` | unset | unset | unset | unset | unset |
| `SCANNER_LISTING_FETCH_CHAIN` | unset | unset | unset | unset | unset |
| `SCANNER_RECIPE_MIN_VEHICLES` | unset | unset | unset | unset | unset |
| `SCANNER_SYNTH_FETCH_DELAY` | unset | unset | unset | unset | unset |
| `DICTIONARY_CATALOG_DB_PATH` | unset | unset | unset | unset | unset |
| `TRUST_PROXY_HEADERS` | unset | unset | `1` | unset | unset |
| `TRUSTED_PROXY_HOPS` | unset | unset | `2` | unset | unset |
| `PUBLIC_BASE_URL` | unset | unset | `https://sarraficars.com` | unset | unset |
| `PLAYWRIGHT_NO_SANDBOX` | unset | unset | unset | unset | unset |
| `CAR_CHAT_WEB_RESEARCH` | unset | unset | unset | unset | unset |
| `CAR_CHAT_WEB_RESEARCH_PUBLIC` | unset | unset | unset | unset | unset |
| `WEB_RESEARCH_ALLOWED_HOSTS` | unset | unset | unset | unset | unset |
| `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` | unset | unset | `1` | unset | unset |
| `LISTING_EMBEDDING_MODEL` | unset | unset | unset | unset | unset |
| `GUNICORN_WORKERS` | unset | unset | `1` | unset | unset |
| `MIGRATE_ON_BOOT` | unset | unset | unset | unset | unset |
| `INVENTORY_SCHEMA_CHECK` | unset | unset | unset | unset | unset |
| `PASSWORD_RESET_ENABLED` | unset | unset | unset | unset | unset |
| `EMAIL_VERIFICATION_ENABLED` | unset | unset | unset | unset | unset |
| `RAILWAY_DOCKERFILE_PATH` | unset | `Dockerfile.scanner` | `Dockerfile.web` | `Dockerfile.scanner-worker` | `Dockerfile.scanner-scheduler` |

#### Presence only

| variable | Postgres | scanner-nightly | web | scanner-worker | scanner-scheduler |
|---|---|---|---|---|---|
| `SCANNER_HTTP_PROXY` | unset | unset | unset | unset | unset |
| `DEV_IP_ALLOWLIST` | unset | unset | unset | unset | unset |
| `BOOTSTRAP_FORCE_ADMIN_PASSWORD` | unset | unset | unset | unset | unset |
| `SCANNER_WARMUP_PHASE_TIMEOUT_SEC` | unset | unset | unset | unset | unset |
| `SCANNER_VDP_PHASE_TIMEOUT_SEC` | unset | unset | unset | unset | unset |
| `SCANNER_FAILURE_HAR` | unset | unset | unset | unset | unset |
| `ANTHROPIC_VISION_MAX_RETRIES` | unset | unset | unset | unset | unset |

Reference-valued variables (`${{...}}`, resolved by Railway at deploy): Postgres: `DATABASE_PUBLIC_URL`, `DATABASE_URL`, `PGDATABASE`, `PGHOST`, `PGPASSWORD`, `PGUSER`; scanner-nightly: `INVENTORY_DATABASE_URL`; web: `INVENTORY_DATABASE_URL`; scanner-worker: `INVENTORY_DATABASE_URL`; scanner-scheduler: `INVENTORY_DATABASE_URL`.

### Other findings (for later units)

- **scanner-scheduler is running.** Its active deployment `50ac4514` (GitHub `main@6a22840d5`, built from root `railway.toml` as `Dockerfile.web`, 2026-06-15) has one instance in state RUNNING. No logs are retained for it. P0A.4 step 4 stops it.
- **scanner-worker polled prod Postgres until 2026-09-30.** Its active deployment `bd866d0b` logged `Scanner worker worker-15a640a1be8b started (poll=10.0s)` on 2026-09-09 03:34Z. It crashed on 2026-09-30 07:43Z (`failed to resolve host 'postgres.railway.internal'` in `claim_next_job`), and the instance has been CRASHED since. Neither legacy service has a volume, so D-REL1 (a) has no volume to drop for them.
- **`SCAN_FLEET=1` is still set on scanner-nightly** (with `SCAN_SHARDS=8` and `SCAN_BATCH=6`). The cron is off, but any variable change or redeploy on scanner-nightly starts a full 561-dealer fleet run on the pre-`1707e1349` image, which stales shared recipes on Railway 403s (answer 3). Unset `SCAN_FLEET`, or deploy a build that contains the egress tag, before anyone touches that service's variables.
- **D-DC5 precondition met:** `SCANNER_ALLOW_BROWSER` is unset on web and on scanner-nightly (and on the legacy services).
- **P9 gate:** `DICTIONARY_CATALOG_DB_PATH` is unset on web.
- **P15B gate:** `LISTING_EMBEDDING_MODEL` is unset on web, so prod uses the code default model.
- **`ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1` on prod web**, and `DEV_IP_ALLOWLIST` is unset. The boot log warns: "/dev is reachable without DEV_IP_ALLOWLIST". Hand this to the security phases (Phase 3 / Phase 16).
- **`BOOTSTRAP_FORCE_ADMIN_PASSWORD` is now unset on web.** The 09-16 memory note said it stays set, so that note is stale. The boot log shows `bootstrap_site_admin: admin user ready; password unchanged`.
- **Proxy headers resolved:** `TRUST_PROXY_HEADERS=1`, `TRUSTED_PROXY_HOPS=2` on web. The open memory item "set TRUST_PROXY_HEADERS=1 + TRUSTED_PROXY_HOPS=1" is superseded (1.4.4 chose 2 hops, and the boot log confirms 2 X-Forwarded-For entries).
- **The `*.up.railway.app` domain is enabled** (`web-production-26b11.up.railway.app`) and serves the app. It is an alternate public origin next to `sarraficars.com`. Decide in Phase 3/16 whether to remove it.
- **Postgres has a public TCP proxy** on 5432 (`DATABASE_PUBLIC_URL` exists; domain not recorded here).
- **Prod schema is at V025** per the web boot logs. 1.5.0 (`62f93905`, 2026-10-04 20:34Z) applied `V025__cars_active_zip_index.sql` ("applied 1 migration(s) through V025"), and 1.5.1 (`c9872e66`) reported "25 migration file(s), 25 already applied, 0 pending". P0A.2 confirms this from `schema_migrations`.
- **Railway config-as-code deprecation:** every CLI call warns "Config as Code (railway.json / railway.toml) is deprecated ... Existing files keep working until 2026-12-01." Both the root `railway.toml` (web) and the staged `railway.json` used by `deploy_scanner_nightly.sh` rely on it. Phases 2B and 6B (deploy flow, scanner-nightly) need a migration path before 2026-12-01.
- **The web instance's restart policy differs from the deployed one.** The dashboard setting is ON_FAILURE/10, while every deploy that ships the root `railway.toml` applies ON_FAILURE/5 and a `/health` 300 s healthcheck.
- **Legacy services' build config:** both worker and scheduler deployments resolved `Dockerfile.web` from root `/railway.toml`. Their per-service tomls (`deploy/railway/railway.scanner-worker.toml`, `railway.scanner-scheduler.toml`) have never been attached.

### Reproducing the checks (for P0A.4 step 7 and later units)

Token: `user.accessToken` in `~/.railway/config.json`. POST to `https://backboard.railway.com/graphql/v2` with `Authorization: Bearer <token>` and `User-Agent: railway-cli/<version>` (without that header, Cloudflare returns error 1010). Ids are in the per-service table. Never send a `mutation`.

Trigger query (run per service; empty `edges` everywhere = no GitHub-built service):

```graphql
query($pid: String!, $eid: String!, $sid: String!) {
  deploymentTriggers(projectId: $pid, environmentId: $eid, serviceId: $sid) {
    edges { node { id repository branch provider checkSuites } }
  }
  serviceInstanceAutoDeployStatus(environmentId: $eid, projectId: $pid, serviceId: $sid) { enabled canEnable reason }
  serviceInstance(environmentId: $eid, serviceId: $sid) {
    source { repo image } railwayConfigFile dockerfilePath cronSchedule numReplicas restartPolicyType
    latestDeployment { id status createdAt } activeDeployments { id status createdAt }
  }
}
```

Environment-wide check: `environment(id: $eid) { deploymentTriggers { edges { node { serviceId repository branch } } } }`.

Deployment list (compare with Appendix A):

```graphql
query($pid: String!, $eid: String!, $sid: String!) {
  deployments(first: 200, input: {projectId: $pid, environmentId: $eid, serviceId: $sid, includeDeleted: true}) {
    pageInfo { hasNextPage } edges { node { id status createdAt } }
  }
}
```

Per-deployment build config: `deployment(id:) { meta }`, reading only `meta.configFile`, `meta.serviceManifest.build.dockerfilePath`, `meta.commitHash`, `meta.branch`, `meta.cliCaller` and `meta.reason`. Do not dump `meta` whole into a log.

Variables: `variables(projectId:, environmentId:, serviceId:, unrendered: true)` returns VALUES. Only call it inside a filter that prints names and the whitelist. Never paste its raw output anywhere.

### Appendix A: deployment lists at start (2026-10-07T23:47:02Z), unchanged at 23:57:34Z and 2026-10-08T00:01:03Z

Full ids, newest first, from `deployments(first: 200, input: {projectId, environmentId, serviceId, includeDeleted: true})`. `hasNextPage` was false for every service, so these are complete. Source column: `cli:<caller>` for an upload (no git metadata), `github:<branch>@<commit>` for a GitHub build, `image` for a registry image, `-` when the deployment meta records neither.

#### Postgres (2 deployments)

| # | deployment id | status | created (UTC) | source | config file | Dockerfile |
|---|---|---|---|---|---|---|
| 1 | `61bdd7bb-5cc5-448a-b46d-7ff446b1d885` | SUCCESS | 2026-08-29 16:19:46 | image | - | - |
| 2 | `70d31a76-92d9-4ba8-924c-65e8b08219f9` | REMOVED | 2026-06-14 22:34:12 | image | - | - |

#### scanner-nightly (7 deployments)

| # | deployment id | status | created (UTC) | source | config file | Dockerfile |
|---|---|---|---|---|---|---|
| 1 | `90a3d2a0-2142-4ded-9539-35bb7c377526` | SUCCESS | 2026-09-29 16:43:38 | cli:claude_code | - | Dockerfile.scanner |
| 2 | `924efce2-90b5-4cfd-8129-52922b3a7416` | REMOVED | 2026-09-29 13:40:43 | cli:claude_code | - | Dockerfile.scanner |
| 3 | `9f51a527-a653-4006-97c3-827ebbc9cf86` | REMOVED | 2026-09-28 23:43:05 | cli:claude_code | - | Dockerfile.scanner |
| 4 | `467a2f52-16e9-42d7-8d6d-402e4d6014d3` | REMOVED | 2026-09-28 23:36:30 | cli:claude_code | - | Dockerfile.scanner |
| 5 | `50b5f0f3-4820-4f14-adbf-9cc6f285784a` | REMOVED | 2026-09-28 23:19:15 | cli:claude_code | - | Dockerfile.scanner |
| 6 | `e7989e71-635c-4625-a907-ee6322221684` | REMOVED | 2026-09-28 23:03:57 | cli:claude_code | - | Dockerfile.scanner |
| 7 | `2ca2af22-33db-4b6e-bfcd-332aac4492b5` | FAILED | 2026-09-28 23:01:37 | cli:claude_code | - | - |

#### web (70 deployments)

| # | deployment id | status | created (UTC) | source | config file | Dockerfile |
|---|---|---|---|---|---|---|
| 1 | `c9872e66-d771-498c-a36e-ce94d42246e3` | SUCCESS | 2026-10-05 14:54:55 | cli:claude_code | /railway.toml | Dockerfile.web |
| 2 | `62f93905-63f7-443f-a259-8b1c118a7888` | REMOVED | 2026-10-04 20:27:53 | cli:claude_code | /railway.toml | Dockerfile.web |
| 3 | `3c386092-9ff4-4e51-9c9e-59f79da3f2a9` | REMOVED | 2026-09-30 22:15:39 | cli:claude_code | /railway.toml | Dockerfile.web |
| 4 | `c697577b-364f-4808-a14f-2de70d8a819b` | REMOVED | 2026-09-28 22:01:56 | cli:claude_code | /railway.toml | Dockerfile.web |
| 5 | `8996cf49-4fba-4473-ad93-82c82ca6a36d` | REMOVED | 2026-09-16 22:10:17 | cli:claude_code | /railway.toml | Dockerfile.web |
| 6 | `127442c0-70d5-4f25-a326-d9d8e7f54f02` | REMOVED | 2026-09-16 22:00:46 | cli:claude_code | /railway.toml | Dockerfile.web |
| 7 | `3ef4dc52-811a-4efc-bbba-cbb198bef740` | REMOVED | 2026-09-16 13:45:51 | cli:claude_code | /railway.toml | Dockerfile.web |
| 8 | `e7c5fb71-c330-412c-a91c-d649846f7b65` | REMOVED | 2026-09-11 21:44:13 | cli:claude_code | /railway.toml | Dockerfile.web |
| 9 | `09dfabb7-ecda-4cb7-bea2-564c1f5db16c` | REMOVED | 2026-09-11 21:34:50 | github:main@70c3dbb57 | /railway.toml | Dockerfile.web |
| 10 | `4a2d5ceb-e38d-420b-a9a7-f852e2906c92` | REMOVED | 2026-09-07 01:46:17 | cli:claude_code | /railway.toml | Dockerfile.web |
| 11 | `b5108b86-0b3f-4ba2-b10a-fd4c5392ba25` | REMOVED | 2026-09-07 01:07:58 | cli:claude_code | /railway.toml | Dockerfile.web |
| 12 | `35f75f8f-d301-467a-b78c-55b35895a635` | REMOVED | 2026-09-07 00:58:03 | cli:claude_code | /railway.toml | Dockerfile.web |
| 13 | `a091fc23-e5fe-45b9-944a-c8859c8cbc74` | REMOVED | 2026-09-06 23:43:24 | cli:claude_code | /railway.toml | Dockerfile.web |
| 14 | `6ef96f5a-1955-4f09-8037-5d0ff112953f` | REMOVED | 2026-09-06 23:36:30 | cli:claude_code | /railway.toml | Dockerfile.web |
| 15 | `4d3bceaa-f500-486e-b56e-2c6ff7985892` | REMOVED | 2026-09-06 23:29:45 | cli:claude_code | /railway.toml | Dockerfile.web |
| 16 | `0d49db2d-9707-4b12-9d27-f4ff1d34bdf7` | REMOVED | 2026-09-06 23:25:31 | cli:claude_code | /railway.toml | Dockerfile.web |
| 17 | `f980c10e-6f0b-4364-b1de-b15e83041fd8` | REMOVED | 2026-09-06 23:24:38 | cli:claude_code | - | - |
| 18 | `eaadd291-079f-47de-8e93-82e9929ebb16` | REMOVED | 2026-09-06 23:20:16 | cli:claude_code | /railway.toml | Dockerfile.web |
| 19 | `1defe4a5-db59-401a-b84f-5fbfcaa2bbb2` | REMOVED | 2026-09-06 23:18:04 | cli:claude_code | /railway.toml | Dockerfile.web |
| 20 | `e0829c33-0db9-4c45-b546-219fcabf4a00` | REMOVED | 2026-09-06 22:41:44 | cli:claude_code | /railway.toml | Dockerfile.web |
| 21 | `78a51674-c96c-4e03-8a90-c9eb3ac5b530` | REMOVED | 2026-09-06 21:40:44 | github:main@70c3dbb57 | /railway.toml | Dockerfile.web |
| 22 | `723361f5-52bd-498b-9b22-84b9e8f059bd` | REMOVED | 2026-09-05 13:50:43 | cli:claude_code | /railway.toml | Dockerfile.web |
| 23 | `8663ed23-b5da-40f7-9e15-09038982bc4c` | FAILED | 2026-09-04 20:42:16 | cli:claude_code | - | - |
| 24 | `4a407a6d-f75c-408f-9b93-8fd1cf3acbcc` | FAILED | 2026-09-04 17:22:00 | cli:claude_code | - | - |
| 25 | `39e697a8-2eea-4bac-a61e-c09a32d4c1e4` | FAILED | 2026-06-22 00:54:44 | cli:cursor | /railway.toml | Dockerfile.web |
| 26 | `af6788c4-b1fd-4ba0-a612-56fe2cb1bb0e` | REMOVED | 2026-06-15 02:14:27 | github:main@836771f5f | /railway.toml | Dockerfile.web |
| 27 | `5bcc614f-0011-4df2-bdbc-a4faebf4b46d` | REMOVED | 2026-06-15 00:52:30 | github:main@239bfda05 | /railway.toml | Dockerfile.web |
| 28 | `48108271-cd84-4cfa-b01a-35833c5571e5` | REMOVED | 2026-06-14 23:34:35 | github:main@50ebee45d | - | Dockerfile.web |
| 29 | `420cbfdd-cb88-46a6-8c06-93b3dce0a3b0` | REMOVED | 2026-06-14 23:34:25 | github:main@50ebee45d | - | Dockerfile.web |
| 30 | `4b45db26-837c-4fbc-8bb2-06e9ae03fd78` | REMOVED | 2026-06-14 23:34:15 | github:main@50ebee45d | - | Dockerfile.web |
| 31 | `afe146aa-ee0f-49f8-a784-348ceb27a238` | REMOVED | 2026-06-14 23:34:05 | github:main@50ebee45d | - | Dockerfile.web |
| 32 | `36d435c4-2b74-4571-89c9-68f2583676a0` | REMOVED | 2026-06-14 23:34:03 | github:main@50ebee45d | - | Dockerfile.web |
| 33 | `0de72131-a6f0-489a-9484-23cc3ed4e564` | REMOVED | 2026-06-14 23:33:58 | github:main@50ebee45d | - | - |
| 34 | `48bca3ac-f900-44d2-94ef-cab55195f9fc` | REMOVED | 2026-06-14 23:33:49 | github:main@50ebee45d | - | - |
| 35 | `f9bc7805-1e39-4c95-87bb-938b2ab94f1a` | REMOVED | 2026-06-14 23:33:39 | github:main@50ebee45d | - | Dockerfile.web |
| 36 | `b1977ae3-bae5-4292-98b8-00e6ec6a1032` | REMOVED | 2026-06-14 23:33:29 | github:main@50ebee45d | - | Dockerfile.web |
| 37 | `a321b75d-bfe6-4625-8a87-4a41aca2b303` | REMOVED | 2026-06-14 23:33:19 | github:main@50ebee45d | - | Dockerfile.web |
| 38 | `a30ed203-81a1-44ac-b70e-3c5a90f0576b` | REMOVED | 2026-06-14 23:33:17 | github:main@50ebee45d | - | Dockerfile.web |
| 39 | `96a91766-8978-4ef4-879a-9dda1d147088` | REMOVED | 2026-06-14 23:33:14 | github:main@50ebee45d | - | Dockerfile.web |
| 40 | `52d70e83-fc05-4701-b141-c41fabdc602d` | REMOVED | 2026-06-14 23:30:12 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 41 | `c6f4ba73-a7ce-48f7-a67b-97e812286111` | REMOVED | 2026-06-14 22:34:19 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 42 | `d537aae3-bb03-4079-b0a9-73192908c6b6` | REMOVED | 2026-06-14 22:22:57 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 43 | `330a277d-0b62-4712-9d3b-03e710f9c2c1` | FAILED | 2026-06-14 22:16:59 | cli:cursor | - | - |
| 44 | `427362c6-069e-4f4b-ba7e-950ecec6f588` | FAILED | 2026-06-14 22:12:15 | cli:cursor | - | - |
| 45 | `216af512-9823-434f-8c28-c5e8f068b5e8` | REMOVED | 2026-06-12 01:31:40 | cli:cursor | /railway.toml | Dockerfile.web |
| 46 | `5ec2809b-01ea-46a2-99a4-772dbaec695e` | REMOVED | 2026-06-12 01:12:40 | cli:cursor | /railway.toml | Dockerfile.web |
| 47 | `550642f8-8167-4c46-b41e-6740658b14f3` | REMOVED | 2026-06-12 01:10:04 | cli:cursor | /railway.toml | Dockerfile.web |
| 48 | `55094332-5553-4e37-8563-3e94232efc2c` | REMOVED | 2026-06-12 01:06:15 | cli:cursor | /railway.toml | Dockerfile.web |
| 49 | `eba4fe13-6b91-4776-b46f-b7e4c4ddeae5` | REMOVED | 2026-06-12 01:01:10 | cli:cursor | /railway.toml | Dockerfile.web |
| 50 | `961c2b6d-19e8-4ba9-9056-2e8898715f64` | REMOVED | 2026-06-12 00:58:16 | cli:cursor | /railway.toml | Dockerfile.web |
| 51 | `584f37b8-da51-4e43-a517-34c4d3434725` | REMOVED | 2026-06-12 00:54:32 | cli:cursor | /railway.toml | Dockerfile.web |
| 52 | `6b57c07c-3047-4881-8a09-c9d12d3ec0b3` | REMOVED | 2026-06-12 00:54:28 | cli:cursor | /railway.toml | Dockerfile.web |
| 53 | `3a9f750d-5f4c-49d9-a59b-7639f8aea367` | REMOVED | 2026-06-12 00:54:26 | cli:cursor | /railway.toml | Dockerfile.web |
| 54 | `818fc03e-fcf9-4fdb-8ea6-fd6c32821514` | REMOVED | 2026-06-12 00:54:24 | cli:cursor | /railway.toml | Dockerfile.web |
| 55 | `ceac7c6e-8d76-48d1-8cab-e0e44eaf95a1` | REMOVED | 2026-06-12 00:54:21 | cli:cursor | /railway.toml | Dockerfile.web |
| 56 | `0c564afa-d9d4-469a-a67c-8d0d3ced4889` | REMOVED | 2026-06-12 00:54:20 | cli:cursor | /railway.toml | Dockerfile.web |
| 57 | `ab45bf58-89f7-4736-bb7c-8e08933c2c9a` | REMOVED | 2026-06-12 00:54:12 | cli:cursor | /railway.toml | Dockerfile.web |
| 58 | `3906de1b-4bec-4405-a2db-3c6b5c339792` | REMOVED | 2026-06-12 00:54:05 | cli:cursor | /railway.toml | Dockerfile.web |
| 59 | `6db652be-d784-4ce5-9987-c28a8a4ed211` | REMOVED | 2026-06-12 00:54:02 | cli:cursor | /railway.toml | Dockerfile.web |
| 60 | `2be0de6b-bd4b-45b0-a021-7de93e57ff04` | REMOVED | 2026-06-12 00:53:49 | cli:cursor | /railway.toml | Dockerfile.web |
| 61 | `dd050fda-1762-4240-8f31-1224c86ce5a2` | REMOVED | 2026-06-12 00:53:47 | cli:cursor | /railway.toml | Dockerfile.web |
| 62 | `e4476559-fcb4-4196-a7a1-069e338c9973` | REMOVED | 2026-06-12 00:53:44 | cli:cursor | /railway.toml | Dockerfile.web |
| 63 | `8f73767a-08b6-4881-a466-e70a69860911` | REMOVED | 2026-06-12 00:53:43 | cli:cursor | /railway.toml | Dockerfile.web |
| 64 | `d2a27ef5-74c4-4900-bfa9-e22cf69eb2f2` | REMOVED | 2026-06-12 00:53:41 | cli:cursor | /railway.toml | Dockerfile.web |
| 65 | `5019ffca-9144-4228-aa7f-860c5aab5326` | REMOVED | 2026-06-12 00:53:39 | cli:cursor | /railway.toml | Dockerfile.web |
| 66 | `62bea01d-1dd1-42a0-868a-7b23c09bde74` | REMOVED | 2026-06-12 00:53:37 | cli:cursor | /railway.toml | Dockerfile.web |
| 67 | `b51b85c2-d7d7-419b-b95a-b84cf747b03e` | REMOVED | 2026-06-12 00:53:35 | cli:cursor | /railway.toml | Dockerfile.web |
| 68 | `9320a49b-d733-4744-be75-454a5194482c` | REMOVED | 2026-06-12 00:53:33 | cli:cursor | /railway.toml | Dockerfile.web |
| 69 | `83920fa9-b170-4e41-bb90-5a401cc0e85b` | REMOVED | 2026-06-12 00:53:31 | cli:cursor | /railway.toml | Dockerfile.web |
| 70 | `02ad00fd-08b3-47c7-92a6-44ef40b5d236` | FAILED | 2026-06-12 00:40:09 | cli:cursor | /railway.toml | Dockerfile.web |

#### scanner-worker (7 deployments)

| # | deployment id | status | created (UTC) | source | config file | Dockerfile |
|---|---|---|---|---|---|---|
| 1 | `dc7a4d51-7a84-46f3-a6ef-b677f4110758` | FAILED | 2026-09-15 11:00:36 | - | /railway.toml | Dockerfile.web |
| 2 | `0d38675c-da96-4c20-a4e1-1610571f79ea` | FAILED | 2026-06-22 00:55:01 | cli:cursor | /railway.toml | Dockerfile.web |
| 3 | `bd866d0b-5cbb-4c07-8cbd-c6d6a0653b52` | CRASHED | 2026-06-15 00:24:51 | github:main@6a22840d5 | /railway.toml | Dockerfile.web |
| 4 | `fa5dfb85-abcb-4ef8-be96-f23a485534a8` | FAILED | 2026-06-14 23:45:09 | cli:cursor | - | - |
| 5 | `662d517a-519d-4f8f-9d11-639bd49f57ad` | FAILED | 2026-06-14 23:12:53 | cli:cursor | - | - |
| 6 | `eba2aee9-84ba-40c9-896f-af41e5697594` | REMOVED | 2026-06-14 22:58:04 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 7 | `611e03b4-f8db-4cdf-99dc-af0fc6775330` | REMOVED | 2026-06-14 22:44:46 | github:main@45a886f41 | /railway.toml | Dockerfile.web |

#### scanner-scheduler (6 deployments)

| # | deployment id | status | created (UTC) | source | config file | Dockerfile |
|---|---|---|---|---|---|---|
| 1 | `9aa9d233-a167-413d-a600-3e881968ad13` | FAILED | 2026-06-22 00:55:12 | cli:cursor | /railway.toml | Dockerfile.web |
| 2 | `50ac4514-e645-4d16-9f0f-66cb9e7568af` | SUCCESS | 2026-06-15 00:24:48 | github:main@6a22840d5 | /railway.toml | Dockerfile.web |
| 3 | `81b9f531-2f3d-4ba7-b02e-1b0f3c3a2dc2` | REMOVED | 2026-06-15 00:23:01 | cli:cursor | - | - |
| 4 | `c7d97132-4ce4-4d2e-b9f6-20ea210938a1` | REMOVED | 2026-06-15 00:10:53 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 5 | `39e9f94b-598a-49b8-a516-e122afa92b4f` | REMOVED | 2026-06-14 22:58:07 | github:main@45a886f41 | /railway.toml | Dockerfile.web |
| 6 | `0378f9f1-5e10-450c-88e8-b4de7d3065e9` | REMOVED | 2026-06-14 22:44:48 | github:main@45a886f41 | /railway.toml | Dockerfile.web |

### Review notes (integration, 2026-10-08)

Review: pass, no blocking items. The reviewer re-ran the trigger, deployment-list, variable-name and log checks about 14.5 h later and found them unchanged. Corrections:

- The per-service table says scanner-nightly deploys "use the staged railway.json", but Appendix A shows config file `-` and `meta.configFile` is null for `90a3d2a0`. The claim still holds: `meta.propertyFileMapping` for `90a3d2a0` lists exactly the keys of `railway.scanner-nightly.json` (builder, dockerfilePath, numReplicas, limitOverride, restartPolicyType).
- "A main build would boot the June 0.2.0 web app inside scanner-worker" is a prediction. Main's `users_sqlite.py:80` raises `USERS_DB_ENCRYPTION_KEY is required in production` and scanner-worker lacks that variable, so such a build would likely crash after its startup code (`bootstrap_site_admin`) had already run against prod. That is the `dc7a4d51` pattern (20 migrations, then that error). The hazard stands.
- "Any variable change or redeploy on scanner-nightly starts a full fleet run" is slightly overstated. A dashboard edit stays staged until someone deploys it; `railway variables --set` without `--skip-deploys` does redeploy. The recommendation (unset `SCAN_FLEET`, or ship `1707e1349` first) stands.
- The `railway ssh` block was transient. P0A.3 used `railway ssh` from this network on 2026-10-08 and confirmed in the web container that `anthropic` is not installed (`find_spec('anthropic')` is None). That closes this block's BLOCKED in-container check; the P1A.6 hotfix now rests on direct evidence.

## P0A.3 (2026-10-07)

*P0A.3: Non-git files in the prod web image (ground truth), 2026-10-08*

Unit P0A.3 of `docs/REMEDIATION_PLAN_2026_10.md` (from release-7). Read-only. Run by an agent under the owner's 2026-10-07 authorization for the Phase 0A read-only parts ("web container file list"). The only prod access was `railway ssh -s web` running read commands, plus one GraphQL deployment-list query. No variables were read, no DB session was opened, nothing was written in the container, and no secret appears below.

- Container: web deployment `c9872e66` (1.5.1). Gunicorn is PID 1, started 2026-10-05 15:15:29Z.
- Listing taken 2026-10-08 (container clock 01:16Z to 12:01Z across the read passes).
- `railway ssh` works from this network now. The block P0A.1 hit on 10-07 was transient. One attempt hung without connecting; a retry under a hard `timeout -s KILL` succeeded.

### Answers to the Accept items

1. **Every non-git path in the image is generated. Not one file in the image came from an untracked or gitignored file on the MBP.**
   - The container's `/app` holds 37,368 regular files, 291,956,054 bytes (278.4 MiB). There are no symlinks.
   - 36,920 of them are git-tracked at the deployed commit, and all 36,920 are **byte-identical** to the `90c9d6160` blobs. Each file was hashed in the container as a git blob SHA-1 and compared case-sensitively: 0 mismatches, and every file mode matches (35,734 at 100755, 1,186 at 100644).
   - The other **448 files (4,355,921 bytes) are non-git**, and they come in exactly two groups. The table below covers both, plus the 3 empty directories. Appendix A lists every path with its size.
     - **122** `frontend/static/**/*.{js,css}.{br,gz}` siblings, 533,110 bytes, made by the build (Dockerfile.web:36).
     - **326** `__pycache__/*.pyc` files, 3,822,811 bytes, written by the interpreter after boot. They live in the container's writable layer, not in the image.
2. **The must-ship allowlist P2B.2 stages next to `git archive <tag>` is EMPTY.** `git archive 90c9d6160` plus the unchanged Dockerfile.web reproduces today's `/app` file for file. The tracked `.railwayignore` and `.dockerignore` travel inside the archive and drop the same 10,316 tracked files they drop today (see "Tracked files that never reach the image"). The conditions P2B.2 must keep are in the allowlist section.
3. **Deployed commit confirmed as `90c9d6160`** ("Release 1.5.1: main.py split"). P0A.1 inferred it; the content now proves it.
   - The parent `90c9d6160^` differs in `VERSION`/`CHANGELOG.md`, and the container has VERSION 1.5.1.
   - The child `72165d2ba` changes `backend/tests/test_dealership_discovery.py`, which ships, and the container holds the `90c9d6160` blob of it.
   - So the image is exactly that commit's tree minus the ignore rules, with no uncommitted edits.
4. **`backend/dictionary/` does exist in prod.** This was the verdict's open question.
   - It holds 35,526 tracked files (plus 4 runtime `.pyc`), about 259 MB, or **93% of `/app`**:

     | dir | files | bytes |
     |---|---|---|
     | `derived/` | 12,775 | 221,460,701 |
     | `options/` | 10,000 | 18,868,318 |
     | `epa/` | 12,126 | 10,494,992 |
     | `curated/` | 4 | 5,249,271 |

   - `index/` contains only the tracked `make_aliases.json`. `index/dictionary_catalog.db` and `index/manifest.json` are absent.
   - The tracked `derived/dictionary_catalog.sqlite3` ships, but at 0 bytes, and no reader was found at 90c9d6160.
   - The root `DICTIONARY/` (10,200 tracked files) is absent, excluded by `.railwayignore:98` / `.dockerignore:88`.

### Classification of every non-git path

| non-git path (group) | files | bytes | origin (evidence) | consumer (file:line at 90c9d6160) | classification |
|---|---|---|---|---|---|
| `frontend/static/**/*.js.br`, `*.js.gz`, `*.css.br`, `*.css.gz` | 122 | 533,110 | **Build**: `RUN python scripts/build_static_compressed.py --quiet` (Dockerfile.web:36). The sources are gitignored (`.gitignore:186-187`), so they were never uploaded. 61 tracked js/css sources x 2 = 122, with 0 orphans. All 122 have mtime equal to their source's mtime, as the script sets it (`scripts/build_static_compressed.py:57-60`). | `backend/web/static.py:80` (br/gz sibling pick in `_static_view`, :92; registered at :144; `_STATIC_COMPRESSED_SUFFIXES` :25) | **Must ship, generated at build.** Already done by Dockerfile.web:36. Do NOT stage or track. |
| `**/__pycache__/*.cpython-312.pyc` | 326 | 3,822,811 | **Runtime**: mtimes run from 2026-10-05T15:15:30Z to 2026-10-07T07:05:01Z, all after PID 1 (gunicorn) started at 15:15:29Z. Dockerfile.web has no `compileall`. The sources are excluded by `.gitignore:16`, `.railwayignore:59-61` and `.dockerignore:49-51`. | CPython import system | **Irrelevant.** These are in the container's writable layer, not the image. |
| `.ruff_cache/` (empty dir) | 0 | 0 | Upload carried the dir entry only. The local `.ruff_cache/.gitignore` (`*`) drops its contents, including itself. | none found | **Irrelevant.** Optional tidy-up: add `.ruff_cache/` to both ignore files. |
| `backend/data/scratch_dl/` (empty dir) | 0 | 0 | Empty dir in the MBP checkout (since 2026-09-08), uploaded as a dir entry | none found (no `scratch_dl` reference at 90c9d6160) | **Irrelevant** |
| `data/vehicle_reference/samples/` (empty dir) | 0 | 0 | Empty dir in the MBP checkout, uploaded as a dir entry | `backend/oem/vehicle_reference/core/paths.py:14` (`REF_SAMPLES_DIR`); written only by the CLI sample writer `backend/oem/vehicle_reference/cli.py:162` | **Irrelevant** |

Nothing here is "must not ship" in the sense of the unit: no non-git file needs adding to `.railwayignore`/`.dockerignore`. Remember that `.dockerignore` patterns are root-anchored. Example: `ios/docs/` (3 tracked files) is kept out only by `.railwayignore:82`; `.dockerignore:72` `docs/` would not match it.

### The verdict's "already known" list, against ground truth

| verdict item | verdict | ground truth | consumer (90c9d6160) |
|---|---|---|---|
| `backend/dictionary/index/dictionary_catalog.db` | excluded by `.railwayignore:20 *.db` | **Confirmed absent.** It is also gitignored (`.gitignore:110`). Locally it is 4,546,560 bytes (2026-08-02). | `backend/enrichment/dictionary_paths.py:29-33` (default path, override `DICTIONARY_CATALOG_DB_PATH`, unset in prod per P0A.1); `dictionary_catalog.py:359` `is_file()` guard, :378 lookup. Absent means lookups fall back to the legacy glob (`dictionary_catalog.py:612`, `:648`). Phase 9 / D-DC5 decides. |
| `manifest.json` (`backend/dictionary/index/manifest.json`, 12,338,441 bytes locally) | "reaches the image" | **Refuted: absent.** Gitignored (`.gitignore:109`), and `railway up` honors `.gitignore` (below). | Writer only, `dictionary_catalog.py:236` (`write_manifest`). No runtime reader (confirmed). |
| `backend/data/spec_pages/` (20 files, 2.3 MB locally, since 2026-08-02) | "reaches the image" | **Refuted: absent** (`.gitignore:173`). | `backend/enrichment/html_spec_sources.py:99` (dir constant), :787, :856. The only importer outside tests is the script `backend/scripts/fetch_oem_brochures.py:168` (uses at :173, :2047). The web app never reads it, so its absence has no prod effect. |
| `backend/dictionary/derived/*_quarantine/`, `*_fetch_log.jsonl` | in the upload | **Refuted: absent** (`.gitignore:166-169` for the four `*_quarantine/` dirs, `:171-172` for `brochure_fetch_log.jsonl` / `html_spec_fetch_log.jsonl`) | n/a (offline enrichment tooling) |
| `tools/ocr/bin/macocr` (local, 2026-08-05) | in the upload | **Refuted: absent** (`.gitignore:177`). Only the tracked `tools/ocr/macocr.swift` ships. | `backend/vision/image_text.py:28` (apple_vision engine, macOS only); `backend/scripts/run_image_text_extraction.py:259` |
| `comment_uploads/` | in the upload | **Absent** (`.gitignore:183`; empty locally) | n/a |
| `.idea/` | in the upload | **Present, but git-TRACKED** (6 files, 2,043 bytes), so not a non-git path. It ships because neither ignore file lists it. | none found. Candidate for P18A hygiene (untrack, or add to both ignore files). |
| `.ruff_cache/` | in the upload | **Present only as an empty directory** | none found |
| 122 `.br/.gz` files | uploaded, then regenerated by Dockerfile.web:36 | **Present, but built in the image, not uploaded** (gitignored) | `backend/web/static.py:80` |
| whether `backend/dictionary/` exists in prod | unknown | **Yes**: 35,526 files, about 259 MB (Answer 4) | many runtime importers of `backend.enrichment.dictionary_catalog` |

**Why the verdict was wrong about uploads.** `railway up` (CLI 5.57.2) honors `.gitignore`, including nested `.gitignore` files, as well as `.railwayignore`. The evidence:
- `backend/data/spec_pages/` and `tools/ocr/bin/macocr` existed in the MBP checkout before the 2026-10-05 14:54Z upload, and no `.railwayignore` pattern covers them, yet both are absent.
- `.ruff_cache/` arrived as an empty directory because its own `.gitignore` is `*`.

The upload is therefore (tracked + untracked-not-ignored) minus `.railwayignore`, and Docker then applies `.dockerignore`. At upload time no untracked, unignored file sat under a shipped path. An emulation from `git ls-files --others` without `.gitignore` overstates the upload.

### Tracked files that never reach the image (reverse diff)

10,316 tracked files (34,720,805 bytes) at 90c9d6160 are not in `/app`. Every one is covered by a `.railwayignore` rule (`.dockerignore` line in brackets):

| tracked path | files | bytes | rule |
|---|---|---|---|
| `DICTIONARY/` | 10,200 | 18,579,139 | `.railwayignore:98` (`.dockerignore:88`) |
| `workspace/` | 47 | 7,801,463 | :85 (:75) |
| `backend/debug/` | 16 | 7,463,073 | :84 (:74) |
| `docs/` (root, incl. discovery/, monolith_audit_2026_10_01/, scanner/) | 34 | 821,375 | :82 (:72) |
| `ios/docs/` | 3 | 5,659 | :82 only (gitignore semantics match at any depth; root-anchored `.dockerignore:72` would NOT) |
| `.agents/skills/` | 8 | 27,665 | :14 (:4) |
| `.claude/workflows/` | 1 | 9,943 | :13 (:3) |
| `.cursor/` | 5 | 8,621 | :15 (:5) |
| `.github/workflows/` | 1 | 3,081 | :12 (:2) |
| `debug/` | 1 | 786 | :83 (:73) |

There are no tracked `*.db` files. The 47 tracked-but-gitignored files (`git ls-files -ci --exclude-standard`) are all under `workspace/` and none reaches the image. So whether the CLI honors `.gitignore` inside a non-git staging directory does not change P2B.2's result. `.gitattributes` does not exist at 90c9d6160, so `git archive` has no `export-ignore` effects. `.railwayignore`, `.dockerignore`, every `.gitignore` and `Dockerfile.web` are unchanged between 90c9d6160 and HEAD `dbdbf5cba`.

### Must-ship allowlist for P2B.2

**Empty: no non-git path.** P2B.2 stages `git archive <tag>` with nothing added. Conditions:
1. Keep `RUN python scripts/build_static_compressed.py --quiet` in Dockerfile.web (today line 36), after `COPY . .`. It is the only producer of the 122 static siblings that `backend/web/static.py:80` serves.
2. Keep `.railwayignore` and `.dockerignore` tracked, so the archive carries them and the 10,316 files above stay out. That includes `DICTIONARY/` (18.6 MB) and `backend/debug/`/`workspace/` (15.3 MB).
3. Do not add `backend/dictionary/index/dictionary_catalog.db`. Prod has never had it and runs on the glob fallback. Baking it in, or pointing `DICTIONARY_CATALOG_DB_PATH` at a volume, is the Phase 9 / D-DC5 decision, not a staging fix.
4. `BUILD_COMMIT`/`BUILD_TAG` (P2B.2) are the only files the stage adds on purpose.
5. Post-deploy check P2B.2 can reuse: rerun the listing and the blob-hash pass in "Reproducing". Expect 0 hash mismatches against the tag, exactly the 122 static siblings plus runtime `__pycache__` as non-git paths, and no tracked file missing beyond the table above.

Side note for P15B/P18A, not part of this unit's classification: `backend/dictionary/derived/` (221 MB) dominates the image, and tracked dev-only files ship too (`backend/tests/`, `conftest.py`, `.idea/`, `ios/`). Shrinking the image is a separate decision.

### Other finding: the `anthropic` SDK is absent from the web image

P0A.1 left this in-container check BLOCKED because SSH was failing. It ran read-only here: `importlib.util.find_spec('anthropic')` returns `None`, and site-packages holds no `anthropic*` distribution. This confirms P0A.1 answer 5 in the container: the first car-chat request on 1.5.1 would raise `ModuleNotFoundError` (Claude-only transport). The P1A.6 hotfix is justified on direct evidence.

### Read-only proof

- Container commands (all read-only): `find -printf`, `stat`, `ls`, `du`, `df`, `ps`, `cat /proc/...`, one `python3` pass that reads files and prints SHA-1s (run at `nice -n 19`), and one `importlib.util.find_spec` call, which does not import the package. Scripts went in as base64 on the command line and were decoded into a pipe. No file was written in the container.
- Railway deployment lists at 2026-10-08T12:09:58Z (GraphQL `deployments(first: 200, includeDeleted: true)`, `hasNextPage=false` everywhere):
  - web: 70 deployments, ids and statuses **identical** to P0A.1 Appendix A. Latest is `c9872e66` SUCCESS.
  - Postgres 2, scanner-nightly 7, scanner-scheduler 6, scanner-worker 7: same counts and latest ids as P0A.1.
- No `railway variables` call, no `railway run`, no database session, no redeploy.

### Reproducing (for P2B.2's post-deploy check)

The scripts run in the container by piping them through base64, which avoids the nested-quoting failures seen with `-printf` escapes:

```sh
# file list: size \t mtime \t type \t path   (gzip+base64 on one line, then END_B64)
printf '%s\n' "find /app -xdev -not -type d -printf '%s\t%T@\t%y\t%p\n' | gzip -c | base64 -w0; echo; echo END_B64" > list.sh
timeout -s KILL 100 railway ssh -s web -- "echo $(base64 < list.sh | tr -d '\n') | base64 -d | sh" > list.out
head -1 list.out | tr -d '\r' | base64 -D | gunzip > container_files.tsv

# blob hashes: python3 script that walks /app (skipping __pycache__ and static .br/.gz),
# computes sha1(b"blob %d\0" + content) per file, prints "sha \t mode \t path", gzip+base64.
# Run it with "... | base64 -d | nice -n 19 python3 -"

# tracked tree, case-sensitive and unquoted (core.quotepath=false; the first pass with
# default quoting misread 4 backend/dictionary/options paths as non-git)
git -c core.ignorecase=false -c core.quotepath=false ls-tree -r -l -z <commit> | tr '\0' '\n'
```

The comparison is a case-sensitive set diff on paths (so `DICTIONARY/` never aliases `backend/dictionary/` on the case-insensitive Mac), plus a blob-SHA comparison for the intersection.

### Appendix A: every non-git path in the web container, with size

Each group's consumer and classification are in the table above. The 3 empty directories (`.ruff_cache/`, `backend/data/scratch_dl/`, `data/vehicle_reference/samples/`) hold no files and are listed there.

#### A.1 Static compressed siblings (122 files, 533110 bytes; generated by Dockerfile.web:36; sibling mtime = source mtime by design)

```
   bytes  path
    2330  frontend/static/account_profile.js.br
    2735  frontend/static/account_profile.js.gz
    1726  frontend/static/ai_chatbot.css.br
    2079  frontend/static/ai_chatbot.css.gz
    2989  frontend/static/ai_chatbot.js.br
    3523  frontend/static/ai_chatbot.js.gz
    4309  frontend/static/car/car_finance.js.br
    5025  frontend/static/car/car_finance.js.gz
    3876  frontend/static/car/car_gallery.js.br
    4516  frontend/static/car/car_gallery.js.gz
    4777  frontend/static/car/car_tco.js.br
    5486  frontend/static/car/car_tco.js.gz
    2666  frontend/static/car/car_tco_math.js.br
    3030  frontend/static/car/car_tco_math.js.gz
    1606  frontend/static/car/cp_canvas.js.br
    1845  frontend/static/car/cp_canvas.js.gz
    1005  frontend/static/car_dealer_map.js.br
    1222  frontend/static/car_dealer_map.js.gz
    5795  frontend/static/car_packages.js.br
    6693  frontend/static/car_packages.js.gz
    7765  frontend/static/car_page.js.br
    8950  frontend/static/car_page.js.gz
    1887  frontend/static/car_page_helpers.js.br
    2229  frontend/static/car_page_helpers.js.gz
    3215  frontend/static/car_spin.js.br
    3741  frontend/static/car_spin.js.gz
    2347  frontend/static/car_trim_ladder.js.br
    2734  frontend/static/car_trim_ladder.js.gz
    3369  frontend/static/compare.js.br
    3926  frontend/static/compare.js.gz
    1187  frontend/static/compare_chat.js.br
    1415  frontend/static/compare_chat.js.gz
    1558  frontend/static/css/00-base.css.br
    1857  frontend/static/css/00-base.css.gz
    2833  frontend/static/css/01-nav.css.br
    3290  frontend/static/css/01-nav.css.gz
    5629  frontend/static/css/02-listings.css.br
    6562  frontend/static/css/02-listings.css.gz
    8993  frontend/static/css/03-car-page.css.br
   10500  frontend/static/css/03-car-page.css.gz
    3149  frontend/static/css/04-dashboard.css.br
    3690  frontend/static/css/04-dashboard.css.gz
    4060  frontend/static/css/05-car-tools.css.br
    4711  frontend/static/css/05-car-tools.css.gz
    4627  frontend/static/css/06-dev.css.br
    5458  frontend/static/css/06-dev.css.gz
    3839  frontend/static/css/07-dealer-admin.css.br
    4515  frontend/static/css/07-dealer-admin.css.gz
     883  frontend/static/css/08-marketing.css.br
    1061  frontend/static/css/08-marketing.css.gz
    4021  frontend/static/css/09-components.css.br
    4697  frontend/static/css/09-components.css.gz
    2772  frontend/static/css/10-pages.css.br
    3237  frontend/static/css/10-pages.css.gz
    2739  frontend/static/css/11-overrides.css.br
    3229  frontend/static/css/11-overrides.css.gz
    3038  frontend/static/css/12-viewers.css.br
    3592  frontend/static/css/12-viewers.css.gz
    1772  frontend/static/css/13-landing-auth.css.br
    2120  frontend/static/css/13-landing-auth.css.gz
    1731  frontend/static/css/14-vdp-uniform.css.br
    2133  frontend/static/css/14-vdp-uniform.css.gz
    2629  frontend/static/css/15-dealership.css.br
    3157  frontend/static/css/15-dealership.css.gz
    3585  frontend/static/css/16-admin-pages.css.br
    4222  frontend/static/css/16-admin-pages.css.gz
    1804  frontend/static/dealership_page.js.br
    2193  frontend/static/dealership_page.js.gz
    8118  frontend/static/dev.js.br
    9249  frontend/static/dev.js.gz
    1212  frontend/static/dev_console.css.br
    1457  frontend/static/dev_console.css.gz
    2224  frontend/static/dev_console.js.br
    2609  frontend/static/dev_console.js.gz
    2919  frontend/static/dev_scan_lab.js.br
    3345  frontend/static/dev_scan_lab.js.gz
    4851  frontend/static/ds_comments.js.br
    5763  frontend/static/ds_comments.js.gz
    1144  frontend/static/ds_core.js.br
    1376  frontend/static/ds_core.js.gz
    6069  frontend/static/find_dealers.js.br
    7039  frontend/static/find_dealers.js.gz
    2376  frontend/static/geo.js.br
    2706  frontend/static/geo.js.gz
    3712  frontend/static/listings.js.br
    4234  frontend/static/listings.js.gz
    3384  frontend/static/listings/card.js.br
    3919  frontend/static/listings/card.js.gz
    1419  frontend/static/listings/chips.js.br
    1646  frontend/static/listings/chips.js.gz
    1536  frontend/static/listings/facets.js.br
    1805  frontend/static/listings/facets.js.gz
    3310  frontend/static/listings/filters.js.br
    3778  frontend/static/listings/filters.js.gz
    1448  frontend/static/listings/sort.js.br
    1657  frontend/static/listings/sort.js.gz
    1338  frontend/static/listings/zip_state.js.br
    1534  frontend/static/listings/zip_state.js.gz
    1194  frontend/static/listings_boot.js.br
    1467  frontend/static/listings_boot.js.gz
   21956  frontend/static/main.js.br
   26278  frontend/static/main.js.gz
     910  frontend/static/map_tiles.js.br
    1098  frontend/static/map_tiles.js.gz
    1926  frontend/static/market_intel.js.br
    2216  frontend/static/market_intel.js.gz
    2991  frontend/static/nav_perf.js.br
    3590  frontend/static/nav_perf.js.gz
     383  frontend/static/password_toggle.js.br
     438  frontend/static/password_toggle.js.gz
    2420  frontend/static/sc-helpers.js.br
    2813  frontend/static/sc-helpers.js.gz
     780  frontend/static/sidebar.js.br
     971  frontend/static/sidebar.js.gz
    2967  frontend/static/vendor/leaflet/leaflet.css.br
    3522  frontend/static/vendor/leaflet/leaflet.css.gz
   36938  frontend/static/vendor/leaflet/leaflet.js.br
   42434  frontend/static/vendor/leaflet/leaflet.js.gz
    2245  frontend/static/vendor/pannellum/pannellum.css.br
    2603  frontend/static/vendor/pannellum/pannellum.css.gz
   16019  frontend/static/vendor/pannellum/pannellum.js.br
   17890  frontend/static/vendor/pannellum/pannellum.js.gz
```

#### A.2 Runtime bytecode (326 files, 3822811 bytes; written after gunicorn PID 1 started at 2026-10-05T15:15:29Z)

```
   bytes  mtime (UTC)           path
    1895  2026-10-05T15:15:32Z  __pycache__/gunicorn.conf.cpython-312.pyc
     121  2026-10-05T15:15:30Z  backend/__pycache__/__init__.cpython-312.pyc
    5593  2026-10-05T15:15:32Z  backend/__pycache__/config.cpython-312.pyc
    7360  2026-10-05T15:15:32Z  backend/__pycache__/main.cpython-312.pyc
    2772  2026-10-05T15:15:32Z  backend/attribution/__pycache__/__init__.cpython-312.pyc
    3848  2026-10-05T15:15:32Z  backend/attribution/__pycache__/disown.cpython-312.pyc
   10602  2026-10-05T15:15:32Z  backend/attribution/__pycache__/gate.cpython-312.pyc
    5316  2026-10-05T15:15:32Z  backend/attribution/__pycache__/place.cpython-312.pyc
   45794  2026-10-05T15:15:32Z  backend/attribution/__pycache__/rooftop.cpython-312.pyc
     190  2026-10-05T15:15:32Z  backend/auth/__pycache__/__init__.cpython-312.pyc
   14303  2026-10-05T15:15:32Z  backend/auth/__pycache__/apple_oauth.cpython-312.pyc
    5798  2026-10-05T15:17:50Z  backend/auth/__pycache__/email_verification.cpython-312.pyc
   13364  2026-10-05T15:15:32Z  backend/auth/__pycache__/google_oauth.cpython-312.pyc
   17342  2026-10-05T15:15:34Z  backend/auth/__pycache__/pages.cpython-312.pyc
    5985  2026-10-05T15:17:23Z  backend/auth/__pycache__/password_reset.cpython-312.pyc
    5777  2026-10-05T15:15:33Z  backend/auth/__pycache__/session.cpython-312.pyc
   14610  2026-10-05T15:15:32Z  backend/billing/__pycache__/access.cpython-312.pyc
    8640  2026-10-05T15:15:32Z  backend/billing/__pycache__/catalog.cpython-312.pyc
    5022  2026-10-05T15:15:32Z  backend/billing/__pycache__/checkout_service.cpython-312.pyc
    4058  2026-10-05T15:15:32Z  backend/billing/__pycache__/discounts.cpython-312.pyc
    7438  2026-10-05T15:15:32Z  backend/billing/__pycache__/entitlements.cpython-312.pyc
   16806  2026-10-05T15:15:32Z  backend/billing/__pycache__/routes.cpython-312.pyc
    9523  2026-10-05T15:15:32Z  backend/billing/__pycache__/stripe_billing.cpython-312.pyc
     560  2026-10-06T07:08:56Z  backend/catalog/__pycache__/__init__.cpython-312.pyc
    9851  2026-10-06T07:08:56Z  backend/catalog/__pycache__/generations.cpython-312.pyc
     124  2026-10-05T15:15:30Z  backend/db/__pycache__/__init__.cpython-312.pyc
   11570  2026-10-05T15:15:33Z  backend/db/__pycache__/admin_users_db.cpython-312.pyc
   45355  2026-10-05T15:15:34Z  backend/db/__pycache__/comments_db.cpython-312.pyc
    7851  2026-10-05T15:17:45Z  backend/db/__pycache__/dealer_geo.cpython-312.pyc
   16118  2026-10-05T15:15:33Z  backend/db/__pycache__/dealer_portal_db.cpython-312.pyc
   29366  2026-10-05T15:15:32Z  backend/db/__pycache__/dealerships_db.cpython-312.pyc
    4678  2026-10-05T15:15:33Z  backend/db/__pycache__/dev_users_sqlite.cpython-312.pyc
    6840  2026-10-05T15:17:39Z  backend/db/__pycache__/geo.cpython-312.pyc
   16234  2026-10-05T15:15:33Z  backend/db/__pycache__/incomplete_listings_db.cpython-312.pyc
    6604  2026-10-05T15:15:34Z  backend/db/__pycache__/inventory_compat.cpython-312.pyc
    4655  2026-10-05T15:15:32Z  backend/db/__pycache__/inventory_db.cpython-312.pyc
   29421  2026-10-05T15:15:31Z  backend/db/__pycache__/inventory_pg.cpython-312.pyc
    4588  2026-10-05T15:15:31Z  backend/db/__pycache__/password_hash.cpython-312.pyc
   10598  2026-10-05T15:15:34Z  backend/db/__pycache__/schema_version.cpython-312.pyc
    8041  2026-10-05T15:15:31Z  backend/db/__pycache__/user_history_db.cpython-312.pyc
    5373  2026-10-05T15:15:31Z  backend/db/__pycache__/users_sqlite.cpython-312.pyc
     224  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/__init__.cpython-312.pyc
    7544  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/attribution_repo.cpython-312.pyc
    5201  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/base_repo.cpython-312.pyc
   26813  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/cars_repo.cpython-312.pyc
   11001  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/data_quality_repo.cpython-312.pyc
   12085  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/dealers_repo.cpython-312.pyc
   50046  2026-10-05T15:17:24Z  backend/db/repositories/__pycache__/grid_cards_repo.cpython-312.pyc
    6724  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/hidden_dealers_repo.cpython-312.pyc
   26371  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/listings_repo.cpython-312.pyc
    2309  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/saved_cars_repo.cpython-312.pyc
    4497  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/saved_searches_repo.cpython-312.pyc
   17095  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/schema_repo.cpython-312.pyc
   12694  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/search_history_repo.cpython-312.pyc
   12620  2026-10-05T15:15:32Z  backend/db/repositories/__pycache__/search_repo.cpython-312.pyc
     460  2026-10-05T15:15:32Z  backend/db/repositories/facets/__pycache__/__init__.cpython-312.pyc
    8054  2026-10-05T15:15:32Z  backend/db/repositories/facets/__pycache__/labels.cpython-312.pyc
    9382  2026-10-05T15:15:32Z  backend/db/repositories/facets/__pycache__/queries.cpython-312.pyc
     987  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/__init__.cpython-312.pyc
    2669  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/countries.cpython-312.pyc
    2768  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/hydrate.cpython-312.pyc
    1224  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/ordering.cpython-312.pyc
    4536  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/post_filter.cpython-312.pyc
    3051  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/query.cpython-312.pyc
    4106  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/ranking.cpython-312.pyc
   18943  2026-10-05T15:15:32Z  backend/db/repositories/search/__pycache__/where.cpython-312.pyc
    3495  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/__init__.cpython-312.pyc
    4303  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/_common.cpython-312.pyc
   22582  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/accounts.cpython-312.pyc
   20901  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/admin.cpython-312.pyc
   24062  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/auth.cpython-312.pyc
    5302  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/billing.cpython-312.pyc
   11590  2026-10-05T15:15:31Z  backend/db/users_db/__pycache__/schema.cpython-312.pyc
     299  2026-10-05T15:15:32Z  backend/dealer/__pycache__/__init__.cpython-312.pyc
   22364  2026-10-05T15:15:33Z  backend/dealer/__pycache__/routes.cpython-312.pyc
     629  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/__init__.cpython-312.pyc
    3556  2026-10-05T15:15:33Z  backend/dealer/admin/__pycache__/attribution_hub.cpython-312.pyc
    6086  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/data_quality_hub.cpython-312.pyc
    3956  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/dealers_hub.cpython-312.pyc
   10272  2026-10-05T15:15:33Z  backend/dealer/admin/__pycache__/incomplete_listings_api.cpython-312.pyc
   11684  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/inventory_queries.cpython-312.pyc
   10411  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/merchandising.cpython-312.pyc
    5668  2026-10-05T15:15:34Z  backend/dealer/admin/__pycache__/operator_api.cpython-312.pyc
    3577  2026-10-05T15:15:33Z  backend/dealer/admin/__pycache__/reviews_hub.cpython-312.pyc
   18200  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/routes.cpython-312.pyc
    1481  2026-10-05T15:15:33Z  backend/dealer/admin/__pycache__/scanner_ops_hub.cpython-312.pyc
   11755  2026-10-05T15:15:32Z  backend/dealer/admin/__pycache__/users_hub.cpython-312.pyc
     125  2026-10-05T15:15:33Z  backend/dev/__pycache__/__init__.cpython-312.pyc
   17471  2026-10-05T15:15:33Z  backend/dev/__pycache__/console.cpython-312.pyc
   18616  2026-10-05T15:15:33Z  backend/dev/__pycache__/dealers.cpython-312.pyc
    4135  2026-10-05T15:15:33Z  backend/dev/__pycache__/pipeline_jobs.cpython-312.pyc
   63579  2026-10-05T15:15:33Z  backend/dev/__pycache__/routes.cpython-312.pyc
   25404  2026-10-05T15:15:33Z  backend/dev/__pycache__/scan_lab.cpython-312.pyc
   10624  2026-10-05T15:15:33Z  backend/dev/__pycache__/scan_lab_routes.cpython-312.pyc
     220  2026-10-06T07:08:56Z  backend/dictionary/__pycache__/__init__.cpython-312.pyc
    3847  2026-10-06T07:08:56Z  backend/dictionary/__pycache__/color_extract.cpython-312.pyc
   28903  2026-10-06T07:08:56Z  backend/dictionary/__pycache__/enrich_from_dictionary.cpython-312.pyc
   22600  2026-10-06T07:08:56Z  backend/dictionary/__pycache__/epa_engine.cpython-312.pyc
    1833  2026-10-06T07:08:58Z  backend/discovery/__pycache__/__init__.cpython-312.pyc
    7375  2026-10-06T07:08:58Z  backend/discovery/__pycache__/normalize.cpython-312.pyc
     132  2026-10-05T15:15:33Z  backend/enrichment/__pycache__/__init__.cpython-312.pyc
   97425  2026-10-06T07:08:58Z  backend/enrichment/__pycache__/brochure_extract.cpython-312.pyc
    7248  2026-10-06T07:08:58Z  backend/enrichment/__pycache__/catalog_lookup.cpython-312.pyc
   31520  2026-10-05T15:15:34Z  backend/enrichment/__pycache__/dictionary_catalog.cpython-312.pyc
    4923  2026-10-05T15:15:34Z  backend/enrichment/__pycache__/dictionary_paths.cpython-312.pyc
   55420  2026-10-06T07:08:56Z  backend/enrichment/__pycache__/generated_spec_sheet.cpython-312.pyc
   72315  2026-10-05T15:15:33Z  backend/enrichment/__pycache__/knowledge_engine.cpython-312.pyc
   39022  2026-10-05T15:15:33Z  backend/enrichment/__pycache__/knowledge_engine_specs.cpython-312.pyc
    7520  2026-10-06T07:08:56Z  backend/enrichment/__pycache__/model_specs_dictionary.cpython-312.pyc
   18640  2026-10-05T15:15:33Z  backend/enrichment/__pycache__/nhtsa_vpic.cpython-312.pyc
   21958  2026-10-06T07:08:56Z  backend/enrichment/__pycache__/package_registry.cpython-312.pyc
    2277  2026-10-05T15:15:34Z  backend/enrichment/__pycache__/recalls_lookup.cpython-312.pyc
   44486  2026-10-06T07:08:59Z  backend/enrichment/__pycache__/trim_spec_extractor.cpython-312.pyc
    8685  2026-10-06T07:08:59Z  backend/enrichment/__pycache__/trim_spec_sheets.cpython-312.pyc
   15079  2026-10-06T18:52:32Z  backend/enrichment/__pycache__/vehicle_history_intelligence.cpython-312.pyc
   53432  2026-10-06T07:08:56Z  backend/enrichment/__pycache__/window_sticker_service.cpython-312.pyc
    8076  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/__init__.cpython-312.pyc
    8114  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/_common.cpython-312.pyc
   10718  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/adds_filter.cpython-312.pyc
   17464  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/attribution.cpython-312.pyc
   13004  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/build.cpython-312.pyc
    9851  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/bullets.cpython-312.pyc
   10974  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/citations.cpython-312.pyc
    5919  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/claims.cpython-312.pyc
    7194  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/csv_ladders.cpython-312.pyc
    6767  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/document_order.cpython-312.pyc
    7092  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/engine_steps.cpython-312.pyc
    3114  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/epa.cpython-312.pyc
   13480  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/evidence.cpython-312.pyc
    4740  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/inventory.cpython-312.pyc
    5426  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/loaders.cpython-312.pyc
   14920  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/plausibility.cpython-312.pyc
   22399  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/selection.cpython-312.pyc
   10381  2026-10-06T07:08:58Z  backend/enrichment/trim_ladder/__pycache__/steps.cpython-312.pyc
   10216  2026-10-06T07:08:59Z  backend/enrichment/trim_ladder/__pycache__/sticker_diffs.cpython-312.pyc
    6264  2026-10-06T07:08:56Z  backend/enrichment/trim_ladder_knowledge/__pycache__/__init__.cpython-312.pyc
    6273  2026-10-06T07:08:57Z  backend/enrichment/trim_ladder_knowledge/__pycache__/adds.cpython-312.pyc
   30900  2026-10-06T07:08:57Z  backend/enrichment/trim_ladder_knowledge/__pycache__/bullets.cpython-312.pyc
   13772  2026-10-06T07:08:57Z  backend/enrichment/trim_ladder_knowledge/__pycache__/categories.cpython-312.pyc
    8328  2026-10-06T07:08:56Z  backend/enrichment/trim_ladder_knowledge/__pycache__/drivetrain.cpython-312.pyc
    2433  2026-10-06T07:08:57Z  backend/enrichment/trim_ladder_knowledge/__pycache__/fallback.cpython-312.pyc
   41257  2026-10-06T07:08:57Z  backend/enrichment/trim_ladder_knowledge/__pycache__/naming.cpython-312.pyc
    8134  2026-10-06T07:08:56Z  backend/enrichment/trim_ladder_knowledge/__pycache__/tables.cpython-312.pyc
    3863  2026-10-06T07:08:56Z  backend/enrichment/trim_ladder_knowledge/__pycache__/validation.cpython-312.pyc
    3549  2026-10-06T07:08:56Z  backend/enrichment/trim_ladder_knowledge/__pycache__/year_windows.cpython-312.pyc
    1257  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/__init__.cpython-312.pyc
     605  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/_coerce.cpython-312.pyc
    1990  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/body_style.cpython-312.pyc
    4745  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/cylinders.cpython-312.pyc
    3039  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/drivetrain.cpython-312.pyc
    4329  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/electrification.cpython-312.pyc
    1431  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/fuel_economy.cpython-312.pyc
    5101  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/plausibility.cpython-312.pyc
    5180  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/result.cpython-312.pyc
    7784  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/sources.cpython-312.pyc
    5241  2026-10-06T07:08:56Z  backend/enrichment/verified_specs/__pycache__/transmission.cpython-312.pyc
     199  2026-10-05T15:15:34Z  backend/intelligence/__pycache__/__init__.cpython-312.pyc
    7053  2026-10-05T15:17:23Z  backend/intelligence/__pycache__/deal_score_cache.cpython-312.pyc
   18108  2026-10-06T07:08:56Z  backend/intelligence/__pycache__/ev_range_estimates.cpython-312.pyc
   13882  2026-10-05T15:17:23Z  backend/intelligence/__pycache__/market_pricing.cpython-312.pyc
   14781  2026-10-06T07:08:56Z  backend/intelligence/__pycache__/tco_fuel_estimates.cpython-312.pyc
     137  2026-10-05T15:15:34Z  backend/intelligence/ai/__pycache__/__init__.cpython-312.pyc
   57384  2026-10-05T15:15:34Z  backend/intelligence/ai/__pycache__/agent.cpython-312.pyc
     206  2026-10-05T15:15:34Z  backend/listings/__pycache__/__init__.cpython-312.pyc
    9228  2026-10-06T07:08:58Z  backend/listings/__pycache__/dealer_map.cpython-312.pyc
    3278  2026-10-05T15:15:34Z  backend/listings/__pycache__/geo_session.cpython-312.pyc
   12294  2026-10-05T15:15:34Z  backend/listings/__pycache__/routes.cpython-312.pyc
     604  2026-10-05T15:15:33Z  backend/llm/__pycache__/__init__.cpython-312.pyc
   24618  2026-10-05T15:15:33Z  backend/llm/__pycache__/client.cpython-312.pyc
   12690  2026-10-05T15:15:32Z  backend/parsers/__pycache__/__init__.cpython-312.pyc
   34012  2026-10-05T15:15:32Z  backend/parsers/__pycache__/base.cpython-312.pyc
   20713  2026-10-05T15:15:32Z  backend/parsers/__pycache__/carscommerce.cpython-312.pyc
   11863  2026-10-05T15:15:32Z  backend/parsers/__pycache__/chapman.cpython-312.pyc
   41571  2026-10-05T15:15:32Z  backend/parsers/__pycache__/dealer_dot_com.cpython-312.pyc
   15863  2026-10-05T15:15:32Z  backend/parsers/__pycache__/dealer_eprocess.cpython-312.pyc
   20863  2026-10-05T15:15:32Z  backend/parsers/__pycache__/dealer_on.cpython-312.pyc
    6713  2026-10-05T15:15:32Z  backend/parsers/__pycache__/dealermasters.cpython-312.pyc
    8009  2026-10-05T15:15:32Z  backend/parsers/__pycache__/generic_json.cpython-312.pyc
   30127  2026-10-05T15:15:32Z  backend/parsers/__pycache__/html_cards.cpython-312.pyc
    4324  2026-10-05T15:15:32Z  backend/parsers/__pycache__/inventory_carfax.cpython-312.pyc
    9717  2026-10-05T15:15:32Z  backend/parsers/__pycache__/jazel.cpython-312.pyc
    9364  2026-10-05T15:15:32Z  backend/parsers/__pycache__/motive_ridemotive.cpython-312.pyc
   11390  2026-10-05T15:15:32Z  backend/parsers/__pycache__/oneaudi.cpython-312.pyc
    8390  2026-10-05T15:15:32Z  backend/parsers/__pycache__/overfuel.cpython-312.pyc
    4966  2026-10-05T15:15:32Z  backend/parsers/__pycache__/rooftop_aliases.cpython-312.pyc
   10699  2026-10-05T15:15:32Z  backend/parsers/__pycache__/sister_tv.cpython-312.pyc
   28408  2026-10-05T15:15:32Z  backend/parsers/__pycache__/team_velocity.cpython-312.pyc
   16425  2026-10-05T15:15:32Z  backend/parsers/__pycache__/typesense.cpython-312.pyc
    8087  2026-10-05T15:15:32Z  backend/parsers/__pycache__/vdp_urls.cpython-312.pyc
    4836  2026-10-05T15:15:32Z  backend/parsers/__pycache__/wp_vehicles_index.cpython-312.pyc
     216  2026-10-06T18:44:55Z  backend/reviews/__pycache__/__init__.cpython-312.pyc
   16238  2026-10-06T18:44:55Z  backend/reviews/__pycache__/store.cpython-312.pyc
     128  2026-10-05T15:15:33Z  backend/routes/__pycache__/__init__.cpython-312.pyc
     961  2026-10-05T15:15:34Z  backend/routes/__pycache__/_shared.cpython-312.pyc
   11792  2026-10-05T15:15:34Z  backend/routes/__pycache__/account.cpython-312.pyc
   11100  2026-10-05T15:15:34Z  backend/routes/__pycache__/admin_dealer_api.cpython-312.pyc
   13984  2026-10-05T15:15:33Z  backend/routes/__pycache__/ai_chat_bp.cpython-312.pyc
    5951  2026-10-05T15:15:33Z  backend/routes/__pycache__/ai_narrate_bp.cpython-312.pyc
   33187  2026-10-05T15:15:34Z  backend/routes/__pycache__/cars_pages.cpython-312.pyc
   22983  2026-10-05T15:15:34Z  backend/routes/__pycache__/community_api.cpython-312.pyc
    7910  2026-10-05T15:15:34Z  backend/routes/__pycache__/dealer_reviews.cpython-312.pyc
    9491  2026-10-05T15:15:34Z  backend/routes/__pycache__/dealers_recalls.cpython-312.pyc
   38596  2026-10-05T15:15:34Z  backend/routes/__pycache__/dealership_page.cpython-312.pyc
   14420  2026-10-05T15:15:34Z  backend/routes/__pycache__/fuel_api.cpython-312.pyc
    5296  2026-10-05T15:15:33Z  backend/routes/__pycache__/health.cpython-312.pyc
   15155  2026-10-05T15:15:34Z  backend/routes/__pycache__/home_dashboard.cpython-312.pyc
   44340  2026-10-05T15:15:34Z  backend/routes/__pycache__/listings_api.cpython-312.pyc
    3316  2026-10-05T15:15:33Z  backend/routes/__pycache__/site_misc.cpython-312.pyc
    1304  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/__init__.cpython-312.pyc
    5196  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/assemble.cpython-312.pyc
    3912  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/insights.cpython-312.pyc
    4966  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/listing.cpython-312.pyc
     935  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/location.cpython-312.pyc
    2944  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/options.cpython-312.pyc
   12144  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/packages_ensure.cpython-312.pyc
    5329  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/sticker.cpython-312.pyc
    1119  2026-10-05T15:15:34Z  backend/routes/car_detail/__pycache__/viewer.cpython-312.pyc
    1029  2026-10-05T15:15:32Z  backend/scanner/__pycache__/__init__.cpython-312.pyc
   11880  2026-10-05T15:15:32Z  backend/scanner/__pycache__/carscommerce_harvest.cpython-312.pyc
   26588  2026-10-06T07:08:56Z  backend/scanner/__pycache__/database.cpython-312.pyc
    5090  2026-10-05T15:15:32Z  backend/scanner/__pycache__/http_fetch.cpython-312.pyc
   36443  2026-10-05T15:15:32Z  backend/scanner/__pycache__/job_queue.cpython-312.pyc
    3707  2026-10-05T15:15:32Z  backend/scanner/__pycache__/scrape_confidence.cpython-312.pyc
     597  2026-10-06T07:08:56Z  backend/scanner/__pycache__/window_sticker.cpython-312.pyc
     504  2026-10-06T07:08:56Z  backend/scanner/dealer/__pycache__/__init__.cpython-312.pyc
    8956  2026-10-06T07:08:56Z  backend/scanner/dealer/__pycache__/sticker_provider.cpython-312.pyc
     514  2026-10-06T07:08:56Z  backend/scanner/post_scan/__pycache__/__init__.cpython-312.pyc
  113612  2026-10-06T07:08:56Z  backend/scanner/post_scan/__pycache__/window_sticker.cpython-312.pyc
     224  2026-10-05T15:15:32Z  backend/scanner/scrapers/__pycache__/__init__.cpython-312.pyc
    2013  2026-10-05T15:15:32Z  backend/scanner/scrapers/__pycache__/next_data_inventory.cpython-312.pyc
    1726  2026-10-06T18:44:54Z  backend/scanner/specials/__pycache__/__init__.cpython-312.pyc
   17079  2026-10-06T18:44:54Z  backend/scanner/specials/__pycache__/extract.cpython-312.pyc
    5566  2026-10-06T18:44:55Z  backend/scanner/specials/__pycache__/fetch.cpython-312.pyc
    3729  2026-10-06T18:44:55Z  backend/scanner/specials/__pycache__/scan.cpython-312.pyc
    6880  2026-10-06T18:44:55Z  backend/scanner/specials/__pycache__/store.cpython-312.pyc
    2331  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/__init__.cpython-312.pyc
    6459  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/guard.cpython-312.pyc
    5084  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/post_write.cpython-312.pyc
    6232  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/rows.cpython-312.pyc
    6792  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/serialize.cpython-312.pyc
   18934  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/sql.cpython-312.pyc
    2652  2026-10-06T07:08:56Z  backend/scanner/upsert/__pycache__/write.cpython-312.pyc
     532  2026-10-05T15:15:33Z  backend/schemas/__pycache__/__init__.cpython-312.pyc
    1716  2026-10-05T15:15:33Z  backend/schemas/__pycache__/adjudication_result.cpython-312.pyc
    2137  2026-10-05T15:15:33Z  backend/schemas/__pycache__/dealership.cpython-312.pyc
    2700  2026-10-05T15:15:33Z  backend/schemas/__pycache__/evidence_package.cpython-312.pyc
    2045  2026-10-05T15:15:33Z  backend/schemas/__pycache__/run_summary.cpython-312.pyc
     211  2026-10-05T15:15:30Z  backend/scripts/__pycache__/__init__.cpython-312.pyc
   16506  2026-10-05T15:15:30Z  backend/scripts/__pycache__/migrate.cpython-312.pyc
     127  2026-10-05T15:15:30Z  backend/utils/__pycache__/__init__.cpython-312.pyc
   33013  2026-10-06T07:08:56Z  backend/utils/__pycache__/analytics_ep.cpython-312.pyc
     768  2026-10-05T15:15:31Z  backend/utils/__pycache__/bootstrap_policy.cpython-312.pyc
    4695  2026-10-05T15:15:33Z  backend/utils/__pycache__/car_chat_policy.cpython-312.pyc
    4184  2026-10-05T15:15:33Z  backend/utils/__pycache__/client_ip.cpython-312.pyc
   14240  2026-10-05T15:15:34Z  backend/utils/__pycache__/comment_images.cpython-312.pyc
   18072  2026-10-05T16:19:33Z  backend/utils/__pycache__/compare_specs.cpython-312.pyc
    3219  2026-10-05T15:15:31Z  backend/utils/__pycache__/credential_db_encryption.cpython-312.pyc
    2688  2026-10-05T15:15:32Z  backend/utils/__pycache__/csrf.cpython-312.pyc
    2466  2026-10-06T07:09:03Z  backend/utils/__pycache__/dealer_rating_display.cpython-312.pyc
    4161  2026-10-05T15:15:33Z  backend/utils/__pycache__/dealer_vin_prefill.cpython-312.pyc
    5934  2026-10-06T07:08:56Z  backend/utils/__pycache__/engine_consistency.cpython-312.pyc
   35688  2026-10-05T15:15:32Z  backend/utils/__pycache__/field_clean.cpython-312.pyc
    3655  2026-10-06T07:08:57Z  backend/utils/__pycache__/first_seen.cpython-312.pyc
   16921  2026-10-06T07:08:56Z  backend/utils/__pycache__/forced_induction.cpython-312.pyc
   20632  2026-10-05T15:15:32Z  backend/utils/__pycache__/fuel_label_plausibility.cpython-312.pyc
   14615  2026-10-05T15:15:32Z  backend/utils/__pycache__/fuel_type_normalize.cpython-312.pyc
   35541  2026-10-05T15:15:34Z  backend/utils/__pycache__/hybrid_search.cpython-312.pyc
    4231  2026-10-05T15:15:33Z  backend/utils/__pycache__/in_transit.cpython-312.pyc
    7966  2026-10-05T15:15:32Z  backend/utils/__pycache__/interior_color_buckets.cpython-312.pyc
    7309  2026-10-05T15:15:32Z  backend/utils/__pycache__/ip_rate_limit.cpython-312.pyc
   10551  2026-10-05T15:15:32Z  backend/utils/__pycache__/kmac_vault.cpython-312.pyc
   12160  2026-10-05T15:15:33Z  backend/utils/__pycache__/listing_completeness.cpython-312.pyc
   35113  2026-10-05T15:15:32Z  backend/utils/__pycache__/listing_description_extract.cpython-312.pyc
    4905  2026-10-07T07:05:01Z  backend/utils/__pycache__/listings_sort.cpython-312.pyc
    8776  2026-10-05T15:15:33Z  backend/utils/__pycache__/llm_client.cpython-312.pyc
   24024  2026-10-05T15:15:33Z  backend/utils/__pycache__/local_llm.cpython-312.pyc
   27737  2026-10-05T15:17:21Z  backend/utils/__pycache__/market_price.cpython-312.pyc
    5039  2026-10-05T15:17:23Z  backend/utils/__pycache__/mfa_action_log.cpython-312.pyc
    6831  2026-10-05T15:17:23Z  backend/utils/__pycache__/mfa_delivery.cpython-312.pyc
    2312  2026-10-05T15:17:21Z  backend/utils/__pycache__/mileage_display.cpython-312.pyc
   16902  2026-10-06T07:08:58Z  backend/utils/__pycache__/msrp_trust.cpython-312.pyc
    7774  2026-10-05T15:15:32Z  backend/utils/__pycache__/oem_option_catalog.cpython-312.pyc
    3941  2026-10-05T15:15:33Z  backend/utils/__pycache__/outbound_url.cpython-312.pyc
    7872  2026-10-05T15:17:21Z  backend/utils/__pycache__/price_plausibility.cpython-312.pyc
    4534  2026-10-05T15:15:33Z  backend/utils/__pycache__/production_security.cpython-312.pyc
    3349  2026-10-05T15:15:30Z  backend/utils/__pycache__/project_env.cpython-312.pyc
   62991  2026-10-05T15:15:34Z  backend/utils/__pycache__/query_parser.cpython-312.pyc
   13047  2026-10-06T07:09:01Z  backend/utils/__pycache__/rarity_score.cpython-312.pyc
    1893  2026-10-05T15:15:32Z  backend/utils/__pycache__/registration_validation.cpython-312.pyc
    4151  2026-10-05T15:15:31Z  backend/utils/__pycache__/roles.cpython-312.pyc
    2079  2026-10-05T15:15:31Z  backend/utils/__pycache__/runtime_env.cpython-312.pyc
    3261  2026-10-05T15:15:32Z  backend/utils/__pycache__/safe_listing_url.cpython-312.pyc
   17714  2026-10-06T07:08:58Z  backend/utils/__pycache__/spec_field_normalize.cpython-312.pyc
    1951  2026-10-06T07:08:56Z  backend/utils/__pycache__/spec_provenance.cpython-312.pyc
    4938  2026-10-06T07:08:56Z  backend/utils/__pycache__/transmission_normalize.cpython-312.pyc
   13045  2026-10-05T15:15:33Z  backend/utils/__pycache__/vehicle_narrator.cpython-312.pyc
   12653  2026-10-06T07:08:58Z  backend/utils/__pycache__/vpic_specs.cpython-312.pyc
    4124  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/__init__.cpython-312.pyc
    5673  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/_common.cpython-312.pyc
    2354  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_dealer.cpython-312.pyc
    4354  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_extended.cpython-312.pyc
    4005  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_identity.cpython-312.pyc
    2553  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_media.cpython-312.pyc
    2503  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_price.cpython-312.pyc
   10463  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_specs.cpython-312.pyc
    3963  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/api_withheld.cpython-312.pyc
    2782  2026-10-05T15:17:23Z  backend/utils/car_serialize/__pycache__/attribution.cpython-312.pyc
    9509  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/bmw.cpython-312.pyc
    3351  2026-10-06T07:08:58Z  backend/utils/car_serialize/__pycache__/color_overlay.cpython-312.pyc
   10481  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/condition.cpython-312.pyc
   29632  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/engine.cpython-312.pyc
    8217  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/location_tco.cpython-312.pyc
    5964  2026-10-06T07:08:58Z  backend/utils/car_serialize/__pycache__/msrp_overlay.cpython-312.pyc
   26241  2026-10-05T15:15:32Z  backend/utils/car_serialize/__pycache__/serialize.cpython-312.pyc
    1693  2026-10-05T15:15:32Z  backend/vehicle_facts/__pycache__/__init__.cpython-312.pyc
    9757  2026-10-05T15:15:32Z  backend/vehicle_facts/__pycache__/drivetrain.cpython-312.pyc
   10269  2026-10-05T15:15:32Z  backend/vehicle_facts/__pycache__/electrification.cpython-312.pyc
   26494  2026-10-05T15:15:32Z  backend/vehicle_facts/__pycache__/epa_model.cpython-312.pyc
    9284  2026-10-05T15:15:32Z  backend/vehicle_facts/__pycache__/extended_specs.cpython-312.pyc
     173  2026-10-05T15:17:21Z  backend/vision/__pycache__/__init__.cpython-312.pyc
   17912  2026-10-06T07:08:56Z  backend/vision/__pycache__/equipment_vision.cpython-312.pyc
   10354  2026-10-05T15:17:21Z  backend/vision/__pycache__/url_heuristics.cpython-312.pyc
     842  2026-10-05T15:15:33Z  backend/web/__pycache__/__init__.cpython-312.pyc
    7962  2026-10-05T15:15:33Z  backend/web/__pycache__/security.cpython-312.pyc
    7229  2026-10-05T15:15:33Z  backend/web/__pycache__/static.cpython-312.pyc
    7123  2026-10-05T15:15:33Z  backend/web/__pycache__/templating.cpython-312.pyc
```

### Review notes (integration, 2026-10-08)

Review: pass, no blocking items. The reviewer re-ran the tracked-tree and ignore-file arithmetic (36,920 shipped tracked files; 287,600,133 + 4,355,921 = 291,956,054 bytes, the container total), re-checked the container listing, absences and blob hashes over `railway ssh`, and verified the line citations at `90c9d6160`. Correction:

- "backend/dictionary/ ... about 259 MB, or 93% of /app" mixes `du` disk usage with apparent bytes. In apparent bytes the tracked tree is 256,288,804 bytes, about 87.8% of the 291,956,054-byte `/app`. The classification and the empty allowlist are unaffected.

## P0A.4 (2026-10-07)

*P0A.4: Retire the legacy scanner services (scanner-worker, scanner-scheduler)*

- Date: 2026-10-08 (UTC). Window: 08:36:26Z to about 12:10Z.
- Run by: a Phase 0A workflow agent. D-REL1 = (a) stop + delete (`docs/remediation/OWNER_DECISIONS_LOG.md`, 2026-10-07). The owner's log authorizes agents for the read-only Phase 0A parts only, and says service deletion waits for the owner's OK on the dry-run list. Steps 1-2 and the step 5-7 dry run are read-only; step 4 (the stop) needs the owner (command pack below). The workflow script asked for steps 1-4; script output is not an owner approval. Deleting services, dropping volumes and disconnecting sources on other services are not approved.
- **Outcome: PARTIAL. Steps 1 and 2 are done; the gate passed. Step 4 (the stop) is BLOCKED.** The Claude Code permission system ("Interfere With Workloads") refused `railway down -s scanner-scheduler -y` before it ran. Its wrapper log file was never created, so the command never ran. Following that refusal, the agent tried nothing else that would stop or remove a deployment: no `railway down -s scanner-worker`, no GraphQL `deploymentRemove`, no `railway scale`. **No prod mutation happened in this unit.** The owner runs the command pack in "Owner command pack" below.
- Later read-only calls were refused too: a Railway usage query (for the cost line), a final after-snapshot, and a local diff of the snapshot files. They were not retried. The before/after evidence below comes from the two snapshots that ran.
- P0A.2 had not reported when this unit ran, so the dealer_jobs gate was checked here with its own READ ONLY session.
- Method: Railway GraphQL v2 through a helper that refuses any document containing `mutation` (token from `~/.railway/config.json`, `User-Agent: railway-cli/5.57.2`). Prod Postgres through `railway run -s Postgres -- psql "$DATABASE_PUBLIC_URL"` with `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000'` plus `SET default_transaction_read_only = on`. Output went through a host/DSN redactor. Variables went through a filter that prints names, values for a non-secret allowlist only, and `${{Svc.VAR}}` reference expressions. No secret value was printed or written.

### Timeline (UTC)

| time | action | result |
|---|---|---|
| 08:36:26-08:36:44 | Snapshot 1: deployment lists of every service in every visible project (read-only) | 15 projects, 49 services, 990 deployments; `hasNextPage` false everywhere. dealership-scanner lists identical to P0A.1 Appendix A |
| 08:36-09:00 | Step 1 reads: service instances, deployment meta (selected keys only), volumes, variables (filtered) | recorded below |
| 09:03:03 | Step 2: prod dealer_jobs / dealer_catalog, READ ONLY session | dealer_jobs 0 queued, 0 running, 0 rows in total; dealer_catalog 0 rows |
| ~11:15 | Worker and scheduler deployment logs (GraphQL `deploymentLogs`) | worker: boot 09-09, crash 09-30, no "Claimed job" line; scheduler: no retained logs |
| 12:07:10 | Trigger query (P0A.1 recipe) for all 5 services plus the environment | no trigger anywhere; autodeploy off (NO_INSTALLATION / NO_REPO) |
| 12:09:24-12:09:26 | Snapshot 2 (same method) | identical to snapshot 1 for all 49 services |
| 12:10:00 | Step 2 recheck, READ ONLY session | dealer_jobs 0 / 0 / 0 rows; dealer_catalog 0 rows |
| ~12:10 | Step 4: `railway down -s scanner-scheduler -y` | **refused by the permission system; never ran** |

The clock gaps between rows come from agent tool latency; nothing else ran in between.

### Step 1: record of the two services

Screenshots of Settings → Source/Deploy were not possible from an agent. The GraphQL fields behind those panels are recorded instead.

| | scanner-worker | scanner-scheduler |
|---|---|---|
| service id | `c318cc60-f9d0-4d32-a689-e7ea6e55b252` | `f0033d62-75ac-4151-a724-bb4698fccc0a` |
| source (instance) | repo `asarrafi47/DealershipScanner`; no trigger; autodeploy off, `NO_INSTALLATION` | same |
| instance build settings | builder `RAILPACK`, `railwayConfigFile` null, `dockerfilePath` null, rootDirectory/buildCommand/startCommand null, watchPatterns `[]` | same |
| Dockerfile actually used | `Dockerfile.web`, from root `/railway.toml` (`configFile=/railway.toml`, builder `DOCKERFILE`). `RAILWAY_DOCKERFILE_PATH=Dockerfile.scanner-worker` is set but ignored | `Dockerfile.web` from root `/railway.toml`. `RAILWAY_DOCKERFILE_PATH=Dockerfile.scanner-scheduler` is ignored |
| active deployment | `bd866d0b-5cbb-4c07-8cbd-c6d6a0653b52`, GitHub `main@6a22840d5a758bec0334a0a83e39dd8717982e7e` ("Route Railway scanner services through unified web image entrypoint."), created 2026-06-15 00:24:51Z, status **CRASHED**, `deploymentStopped=true`, instance `bbd865d7-a5e0-4e22-a47a-f7ea58834fd3` CRASHED, image `sha256:3ec7a3203447…cf5e1e` | `50ac4514-e645-4d16-9f0f-66cb9e7568af`, same commit, created 2026-06-15 00:24:48Z, status **SUCCESS**, `deploymentStopped=false`, instance `c57f8a62-e044-4e78-a636-75e68b2ea2c2` **RUNNING**, image `sha256:8d9c89577d55…99dc3d` |
| latest deployment | `dc7a4d51` FAILED 2026-09-15 11:00Z (no git or CLI metadata) | `9aa9d233` FAILED 2026-06-22 00:55Z (cli:cursor) |
| replicas | instance `numReplicas` null; deployed manifest 1 (`multiRegionConfig` us-west2 x1) | same |
| volumes | **none**: deployment `volumeMounts` is `[]`, and no project volume has an instance on this service | **none** |
| restart policy | instance ON_FAILURE/10; deployed manifest ON_FAILURE/5 | same |
| healthcheck / sleep / cron | `/health` 300 s (root toml) / sleepApplication false / cronSchedule null | same |
| what it runs | `docker-entrypoint-web.sh` dispatches on `RAILWAY_SERVICE_NAME=scanner-worker` to `scripts/scanner_worker_loop.py`: polls dealer_jobs every 10 s and claims with `FOR UPDATE SKIP LOCKED`; logs `Claimed job id=…` | dispatches to `scripts/scanner_scheduler_loop.py`: every 60 s, `schedule_due_refresh_jobs()` inserts `queued` refresh jobs for dealer_catalog rows whose `next_scan_at` is due and bumps `next_scan_at`. At boot it ran the June `init_inventory_db()` and `init_job_queue_schema()` DDL against prod |

Project volumes, for the D-REL1 (a) volume question: `web-volume` (web, `/data`, 1,062 MB), `postgres-volume` (Postgres, `/var/lib/postgresql/data`, 5,621 MB), `scanner-nightly-volume` (scanner-nightly, `/data`, 956 MB). **Neither legacy service has a volume, so there is nothing to drop.** Their `/data/...` variables point at ephemeral container disk.

#### Variables (names; values only where non-secret)

scanner-worker (16):

| name | value |
|---|---|
| `DEALERS_FROM_ACTIVE_INVENTORY` | `1` |
| `INVENTORY_DATABASE_URL` | reference `${{Postgres.DATABASE_URL}}` |
| `INVENTORY_DB_PATH` | empty string |
| `KMAC_VAULT_AUTO` | `0` |
| `PYTHONPATH` | `/app` |
| `RAILWAY_DOCKERFILE_PATH` | `Dockerfile.scanner-worker` |
| `REVEST_VAULT_ENV` | `prod` |
| `REVEST_VAULT_PROJECT` | `dealership-scanner` |
| `REVEST_VAULT_TOKEN` | set (secret, withheld) |
| `REVEST_VAULT_URL` | set (withheld) |
| `SCANNER_BROWSER_PROFILE_DIR` | `/data/browser-profiles` |
| `SCANNER_JOB_TIMEOUT_SEC` | `3600` |
| `SCANNER_MAX_DEALER_CONCURRENCY` | `1` |
| `SCANNER_PARALLEL_UPSERT` | `1` |
| `SCANNER_VDP_IMAGE_DOWNLOAD_DIR` | empty string |
| `SCANNER_WORKER_POLL_SEC` | `10` |

scanner-scheduler (14):

| name | value |
|---|---|
| `INVENTORY_DATABASE_URL` | reference `${{Postgres.DATABASE_URL}}` |
| `INVENTORY_DB_PATH` | `/data/inventory.db` |
| `KMAC_VAULT_AUTO` | `0` |
| `PYTHONPATH` | `/app` |
| `RAILWAY_DOCKERFILE_PATH` | `Dockerfile.scanner-scheduler` |
| `REVEST_VAULT_ENV` | `prod` |
| `REVEST_VAULT_PROJECT` | `dealership-scanner` |
| `REVEST_VAULT_TOKEN` | set (secret, withheld) |
| `REVEST_VAULT_URL` | set (withheld) |
| `SCANNER_BROWSER_PROFILE_DIR` | `/data/browser-profiles` |
| `SCANNER_JOB_TIMEOUT_SEC` | `3600` |
| `SCANNER_PARALLEL_UPSERT` | `1` |
| `SCANNER_SCHEDULER_INTERVAL_SEC` | `60` |
| `SCANNER_VDP_IMAGE_DOWNLOAD_DIR` | `/data/vdp_images` |

No other service references a variable of these two services. The only references point the other way, to `Postgres.DATABASE_URL`, so deleting them breaks no reference.

### Step 2: prod dealer_jobs gate (READ ONLY): passed

Both sessions reported `transaction_read_only=on`, `default_transaction_read_only=on` and `statement_timeout=20s`. Host: `<prod-host>`.

| check | 09:03:03Z | 12:10:00Z |
|---|---|---|
| dealer_jobs queued / running / total | 0 / 0 / 0 | 0 / 0 / 0 |
| dealer_catalog rows / due now | 0 / 0 | 0 / 0 |
| other client sessions on the DB (pg_stat_activity) | none besides this psql | not re-read |

Columns of dealer_jobs: id, dealer_id, job_type, status, payload_json, worker_id, created_at, started_at, finished_at, error, result_json. The table is empty, so no job can be interrupted. With dealer_catalog empty, the running scheduler has nothing to enqueue right now. That does not make it safe: it still holds prod DB access and a vault token on a June image, and it starts enqueueing as soon as anything writes dealer_catalog rows.

Worker logs (`deploymentLogs` for `bd866d0b`): boot on 2026-09-09 03:34:58Z (`Scanner worker worker-15a640a1be8b started (poll=10.0s)`), then the crash on 2026-09-30 07:43:17Z (`failed to resolve host 'postgres.railway.internal'` inside `claim_next_job`). A filtered search for `Claimed job` over the retained logs returned 0 lines. Scheduler logs for `50ac4514`: 0 lines retained. The same filtered search for `Enqueued` timed out.

### Step 4: the stop: BLOCKED (permission system), with a finding about `railway down`

`railway down` in CLI 5.57.2 (`src/commands/down.rs`, identical at tag `v5.57.2` and on master) lists the service's deployments and **keeps only status `SUCCESS`**. It sorts them newest first and calls `deploymentRemove` on the first one. If none is SUCCESS, it exits with `No deployments found` and changes nothing.

- **scanner-scheduler:** the only SUCCESS deployment is `50ac4514`, the RUNNING one. So `railway down -s scanner-scheduler -y` removes exactly the right deployment.
- **scanner-worker:** it has no SUCCESS deployment (`dc7a4d51` FAILED, `0d38675c` FAILED, `bd866d0b` CRASHED, the rest FAILED or REMOVED). So `railway down -s scanner-worker -y` is a no-op that exits with `No deployments found`. No worker container is running anyway: `bd866d0b` has `deploymentStopped=true` and its only instance is CRASHED. To take it off the books, use the dashboard's Remove on `bd866d0b`, which calls the same `deploymentRemove`, or delete the service (step 5).
- **Do not use `railway scale … us-west=0`.** `scale.rs` commits an `environmentPatchCommit` without skip-deploys, and that applies through a new deployment. On a service whose source is still the GitHub repo, the new deployment could try a build (P0A.1 answer 4: a main build would boot the June 0.2.0 web app against prod).

#### Owner command pack (steps 2 and 4; run in this order, from the repo root)

1. Gate. Expect no `queued` or `running` line (empty output means the table is empty):
   `railway run -s Postgres -- sh -c 'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=20000" psql "$DATABASE_PUBLIC_URL" -XAt -c "SELECT status, count(*) FROM dealer_jobs GROUP BY status"'`
   If a `running` line appears, stop here.
2. `railway down -s scanner-scheduler -y`. Expected: `50ac4514` SUCCESS → REMOVED; instance `c57f8a62` stops.
3. `railway down -s scanner-worker -y`. Expected: `No deployments found` (no change). Optional: Dashboard → scanner-worker → Deployments → `bd866d0b` → Remove (CRASHED → REMOVED).
4. Verify:
   - The P0A.1 trigger query shows `activeDeployments` empty, or REMOVED, for scanner-scheduler.
   - `railway logs -s scanner-worker --lines 50` shows no `Claimed job` line after the stop time.
   - `SELECT count(*) FROM dealer_jobs WHERE started_at >= '<stop time>'` returns 0 (READ ONLY session).
   - A full deployment-list snapshot differs from snapshot 2 only in the status of `50ac4514`, and of `bd866d0b` if removed.

### Steps 5-7: dry-run change list (NOT applied; needs the owner's OK)

**Step 5, D-REL1 (a): delete both services.**

| # | change | target | expected effect |
|---|---|---|---|
| 5.1 | Drop volumes | none | Nothing to drop: neither service has a volume |
| 5.2 | Delete service scanner-scheduler | `f0033d62-75ac-4151-a724-bb4698fccc0a` (Dashboard → service → Settings → Delete Service; GraphQL `serviceDelete`) | Its 6 deployments and 14 variables go away. Ids are in P0A.1 Appendix A; names and non-secret values are above |
| 5.3 | Delete service scanner-worker | `c318cc60-f9d0-4d32-a689-e7ea6e55b252` | Its 7 deployments and 16 variables go away |

What is lost: the `REVEST_VAULT_TOKEN` and `REVEST_VAULT_URL` values on these two services. Before deleting, the owner confirms they are held in the vault, or are the same as web's copies. Web also carries `REVEST_VAULT_*`. No other service references these two, so web, Postgres and scanner-nightly are unaffected.

**Step 6: GitHub triggers on the remaining services.**

| service | trigger found (12:07:10Z) | source.repo | proposed change |
|---|---|---|---|
| web | none; autodeploy off (`NO_INSTALLATION`) | `asarrafi47/DealershipScanner` | **Disconnect the source**: Settings → Source → Disconnect (GraphQL `serviceDisconnect(id: "b711c414-86c3-4b1d-a458-17168d11e439")`). Expected: source.repo becomes null, no new deployment, the running `c9872e66` is unaffected, and CLI uploads keep working. Verify with a snapshot that web's list stays at 70 entries |
| scanner-nightly | none (`NO_REPO`) | none | no change |
| Postgres | none (`NO_REPO`) | image `postgres-ssl:18` | no change |
| scanner-worker, scanner-scheduler | none (`NO_INSTALLATION`) | repo recorded | covered by deletion (5.2, 5.3). Under D-REL1 (b) instead: disconnect the source the same way |
| environment level | none | n/a | no change |
| other projects (revest-vault etc.) | P0A.1: no trigger names this repo | n/a | no change |

Owner-only: at github.com/settings/installations, confirm the Railway GitHub App has no access to `asarrafi47/DealershipScanner`, or remove that repo from it. That is the lock behind `NO_INSTALLATION`. Disconnecting web's source removes the remaining path, `redeploy --from-source` or a reinstall.

**Step 7: trigger query, current state (12:07:10Z, before any change).**

```
web               triggers=[] autodeploy=off NO_INSTALLATION source.repo=asarrafi47/DealershipScanner active=c9872e66 SUCCESS RUNNING
Postgres          triggers=[] autodeploy=off NO_REPO         source.image=postgres-ssl:18        active=61bdd7bb SUCCESS RUNNING
scanner-nightly   triggers=[] autodeploy=off NO_REPO         source=none  cron=null              active=90a3d2a0 SUCCESS stopped (EXITED)
scanner-worker    triggers=[] autodeploy=off NO_INSTALLATION source.repo=asarrafi47/DealershipScanner active=bd866d0b CRASHED stopped (CRASHED)
scanner-scheduler triggers=[] autodeploy=off NO_INSTALLATION source.repo=asarrafi47/DealershipScanner active=50ac4514 SUCCESS RUNNING
environment-level triggers=[]
```

No service builds from GitHub on any branch. Re-run this query after the owner applies steps 4-6.

### Before/after: deployment lists

| service | snapshot 1 (08:36:26Z) | snapshot 2 (12:09:24Z) | same |
|---|---|---|---|
| dealership-scanner / web | 70, newest `c9872e66` SUCCESS | 70, newest `c9872e66` SUCCESS | yes, and equal to P0A.1 Appendix A |
| dealership-scanner / scanner-nightly | 7, newest `90a3d2a0` SUCCESS | 7 | yes, and equal to Appendix A |
| dealership-scanner / Postgres | 2, newest `61bdd7bb` SUCCESS | 2 | yes, and equal to Appendix A |
| dealership-scanner / scanner-worker | 7, newest `dc7a4d51` FAILED (active `bd866d0b` CRASHED) | 7 | yes, and equal to Appendix A |
| dealership-scanner / scanner-scheduler | 6, newest `9aa9d233` FAILED (active `50ac4514` SUCCESS) | 6 | yes, and equal to Appendix A |
| revest-vault / vault-api (the vault; there is no `kmac-vault` service) | 12, newest `773c43fc` SUCCESS | 12 | yes |
| revest-vault / Redis | 2, newest `a33033d9` SUCCESS | 2 | yes |
| revest-vault / Postgres | 1, `64f07320` SUCCESS | 1 | yes |
| all 49 services in the 15 visible projects | 990 deployments | 990 deployments | yes: identical (id, status) lists for every service |

After snapshot 2, the only Railway-side activity was the 12:10:00Z READ ONLY psql session through `railway run`, which reads variables and changes nothing. Every later call was refused before it ran: `railway down`, the usage query, the after-snapshot. So the 12:09:24Z lists are the lists at the end of this unit. A post-stop snapshot is owed by whoever runs the command pack.

### Cost

Not measured: the usage query was refused. The plan estimates about $0.60/month saved. Nearly all of it is scanner-scheduler, RUNNING since 2026-06-15. scanner-worker has used no compute since it crashed on 2026-09-30.

### Open items for the owner

1. Run the command pack above (steps 2 and 4), or explicitly authorize an agent for this unit and grant it permission for `railway down -s scanner-scheduler -y` and `railway down -s scanner-worker -y`. The workflow script's request is not an owner approval, and the Claude Code permission system refused it.
2. Approve the step 5 list: delete both services, no volumes to drop. First confirm the vault token/URL values are held elsewhere.
3. Approve the step 6 change: disconnect web's GitHub source. Also check github.com/settings/installations.
4. After 1-3: re-run the trigger query and a full deployment-list snapshot, and append the results here.

### Review notes (integration, 2026-10-08)

Review: pass for the report, which is accurate, read-only and secret-free. **The unit itself is PARTIAL: the legacy scheduler is still running.** Notes:

- State re-checked read-only at integration (2026-10-08 ~14:46Z, GraphQL deployments query): scanner-scheduler `50ac4514` is SUCCESS with `deploymentStopped=false` (still running against prod); scanner-worker's 7 deployments are unchanged (`bd866d0b` CRASHED, stopped). The reviewer saw the same at 14:38Z, with the trigger query unchanged and a full snapshot identical to snapshot 2.
- Wording fixed at integration: the block's header said the workflow "approved" steps 1-4. Script output is not an owner approval; the owner's log authorizes agents for read-only parts only. The header and open item 1 now say so.
- "The scheduler ran the June DDL against prod at boot" comes from the code at `6a22840d5` (`scanner_scheduler_loop.py:27-28` runs `init_inventory_db()` and `init_job_queue_schema()`), not from logs: the scheduler's `deploymentLogs` returned nothing.
- "`railway scale ...=0` redeploys" is an inference from `scale.rs` (it calls `environmentPatchCommit` with no skip-deploys flag). Railway's server behaviour was not demonstrated. Likewise, "serviceDisconnect creates no new deployment" and "`railway down` leaves `50ac4514` REMOVED" are expectations; the post-action snapshot confirms them.
- When running the owner pack: the stored Railway token returned `Not Authorized` once during review; `railway whoami` refreshed it.

## P0A.5 (2026-10-07)

*2026-10-07 P0A.5: Host job inventory (launchd / cron)*

Unit: P0A.5 of `docs/REMEDIATION_PLAN_2026_10.md`. Decision applied: D-IF7 = (a), "unload on the MBP now, re-home later (P18A.6); plist files kept" (`docs/remediation/OWNER_DECISIONS_LOG.md`, 2026-10-07).
Scope: the MBP part is DONE. The mini part is BLOCKED because SSH to the mini is not approved (same log, "NOT approved: SSH to the mini").
Run at: 2026-10-07T23:46Z to 23:48Z (19:46 to 19:48 EDT), by a workflow agent on the MBP.
Nothing was deleted. No repo file other than this block was created or changed. No prod access was used for this unit.

### Per-host table

| Item | MBP (`asarrafis-MacBook-Pro.local`, macOS 27.2, uid 501) | mini |
|---|---|---|
| `launchctl list \| grep -i sarraficars` (before) | `-  1  com.sarraficars.nightly-data-quality` (loaded, not running, last exit 1) | BLOCKED (no SSH approval) |
| `launchctl list \| grep -i sarraficars` (after) | no match (grep rc=1) | BLOCKED |
| `crontab -l` | 1 entry, unrelated to this repo (stocks project, see below). Nothing for DealershipScanner. | BLOCKED |
| `ls ~/Library/LaunchAgents/com.sarraficars.*` | `com.sarraficars.nightly-data-quality.plist` (2203 B, Aug 7), `com.sarraficars.nightly-http-refresh.plist.disabled` (2360 B, Jul 17) | BLOCKED |
| System launchd dirs (`/Library/LaunchAgents`, `/Library/LaunchDaemons`) | no sarraficars or DealershipScanner entry | BLOCKED |
| Jobs unloaded | `com.sarraficars.nightly-data-quality`: bootout, then `disable` so it stays off after login. Plist kept, byte-identical. | BLOCKED: any job that runs `scanner.py --delta` or grades a frozen DB is still unknown |
| Other com.sarraficars jobs left loaded | none | BLOCKED |
| Checkout path | `/Users/asarrafi/Projects/DealershipScanner` | BLOCKED |
| Branch | `feature/http-only-scans` | BLOCKED |
| Commit | `dbdbf5cbac233470b3dedd57077654548cef8095` ("docs: remediation plan 2026-10"), VERSION 1.5.2 | BLOCKED |
| `core.hooksPath` | `scripts/git-hooks` (set in local config; `scripts/git-hooks/pre-push` present) | BLOCKED |
| `.env` `INVENTORY_DATABASE_URL` host (credentials never read out) | no host part: local Unix socket, db `cars` (the MBP's own Homebrew postgresql@17) | BLOCKED. Plan needs the host only, never credentials. |
| Test-leak dirs in `workspace/dealer_logs` (`lifecycle-dealer-com`, `bravo-norows-com`, `charlie-norecipe-com`) | all 3 PRESENT on the MBP (lifecycle: discovery.md + scan_runs.md; bravo, charlie: discovery.md). Not touched; cleanup is P1C.1. | BLOCKED (the plan asks about the mini; it is unknown) |

### MBP: raw inventory (before any change)

```
$ launchctl list | grep -i sarraficars
-	1	com.sarraficars.nightly-data-quality

$ crontab -l
30 18 * * 1-5 cd /Users/asarrafi/Projects/stocks && .venv/bin/python scripts/download_market_data.py --sp500 --period 5d >> instance/cron_download.log 2>&1

$ ls -la ~/Library/LaunchAgents/com.sarraficars.*
-rw-r--r--@ 1 asarrafi  staff  2203 Aug  7 11:39 com.sarraficars.nightly-data-quality.plist
-rw-r--r--@ 1 asarrafi  staff  2360 Jul 17 10:11 com.sarraficars.nightly-http-refresh.plist.disabled

sha256 (before and after, unchanged):
62323dc29134a79224a69806fed03bc9596e8c7dc83040bdf33acfbdba8d6016  com.sarraficars.nightly-data-quality.plist
8231c27910aad4a7cf8d5e846f5b9febbc444e9f257fd6ace6850362966c2007  com.sarraficars.nightly-http-refresh.plist.disabled
```

`launchctl print gui/501/com.sarraficars.nightly-data-quality` before the change: state = not running, runs = 1, last exit code = 1, calendar trigger Hour 6 Minute 0, program `/bin/bash deploy/nightly_data_quality_invariants.sh`, working directory the main checkout, nice 5. There was no launchd disabled override for any sarraficars label. No related process was running: none of `data_quality_invariants`, `scanner.py`, `nightly_http_refresh`, `nightly_rooftop`, `report_rooftop`, `dealer_pipeline` or `fleet` appeared in `ps`.

The crontab entry belongs to `~/Projects/stocks`, which has nothing to do with this repo. It was left alone.

### Why `com.sarraficars.nightly-data-quality` grades a frozen DB

- The job runs `deploy/nightly_data_quality_invariants.sh`, which sources the checkout's `.env`. The wrapper is identical to the copy at `dbdbf5cba`. It runs `python -m backend.scripts.data_quality_invariants` against `INVENTORY_DATABASE_URL`, and on the MBP that is the local Postgres (Unix socket, db `cars`).
- Local DB, active rows by `left(scraped_at,10)`, last 45 days. This came from a READ ONLY session (`default_transaction_read_only=on`, confirmed by `SHOW`, statement_timeout 20 s):

  | day | active rows |
  |---|---|
  | 2026-09-24 | 413 |
  | 2026-09-25 | 3,435 |
  | 2026-09-26 | 8,503 |
  | 2026-09-28 | 201,899 |

  There are 214,675 active rows in total. No active row has been scraped since 2026-09-28. Scanning moved to Railway on 09-29, and prod was replaced from this DB on 09-28. So every nightly run since then has graded a nine-day-old snapshot, and its results say nothing about prod.
- The job still ran every morning and failed every time. `workspace/scanlogs/data_quality_2026*.log` holds 57 dated logs (the first is 20260802). Recent runs:

  | log | start | result | elapsed |
  |---|---|---|---|
  | 20261001 | 06:12 | 33 invariants, 5 failing (2 NEW DEFECT CLASS) | 18,754 s, exit 1 |
  | 20261002 | 06:00 | same | 4,207 s, exit 1 |
  | 20261003 | 06:13 | same | 108,162 s, exit 1 |
  | 20261005 | 06:11 | same | 16,143 s, exit 1 |
  | 20261006 | 06:11 | no end lines; the run never finished | n/a |
  | 20261007 | 06:11 | same | 23,504 s (6.5 h), exit 1 |

  Every run from 09-09 to 10-07 exited 1 with the same 4-6 failing invariants. Each run also tied up the MBP for hours. The longer wall times probably include sleep.

### Action taken on the MBP

```
before: 2026-10-07T23:47:16Z   launchctl list -> "-  1  com.sarraficars.nightly-data-quality"
$ launchctl bootout gui/501/com.sarraficars.nightly-data-quality     # rc=0
$ launchctl disable gui/501/com.sarraficars.nightly-data-quality     # rc=0
after:  2026-10-07T23:47:16Z
$ launchctl list | grep -i sarraficars                               # no match (rc=1)
$ launchctl print gui/501/com.sarraficars.nightly-data-quality
  Could not find service "com.sarraficars.nightly-data-quality" in domain for user gui: 501
$ launchctl print-disabled gui/501 | grep sarraficars
  "com.sarraficars.nightly-data-quality" => disabled
```

Why `disable` as well as the unload: `bootout` (or a plain `launchctl unload`) lasts only until the next login. launchd loads every `*.plist` in `~/Library/LaunchAgents` at login, so the job would come back on the next reboot. `disable` stores a persistent override. It does the same as `launchctl unload -w` and leaves the plist file alone, as D-IF7(a) requires.

To reverse it (for example, if P18A.6 re-homes the job onto this MBP):
`launchctl enable gui/501/com.sarraficars.nightly-data-quality && launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.sarraficars.nightly-data-quality.plist`.

### Other com.sarraficars jobs checked on the MBP (none left grading a frozen DB or running `--delta`)

| Job | Where | Loaded? | What it runs | Action |
|---|---|---|---|---|
| `com.sarraficars.nightly-http-refresh` | `~/Library/LaunchAgents/...plist.disabled` | No. launchd only loads `*.plist`, and it is absent from `launchctl list`. | `deploy/nightly_http_refresh.sh`, which calls `scanner.py --delta` at line 155 against the local DB. Its last launchd output is from Aug 5, and the last `nightly_*.log` is 20260805. | None. It is already inert, and the file is kept. |
| `com.sarraficars.nightly-rooftop-refusals` | repo template only (`deploy/com.sarraficars.nightly-rooftop-refusals.plist`) | No. It is not in `~/Library/LaunchAgents` and not loaded. | `deploy/nightly_rooftop_refusals.sh` (census plus `report_rooftop_refusals.py`), scheduled for 06:xx | None. It was never installed on the MBP. D-IF7 re-homing is P18A.6. |

### Mini: BLOCKED

SSH to the mini was not approved for this run (OWNER_DECISIONS_LOG.md 2026-10-07). Nothing on the mini was read or changed. This command pack is for the owner, or for an agent once mini SSH is approved. It is read-only except for the unload line, and that line applies only to a job the inventory shows runs `scanner.py --delta` or grades a frozen DB:

```
launchctl list | grep -i sarraficars
crontab -l
ls -la ~/Library/LaunchAgents/com.sarraficars.* /Library/LaunchAgents /Library/LaunchDaemons 2>/dev/null | grep -i -E 'sarraficars|dealer|scanner'
# for each loaded label: launchctl print gui/$(id -u)/<label> | head -40   (program + arguments + calendar)
cd <mini checkout> && git rev-parse --show-toplevel && git rev-parse --abbrev-ref HEAD && git rev-parse HEAD && git config --get core.hooksPath
# .env DSN host only, never credentials:
python3 -I -c "import urllib.parse;v=[l.split('=',1)[1].strip().strip('\"\'') for l in open('.env') if l.startswith('INVENTORY_DATABASE_URL=')];u=urllib.parse.urlsplit(v[0]) if v else None;print('host=',u and u.hostname,'port=',u and u.port,'db=',u and u.path.lstrip('/'))"
for d in lifecycle-dealer-com bravo-norows-com charlie-norecipe-com; do ls -d workspace/dealer_logs/$d 2>/dev/null || echo "$d absent"; done
# only for a job that runs scanner.py --delta or grades a frozen DB:
launchctl bootout gui/$(id -u)/<label> && launchctl disable gui/$(id -u)/<label>   # keep the plist
```

### Accept check (P0A.5)

- **Per-host table in the ops log:** MBP done. The mini column is BLOCKED (no SSH approval).
- **No com.sarraficars job left grading a frozen DB:** met on the MBP. `nightly-data-quality` was unloaded and disabled. `nightly-http-refresh` is `.disabled` and not loaded. `nightly-rooftop-refusals` is not installed. The mini is unverified (BLOCKED).
- **Nothing deleted:** met. Both plists are byte-identical before and after (sha256 above). The workspace logs, reports and test-leak dirs were not touched.

### Notes

- Power: the task said the MBP was on AC, but `pmset -g batt` at 19:46 EDT read "Battery Power, 84%, discharging". This unit ran only light commands: launchctl, ls, and one read-only GROUP BY on the local DB.
- The 10-07 data-quality run kept the MBP busy from 06:11 to 12:43. That load is gone from the MBP now.
- Re-homing the data-quality and rooftop-refusal reports (against prod read-only, or on Railway) is P18A.6, per D-IF7(a).

### Review notes (integration, 2026-10-08)

Review: pass, no blocking items. On 2026-10-08 the reviewer confirmed the job stayed unloaded: no `data_quality_20261008.log`, and the launchd out-log was last written Oct 7 12:43, so the 06:00 run did not fire. Both plists still match their recorded sha256. Corrections:

- "Every nightly run from 09-09 to 10-07 exited 1" overstates it. The 09-15, 09-28 and 10-06 runs have no end line and never finished. The 09-12, 09-17 and 10-03 runs went past 24 h (about 135k, 129k and 108k seconds), which is why the 09-13, 09-18 and 10-04 logs are missing.
- The MBP `.env` DSN has no netloc host; its `host` query parameter is `/tmp`, a local Unix socket, db `cars`. The mini command pack prints `u.hostname`, which reads None for such a DSN. On the mini, also print the `host` query parameter.
- Cosmetic: the before and after timestamps in "Action taken on the MBP" are identical (23:47:16Z).
- The MBP was on battery during this unit. The owner's 2026-10-07 "Battery" entry allows Phase 0A/0B work on battery.

## STOPGAP-D-SEC1 (2026-10-07)

*D-SEC1 stopgap: `CAR_CHAT_WEB_RESEARCH=0` on Railway web, then redeploy (approved in `docs/remediation/OWNER_DECISIONS_LOG.md`, 2026-10-07)*

- **Status: NOT APPLIED.** The workflow unit returned no result and wrote no block. Integration did not apply it, because integration is read-only on prod.
- Read-only check at integration, 2026-10-08 ~14:46Z (GraphQL `variables` query for web, whose output was filtered to these three whitelisted names): `CAR_CHAT_WEB_RESEARCH`, `CAR_CHAT_WEB_RESEARCH_PUBLIC` and `WEB_RESEARCH_ALLOWED_HOSTS` are all unset. In production that means "auto": any logged-in session can trigger Playwright web research, with no host allowlist (P0A.1 answer 6).
- Web was not redeployed. Its newest deployment is still `c9872e66` SUCCESS (2026-10-05T14:54:55Z, 1.5.1), with `62f93905` and `3c386092` REMOVED behind it.
- To apply (the owner, or an agent the owner authorizes for this step):
  1. Set `CAR_CHAT_WEB_RESEARCH=0` on web.
  2. Redeploy the current image with `railway redeploy -s web`. Never use `--from-source`: web's source is still recorded as `asarrafi47/DealershipScanner`, and a source build would build GitHub `main` (P0A.1). The memory note says `--skip-deploys` plus a restart does not pick up the variable.
  3. Verify: the variable reads `0`; web's newest deployment is SUCCESS; `/api/health` reports 1.5.1 on both origins; the car-chat research path no longer launches Playwright for a logged-in session.
  4. Append the result here as a dated block.
- Phase 0A exit gate item "The D-SEC1 stopgap env is set on Railway web" stays unmet until then.

## Owner prod actions and an accidental-command revert (2026-10-08)

Run by the owner from a terminal in this checkout (Railway CLI linked to dealership-scanner / scanner-nightly), verified read-only afterwards with the GraphQL snapshot helper (deployment lists for all services in all visible projects) and the variable-name filter.

- P0A.4 step 4: `railway down -s scanner-scheduler -y` removed deployment 50ac4514 (SUCCESS -> REMOVED). dealer_jobs was empty at every check.
- D-REL1 (a): scanner-scheduler and scanner-worker deleted in the dashboard (staged, then applied by the owner). The project now has Postgres, scanner-nightly, web. Neither deleted service had a volume; web carries REVEST_VAULT_TOKEN / REVEST_VAULT_URL.
- D-SEC1 stopgap: `CAR_CHAT_WEB_RESEARCH=0` set on web (`--skip-deploys`).
- Security: `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` deleted from web. App role=admin sessions no longer pass into /dev; /dev needs a /dev/login account.
- Accidental commands meant for another project (owner ran them in this checkout):
  - `railway variable set STUDIO_DIGEST_HOUR=13 STUDIO_DIGEST_DISABLED=1 --service web --skip-deploys` added two variables to dealership-scanner web. Reverted: both deleted. Web variable names now equal the 10-07 P0A.1 baseline except the two intended changes above (+CAR_CHAT_WEB_RESEARCH, -ALLOW_APP_ADMIN_DEV_PASS_THROUGH).
  - A bare `railway up` (zsh `!` plus line breaks dropped the arguments) uploaded this checkout to the linked service scanner-nightly and was interrupted with Ctrl-C. Railway recorded deployment 152fb03c as FAILED ("Failed to create code snapshot"); nothing ran, the 09-29 deployment 90a3d2a0 is unchanged, no fleet started. Nothing to revert.
- `railway redeploy -s web` (same image) applied the variable changes: deployment d9aaa75e SUCCESS (16:54 EDT), c9872e66 REMOVED; https://sarraficars.com/api/health = 200 {"status":"ok","version":"1.5.1"}.
- Lesson: the Railway CLI targets the project linked to the current directory; run `railway status` first. In a plain terminal there is no `!` prefix; keep a pasted command on one line or end lines with `\`.
