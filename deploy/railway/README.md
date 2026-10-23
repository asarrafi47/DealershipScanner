# Deploy DealershipScanner to Railway

DealershipScanner is its own Railway project, `dealership-scanner`, environment
`production`. The service list below is the current one. The vault material in sections
2 and 3 and in "Local vs Railway" describes a kmac-vault design that prod does not use;
see the note under the service table.

## Services

As recorded in `docs/SCANNING_OPS_LOG.md` (P0A.1 per-service table, the 2026-10-08 owner
block and the 2026-10-16 N1.5 block). The project has three services:

| Service | What it runs | How code reaches it | State and hazards |
|---|---|---|---|
| `web` | The site: `Dockerfile.web`, gunicorn, healthcheck `/health` | `deploy/railway/deploy_web.sh` from a release tag (section 1) | Volume `web-volume` at `/data` for the user-account SQLite files (section 4). Domains: `sarraficars.com`, proxied by Cloudflare, and `web-production-26b11.up.railway.app`, which also serves the site until N3.13 step 6 removes it (D-SEC6). GitHub source disconnected 2026-10-16. Proxy settings: section 7. |
| `scanner-nightly` | The fleet scan job: `Dockerfile.scanner`, restart policy NEVER, cron off | `deploy/railway/deploy_scanner_nightly.sh` from a release tag, and not before P6B.1 (D-REL8 (b)) | `SCAN_FLEET=0` staged 2026-10-16 (N1.5). The 2026-09-29 deployment `90a3d2a0` keeps its own `SCAN_FLEET=1`, so a restart or rollback of it runs a full fleet against prod Postgres. Do not restart, roll back or redeploy scanner-nightly until N12.2. Volume `scanner-nightly-volume` at `/data`. |
| `Postgres` | The inventory database, Railway's `postgres-ssl:18` image | Railway image updates only | web and scanner-nightly reach it through `INVENTORY_DATABASE_URL=${{Postgres.DATABASE_URL}}` (section 4). A public TCP proxy on 5432 exists. |

**Retired:** the legacy `scanner-worker` and `scanner-scheduler` services were deleted in
the dashboard on 2026-10-08 (P0A.4, D-REL1 (a)), after `railway down -s scanner-scheduler`
removed the scheduler's last running deployment. Nothing has consumed `dealer_jobs` since.
Their leftover config files in the repo leave in P15B.6 (N4).

**Vault:** there is no kmac-vault service in this project or any visible one (P0A.1). Web
carries its secrets as Railway variables, plus `REVEST_VAULT_*` names that point at
`vault-api` in the separate `revest-vault` project; no code in this repo reads those names
(`backend/utils/kmac_vault.py` reads `VAULT_ADDR` / `VAULT_TOKEN`, which the P0A.1 variable list does not show on web).

## Prerequisites

- [Railway CLI](https://docs.railway.com/guides/cli): `npm i -g @railway/cli` or `brew install railway`
- Logged in: `railway login`
- `railway status` shows project `dealership-scanner`, environment `production`. The
  main checkout is linked to scanner-nightly, so pass `-s <service>` on every command
  that acts on a service ([`docs/RELEASING.md`](../../docs/RELEASING.md), one-time
  setup). Relinking to web only moves the bare-`railway up` hazard onto the site.

## 1. Deploy the web service

Deploys come only from a tagged release on `main`, through the guarded script. The full
release checklist, the rollback recipe and the source policy are in
[`docs/RELEASING.md`](../../docs/RELEASING.md).

```bash
deploy/railway/deploy_web.sh --dry-run    # guard + stage + plan; never calls railway
deploy/railway/deploy_web.sh              # railway up <stage> --path-as-root --service web --detach
```

The script refuses (exit 1: nothing staged, railway not called) unless the tracked tree is
clean, HEAD is `main` on origin, and HEAD carries the annotated tag `v$(cat VERSION)`,
which origin must also have. It uploads a `git archive` of that tag plus `BUILD_COMMIT`
(the full SHA) and `BUILD_TAG`. Railway builds it from the root `railway.toml`, which
selects `Dockerfile.web`. `--keep-stage DIR` keeps the stage for a local
`docker build -f DIR/Dockerfile.web DIR`. scanner-nightly has its own script,
`deploy/railway/deploy_scanner_nightly.sh`, with the same guard.

Run either script only from the release checkout. Never check out an older tag to run
its deploy script: every release before the first one cut by remediation P2B.3 predates
the guard. In 1.4.2 to 1.5.3, `deploy_scanner_nightly.sh` ignores `--dry-run` and
deploys at once; older releases have no such script, and none has `deploy_web.sh`. The
rollback recipe in `docs/RELEASING.md` explains.

**Never attach a GitHub source to a Railway service**, and never turn on autodeploy or
run `railway redeploy --from-source`. On 2026-09-11 a GitHub build of `main` replaced
prod with the June 0.2.0 app. Do not run a bare `railway up` from a checkout either: it
uploads the working tree, uncommitted files included, to whatever service the directory
is linked to.

**Never change a web variable, in the dashboard or with the CLI, while web's
`serviceInstance.source.repo` is set.** That applies to every variable in sections 2 to 7
below. On 2026-09-11 a web variable change rebuilt web from GitHub `main`. Check first,
every time, with `web_source_check`, the read-only query in
[`docs/RELEASING.md`](../../docs/RELEASING.md) (Railway source policy, "Web's GitHub
source"). It must print `web-source-clear`. The owner disconnected web's source on
2026-10-16 (Settings → Source → Disconnect) and the check printed `web-source-clear`
(N1.5). While it does, web variable changes deploy the current image, as the 2026-10-08
redeploy `d9aaa75e` did; stage them with `--skip-deploys` and redeploy once, as
RELEASING.md describes.

## 2. Wire vault connection (recommended)

**Do not run `sync-vault-to-railway.sh` as written.** It sets about a dozen variables
and deletes three without `--skip-deploys`, so each change can be a deploy of its own. It
also sets the kmac-vault variables `VAULT_ADDR` and `VAULT_TOKEN`, which prod does not use
(Services, above). Apply variable changes as
[`docs/RELEASING.md`](../../docs/RELEASING.md) (Railway source policy) describes, and on
web only after `web_source_check` prints `web-source-clear`.

Since P3.7 the script refuses (exit 1, before its first `railway` call, so nothing
changes) unless two variables are set in the environment:

- `TRUSTED_PROXY_HOPS`, a positive integer. It no longer defaults to `1`; prod web needs
  `2` since 1.4.4 (section 7). Read the live value first:
  `railway variable list -s web --kv | grep '^TRUSTED_PROXY_HOPS='`.
- `RAILWAY_SERVICE`, a service name. Every call the script makes, the deletes and the
  `--mirror-secrets` sets included, passes `--service` with it. Before P3.7 an unset
  `RAILWAY_SERVICE` sent every call to the checkout's linked service, which is
  scanner-nightly in the main checkout, where a deploying variable change is a deployment
  before P6B.1.

If the owner does decide to run it, the form is:

```bash
TRUSTED_PROXY_HOPS=2 RAILWAY_SERVICE=web ./deploy/railway/sync-vault-to-railway.sh
```

This sets:

| Railway variable | Value |
|------------------|--------|
| `KMAC_VAULT_AUTO` | `1` |
| `VAULT_ADDR` | `http://kmac-vault.railway.internal:9999` (override with `--vault-addr`) |
| `VAULT_TOKEN` | Copied from local kmac vault token (must match Railway vault) |
| `VAULT_LOAD_RETRIES` | `8` (vault may cold-start after deploy) |
| `FLASK_ENV` | `production` |
| `TRUST_PROXY_HEADERS` / `TRUSTED_PROXY_HOPS` | `1` / the `TRUSTED_PROXY_HOPS` value from your shell, which is required (prod web: `2`, section 7) |
| DB paths | `/data/*.db` (users/dev_users/dealer_portal only — inventory is Postgres) |

If your vault service uses a different name or port, set `VAULT_ADDR` explicitly:

```bash
TRUSTED_PROXY_HOPS=2 RAILWAY_SERVICE=web ./deploy/railway/sync-vault-to-railway.sh --vault-addr 'http://my-vault.railway.internal:9999'
```

### Railway variable references (dashboard alternative)

In the web service → Variables, you can reference the vault service:

```
VAULT_ADDR=http://${{kmac-vault.RAILWAY_PRIVATE_DOMAIN}}:${{kmac-vault.PORT}}
```

Set `VAULT_TOKEN` to the same bearer token the kmac-vault service uses (copy once from vault deploy config).

### Fallback: mirror secrets into Railway

Not for prod. `--mirror-secrets` copies `Dealer:*` values from a local kmac vault into
Railway variables on `RAILWAY_SERVICE`, one deploying set per secret. Prod web already
carries its secrets as Railway variables (Services, above); change one with the
variable-change rule in [`docs/RELEASING.md`](../../docs/RELEASING.md) instead.

## 3. Required Railway variables (web service)

| Variable | Source | Notes |
|----------|--------|--------|
| `KMAC_VAULT_AUTO` | script | `1` |
| `VAULT_ADDR` | script | Private URL to kmac-vault |
| `VAULT_TOKEN` | script / manual | Bearer token only |
| `FLASK_ENV` | script | `production` |
| `PUBLIC_BASE_URL` | manual | `https://sarraficars.com`: the public domain, never the `*.up.railway.app` one |
| `APP_ADMIN_USERNAMES` | script | `asarrafi` |
| `APP_ADMIN_EMAILS` | script | `asarrafi@sarraficars.com` |
| `TRUST_PROXY_HEADERS` | script / manual | `1` (section 7) |
| `TRUSTED_PROXY_HOPS` | script (required input, no default) / manual | `2` (section 7) |
| `TRUSTED_PROXY_CIDRS` | manual, after the N3 deploy | `cloudflare` (added by P3.2; set in N3.13 step 5; section 7) |

App secrets (`GOOGLE_MAPS_API_KEY`, `SECRET_KEY`, SQLCipher keys, etc.) live in vault under `Dealer:*` — **do not** duplicate unless using `--mirror-secrets`.

## 4. Persistent data

**Inventory is Postgres-only in production (SEC-102).** The app refuses to boot unless
`INVENTORY_DATABASE_URL` (or `DATABASE_URL`) is a `postgresql://` DSN — there is no SQLite
inventory fallback on Railway. Provision the Railway Postgres plugin on the project, then
reference it from the web service:

```
INVENTORY_DATABASE_URL=${{Postgres.DATABASE_URL}}
```

Apply the schema before first boot (from a machine that can reach the Railway DB):

```bash
INVENTORY_DATABASE_URL=... python3 -m backend.scripts.migrate --apply
```

The user-account databases have not yet moved to Postgres. Attach a **Volume** on the web
service mounted at `/data` for them (prod: `web-volume`, attached):

```
USERS_DB_PATH=/data/users.db
DEV_USERS_DB_PATH=/data/dev_users.db
DEALER_PORTAL_DB_PATH=/data/dealer_portal.db
SCANNER_VDP_DOWNLOAD_IMAGES=0
```

The volume holds databases only. Vehicle photos are served from the dealer's own CDN via the
URLs in `cars.gallery`, so the scanner does not save image bytes. If an earlier deploy set
`SCANNER_VDP_IMAGE_DOWNLOAD_DIR` or `INVENTORY_DB_PATH`, delete those variables — nothing
reads them.

Upload initial DBs or run discovery/scans after first deploy.

## 5. Health check

Railway uses `GET /health` (configured in `railway.toml`), which only needs a 200. App binds to `0.0.0.0:$PORT` automatically. `/health` and `/api/health` both return `{status, version, commit}`. `commit` is the `BUILD_COMMIT` the deploy script wrote, or `unknown` for an image built any other way.

## 6. Custom domain

In Railway → Settings → Networking → add domain, then set `PUBLIC_BASE_URL` to that URL.
Prod's domain is `sarraficars.com`, with proxied DNS in Cloudflare, so its traffic passes
two proxies; section 7 sets the proxy variables to match. Adding a domain that does not go
through Cloudflare changes that count: revisit section 7 first.

## 7. Client IP and proxy hops

Per-IP rate limits (login, register, search, car chat, `/dev` auth) and
`DEV_IP_ALLOWLIST` key on `client_ip()` (`backend/utils/client_ip.py`). A request to
`sarraficars.com` passes two proxies before the app: Cloudflare, then Railway's edge. Each
appends the address it saw to `X-Forwarded-For`, so the header arrives as
`<client>, <Cloudflare address>`, with anything the client sent itself further left.
`client_ip()` counts `TRUSTED_PROXY_HOPS` entries from the right.

**The rule:** `TRUST_PROXY_HEADERS=1` and `TRUSTED_PROXY_HOPS=2` on web, the number of
proxies in front of the Cloudflare-fronted domain (live since 1.4.4, 2026-09-30, per the
P0A.1 variable table). With `1`, every visitor's key is a Cloudflare address, so they all
share a few buckets. With a value above the real count, a client chooses its own key by
sending its own `X-Forwarded-For`. Each app process logs the entry count once
(`client_ip: X-Forwarded-For has 2 entries; TRUSTED_PROXY_HOPS=2` when the value is right),
so `railway logs -s web` shows whether the value matches the real chain.
Before changing the value, read it:
`railway variable list -s web --kv | grep '^TRUSTED_PROXY_HOPS='`. Change it only under the
variable-change rule in [`docs/RELEASING.md`](../../docs/RELEASING.md), after
`web_source_check` prints `web-source-clear`. `sync-vault-to-railway.sh` has no default for
it and refuses to run without it (section 2).

The hop count alone does not stop a spoof sent straight to Railway. Such a request passes
only Railway's edge, so with hops `2` the entry the app picks is one the sender wrote.
D-SEC6 (c) closes this with two N3 steps, both after `web-source-clear`:

- `TRUSTED_PROXY_CIDRS=cloudflare` (added by P3.2, same release). `client_ip()` then skips
  Cloudflare addresses from the right and returns the first other entry, so a direct
  request resolves to the sender's own address. The owner sets it after the N3 deploy
  (N3.13 step 5). Once it is set, `client_ip()` uses `TRUSTED_PROXY_HOPS` only to bound
  the walk and as the fallback, so keep it at `2`.
- Removing the `web-production-26b11.up.railway.app` domain (N3.13 step 6). Alone it is
  not enough: Railway's edge routes by Host header, so a direct request can still name
  `sarraficars.com` (remediation plan D-SEC6).

`DEV_IP_ALLOWLIST` stays unset until the CIDRs are live (D-SEC5).

## Local vs Railway

| | Local Docker | Railway |
|--|--------------|---------|
| Vault | `host.docker.internal:9999` | `kmac-vault.railway.internal:9999` |
| Auto-load | `KMAC_VAULT_AUTO=1` | `KMAC_VAULT_AUTO=1` + `VAULT_ADDR` + `VAULT_TOKEN` |
| Port | `WEB_PORT` (default 18000) | Railway `PORT` |
| DB encryption | `ALLOW_UNENCRYPTED_USER_DB=1` ok | SQLCipher keys from vault |

## Verify after deploy

```bash
curl -fsS https://sarraficars.com/api/health   # version = VERSION (the tag without its v), commit = the tag's full SHA
railway logs --service web
```

Look for: `Loaded N secret(s) from kmac vault: GOOGLE_MAPS_API_KEY, ...`

Dealer finder API should report `google_available: true` when `Dealer:google_maps_api_key` is in Railway vault.
