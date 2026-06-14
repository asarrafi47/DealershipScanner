# Autonomous local development (AI loops)

Rules for unattended agent development:

- **No git push** until local stack tests pass
- **Secrets:** `deploy/load-vault-env.sh` → kmac vault; scaffold if key missing
- **Stack:** `./deploy/up.sh` → postgres + web + workers + scheduler (Postgres required)
- **Verify:** health, `/billing/plans`, `docker compose ps`, worker logs; pytest gate below
- **Full-stack env:** `deploy/up.sh` writes `deploy/.env` with `INVENTORY_DATABASE_URL`

## Local verify (after sprint chunks)

```bash
curl -sf http://127.0.0.1:8000/health   # or :18000 if WEB_PORT set
curl -sf http://127.0.0.1:8000/billing/plans
docker compose -f deploy/docker-compose.yml ps
docker exec dealership-scanner-web python3 -m pytest \
  backend/tests/test_billing_portal.py \
  backend/tests/test_email_verification.py \
  backend/tests/test_password_reset.py \
  backend/tests/test_admin_users.py \
  backend/tests/test_job_queue.py \
  backend/tests/test_store_admin.py \
  backend/tests/test_billing_catalog.py -q
```

## Optional feature flags (default off)

| Env | Purpose |
|-----|---------|
| `EMAIL_VERIFICATION_ENABLED=1` | B1 — send verify link on register (`MFA_DELIVERY_MODE=log` for dev) |
| `PASSWORD_RESET_ENABLED=1` | B2 — `/forgot-password` + `/reset-password` |
| `SCANNER_DEFAULT_INTERVAL_HOURS` | A3 — catalog refresh interval (default 24) |

Site admin (`asarrafi`): `/admin/dealers`, `/admin/users`. Signed-in users: `/account/billing` (C3 portal when Stripe customer id set).

## Start full stack

```bash
./deploy/up.sh
# WEB_PORT=8000 WORKER_REPLICAS=2
```

## Loop commands (paste in Cursor)

### Primary master-plan driver (30m)

```text
/loop 30m Continue PLATFORM_MASTER_PLAN until complete: verify Postgres stack (./deploy/up.sh), review+test completed phases (pytest), implement next open sprint item, update plan+SECURITY_MASTER_TODO. NO git push. Work autonomously.
```

Sentinel: `AGENT_LOOP_TICK_master_plan`

### Sprint 1 verify (fixed 10m)

```text
/loop 10m Verify Docker stack: docker compose -f deploy/docker-compose.yml ps; curl -sf http://127.0.0.1:8000/health; check scanner-worker and scanner-scheduler logs; if down run ./deploy/up.sh; report changes.
```

### Test gate (fixed 5m)

```text
/loop 5m PYTHONPATH=. python3 -m pytest backend/tests/test_billing_catalog.py backend/tests/test_apple_oauth.py -q; fix failures in scope; stop when green 2 ticks in a row.
```

### Stop

```text
stop the loop
```

## Vault keys (optional — scaffold works without)

| Env | Vault key |
|-----|-----------|
| `GOOGLE_MAPS_API_KEY` | `Dealer:google_maps_api_key` |
| `STRIPE_SECRET_KEY` | `Dealer:stripe_secret_key` |
| `STRIPE_PRICE_*` | `Dealer:stripe_price_*` |
| `GOOGLE_OAUTH_*` | `Dealer:google_oauth_*` |
| `APPLE_OAUTH_*` | `Dealer:apple_oauth_*` |

## Services (full profile)

| Container | Role |
|-----------|------|
| `dealership-scanner-postgres` | Inventory + `dealer_jobs` + `dealer_catalog` |
| `dealership-scanner-web` | Flask |
| `dealership-scanner-scanner-worker-N` | Job claim loop |
| `dealership-scanner-scanner-scheduler-1` | Refresh enqueue |
