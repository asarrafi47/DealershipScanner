#!/usr/bin/env bash
# Wire the Railway web service to the central kmac-vault (separate Railway project).
# App secrets stay in vault — only connection vars + non-secret config go to Railway.
#
# Optional: ./sync-vault-to-railway.sh --mirror-secrets
#   copies vault values into Railway Variables as a fallback (not recommended).
#
# Required in the environment (P3.7): TRUSTED_PROXY_HOPS (2 for prod web: Cloudflare, then
# Railway's edge) and RAILWAY_SERVICE (the service every call targets, e.g. web). The script
# refuses before any railway call when either is missing or malformed. Read
# deploy/railway/README.md section 2 before running this at all.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MIRROR_SECRETS=0
VAULT_ADDR="${VAULT_ADDR:-https://kmac-vault-production.up.railway.app}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mirror-secrets) MIRROR_SECRETS=1; shift ;;
    --vault-addr)
      VAULT_ADDR="${2:?}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: TRUSTED_PROXY_HOPS=<n> RAILWAY_SERVICE=<service> $0 [--vault-addr URL] [--mirror-secrets]"
      echo "  Default: set KMAC_VAULT_AUTO, VAULT_ADDR, VAULT_TOKEN, paths, admin config."
      echo "  --mirror-secrets: also copy Dealer:* values into Railway Variables."
      echo "  TRUSTED_PROXY_HOPS is required (no default): prod web, behind Cloudflare"
      echo "  and Railway's edge, uses 2. RAILWAY_SERVICE is required too: every call"
      echo "  passes --service, never the checkout's linked service. See deploy/railway/README.md."
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# P3.7 guard. TRUSTED_PROXY_HOPS has no default: it must equal the number of proxies in
# front of the app. Prod web is reached through Cloudflare and then Railway's edge, so it
# needs 2 (set since 1.4.4). The old silent default of 1 made client_ip pick Cloudflare's
# address, so every per-IP rate limit shared a few buckets. This check runs before the
# first railway call, so a refusal changes nothing on Railway.
hops="${TRUSTED_PROXY_HOPS:-}"
case "$hops" in
  "")
    echo "ERROR: TRUSTED_PROXY_HOPS is not set. Nothing was changed on Railway." >&2
    echo "  Set it to the number of proxies in front of the app. Prod web" >&2
    echo "  (sarraficars.com: Cloudflare, then Railway's edge) uses 2." >&2
    echo "  Read the live value first (prints that one line only):" >&2
    echo "    railway variable list -s web --kv | grep '^TRUSTED_PROXY_HOPS='" >&2
    echo "  See deploy/railway/README.md, \"Client IP and proxy hops\"." >&2
    exit 1
    ;;
  0*|*[!0-9]*)
    echo "ERROR: TRUSTED_PROXY_HOPS must be a positive integer, got '${hops}'." >&2
    echo "  Nothing was changed on Railway. Prod web uses 2." >&2
    exit 1
    ;;
esac

# RAILWAY_SERVICE is required as well: without it every call would act on whichever
# service the checkout is linked to (the main checkout is linked to scanner-nightly, where
# a deploying variable change is a deployment before P6B.1). docs/RELEASING.md: always pass
# -s on a command that acts on a service.
case "${RAILWAY_SERVICE:-}" in
  "")
    echo "ERROR: RAILWAY_SERVICE is not set. Nothing was changed on Railway." >&2
    echo "  Name the service every call targets, e.g. RAILWAY_SERVICE=web." >&2
    exit 1
    ;;
  -*|*[!A-Za-z0-9_-]*)
    echo "ERROR: RAILWAY_SERVICE must be a service name, got '${RAILWAY_SERVICE}'." >&2
    echo "  Nothing was changed on Railway." >&2
    exit 1
    ;;
esac

if ! command -v railway >/dev/null 2>&1; then
  echo "Install Railway CLI: https://docs.railway.com/guides/cli" >&2
  exit 1
fi

_read_railway_vault_token() {
  if [[ -n "${RAILWAY_VAULT_TOKEN:-}" ]]; then
    printf '%s' "$RAILWAY_VAULT_TOKEN"
    return 0
  fi
  if [[ -f "${HOME}/railway-vault-token.txt" ]]; then
    tr -d '[:space:]' < "${HOME}/railway-vault-token.txt"
    return 0
  fi
  if [[ -n "${VAULT_TOKEN:-}" ]]; then
    printf '%s' "$VAULT_TOKEN"
    return 0
  fi
  return 1
}

_railway_service_args() {
  if [[ -n "${RAILWAY_SERVICE:-}" ]]; then
    printf '%s' "--service ${RAILWAY_SERVICE}"
  fi
}

echo "==> Configuring Railway web service for kmac vault"
echo "    VAULT_ADDR=${VAULT_ADDR}"

SVC_ARGS=$(_railway_service_args)
# shellcheck disable=SC2086
railway variables set FLASK_ENV=production ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set KMAC_VAULT_AUTO=1 ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables --set "VAULT_ADDR=${VAULT_ADDR}" ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set VAULT_LOAD_RETRIES=8 ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set APP_ADMIN_USERNAMES="${APP_ADMIN_USERNAMES:-asarrafi}" ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set APP_ADMIN_EMAILS="${APP_ADMIN_EMAILS:-asarrafi@sarraficars.com}" ${SVC_ARGS}
# Inventory is Postgres-only in production (SEC-102): assert_inventory_backend_configured()
# refuses to boot without a postgresql:// INVENTORY_DATABASE_URL/DATABASE_URL. Provision
# Railway Postgres and set the reference variable — this script cannot invent the DSN.
# shellcheck disable=SC2086
if ! railway variables ${SVC_ARGS} 2>/dev/null | grep -qE "INVENTORY_DATABASE_URL|^.?DATABASE_URL"; then
  echo "WARN: INVENTORY_DATABASE_URL is not set on the service. The app will REFUSE TO" >&2
  echo "      BOOT (SEC-102) until you provision Railway Postgres and set, e.g.:" >&2
  echo "        railway variables --set 'INVENTORY_DATABASE_URL=\${{Postgres.DATABASE_URL}}'" >&2
fi
# Users/dev-users/dealer-portal are still SQLite (consolidation pending) — they live on
# the /data volume. Inventory does NOT: INVENTORY_DB_PATH is gone on purpose.
# shellcheck disable=SC2086
railway variables set USERS_DB_PATH=/data/users.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set DEV_USERS_DB_PATH=/data/dev_users.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set DEALER_PORTAL_DB_PATH=/data/dealer_portal.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set SCANNER_VDP_DOWNLOAD_IMAGES=0 ${SVC_ARGS}
# Behind proxies the client IP arrives in X-Forwarded-For; without these every per-IP
# rate limit shares one bucket. Hops = proxies in front of the app: 2 for prod web
# (Cloudflare, then Railway's edge). $hops was checked by the guard above (no default).
# shellcheck disable=SC2086
railway variables set TRUST_PROXY_HEADERS=1 ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set TRUSTED_PROXY_HOPS="${hops}" ${SVC_ARGS}
echo "  set FLASK_ENV, KMAC_VAULT_AUTO, proxy trust (TRUSTED_PROXY_HOPS=${hops}), paths, admin usernames"

# Retired: inventory SQLite on the volume (Postgres-only now, SEC-102).
# shellcheck disable=SC2086
railway variables delete INVENTORY_DB_PATH ${SVC_ARGS} 2>/dev/null || true

# Remove legacy disable flag if present (vault is used on Railway now).
# shellcheck disable=SC2086
railway variables delete KMAC_VAULT_DISABLE ${SVC_ARGS} 2>/dev/null || true

# Serving reads the dealer's remote URLs from cars.gallery; nothing ever read the downloaded
# bytes, so the download is off and the volume path it wrote to is retired. Delete the variable
# so a stale value can't redirect a future opt-in back onto the volume.
# shellcheck disable=SC2086
railway variables delete SCANNER_VDP_IMAGE_DOWNLOAD_DIR ${SVC_ARGS} 2>/dev/null || true

token="$(_read_railway_vault_token || true)"
if [[ -z "$token" ]]; then
  echo "WARN: No Railway vault token — set VAULT_TOKEN on the web service manually." >&2
  echo "      (Use ~/railway-vault-token.txt or RAILWAY_VAULT_TOKEN.)" >&2
else
  # shellcheck disable=SC2086
  railway variables --set "VAULT_TOKEN=${token}" ${SVC_ARGS}
  echo "  set VAULT_TOKEN (Railway kmac-vault bearer)"
fi

if [[ "$MIRROR_SECRETS" -eq 1 ]]; then
  # shellcheck source=/dev/null
  source "$ROOT/deploy/load-vault-env.sh"
  echo "==> Mirroring vault secrets into Railway Variables (fallback mode)"

  set_railway() {
    local name="$1"
    local value="$2"
    if [[ -z "$value" ]]; then
      echo "  skip $name (not in vault)"
      return 0
    fi
    # shellcheck disable=SC2086
    railway variables --set "${name}=${value}" ${SVC_ARGS}
    echo "  set $name"
  }

  set_railway GOOGLE_MAPS_API_KEY "${GOOGLE_MAPS_API_KEY:-}"
  set_railway ADMIN_PASSWORD "${ADMIN_PASSWORD:-}"

  for pair in \
    "SECRET_KEY:Dealer:secret_key" \
    "ANTHROPIC_API_KEY:Dealer:anthropic_api_key" \
    "STRIPE_SECRET_KEY:Dealer:stripe_secret_key" \
    "USERS_DB_ENCRYPTION_KEY:Dealer:users_db_encryption_key" \
    "DEV_USERS_DB_ENCRYPTION_KEY:Dealer:dev_users_db_encryption_key"
  do
    env_name="${pair%%:*}"
    vault_key="${pair#*:}"
    if [[ -z "${!env_name:-}" ]]; then
      if val="$(kmac_get "$vault_key" 2>/dev/null || true)"; then
        set_railway "$env_name" "$val"
      else
        echo "  skip $env_name (vault key $vault_key missing)"
      fi
    else
      set_railway "$env_name" "${!env_name}"
    fi
  done
fi

echo "==> Done."
echo "    1. Central kmac-vault is a separate Railway project (public URL above)."
echo "    2. Provision Railway Postgres and set INVENTORY_DATABASE_URL (SEC-102 refuses"
echo "       to boot without it): railway variables --set 'INVENTORY_DATABASE_URL=\${{Postgres.DATABASE_URL}}'"
echo "    3. Attach a /data volume on the web service (users/dev_users/dealer_portal SQLite)."
echo "    4. Set PUBLIC_BASE_URL to the public domain (prod: https://sarraficars.com),"
echo "       never the *.up.railway.app domain."
echo "    5. Code reaches Railway only through deploy/railway/deploy_web.sh from a release"
echo "       tag (docs/RELEASING.md). Never run a bare 'railway up'."
