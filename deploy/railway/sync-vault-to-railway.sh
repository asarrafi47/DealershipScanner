#!/usr/bin/env bash
# Wire the Railway web service to the central kmac-vault (separate Railway project).
# App secrets stay in vault — only connection vars + non-secret config go to Railway.
#
# Optional: ./sync-vault-to-railway.sh --mirror-secrets
#   copies vault values into Railway Variables as a fallback (not recommended).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
MIRROR_SECRETS=0
ALL_SERVICES=0
VAULT_ADDR="${VAULT_ADDR:-https://kmac-vault-production.up.railway.app}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mirror-secrets) MIRROR_SECRETS=1; shift ;;
    --all-services) ALL_SERVICES=1; shift ;;
    --vault-addr)
      VAULT_ADDR="${2:?}"
      shift 2
      ;;
    -h|--help)
      echo "Usage: $0 [--vault-addr URL] [--mirror-secrets] [--all-services]"
      echo "  Default: set KMAC_VAULT_AUTO, VAULT_ADDR, VAULT_TOKEN, paths, admin config."
      echo "  --all-services: also wire scanner-worker and scanner-scheduler."
      echo "  --mirror-secrets: also copy Dealer:* values into Railway Variables."
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

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
# shellcheck disable=SC2086
railway variables set INVENTORY_DB_PATH=/data/inventory.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set USERS_DB_PATH=/data/users.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set DEV_USERS_DB_PATH=/data/dev_users.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set DEALER_PORTAL_DB_PATH=/data/dealer_portal.db ${SVC_ARGS}
# shellcheck disable=SC2086
railway variables set SCANNER_VDP_IMAGE_DOWNLOAD_DIR=/data/vdp_images ${SVC_ARGS}
echo "  set FLASK_ENV, KMAC_VAULT_AUTO, paths, admin usernames"

# Remove legacy disable flag if present (vault is used on Railway now).
railway variables delete KMAC_VAULT_DISABLE 2>/dev/null || true

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
    railway variables --set "${name}=${value}"
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
echo "    2. Attach a /data volume on the web service."
echo "    3. Set PUBLIC_BASE_URL to your Railway domain."
echo "    4. Deploy: railway up"

if [[ "${ALL_SERVICES:-0}" -eq 1 ]]; then
  for svc in scanner-worker scanner-scheduler; do
    echo "==> Configuring Railway ${svc} (vault + Postgres)"
    railway variable set KMAC_VAULT_AUTO=1 --service "$svc" --skip-deploys
    railway variables --set "VAULT_ADDR=${VAULT_ADDR}" --service "$svc" --skip-deploys
    railway variable set VAULT_LOAD_RETRIES=8 --service "$svc" --skip-deploys
    railway variable set "INVENTORY_DATABASE_URL=\${{Postgres.DATABASE_URL}}" --service "$svc" --skip-deploys
    railway variable set INVENTORY_DB_PATH=/data/inventory.db --service "$svc" --skip-deploys
    railway variable set SCANNER_BROWSER_PROFILE_DIR=/data/browser-profiles --service "$svc" --skip-deploys
    railway variable set SCANNER_VDP_IMAGE_DOWNLOAD_DIR=/data/vdp_images --service "$svc" --skip-deploys
    railway variable set PYTHONPATH=/app --service "$svc" --skip-deploys
    if [[ "$svc" == "scanner-worker" ]]; then
      railway variable set RAILWAY_DOCKERFILE_PATH=Dockerfile.scanner-worker --service "$svc" --skip-deploys
      railway variable set SCANNER_WORKER_POLL_SEC=10 --service "$svc" --skip-deploys
    else
      railway variable set RAILWAY_DOCKERFILE_PATH=Dockerfile.scanner-scheduler --service "$svc" --skip-deploys
      railway variable set SCANNER_SCHEDULER_INTERVAL_SEC=60 --service "$svc" --skip-deploys
    fi
    if [[ -n "${token:-}" ]]; then
      railway variables --set "VAULT_TOKEN=${token}" --service "$svc" --skip-deploys
    fi
    echo "  ${svc}: vault + INVENTORY_DATABASE_URL set"
  done
fi
