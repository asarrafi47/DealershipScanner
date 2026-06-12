#!/usr/bin/env bash
# Source from deploy scripts — loads kmac vault secrets into the shell (no .env file).
set -euo pipefail

VAULT_ADDR="${VAULT_ADDR:-http://127.0.0.1:9999}"
if [[ -z "${VAULT_TOKEN:-}" && -f "${HOME}/.config/kmac/docker-vault-token" ]]; then
  VAULT_TOKEN="$(tr -d '[:space:]' < "${HOME}/.config/kmac/docker-vault-token")"
fi
if [[ -z "${VAULT_TOKEN:-}" ]]; then
  VAULT_TOKEN="$(docker exec kmac-vault cat /vault/token 2>/dev/null || true)"
fi

kmac_get() {
  local vault_key="$1"
  local body value url
  if [[ -z "${VAULT_TOKEN:-}" ]]; then
    return 1
  fi
  for url in \
    "${VAULT_ADDR%/}/get/${vault_key}" \
    "${VAULT_ADDR%/}/get/${vault_key//\//:}" \
    "${VAULT_ADDR%/}/get/${vault_key//:/\/}"
  do
    body="$(curl -sf -H "Authorization: Bearer ${VAULT_TOKEN}" "${url}" 2>/dev/null || true)"
    if [[ -n "${body}" ]]; then
      value="$(printf '%s' "${body}" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("value",""))' 2>/dev/null || true)"
      if [[ -n "${value}" ]]; then
        echo "${value}"
        return 0
      fi
    fi
  done
  return 1
}

_load_if_empty() {
  local env_name="$1"
  local vault_key="$2"
  if [[ -n "${!env_name:-}" ]]; then
    return 0
  fi
  local from_vault
  if from_vault="$(kmac_get "$vault_key")"; then
    export "${env_name}=${from_vault}"
  fi
}

if [[ -z "${GOOGLE_MAPS_API_KEY:-}" && -n "${google_maps_api_key:-}" ]]; then
  export GOOGLE_MAPS_API_KEY="$google_maps_api_key"
fi
_load_if_empty GOOGLE_MAPS_API_KEY "Dealer:google_maps_api_key"
_load_if_empty ADMIN_PASSWORD "Dealer:site_admin_password"
_load_if_empty ANTHROPIC_API_KEY "Dealer:anthropic_api_key"
_load_if_empty STRIPE_SECRET_KEY "Dealer:stripe_secret_key"
_load_if_empty SECRET_KEY "Dealer:secret_key"
_load_if_empty USERS_DB_ENCRYPTION_KEY "Dealer:users_db_encryption_key"
_load_if_empty DEV_USERS_DB_ENCRYPTION_KEY "Dealer:dev_users_db_encryption_key"

export APP_ADMIN_USERNAMES="${APP_ADMIN_USERNAMES:-asarrafi}"
export APP_ADMIN_EMAILS="${APP_ADMIN_EMAILS:-asarrafi@sarraficars.com}"
export ADMIN_USERNAME="${ADMIN_USERNAME:-admin}"
export ALLOW_APP_ADMIN_DEV_PASS_THROUGH="${ALLOW_APP_ADMIN_DEV_PASS_THROUGH:-1}"
