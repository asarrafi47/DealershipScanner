#!/usr/bin/env bash
# Security validation runbook (SEC-076). Run from repo root after pip install -r requirements.txt
# (plus requirements-test.txt for pytest). The pip-audit step resolves requirements.txt and
# requirements-scanner.txt afresh, so it needs network access to PyPI and the advisory DB.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "== Security pytest =="
python -m pytest \
  backend/tests/test_app_security_basics.py \
  backend/tests/test_car_chat_policy.py \
  backend/tests/test_billing_gate.py \
  backend/tests/test_dev_ip_allowlist.py \
  backend/tests/test_outbound_url.py \
  backend/tests/test_outbound_url_dns.py \
  backend/tests/test_safe_listing_url.py \
  backend/tests/test_registration_password_limits.py \
  backend/tests/test_smart_search.py \
  backend/tests/test_google_oauth.py \
  backend/tests/test_registration_email.py \
  backend/tests/test_mobile_auth_api.py \
  backend/tests/test_mobile_auth_register.py \
  backend/tests/test_audit_fixes.py \
  backend/tests/test_premium_checkout.py \
  backend/tests/test_stripe_premium_verify.py \
  backend/tests/test_compare_specs.py \
  backend/tests/test_compare_and_stats.py \
  backend/tests/test_dealer_portal.py \
  backend/tests/test_store_admin.py \
  backend/tests/test_client_ip.py \
  backend/tests/test_window_sticker_urls.py \
  backend/tests/test_scanner_intercept_filter.py \
  backend/tests/test_db_admin_security.py \
  backend/tests/test_production_security.py \
  backend/tests/test_password_hash.py \
  backend/tests/test_requirements_hygiene.py \
  -q --tb=short

echo "== Production import guards (SEC-001, SEC-003) =="
if FLASK_ENV=production python -c "import backend.main" 2>/dev/null; then
  echo "FAIL: production import without SECRET_KEY should raise"
  exit 1
fi
FLASK_ENV=production SECRET_KEY=test-secret ADMIN_PASSWORD=test-admin \
  USERS_DB_ENCRYPTION_KEY="$(python -c 'print("x"*32)')" \
  DEV_USERS_DB_ENCRYPTION_KEY="$(python -c 'print("y"*32)')" \
  USERS_DB_PATH="$(mktemp -u).db" \
  DEV_USERS_DB_PATH="$(mktemp -u).db" \
  python -c "import backend.main" >/dev/null
echo "OK: production import with secrets"

echo "== pip-audit (SEC-076, D-DEP1) =="
PIP_AUDIT="${PIP_AUDIT:-pip-audit}"
if ! command -v "$PIP_AUDIT" >/dev/null 2>&1; then
  echo "WARN: pip-audit not installed; run: pip install pip-audit (or set PIP_AUDIT=/path/to/pip-audit)"
  exit 0
fi
# Requirements mode, not environment mode: it audits what the images install (a
# fresh resolve of both files), so a stale dev venv can neither fail nor pass it.
# No --ignore-vuln (P15B.1): crawl4ai, whose lxml~=5.3 pin forced the PYSEC-2026-87
# ignore, is gone, and the orphan-chromadb ignore only ever applied to venv installs.
if ! "$PIP_AUDIT" --desc on -r requirements.txt -r requirements-scanner.txt; then
  echo "FAIL: pip-audit reported vulnerabilities (see above)"
  exit 1
fi
echo "OK: dependency audit passed (requirements.txt + requirements-scanner.txt, no ignores)"

echo "== All security checks passed =="
