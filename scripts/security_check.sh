#!/usr/bin/env bash
# Security validation runbook (SEC-076). Run from repo root after pip install -r requirements.txt
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
  -q --tb=short

echo "== Production import guards (SEC-001, SEC-003) =="
if FLASK_ENV=production python -c "import backend.main" 2>/dev/null; then
  echo "FAIL: production import without SECRET_KEY should raise"
  exit 1
fi
FLASK_ENV=production SECRET_KEY=test-secret ADMIN_PASSWORD=test-admin python -c "import backend.main" >/dev/null
echo "OK: production import with secrets"

echo "== pip-audit (SEC-076) =="
if ! command -v pip-audit >/dev/null 2>&1; then
  echo "WARN: pip-audit not installed; run: pip install pip-audit"
  exit 0
fi
# lxml 5.x: blocked until crawl4ai allows lxml 6 (PYSEC-2026-87)
if ! pip-audit --desc on --ignore-vuln PYSEC-2026-87; then
  echo "FAIL: pip-audit reported vulnerabilities (see above)"
  exit 1
fi
echo "OK: dependency audit passed (lxml PYSEC-2026-87 ignored while crawl4ai pins lxml 5.x)"

echo "== All security checks passed =="
