#!/usr/bin/env python3
"""Push DealershipScanner Dealer:* secrets into Railway kmac-vault.

Reads from local kmac vault when available; generates missing production keys.
Does not print secret values.

Usage:
  export VAULT_URL=https://kmac-vault-production.up.railway.app
  export VAULT_TOKEN=<railway-vault-token>
  PYTHONPATH=. python3 deploy/railway/push-dealer-secrets-to-vault.py
"""

from __future__ import annotations

import json
import os
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.utils.kmac_vault import vault_get  # noqa: E402

DEALER_KEYS: dict[str, str | None] = {
    "Dealer:google_maps_api_key": None,
    "Dealer:site_admin_password": None,
    "Dealer:secret_key": None,
    "Dealer:users_db_encryption_key": None,
    "Dealer:dev_users_db_encryption_key": None,
    "Dealer:anthropic_api_key": None,
    "Dealer:stripe_secret_key": None,
}

GENERATORS: dict[str, callable] = {
    "Dealer:secret_key": lambda: secrets.token_urlsafe(48),
    "Dealer:users_db_encryption_key": lambda: secrets.token_urlsafe(48),
    "Dealer:dev_users_db_encryption_key": lambda: secrets.token_urlsafe(48),
}


def _remote_vault_config() -> tuple[str, str]:
    url = (os.environ.get("VAULT_URL") or "https://kmac-vault-production.up.railway.app").rstrip("/")
    token = (os.environ.get("VAULT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("VAULT_TOKEN is required (Railway kmac-vault bearer token).")
    return url, token


def _local_set(key: str, value: str) -> bool:
    token_path = Path.home() / ".config/kmac/docker-vault-token"
    token = (os.environ.get("VAULT_TOKEN_LOCAL") or "").strip()
    if not token and token_path.is_file():
        token = token_path.read_text(encoding="utf-8").strip()
    if not token:
        return False
    addr = (os.environ.get("VAULT_ADDR_LOCAL") or "http://127.0.0.1:9999").rstrip("/")
    body = json.dumps({"key": key, "value": value}).encode("utf-8")
    req = urllib.request.Request(
        f"{addr}/set",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status == 200
    except (urllib.error.URLError, urllib.error.HTTPError):
        return False


def _remote_set(url: str, token: str, key: str, value: str) -> None:
    body = json.dumps({"key": key, "value": value}).encode("utf-8")
    req = urllib.request.Request(
        f"{url}/set",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status != 200:
            raise RuntimeError(f"set {key} failed: HTTP {resp.status}")


def _remote_has(url: str, token: str, key: str) -> bool:
    enc = urllib.parse.quote(key, safe=":/")
    req = urllib.request.Request(
        f"{url}/get/{enc}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return bool((data.get("value") or "").strip())
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def _local_vault_get(key: str) -> str | None:
    """Read from local kmac vault without Railway VAULT_TOKEN shadowing."""
    saved = os.environ.pop("VAULT_TOKEN", None)
    saved_addr = os.environ.pop("VAULT_ADDR", None)
    try:
        return vault_get(key)
    finally:
        if saved is not None:
            os.environ["VAULT_TOKEN"] = saved
        if saved_addr is not None:
            os.environ["VAULT_ADDR"] = saved_addr


def main() -> int:
    url, token = _remote_vault_config()
    health_req = urllib.request.Request(f"{url}/health")
    with urllib.request.urlopen(health_req, timeout=10) as resp:
        if resp.status != 200:
            print(f"Vault health check failed: HTTP {resp.status}", file=sys.stderr)
            return 1

    pushed = 0
    skipped = 0
    generated = 0

    for key in DEALER_KEYS:
        value = _local_vault_get(key)
        source = "local"
        if not value and key in GENERATORS:
            value = GENERATORS[key]()
            source = "generated"
            generated += 1
            _local_set(key, value)
        if not value:
            print(f"skip  {key} (not in local vault)")
            skipped += 1
            continue
        if _remote_has(url, token, key):
            print(f"keep  {key} (already in Railway vault)")
            skipped += 1
            continue
        _remote_set(url, token, key, value)
        print(f"set   {key} ({source})")
        pushed += 1

    print(f"done: pushed={pushed} generated={generated} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
