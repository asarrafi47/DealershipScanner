"""Load DealershipScanner secrets from the kmac vault into ``os.environ``.

Vault is the source of truth locally (Docker → host kmac-vault) and on Railway
(web service → ``kmac-vault.railway.internal`` in the same project).

Only fills env vars that are empty so explicit exports / platform vars always win.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from backend.utils.runtime_env import is_production_env

_log = logging.getLogger(__name__)

# Env var name -> kmac vault key (Dealer namespace)
VAULT_SECRET_MAP: dict[str, str] = {
    "GOOGLE_MAPS_API_KEY": "Dealer:google_maps_api_key",
    "ADMIN_PASSWORD": "Dealer:site_admin_password",
    "ANTHROPIC_API_KEY": "Dealer:anthropic_api_key",
    "STRIPE_SECRET_KEY": "Dealer:stripe_secret_key",
    "SECRET_KEY": "Dealer:secret_key",
    "FLASK_SECRET_KEY": "Dealer:secret_key",
    "USERS_DB_ENCRYPTION_KEY": "Dealer:users_db_encryption_key",
    "DEV_USERS_DB_ENCRYPTION_KEY": "Dealer:dev_users_db_encryption_key",
}

_DEFAULT_VAULT_ADDR = "http://host.docker.internal:9999"
_LOCAL_VAULT_ADDR = "http://127.0.0.1:9999"
# Same Railway project: private DNS (PORT is Railway-assigned; override with VAULT_ADDR).
_RAILWAY_VAULT_ADDR = "http://kmac-vault.railway.internal:9999"
_RAILWAY_VAULT_PUBLIC_ADDR = "https://kmac-vault-production.up.railway.app"


def _vault_disabled() -> bool:
    return (os.environ.get("KMAC_VAULT_DISABLE") or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _on_railway() -> bool:
    return bool(
        (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip()
        or (os.environ.get("RAILWAY_PROJECT_ID") or "").strip()
    )


def _vault_configured() -> bool:
    if (os.environ.get("VAULT_ADDR") or "").strip():
        return True
    if (os.environ.get("VAULT_TOKEN") or "").strip():
        return True
    if (os.environ.get("VAULT_TOKEN_FILE") or "").strip():
        return True
    for candidate in (
        "/run/secrets/kmac-vault-token",
        str(Path.home() / ".config/kmac/docker-vault-token"),
    ):
        if Path(candidate).is_file():
            return True
    return False


def _vault_auto_enabled() -> bool:
    raw = (os.environ.get("KMAC_VAULT_AUTO") or "").strip().lower()
    if raw in ("0", "false", "no", "off"):
        return False
    if raw in ("1", "true", "yes", "on"):
        return True
    if _vault_configured():
        return True
    if _on_railway():
        return True
    return not is_production_env()


def _vault_addrs() -> list[str]:
    addrs: list[str] = []
    seen: set[str] = set()

    def _add(addr: str) -> None:
        a = (addr or "").strip().rstrip("/")
        if a and a not in seen:
            seen.add(a)
            addrs.append(a)

    _add((os.environ.get("VAULT_ADDR") or "").strip())
    if _on_railway():
        _add((os.environ.get("KMAC_VAULT_RAILWAY_ADDR") or _RAILWAY_VAULT_ADDR).strip())
        _add(_RAILWAY_VAULT_PUBLIC_ADDR)
    _add(_LOCAL_VAULT_ADDR)
    _add(_DEFAULT_VAULT_ADDR)
    return addrs


def _vault_timeout() -> float:
    raw = (os.environ.get("VAULT_HTTP_TIMEOUT") or "").strip()
    if raw:
        try:
            return max(1.0, float(raw))
        except ValueError:
            pass
    return 10.0 if _on_railway() else 3.0


def _read_vault_token() -> str:
    token = (os.environ.get("VAULT_TOKEN") or "").strip()
    if token:
        return token
    token_file = (os.environ.get("VAULT_TOKEN_FILE") or "").strip()
    if not token_file:
        for candidate in (
            "/run/secrets/kmac-vault-token",
            str(Path.home() / ".config/kmac/docker-vault-token"),
        ):
            p = Path(candidate)
            if p.is_file():
                token_file = str(p)
                break
    if token_file:
        try:
            return Path(token_file).read_text(encoding="utf-8").strip()
        except OSError:
            pass
    return ""


def vault_get(vault_key: str, *, token: str | None = None, timeout: float | None = None) -> str | None:
    """Fetch one secret from kmac vault. Returns plaintext value or None."""
    if timeout is None:
        timeout = _vault_timeout()
    tok = (token or _read_vault_token()).strip()
    if not tok:
        return None
    encoded = urllib.parse.quote(vault_key, safe=":/")
    headers = {"Authorization": f"Bearer {tok}"}
    key_variants = [vault_key, vault_key.replace("/", ":"), vault_key.replace(":", "/")]
    seen: set[str] = set()
    for addr in _vault_addrs():
        for key in key_variants:
            if key in seen:
                continue
            seen.add(key)
            enc = urllib.parse.quote(key, safe=":/")
            url = f"{addr.rstrip('/')}/get/{enc}"
            req = urllib.request.Request(url, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    value = (data.get("value") or "").strip()
                    if value:
                        return value
            except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError):
                continue
    return None


def load_kmac_vault_secrets(*, force: bool = False) -> list[str]:
    """
    Populate empty env vars from vault. Returns env names that were set.
    """
    if _vault_disabled():
        return []
    if not force and not _vault_auto_enabled():
        return []

    retries = 1
    if _on_railway():
        try:
            retries = max(1, int((os.environ.get("VAULT_LOAD_RETRIES") or "8").strip()))
        except ValueError:
            retries = 8
    retry_sleep = 2.0

    loaded: list[str] = []
    for attempt in range(retries):
        loaded = []
        for env_name, vault_key in VAULT_SECRET_MAP.items():
            if (os.environ.get(env_name) or "").strip():
                continue
            value = vault_get(vault_key)
            if value:
                os.environ[env_name] = value
                loaded.append(env_name)
        if loaded or attempt + 1 >= retries:
            break
        _log.info(
            "Vault fetch attempt %d/%d returned no new secrets; retrying in %.0fs",
            attempt + 1,
            retries,
            retry_sleep,
        )
        time.sleep(retry_sleep)

    if loaded:
        _log.info("Loaded %d secret(s) from kmac vault: %s", len(loaded), ", ".join(loaded))
    elif _vault_configured() or _on_railway():
        _log.warning(
            "kmac vault reachable config present but no secrets loaded "
            "(check VAULT_ADDR, VAULT_TOKEN, and Dealer:* keys)"
        )
    return loaded
