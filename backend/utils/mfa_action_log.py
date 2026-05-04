from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Mapping

_log = logging.getLogger("mfa_action")
_lock = threading.Lock()
# After the first OSError for a path, skip file I/O to avoid log spam; logger still works.
_file_state: dict[str, str | None] = {"broken_abspath": None}


def _redact_email(s: str) -> str:
    t = (s or "").strip()
    if "@" not in t:
        return "?"
    local, _, domain = t.partition("@")
    if not local or not domain:
        return f"?@{domain}" if domain else "?"
    return f"{local[0]}…@{domain}" if len(local) > 1 else f"?@{domain}"


def _redact_phone(s: str) -> str:
    d = "".join(c for c in (s or "") if c.isdigit())
    if len(d) < 4:
        return "***"
    return f"…{d[-4:]}"


def mfa_sanitize(
    d: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not d:
        return {}
    out: dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if k in ("to_email", "email", "pending_login"):
            out[k] = _redact_email(str(v))
        elif k in ("to_phone", "phone", "mfa_pending_phone", "stored_phone"):
            out[k] = _redact_phone(str(v))
        else:
            out[k] = v
    return out


def log_mfa_action(
    *,
    event: str,
    surface: str = "app",
    fields: Mapping[str, Any] | None = None,
) -> None:
    """
    Append a JSON line for MFA/2FA pipeline debugging. Never includes OTPs.

    - File: set MFA_ACTION_LOG_PATH to a path (e.g. logs/mfa_actions.jsonl).
    - Also emits logger `mfa_action` at INFO (same payload as JSON) for process output.
    """
    row: dict[str, Any] = {
        "ts": int(time.time()),
        "ts_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": event,
        "surface": (surface or "app").strip().lower() or "app",
    }
    f = mfa_sanitize(dict(fields) if fields else None)
    row["fields"] = f
    line = json.dumps(row, ensure_ascii=True, separators=(",", ":")) + "\n"
    _log.info("%s", line.strip())

    path = (os.environ.get("MFA_ACTION_LOG_PATH") or "").strip()
    if not path:
        return
    ap = os.path.abspath(path)
    with _lock:
        if _file_state["broken_abspath"] == ap:
            return
        parent = os.path.dirname(path)
        if parent:
            try:
                os.makedirs(parent, mode=0o750, exist_ok=True)
            except OSError as e:  # noqa: BLE001
                _log.warning(
                    "mfa_action: file log disabled (once): cannot use MFA_ACTION_LOG_PATH=%s — %s. "
                    "Use a writable path, e.g. project-relative: logs/mfa_actions.jsonl",
                    path,
                    e,
                )
                _file_state["broken_abspath"] = ap
                return
        try:
            with open(path, "a", encoding="utf-8", newline="") as fh:  # noqa: PTH123
                fh.write(line)
        except OSError as e:  # noqa: BLE001
            _log.warning(
                "mfa_action: file log disabled (once): cannot write MFA_ACTION_LOG_PATH=%s — %s",
                path,
                e,
            )
            _file_state["broken_abspath"] = ap
