"""
Kelley Blue Book IDWS 4.0 (licensed) client — VIN decode + used values.

Requires a Cox/KBB-issued API key (``KBB_API_KEY``). Endpoint shapes follow the public
quick-start summary (VIN decode → optional configuration → values with mileage + ZIP);
exact query parameters can vary by product tier — see Cox IDWS documentation for your
account and adjust ``KBB_IDWS_*`` env vars if needed.

This module does **not** automate the consumer kbb.com website (fragile and likely ToS issues).
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urljoin

import requests

_log = logging.getLogger(__name__)

_DEFAULT_BASE = "https://api.kbb.com/idws"


def _env_truthy(name: str) -> bool:
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def kbb_api_configured() -> bool:
    return bool((os.environ.get("KBB_API_KEY") or "").strip())


def _base_url() -> str:
    return (os.environ.get("KBB_IDWS_BASE_URL") or _DEFAULT_BASE).rstrip("/")


def _timeout_s() -> float:
    try:
        return max(5.0, float(os.environ.get("KBB_HTTP_TIMEOUT_S") or "45"))
    except (TypeError, ValueError):
        return 45.0


def _min_interval_s() -> float:
    try:
        return max(0.0, float(os.environ.get("KBB_MIN_INTERVAL_S") or "0.35"))
    except (TypeError, ValueError):
        return 0.35


_last_request_mono: float | None = None


def _throttle() -> None:
    global _last_request_mono
    gap = _min_interval_s()
    if gap <= 0:
        return
    import time as _t

    now = _t.monotonic()
    if _last_request_mono is not None:
        wait = gap - (now - _last_request_mono)
        if wait > 0:
            _t.sleep(wait)
    _last_request_mono = _t.monotonic()


def _auth_headers() -> dict[str, str]:
    key = (os.environ.get("KBB_API_KEY") or "").strip()
    if not key:
        return {}
    mode = (os.environ.get("KBB_AUTH_MODE") or "bearer").strip().lower()
    if mode in ("header", "subscription", "ocp"):
        return {"Ocp-Apim-Subscription-Key": key}
    return {"Authorization": f"Bearer {key}"}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "DealershipScanner/1.0 (+https://github.com/)",
            **_auth_headers(),
        }
    )
    return s


def _normalize_vin(vin: str | None) -> str | None:
    if not vin:
        return None
    v = re.sub(r"\s+", "", str(vin).strip().upper())
    if len(v) != 17 or not re.match(r"^[A-HJ-NPR-Z0-9]{17}$", v):
        return None
    return v


def _get_json(url: str) -> tuple[int, Any]:
    _throttle()
    sess = _session()
    resp = sess.get(url, timeout=_timeout_s())
    try:
        body = resp.json()
    except ValueError:
        body = {"_non_json": (resp.text or "")[:2000]}
    return resp.status_code, body


def decode_vehicle_by_vin(vin: str) -> tuple[int, dict[str, Any]]:
    """
    GET ``/vehicle/vin/{vin}/`` (trailing slash per KBB quick-start).
    Returns HTTP status and parsed JSON object (or error wrapper).
    """
    v = _normalize_vin(vin)
    if not v:
        return 400, {"error": "invalid_vin"}
    path = f"vehicle/vin/{quote(v, safe='')}/"
    url = urljoin(_base_url() + "/", path)
    status, data = _get_json(url)
    if not isinstance(data, dict):
        return status, {"error": "unexpected_shape", "data": data}
    return status, data


def _collect_primitive_params(obj: Any, *, depth: int = 0) -> dict[str, Any]:
    """Pull string/int/float leaves from shallow dicts (for query passthrough)."""
    if depth > 4 or not isinstance(obj, dict):
        return {}
    out: dict[str, Any] = {}
    for k, v in obj.items():
        if not isinstance(k, str):
            continue
        if isinstance(v, (str, int, float)) and not isinstance(v, bool):
            if str(k).lower() in {"error", "errors", "message", "messages"}:
                continue
            out[k] = v
        elif isinstance(v, dict) and depth < 2:
            inner = _collect_primitive_params(v, depth=depth + 1)
            for ik, iv in inner.items():
                if ik not in out:
                    out[ik] = iv
    return out


def _first_vehicle_id(decode: dict[str, Any]) -> str | None:
    """Best-effort vehicle / configuration id from decode payload."""
    candidates: list[str] = []

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            for key, val in o.items():
                lk = str(key).lower()
                if lk in (
                    "vehicleid",
                    "pricedvehicleid",
                    "vehicleconfigurationid",
                    "configurationid",
                ):
                    if isinstance(val, (int, float)) and not isinstance(val, bool):
                        candidates.append(str(int(val)))
                    elif isinstance(val, str) and val.strip().isdigit():
                        candidates.append(val.strip())
                walk(val)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(decode)
    for c in candidates:
        if c:
            return c
    return None


def fetch_used_values(
    decode_body: dict[str, Any],
    *,
    mileage: int | None,
    zip_code: str | None,
) -> tuple[int, dict[str, Any]]:
    """
    GET ``/vehicle/values`` with mileage + ZIP + ids inferred from *decode_body*.

    If your tenant uses different parameter names, set ``KBB_IDWS_VALUES_EXTRA_JSON`` to a JSON
    object merged into the query string (e.g. ``{"vehicleId": "123"}`` overrides).
    """
    base = _base_url() + "/"
    params: dict[str, Any] = {}

    extra_raw = (os.environ.get("KBB_IDWS_VALUES_EXTRA_JSON") or "").strip()
    if extra_raw:
        try:
            ex = json.loads(extra_raw)
            if isinstance(ex, dict):
                params.update(ex)
        except (TypeError, ValueError):
            _log.warning("KBB_IDWS_VALUES_EXTRA_JSON is not valid JSON; ignored")

    params.update(_collect_primitive_params(decode_body))

    vid = _first_vehicle_id(decode_body)
    if vid and "vehicleId" not in params and "vehicleid" not in {k.lower() for k in params}:
        params["vehicleId"] = vid

    if mileage is not None:
        try:
            mi = int(float(str(mileage).replace(",", "")))
            if mi >= 0:
                params.setdefault("mileage", mi)
                params.setdefault("odometer", mi)
        except (TypeError, ValueError):
            pass

    z = (zip_code or "").strip()
    if len(z) >= 5:
        z5 = z[:5]
        params.setdefault("zipCode", z5)
        params.setdefault("zipcode", z5)
        params.setdefault("zip", z5)

    # Drop huge / useless keys
    drop_keys = {k for k in list(params) if isinstance(params[k], str) and len(str(params[k])) > 120}
    for dk in drop_keys:
        del params[dk]

    q = "&".join(f"{quote(str(k), safe='')}={quote(str(v), safe='')}" for k, v in params.items())
    url = urljoin(base, f"vehicle/values?{q}")
    status, data = _get_json(url)
    if not isinstance(data, dict):
        return status, {"error": "unexpected_shape", "data": data}
    return status, data


def _num(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        return x if 500 <= x <= 1_500_000 else None
    if isinstance(v, str):
        s = re.sub(r"[^\d.]", "", v)
        if not s:
            return None
        try:
            x = float(s)
        except ValueError:
            return None
        return x if 500 <= x <= 1_500_000 else None


def extract_valuation_numbers(values_payload: dict[str, Any]) -> dict[str, float | None]:
    """
    Heuristic extraction from IDWS ``values`` JSON (schema varies by license).
    Populates fair purchase, fair market range, private party, trade-in when discoverable.
    """
    best: dict[str, float | None] = {
        "fair_purchase": None,
        "range_low": None,
        "range_high": None,
        "private_party": None,
        "trade_in": None,
    }

    def consider(key: str, val: Any) -> None:
        lk = key.lower().replace(" ", "").replace("_", "")
        n = _num(val)
        if n is None:
            return

        def setmax(dst: str) -> None:
            cur = best.get(dst)
            best[dst] = n if cur is None else max(cur, n)

        def setmin(dst: str) -> None:
            cur = best.get(dst)
            best[dst] = n if cur is None else min(cur, n)

        if "fairpurchase" in lk or "usedcarfairpurchase" in lk or "fairpurchaseprice" in lk:
            setmax("fair_purchase")
        if "typicallisting" in lk and "used" in lk:
            setmax("fair_purchase")
        if ("fairmarket" in lk or "fairmarketr" in lk) and ("low" in lk or "min" in lk):
            setmin("range_low")
        if ("fairmarket" in lk or "fairmarketr" in lk) and ("high" in lk or "max" in lk):
            setmax("range_high")
        if "privateparty" in lk:
            setmax("private_party")
        if "tradein" in lk or "trade-in" in key.lower():
            setmax("trade_in")

    def walk(o: Any) -> None:
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str):
                    consider(k, v)
                walk(v)
        elif isinstance(o, list):
            for it in o:
                walk(it)

    walk(values_payload)

    lo, hi = best["range_low"], best["range_high"]
    if lo is not None and hi is not None and hi < lo:
        best["range_low"], best["range_high"] = hi, lo
    return best


@dataclass
class KbbRefreshResult:
    ok: bool
    message: str
    http_status: int | None = None
    normalized: dict[str, Any] | None = None


def refresh_kbb_for_vehicle_row(
    row: dict[str, Any],
    *,
    zip_override: str | None = None,
) -> KbbRefreshResult:
    """
    Call IDWS for one SQLite ``cars`` row; returns normalized numbers + snapshot dict
    suitable for ``update_car_row_partial`` (caller persists).
    """
    if not kbb_api_configured():
        return KbbRefreshResult(False, "kbb_api_key_missing")

    vin = row.get("vin")
    v = _normalize_vin(str(vin) if vin else "")
    if not v:
        return KbbRefreshResult(False, "invalid_vin")

    st_d, decode = decode_vehicle_by_vin(v)
    if st_d >= 400:
        return KbbRefreshResult(False, f"vin_decode_http_{st_d}", http_status=st_d)

    mileage = row.get("mileage")
    z = zip_override or row.get("zip_code") or os.environ.get("KBB_DEFAULT_ZIP") or ""
    z = str(z).strip()

    st_v, values = fetch_used_values(decode, mileage=mileage, zip_code=z)
    if st_v >= 400:
        return KbbRefreshResult(
            False,
            f"values_http_{st_v}",
            http_status=st_v,
            normalized={"decode_status": st_d, "values_status": st_v},
        )

    nums = extract_valuation_numbers(values)
    from datetime import datetime, timezone

    fetched_at = datetime.now(timezone.utc).isoformat()
    zip_used = z[:5] if len(z) >= 5 else z

    if _env_truthy("KBB_SNAPSHOT_FULL"):
        snap: dict[str, Any] = {
            "fetched_at": fetched_at,
            "vin": v,
            "zip_used": zip_used,
            "mileage_used": mileage,
            "normalized": nums,
            "decode": decode,
            "values": values,
        }
    else:
        snap = {
            "fetched_at": fetched_at,
            "vin": v,
            "zip_used": zip_used,
            "mileage_used": mileage,
            "normalized": nums,
            "decode_status": st_d,
            "values_status": st_v,
        }

    if all(nums.get(k) is None for k in ("fair_purchase", "range_low", "range_high", "private_party", "trade_in")):
        return KbbRefreshResult(
            False,
            "no_valuation_numbers_in_response",
            http_status=st_v,
            normalized={"snapshot": snap},
        )

    return KbbRefreshResult(
        True,
        "ok",
        http_status=st_v,
        normalized={
            "snapshot": snap,
            **nums,
        },
    )


def patch_from_refresh_result(res: KbbRefreshResult) -> dict[str, Any]:
    """SQLite patch keys for a successful ``refresh_kbb_for_vehicle_row``."""
    if not res.ok or not res.normalized:
        return {}
    n = res.normalized
    snap = n.get("snapshot")
    from datetime import datetime, timezone

    fetched = None
    if isinstance(snap, dict):
        fetched = snap.get("fetched_at")
    if not fetched:
        fetched = datetime.now(timezone.utc).isoformat()
    return {
        "kbb_fair_purchase": n.get("fair_purchase"),
        "kbb_range_low": n.get("range_low"),
        "kbb_range_high": n.get("range_high"),
        "kbb_private_party": n.get("private_party"),
        "kbb_trade_in": n.get("trade_in"),
        "kbb_fetched_at": fetched,
        "kbb_snapshot_json": json.dumps(snap, ensure_ascii=False) if snap is not None else None,
    }
