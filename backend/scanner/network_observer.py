"""
Tiered network observer for inventory scans (replaces the inline ``page.on("response")``
handler in ``phases/inventory_scrape.py``).

Tier 1 — capture: inventory-shaped JSON is appended to the scan's intercept records
    (same ``(url, body)`` shape as before), plus a replayable endpoint record
    (method / query / POST body sample) for future direct-API scraping.
Tier 2 — fingerprint: JSON that *almost* qualified (VIN-shaped values but weak schema,
    or vehicle payloads on URL-denied hosts) is kept as a compact schema fingerprint
    in a per-path ledger — the raw payload is dropped, only shape/stats survive.
Tier 3 — ignore: everything else, tallied per host.

Beyond the legacy key-based qualifier (``payload_qualifies_for_inventory_intercept``),
capture also accepts payloads by *vehicle-ness score*: VIN-shaped string **values**
(any key name), vehicle field keys (year/make/model/price/…), and schema consistency
across list items. ``xhr``/``fetch`` responses with a non-JSON content-type are
body-sniffed (size-capped, anti-XSSI/JSONP tolerant) so mislabelled APIs aren't lost.

Env flags:
    SCANNER_SCORE_INTERCEPT   default 1 — score-based capture (0 = legacy qualifier only)
    SCANNER_SNIFF_NONJSON     default 1 — sniff xhr/fetch bodies with non-JSON content-type
    SCANNER_SNIFF_MAX_BYTES   default 3000000 — sniff size cap
    SCANNER_NETWORK_LEDGER    default 1 — write per-path ledger JSON to workspace/debug

Pure helpers are unit-tested in ``backend/tests/test_network_observer.py``.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from backend.parsers.base import get_total_count
from backend.scanner.constants import WORKSPACE_DEBUG_DIR
from backend.scanner.scrapers.scanner_intercept_filter import (
    intercept_url_allowed,
    payload_has_inventory_json_structure_signal,
    payload_qualifies_for_inventory_intercept,
    response_content_type_looks_json,
)

logger = logging.getLogger("scanner")

# ── Env knobs ────────────────────────────────────────────────────────────────


def _env_flag(name: str, default: bool) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in ("0", "false", "no", "off")


def score_intercept_enabled() -> bool:
    return _env_flag("SCANNER_SCORE_INTERCEPT", True)


def sniff_nonjson_enabled() -> bool:
    return _env_flag("SCANNER_SNIFF_NONJSON", True)


def network_ledger_enabled() -> bool:
    return _env_flag("SCANNER_NETWORK_LEDGER", True)


def sniff_max_bytes() -> int:
    try:
        return max(65536, int(os.environ.get("SCANNER_SNIFF_MAX_BYTES") or 3_000_000))
    except ValueError:
        return 3_000_000


# ── VIN-shaped value detection ───────────────────────────────────────────────

_VIN_ALPHABET_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")


def looks_like_vin(value: Any) -> bool:
    """17 chars, VIN alphabet (no I/O/Q), at least one letter and one digit."""
    if not isinstance(value, str):
        return False
    s = value.strip().upper()
    if not _VIN_ALPHABET_RE.match(s):
        return False
    return any(c.isdigit() for c in s) and any(c.isalpha() for c in s)


def _has_vin_shaped_value(obj: Any, depth: int = 3) -> bool:
    """Any string value in *obj* (dict/list, depth-limited) that looks like a VIN."""
    if depth < 0:
        return False
    if isinstance(obj, str):
        return looks_like_vin(obj)
    if isinstance(obj, dict):
        return any(_has_vin_shaped_value(v, depth - 1) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_vin_shaped_value(v, depth - 1) for v in obj[:20])
    return False


# ── Vehicle-ness scoring ─────────────────────────────────────────────────────

_VEHICLE_FIELD_KEYS = frozenset(
    {
        "vin", "year", "make", "model", "trim", "price", "msrp", "mileage",
        "odometer", "stock", "stocknumber", "stocknum", "bodystyle", "body",
        "exteriorcolor", "extcolor", "interiorcolor", "drivetrain", "driveline",
        "transmission", "fueltype", "engine", "condition", "certified",
        "internetprice", "saleprice", "sellingprice", "listprice",
    }
)

_KEY_NORM_RE = re.compile(r"[^a-z0-9]")


def _norm_key(k: object) -> str:
    return _KEY_NORM_RE.sub("", str(k).strip().lower())


def _item_vehicle_key_hits(item: dict) -> int:
    """Vehicle-ish keys at the top level or one dict level down."""
    hits = 0
    for k, v in item.items():
        if _norm_key(k) in _VEHICLE_FIELD_KEYS:
            hits += 1
        elif isinstance(v, dict):
            hits += sum(1 for kk in v if _norm_key(kk) in _VEHICLE_FIELD_KEYS)
    return hits


@dataclass
class VehicleListScore:
    score: float = 0.0
    vin_items: int = 0
    n_items: int = 0

    @property
    def qualifies(self) -> bool:
        """Capture-worthy: several VIN-bearing rows, or fewer rows with a rich schema."""
        if self.vin_items >= 3 and self.score >= 0.40:
            return True
        return self.vin_items >= 1 and self.n_items >= 2 and self.score >= 0.65


def score_vehicle_list(lst: list) -> VehicleListScore:
    """
    0..1 vehicle-ness of one candidate list: VIN-shaped values (any key), vehicle
    field keys, and key-schema consistency across items.
    """
    items = [i for i in lst[:50] if isinstance(i, dict) and i]
    if len(items) < 1:
        return VehicleListScore()
    vin_items = sum(1 for i in items if _has_vin_shaped_value(i, depth=3))
    vin_frac = vin_items / len(items)
    key_frac = sum(min(_item_vehicle_key_hits(i), 5) / 5.0 for i in items) / len(items)
    consistency = 0.0
    if len(items) >= 2:  # degenerate (always 1.0) for a single item
        sample = items[:10]
        key_sets = [frozenset(_norm_key(k) for k in i.keys()) for i in sample]
        union = frozenset().union(*key_sets)
        inter = key_sets[0]
        for ks in key_sets[1:]:
            inter = inter & ks
        consistency = (len(inter) / len(union)) if union else 0.0
    score = 0.5 * vin_frac + 0.3 * key_frac + 0.2 * consistency
    return VehicleListScore(score=round(score, 3), vin_items=vin_items, n_items=len(items))


def best_vehicle_list_score(body: Any, max_depth: int = 8) -> VehicleListScore:
    """Recursively score every candidate list in *body*; return the best."""
    best = VehicleListScore()

    def _walk(obj: Any, depth: int) -> None:
        nonlocal best
        if depth > max_depth:
            return
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict):
                s = score_vehicle_list(obj)
                if (s.vin_items, s.score) > (best.vin_items, best.score):
                    best = s
            for item in obj[:40]:
                if isinstance(item, (dict, list)):
                    _walk(item, depth + 1)
        elif isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    _walk(v, depth + 1)

    _walk(body, 0)
    return best


# ── Classification ───────────────────────────────────────────────────────────

TIER_CAPTURE = "capture"
TIER_FINGERPRINT = "fingerprint"
TIER_IGNORE = "ignore"


@dataclass
class Classification:
    tier: str
    reason: str
    url_allowed: bool
    score: VehicleListScore


def classify_payload(url: str, body: Any, dealer_base_url: str) -> Classification:
    """
    Decide the tier for one parsed JSON payload.

    Capture requires the URL gate (deny-list / same-site / allow-list) to pass —
    vehicle-shaped payloads on denied hosts become fingerprints so the allow-list
    can be grown from evidence instead of HAR archaeology.
    """
    allowed = intercept_url_allowed(url, dealer_base_url)
    legacy = payload_qualifies_for_inventory_intercept(body)
    scored = best_vehicle_list_score(body)
    by_score = score_intercept_enabled() and scored.qualifies
    if legacy or by_score:
        if allowed:
            return Classification(TIER_CAPTURE, "legacy" if legacy else "score", allowed, scored)
        return Classification(TIER_FINGERPRINT, "url_denied_vehicle_payload", allowed, scored)
    if scored.vin_items >= 1 or payload_has_inventory_json_structure_signal(body):
        return Classification(TIER_FINGERPRINT, "near_miss", allowed, scored)
    return Classification(TIER_IGNORE, "no_vehicle_signal", allowed, scored)


# ── Fingerprints / ledger ────────────────────────────────────────────────────


def _schema_signature(obj: Any, depth: int = 3) -> Any:
    """Key-tree shape of *obj* (values replaced by type names), depth- and width-capped."""
    if depth <= 0:
        return "…"
    if isinstance(obj, dict):
        return {str(k): _schema_signature(obj[k], depth - 1) for k in sorted(obj, key=str)[:24]}
    if isinstance(obj, list):
        return [_schema_signature(obj[0], depth - 1)] if obj else []
    return type(obj).__name__


def schema_hash(body: Any) -> str:
    sig = json.dumps(_schema_signature(body), sort_keys=True, default=str)
    return hashlib.md5(sig.encode("utf-8")).hexdigest()[:12]


def _host_path(url: str) -> tuple[str, str]:
    try:
        p = urlparse(url)
        return (p.hostname or "").lower(), p.path or "/"
    except ValueError:
        return "", ""


@dataclass
class PayloadFingerprint:
    host: str
    path: str
    method: str
    content_type: str
    schema_hash: str
    top_keys: list[str]
    approx_bytes: int
    best_list_len: int
    vin_items: int
    score: float
    reason: str
    url_allowed: bool
    sniffed: bool
    occurrences: int = 1

    def key(self) -> tuple[str, str, str]:
        return (self.host, self.path, self.schema_hash)


@dataclass
class CapturedEndpoint:
    url: str
    method: str
    content_type: str
    post_data_sample: str | None
    reason: str
    sniffed: bool
    vehicle_rows: int
    total_count: int | None
    auth_headers: dict[str, str] = field(default_factory=dict)
    occurrences: int = 1

    def key(self) -> tuple[str, str, str]:
        host, path = _host_path(self.url)
        return (self.method, host, path)


_AUTH_HEADER_EXACT = frozenset({"authorization"})
_AUTH_HEADER_NEEDLES = (
    "api-key", "apikey", "api_key", "app-id", "application-id", "client-id", "token",
)
_AUTH_HEADER_SKIP = frozenset({"cookie", "set-cookie"})


def extract_auth_headers(headers: dict[str, str], *, max_headers: int = 8) -> dict[str, str]:
    """
    Auth-relevant request headers (search API keys, bearer tokens) for endpoint
    replay recipes. Cookies are excluded — session material stays out of the ledger.
    """
    out: dict[str, str] = {}
    for k, v in (headers or {}).items():
        lk = str(k).strip().lower()
        if lk in _AUTH_HEADER_SKIP or not v:
            continue
        if lk in _AUTH_HEADER_EXACT or any(n in lk for n in _AUTH_HEADER_NEEDLES):
            out[lk] = str(v)[:300]
            if len(out) >= max_headers:
                break
    return out


@dataclass
class ObserverLedger:
    dealer_id: str = ""
    dealer_name: str = ""
    inv_path: str = ""
    endpoints: dict[tuple, CapturedEndpoint] = field(default_factory=dict)
    fingerprints: dict[tuple, PayloadFingerprint] = field(default_factory=dict)
    denied_hosts: dict[str, int] = field(default_factory=dict)
    ignored: int = 0
    sniffed_json: int = 0
    body_read_failures: int = 0

    MAX_FINGERPRINTS = 200

    def add_endpoint(self, ep: CapturedEndpoint) -> None:
        cur = self.endpoints.get(ep.key())
        if cur is None:
            self.endpoints[ep.key()] = ep
        else:
            cur.occurrences += 1
            cur.vehicle_rows = max(cur.vehicle_rows, ep.vehicle_rows)
            if ep.total_count and not cur.total_count:
                cur.total_count = ep.total_count
            if ep.post_data_sample and not cur.post_data_sample:
                cur.post_data_sample = ep.post_data_sample
            if ep.auth_headers and not cur.auth_headers:
                cur.auth_headers = ep.auth_headers

    def add_fingerprint(self, fp: PayloadFingerprint) -> None:
        cur = self.fingerprints.get(fp.key())
        if cur is not None:
            cur.occurrences += 1
            return
        if len(self.fingerprints) < self.MAX_FINGERPRINTS:
            self.fingerprints[fp.key()] = fp

    def is_empty(self) -> bool:
        return not (self.endpoints or self.fingerprints or self.denied_hosts)

    def to_json(self) -> dict[str, Any]:
        return {
            "dealer_id": self.dealer_id,
            "dealer_name": self.dealer_name,
            "inventory_path": self.inv_path,
            "endpoints": [vars(e) for e in self.endpoints.values()],
            "fingerprints": [vars(f) for f in self.fingerprints.values()],
            "denied_hosts": dict(self.denied_hosts),
            "ignored": self.ignored,
            "sniffed_json": self.sniffed_json,
            "body_read_failures": self.body_read_failures,
        }


# ── Body reading (sniff + race-tolerant) ─────────────────────────────────────

_ANTI_XSSI_PREFIXES = (")]}',", ")]}'", "for(;;);", "while(1);")
_JSONP_RE = re.compile(r"^\s*[\w$.]+\s*\((.*)\)\s*;?\s*$", re.DOTALL)
_BINARY_CT_PREFIXES = ("image/", "font/", "video/", "audio/")


def _page_closed_error(exc: BaseException) -> bool:
    """Body read failed because the page/context/browser is gone — retrying can't help."""
    if "targetclosed" in type(exc).__name__.lower():
        return True
    msg = str(exc).lower()
    return "closed" in msg and any(
        w in msg for w in ("target", "page", "context", "browser")
    )


def parse_sniffed_json(text: str) -> Any | None:
    """json.loads tolerant of anti-XSSI prefixes and a single JSONP wrapper."""
    s = (text or "").strip()
    if not s:
        return None
    for prefix in _ANTI_XSSI_PREFIXES:
        if s.startswith(prefix):
            s = s[len(prefix):].lstrip()
            break
    for attempt in range(2):
        try:
            body = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            if attempt == 0:
                m = _JSONP_RE.match(s)
                if m:
                    s = m.group(1).strip()
                    continue
            return None
        return body if isinstance(body, (dict, list)) else None
    return None


# ── The observer ─────────────────────────────────────────────────────────────


class NetworkObserver:
    """
    Per-path response observer. Appends qualifying payloads to the caller-owned
    ``records`` list / ``found_data`` flag (same contract as the legacy inline
    handler), and accumulates a ledger of endpoints + fingerprints on the side.
    """

    def __init__(
        self,
        *,
        dealer_base_url: str,
        dealer_id: str,
        dealer_name: str,
        path: str,
        records: list[tuple[str, Any]],
        found_data: dict[str, bool],
        resp_err: Any = None,
    ) -> None:
        self.base_url = dealer_base_url
        self.dealer_id = dealer_id
        self.dealer_name = dealer_name
        self.path = path
        self.records = records
        self.found_data = found_data
        self._resp_err = resp_err
        self.url_denied = 0
        self.ledger = ObserverLedger(dealer_id=dealer_id, dealer_name=dealer_name, inv_path=path)
        self.capture_event = asyncio.Event()
        self._pending: set[asyncio.Task] = set()

    # -- response entrypoint (Playwright event handler) --

    def on_response(self, response: Any) -> None:
        """
        Sync ``page.on("response")`` entrypoint. Runs the handler in a tracked task so
        callers can :meth:`drain` in-flight body reads before closing the page — with the
        bare async handler, Playwright's fire-and-forget task dies on ``page.close()``
        and the payload (typically the last pagination batch) is silently lost.
        """
        try:
            task = asyncio.get_running_loop().create_task(self.handle_response(response))
        except RuntimeError:
            return
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    async def drain(self, timeout: float = 5.0) -> int:
        """
        Wait up to *timeout* for in-flight body reads to land; returns how many are
        still pending. Stragglers are left running (never cancelled) — the handler
        swallows their eventual failure.
        """
        pending = {t for t in self._pending if not t.done()}
        if not pending:
            return 0
        _, still = await asyncio.wait(pending, timeout=timeout)
        if still:
            logger.debug(
                "Network observer [%s]%s: %d body read(s) still pending after %.1fs drain",
                self.dealer_name, self.path, len(still), timeout,
            )
        return len(still)

    async def handle_response(self, response: Any) -> None:
        try:
            await self._process(response)
        except Exception as e:
            if self._resp_err is None or self._resp_err.should_log():
                logger.debug(
                    "Network observer [%s] path=%s: %s %s",
                    self.dealer_name, self.path, type(e).__name__, str(e)[:200],
                )

    async def _process(self, response: Any) -> None:
        rurl = str(getattr(response, "url", "") or "")
        if not rurl.lower().startswith("http"):
            return
        status = int(getattr(response, "status", 200) or 200)
        if status in (204, 205, 304) or status >= 400:
            return
        headers = response.headers
        if (headers.get("content-length") or "").strip() == "0":
            return
        ct = (headers.get("content-type") or "").strip()
        is_json_ct = response_content_type_looks_json(ct)
        sniffed = False
        if is_json_ct:
            body = await self._read_json(response)
        else:
            if not self._sniffable(response, ct):
                return
            body = await self._read_sniffed(response)
            if body is None:
                return
            sniffed = True
            self.ledger.sniffed_json += 1
        if body is None:
            return

        cls = classify_payload(rurl, body, self.base_url)
        # Preserve legacy denied-URL accounting (JSON content-type + gate failure).
        if is_json_ct and not cls.url_allowed:
            self.url_denied += 1
            host, _ = _host_path(rurl)
            if host:
                self.ledger.denied_hosts[host] = self.ledger.denied_hosts.get(host, 0) + 1

        if cls.tier == TIER_CAPTURE:
            self.records.append((rurl, body))
            self.found_data["value"] = True
            self.capture_event.set()
            await self._record_endpoint(response, rurl, ct, body, cls, sniffed)
            logger.info(
                "Intercepting: %s%s — inventory JSON via %s%s (%s)",
                self.dealer_name, self.path, cls.reason,
                ", sniffed" if sniffed else "", _truncate(rurl, 80),
            )
        elif cls.tier == TIER_FINGERPRINT:
            self._record_fingerprint(response, rurl, ct, body, cls, sniffed)
        else:
            self.ledger.ignored += 1

    # -- body readers --

    async def _read_json(self, response: Any) -> Any | None:
        for attempt in (0, 1):
            try:
                return await response.json()
            except Exception as e:
                if _page_closed_error(e):
                    self.ledger.body_read_failures += 1
                    return None
                if attempt == 0:
                    # Body may not be buffered yet; brief settle then one retry.
                    # (response.finished() would be exact but leaks un-retrieved
                    # task exceptions when the page closes mid-wait.)
                    await asyncio.sleep(0.4)
                else:
                    self.ledger.body_read_failures += 1
        return None

    async def _read_sniffed(self, response: Any) -> Any | None:
        cap = sniff_max_bytes()
        try:
            clen = int(response.headers.get("content-length") or 0)
        except (TypeError, ValueError):
            clen = 0
        if clen > cap:
            return None
        for attempt in (0, 1):
            try:
                text = await response.text()
                break
            except Exception as e:
                if _page_closed_error(e):
                    self.ledger.body_read_failures += 1
                    return None
                if attempt == 0:
                    await asyncio.sleep(0.4)
                else:
                    self.ledger.body_read_failures += 1
                    return None
        if not text or len(text) > cap:
            return None
        return parse_sniffed_json(text)

    def _sniffable(self, response: Any, ct: str) -> bool:
        if not sniff_nonjson_enabled():
            return False
        base_ct = ct.split(";")[0].strip().lower()
        if base_ct.startswith(_BINARY_CT_PREFIXES) or base_ct == "application/octet-stream":
            return False
        rtype = ""
        try:
            rtype = (response.request.resource_type or "").lower()
        except Exception:
            pass
        return rtype in ("xhr", "fetch")

    # -- ledger writers --

    def _request_info(self, response: Any) -> tuple[str, str | None]:
        method, post_sample = "GET", None
        try:
            req = response.request
            method = (req.method or "GET").upper()
            if method != "GET":
                pd = req.post_data
                if pd:
                    post_sample = str(pd)[:2000]
        except Exception:
            pass
        return method, post_sample

    async def _request_auth_headers(self, response: Any) -> dict[str, str]:
        """Auth headers for the replay recipe — ``all_headers()`` includes JS-set ones."""
        try:
            req = response.request
        except Exception:
            return {}
        headers: dict[str, str] = {}
        try:
            headers = await req.all_headers()
        except Exception:
            try:
                headers = dict(req.headers or {})
            except Exception:
                return {}
        return extract_auth_headers(headers)

    async def _record_endpoint(
        self, response: Any, rurl: str, ct: str, body: Any, cls: Classification, sniffed: bool,
    ) -> None:
        method, post_sample = self._request_info(response)
        total = None
        if isinstance(body, dict):
            try:
                total = get_total_count(body)
            except Exception:
                total = None
        self.ledger.add_endpoint(
            CapturedEndpoint(
                url=rurl[:500],
                method=method,
                content_type=ct[:100],
                post_data_sample=post_sample,
                reason=cls.reason,
                sniffed=sniffed,
                vehicle_rows=cls.score.vin_items,
                total_count=int(total) if total else None,
                auth_headers=await self._request_auth_headers(response),
            )
        )

    def _record_fingerprint(
        self, response: Any, rurl: str, ct: str, body: Any, cls: Classification, sniffed: bool,
    ) -> None:
        method, _ = self._request_info(response)
        host, path = _host_path(rurl)
        top_keys: list[str] = []
        if isinstance(body, dict):
            top_keys = [str(k) for k in list(body.keys())[:16]]
        try:
            approx = len(json.dumps(body, default=str)[:2_000_000])
        except (TypeError, ValueError):
            approx = 0
        best_len = 0
        if cls.score.n_items:
            best_len = cls.score.n_items
        self.ledger.add_fingerprint(
            PayloadFingerprint(
                host=host,
                path=path[:200],
                method=method,
                content_type=ct[:100],
                schema_hash=schema_hash(body),
                top_keys=top_keys,
                approx_bytes=approx,
                best_list_len=best_len,
                vin_items=cls.score.vin_items,
                score=cls.score.score,
                reason=cls.reason,
                url_allowed=cls.url_allowed,
                sniffed=sniffed,
            )
        )

    # -- finalization --

    def log_summary(self) -> None:
        led = self.ledger
        logger.info(
            "Network observer [%s]%s: captured=%d endpoints=%d fingerprints=%d "
            "denied_urls=%d sniffed_json=%d ignored=%d read_failures=%d",
            self.dealer_name, self.path, len(self.records), len(led.endpoints),
            len(led.fingerprints), self.url_denied, led.sniffed_json,
            led.ignored, led.body_read_failures,
        )

    def save_ledger(self) -> str | None:
        """Write the ledger JSON to workspace/debug (when enabled and non-empty)."""
        if not network_ledger_enabled() or self.ledger.is_empty():
            return None
        try:
            WORKSPACE_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            slug = re.sub(r"[^a-z0-9]+", "-", (self.path or "root").lower()).strip("-") or "root"
            out = WORKSPACE_DEBUG_DIR / f"netledger_{self.dealer_id}_{slug}_{int(time.time())}.json"
            with open(out, "w", encoding="utf-8") as f:
                json.dump(self.ledger.to_json(), f, indent=1, default=str)
            logger.debug("Network observer ledger saved: %s", out)
            return str(out)
        except OSError as e:
            logger.debug("Network observer ledger save failed [%s]: %s", self.dealer_name, e)
            return None


def _truncate(u: str, max_len: int = 120) -> str:
    return u if len(u) <= max_len else u[: max_len - 1] + "…"
