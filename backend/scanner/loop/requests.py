"""Scan-loop requests, proposals and statuses (card L11, docs/scan_loop/BUNDLE.md section 5).

Pure and standard library only (plus the sibling pure modules): no network, no database, no file read, no clock or
environment read. The wrappers (``ops_requests.py`` and the Railway tick) gather the context and call these functions.
Everything that comes from the data repository is hostile: parsing is strict, schemas are closed, ids and paths are
matched against fixed patterns, and no value reaches a shell, a SQL string or a file name unless it matched.
"""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlsplit

from backend.scanner.loop.config import (
    DEALER_ID_RE,
    KINDS,
    MAX_DEALERS_PER_REQUEST,
    MAX_PROPOSAL_BYTES,
    MAX_REQUEST_BYTES,
    MAX_STATUS_BYTES,
    REASON_MAX_CHARS,
)
from backend.scanner.loop.scrub import clean_line

REJECT_CODES = (
    "schema", "unknown_dealer", "do_not_scan", "recipe_only_not_revived", "too_many_dealers", "blocked_egress",
    "rate_limited", "base_changed", "validation_reject", "canary_fail", "interrupted", "executor_error",
)
STATES = ("rejected", "done", "rolled_back", "failed")
MAX_DEPTH = 8
MAX_UNPROCESSED = 50
NOTE_MAX = 200

REQUEST_KEYS = ("schema", "kind", "dealers", "proposal", "fix_cycle", "executor", "note")
PROPOSAL_KEYS = ("schema", "dealer_id", "base_canonical_sha256", "recipes", "scan_hints", "evidence_refs", "rationale")
RECIPE_KEYS = ("dealer_id", "url", "method", "content_type", "post_template", "auth_headers", "pagination",
               "vehicle_rows", "total_count", "provider_hint", "stale", "stale_reason")
SCAN_HINT_KEYS = ("needs_http_proxy", "price_source", "requires_browser", "skip_reason", "notes", "hint_source")
PRICE_SOURCES = ("list", "vdp", "second_endpoint")
# The PAGINATION_* values of backend/scanner/recipes.py (a test keeps this tuple equal to them).
PAGINATIONS = ("carscommerce_page", "typesense_page", "dealer_com_start", "page_query", "dep_srp_page", "algolia_page",
               "html_page_query", "jazel_srp_page", "cosmos_pt", "graphql_offset", "none")

_REQUEST_NAME_RE = re.compile(r"^rq-[0-9]{8}T[0-9]{6}Z-(0[1-9]|[1-9][0-9])$")
_PROPOSAL_STAMP = r"pr-[0-9TZ-]{8,24}"
_SHA_RE = re.compile(r"^[0-9a-f]{64}$")
_HOST_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$")
_BAD_SUFFIXES = (".internal", ".local", ".localhost")
_CTRL_RE = re.compile(r"[\x00-\x20\x7f]")
_PATH_BAD_RE = re.compile(r"[\\%\x00-\x1f\x7f]|//|(^|/)\.\.?(/|$)")


class ParseError(Exception):
    """Input broke the closed schema; the message is a fixed template and never echoes the input."""

    code = "schema"


@dataclass(frozen=True)
class Accept:
    dealers: tuple


@dataclass(frozen=True)
class Reject:
    code: str
    per_dealer: dict = field(default_factory=dict)
    detail: str = ""


# --- strict parsing ------------------------------------------------------------------------------


def _no_dup_pairs(pairs):
    seen = {}
    for key, value in pairs:
        if key in seen:
            raise ParseError("duplicate key")
        seen[key] = value
    return seen


def _reject_constant(_name):
    raise ParseError("non-finite number")


def _depth_ok(node, depth=1) -> bool:
    if depth > MAX_DEPTH:
        return False
    if isinstance(node, dict):
        return all(_depth_ok(v, depth + 1) for v in node.values())
    if isinstance(node, list):
        return all(_depth_ok(v, depth + 1) for v in node)
    return True


def parse_object(raw, limit: int) -> dict:
    """One JSON object from at most ``limit`` bytes: UTF-8 without BOM, no duplicate keys, no NaN, depth at most 8."""
    if isinstance(raw, str):
        raw = raw.encode("utf-8", errors="strict")
    if not isinstance(raw, (bytes, bytearray)):
        raise ParseError("not bytes")
    data = bytes(raw)
    if len(data) > limit:
        raise ParseError("file too large")
    if data.startswith(b"\xef\xbb\xbf"):
        raise ParseError("byte order mark")
    try:
        text = data.decode("utf-8")
        obj = json.loads(text, object_pairs_hook=_no_dup_pairs, parse_constant=_reject_constant)
    except ParseError:
        raise
    except (ValueError, RecursionError):
        raise ParseError("not valid JSON") from None
    if not isinstance(obj, dict):
        raise ParseError("not a JSON object")
    if not _depth_ok(obj):
        raise ParseError("nesting too deep")
    return obj


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_id(value) -> bool:
    return isinstance(value, str) and DEALER_ID_RE.fullmatch(value) is not None


def _keys(obj: dict, expected: tuple, what: str) -> None:
    if set(obj) != set(expected):
        raise ParseError(what + " keys are not exactly the closed set")


def _text(value, limit: int, what: str, allow_empty=True) -> str:
    if not isinstance(value, str) or len(value) > limit or (not allow_empty and not value):
        raise ParseError(what + " is not a string of at most " + str(limit) + " characters")
    return value


def proposal_path_ok(path, dealer_id) -> bool:
    return isinstance(path, str) and re.fullmatch(
        r"proposals/" + re.escape(str(dealer_id)) + "/" + _PROPOSAL_STAMP + r"\.json", path) is not None and len(path) <= 200


def _repo_path_ok(path, dealer_id, prefixes) -> bool:
    if not isinstance(path, str) or not path or len(path) > 200 or path.startswith("/") or _PATH_BAD_RE.search(path):
        return False
    return any(path.startswith(p.format(d=dealer_id)) and len(path) > len(p.format(d=dealer_id)) for p in prefixes)


def parse_request(raw) -> dict:
    """Validate a request file's bytes; returns the dict or raises ``ParseError`` (reject code ``schema``)."""
    obj = parse_object(raw, MAX_REQUEST_BYTES)
    _keys(obj, REQUEST_KEYS, "request")
    if obj["schema"] != 1 or not _is_int(obj["schema"]):
        raise ParseError("schema must be 1")
    kind = obj["kind"]
    if kind not in KINDS:
        raise ParseError("unknown kind")
    dealers = obj["dealers"]
    if not isinstance(dealers, list) or not 1 <= len(dealers) <= 64:
        raise ParseError("dealers must list 1 to 64 ids")
    if not all(_is_id(d) for d in dealers):
        raise ParseError("a dealer id breaks the id pattern")
    if len(set(dealers)) != len(dealers):
        raise ParseError("dealer ids must be distinct")
    if kind in ("validate", "apply"):
        if len(dealers) != 1:
            raise ParseError(kind + " takes exactly one dealer")
        if not proposal_path_ok(obj["proposal"], dealers[0]):
            raise ParseError("proposal path is not proposals/<dealer>/pr-<stamp>.json for the dealer")
    elif obj["proposal"] is not None:
        raise ParseError(kind + " takes no proposal")
    if not _is_int(obj["fix_cycle"]) or not 1 <= obj["fix_cycle"] <= 9:
        raise ParseError("fix_cycle must be an integer 1 to 9")
    if obj["executor"] != "railway":
        raise ParseError("executor must be railway")
    _text(obj["note"], NOTE_MAX, "note")
    return obj


def url_ok(url) -> bool:
    """https only, a public-looking DNS name, no userinfo, port 443 or none, at most 2,048 characters."""
    if not isinstance(url, str) or not url or len(url) > 2048 or _CTRL_RE.search(url):
        return False
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError:
        return False
    if parts.scheme != "https" or "@" in parts.netloc or parts.username is not None or parts.password is not None:
        return False
    if port not in (None, 443):
        return False
    host = parts.hostname or ""
    if not host or host != host.lower() or host.endswith("."):
        return False
    try:
        ipaddress.ip_address(host)
        return False  # an IP literal, private range or metadata address alike
    except ValueError:
        pass
    if not _HOST_RE.fullmatch(host):  # needs a dot: a single-label host is refused
        return False
    return not any(host.endswith(suffix) or host == suffix[1:] for suffix in _BAD_SUFFIXES)


def _check_recipe(row, dealer_id) -> None:
    if not isinstance(row, dict):
        raise ParseError("a recipe is not an object")
    _keys(row, RECIPE_KEYS, "recipe")
    if row["dealer_id"] != dealer_id:
        raise ParseError("recipe dealer_id differs from the proposal's")
    if row["method"] not in ("GET", "POST"):
        raise ParseError("recipe method must be GET or POST")
    if not url_ok(row["url"]):
        raise ParseError("recipe url is not an allowed https URL")
    _text(row["content_type"], 100, "content_type")
    if row["post_template"] is not None:
        _text(row["post_template"], 16384, "post_template")
    headers = row["auth_headers"]
    if not isinstance(headers, dict) or len(headers) > 8:
        raise ParseError("auth_headers must be an object of at most 8 pairs")
    for name, value in headers.items():
        _text(name, 64, "header name", allow_empty=False)
        _text(value, 512, "header value")
        if _CTRL_RE.search(name) or re.search(r"[\r\n\x00]", value):
            raise ParseError("header carries control characters")
    if row["pagination"] not in PAGINATIONS:
        raise ParseError("unknown pagination")
    if not _is_int(row["vehicle_rows"]) or row["vehicle_rows"] < 0:
        raise ParseError("vehicle_rows must be a non-negative integer")
    total = row["total_count"]
    if total is not None and (not _is_int(total) or total < 0):
        raise ParseError("total_count must be a non-negative integer or null")
    _text(row["provider_hint"], 64, "provider_hint")
    if not isinstance(row["stale"], bool):
        raise ParseError("stale must be a boolean")
    _text(row["stale_reason"], 200, "stale_reason")


def _check_hints(hints) -> None:
    if not isinstance(hints, dict) or not set(hints) <= set(SCAN_HINT_KEYS):
        raise ParseError("scan_hints holds a key outside the allowed six")
    for key in ("needs_http_proxy", "requires_browser"):
        if key in hints and not isinstance(hints[key], bool):
            raise ParseError(key + " must be a boolean")
    if "price_source" in hints and hints["price_source"] not in PRICE_SOURCES:
        raise ParseError("price_source must be list, vdp or second_endpoint")
    for key in ("skip_reason", "notes", "hint_source"):
        if key in hints:
            _text(hints[key], 200, key)


def parse_proposal(raw, dealer_id=None) -> dict:
    """Validate a proposal file's bytes; with ``dealer_id`` it must be that dealer's."""
    obj = parse_object(raw, MAX_PROPOSAL_BYTES)
    _keys(obj, PROPOSAL_KEYS, "proposal")
    if obj["schema"] != 1 or not _is_int(obj["schema"]):
        raise ParseError("schema must be 1")
    dealer = obj["dealer_id"]
    if not _is_id(dealer) or (dealer_id is not None and dealer != dealer_id):
        raise ParseError("proposal dealer_id is wrong")
    base = obj["base_canonical_sha256"]
    if base != "none" and not (isinstance(base, str) and _SHA_RE.fullmatch(base)):
        raise ParseError("base_canonical_sha256 must be 64 lowercase hex or none")
    recipes = obj["recipes"]
    if not isinstance(recipes, list) or not 1 <= len(recipes) <= 12:
        raise ParseError("recipes must list 1 to 12 rows")
    for row in recipes:
        _check_recipe(row, dealer)
    _check_hints(obj["scan_hints"])
    refs = obj["evidence_refs"]
    if not isinstance(refs, list) or len(refs) > 10:
        raise ParseError("evidence_refs must list at most 10 paths")
    allowed = ("workspace/dealer_logs/{d}/", "evidence/{d}/", "runs/")
    if not all(_repo_path_ok(r, dealer, allowed) for r in refs):
        raise ParseError("an evidence ref is outside the allowed folders")
    if not _repo_path_ok(obj["rationale"], dealer, ("workspace/dealer_logs/{d}/",)):
        raise ParseError("rationale must be a path under the dealer's logs")
    return obj


# --- ids -------------------------------------------------------------------------------------------


def new_request_id(now: datetime, existing=()) -> str:
    """``rq-<UTC stamp>-<nn>`` with the first free ``nn`` from 01 to 99; ``ValueError`` when all are taken."""
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    stamp = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    taken = set(existing)
    for n in range(1, 100):
        name = "rq-" + stamp + "-" + format(n, "02d")
        if name not in taken and name + ".json" not in taken:
            return name
    raise ValueError("all 99 request ids of this second are taken")


def request_name_ok(name) -> bool:
    return isinstance(name, str) and _REQUEST_NAME_RE.fullmatch(name) is not None


def limit_unprocessed(names, limit: int = MAX_UNPROCESSED):
    """``(considered, overflow)``: the oldest ``limit`` unprocessed request names, the rest are ``rate_limited``."""
    ordered = sorted(names)
    return ordered[:limit], ordered[limit:]


# --- the ledger and the budgets ----------------------------------------------------------------------


def _day(value) -> str:
    return str(value)[:10]


def ledger_view(events, config, now) -> dict:
    """What the budgets need from ``ledger.jsonl`` events (rule 5 of section 6).

    ``events`` are dicts ``{t, ev, rq, sha256, kind, dealers, state, reason_code, tick}`` with ``ev`` in ``start``,
    ``finish``, ``reject`` and ``reset`` (``dealers`` then names the dealers the owner reset). ``tick`` is optional;
    without it each request counts as its own tick.
    """
    today = _day(now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    attempts: dict = {}
    applies_today: dict = {}
    scan_ticks_today = set()
    finished, started = set(), set()
    for event in events:
        ev, kind, dealers = event.get("ev"), event.get("kind"), event.get("dealers") or []
        if ev == "reset":
            for dealer in dealers:
                attempts[dealer] = 0
        elif ev == "start":
            started.add(event.get("rq"))
        elif ev == "finish":
            finished.add(event.get("rq"))
            if kind in ("rescan", "apply"):
                for dealer in dealers:
                    attempts[dealer] = attempts.get(dealer, 0) + 1
                if _day(event.get("t")) == today:
                    scan_ticks_today.add(event.get("tick") or event.get("rq"))
            if kind == "apply" and _day(event.get("t")) == today:
                for dealer in dealers:
                    applies_today[dealer] = applies_today.get(dealer, 0) + 1
    escalated = {d for d, n in attempts.items() if n >= config.attempts_per_dealer}
    return {"attempts": attempts, "escalated": escalated, "applies_today": applies_today,
            "scan_ticks_today": len(scan_ticks_today), "finished": finished, "interrupted": started - finished}


def check_request(req: dict, ctx: dict, config, ledger_events, now):
    """Decide one parsed request: ``Accept`` or ``Reject(code, per_dealer)``, checks in the order of section 5.3.

    ``ctx``: ``manifest_ids`` (collection), ``do_not_scan``, ``recipe_only_not_revived`` and ``blocked`` (collections
    of dealer ids the wrapper took from the roster rule and the egress check). Schema has been checked by
    ``parse_request``.
    """
    dealers = tuple(req["dealers"])
    kind = req["kind"]
    cap = min(MAX_DEALERS_PER_REQUEST, config.dealers_per_cycle)
    if kind in ("probe", "rescan") and len(dealers) > cap:
        return Reject("too_many_dealers", {d: "too_many_dealers" for d in dealers}, "more than " + str(cap) + " dealers")
    manifest = set(ctx.get("manifest_ids") or ())
    groups = (
        ("unknown_dealer", lambda d: d not in manifest),
        ("do_not_scan", lambda d: d in set(ctx.get("do_not_scan") or ())),
        ("recipe_only_not_revived", lambda d: d in set(ctx.get("recipe_only_not_revived") or ())),
        ("blocked_egress", lambda d: d in set(ctx.get("blocked") or ())),
    )
    for code, bad in groups:
        offenders = {d: code for d in dealers if bad(d)}
        if offenders:
            return Reject(code, offenders)
    view = ledger_view(ledger_events, config, now)
    if kind not in config.kinds or config.idle:
        return Reject("rate_limited", {d: "rate_limited" for d in dealers}, "kind disabled")
    offenders = {}
    for dealer in dealers:
        if kind in ("rescan", "apply") and dealer in view["escalated"]:
            offenders[dealer] = "rate_limited"
        elif kind == "apply" and view["applies_today"].get(dealer, 0) >= config.applies_per_dealer_per_day:
            offenders[dealer] = "rate_limited"
    if offenders:
        return Reject("rate_limited", offenders)
    return Accept(dealers)


def plan_tick(accepted, config, ledger_events, now, hold: bool = False):
    """``(to_run, deferred)``: accepted requests in request-id order, within the budgets of rule 5.

    ``accepted`` is a list of ``(request_id, request)``. A deferred request keeps no status and waits for a later
    tick. While ``hold`` is set only ``probe`` and ``validate`` run. At most ``dealers_per_cycle`` distinct dealers per
    tick; a ``rescan`` or ``apply`` needs a free cycle of the UTC day (``cycles_per_day`` ticks at most).
    """
    view = ledger_view(ledger_events, config, now)
    to_run, deferred = [], []
    dealers_used: set = set()
    scan_slot = view["scan_ticks_today"] < config.cycles_per_day
    for rq_id, req in sorted(accepted, key=lambda pair: pair[0]):
        kind = req["kind"]
        scans = kind in ("rescan", "apply")
        fits_dealers = len(dealers_used | set(req["dealers"])) <= config.dealers_per_cycle
        if (scans and (hold or not scan_slot)) or (hold and kind not in ("probe", "validate")) or not fits_dealers:
            deferred.append((rq_id, req))
            continue
        dealers_used |= set(req["dealers"])
        to_run.append((rq_id, req))
    return to_run, deferred


# --- statuses ----------------------------------------------------------------------------------------


def _clean_node(node, limit=REASON_MAX_CHARS):
    if isinstance(node, str):
        return clean_line(node, limit)
    if isinstance(node, dict):
        return {clean_line(str(k), 80): _clean_node(v, limit) for k, v in node.items()}
    if isinstance(node, (list, tuple)):
        return [_clean_node(v, limit) for v in node]
    return node


def status_json(kind: str, state: str, reason_code: str = "", detail: str = "", finished=None, run_id=None,
                per_dealer=None) -> bytes:
    """The bytes of a ``<request>.status.json``: every text cleaned to one line, at most 16,384 bytes."""
    if kind not in KINDS:
        raise ValueError("unknown kind")
    if state not in STATES:
        raise ValueError("unknown state")
    if reason_code and reason_code not in REJECT_CODES:
        raise ValueError("unknown reason code")
    if (state == "done") != (reason_code == ""):
        raise ValueError("reason_code is empty exactly when the state is done")
    body = {"schema": 1, "kind": kind, "state": state, "reason_code": reason_code, "detail": clean_line(detail, 200),
            "finished": finished, "run_id": run_id, "per_dealer": _clean_node(per_dealer or {})}
    data = (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if len(data) > MAX_STATUS_BYTES:
        body["per_dealer"] = {d: {"reason_code": (v or {}).get("reason_code", "") if isinstance(v, dict) else ""}
                              for d, v in (per_dealer or {}).items()}
        data = (json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    return data
