"""Golden pin for ``serialize_car_for_api`` (the car detail / embedded payload serializer).

Written before the 2026-10-01 split of the 577-line function into named steps, so
the refactor can be proved byte-for-byte behaviour-preserving.

The fixture holds real active cars read from the local Postgres fleet (read-only
connection) and, per car, the serializer's output under every flag combination a
caller uses:

* ``detail``       include_verified=False, verified_specs=<merged>      (car page, compare,
                   persist_enrichment, trace/dev detail, data_quality_invariants)
* ``completeness`` the same with include_extended_display=False         (listing_completeness)
* ``listing``      include_verified=False, no verified_specs            (trace/dev listing payload)
* ``default``      no keyword arguments (merges verified specs itself)   (tests, ad-hoc callers)

Everything the serializer reads from a database, the filesystem or the clock is
an *oracle*: its return value was recorded per call (keyed by the call's
arguments) while the fixture was generated, and is replayed here. Each oracle is
patched on EVERY module that binds it, so the pin does not care which module of
``backend.utils.car_serialize`` the serializer's steps live in. All the logic
between those lookups (the precedence rules, suppressions, vPIC-wins transmission
and horsepower fill, gallery/url/packages handling, key order) runs for real.

Regenerate (only when behaviour is meant to change) with::

    PGOPTIONS='-c default_transaction_read_only=on' \\
        .venv/bin/python -m backend.tests.test_serialize_car_for_api_golden --generate
"""
from __future__ import annotations

import base64
import datetime as _dt
import decimal
import gzip
import hashlib
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "serialize_car_for_api_golden.json.gz"

COMBOS: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "detail": lambda vs: {"include_verified": False, "verified_specs": vs},
    "completeness": lambda vs: {
        "include_verified": False,
        "verified_specs": vs,
        "include_extended_display": False,
    },
    "listing": lambda vs: {"include_verified": False},
    "default": lambda vs: {},
}

# (module, attribute): every lookup reached from serialize_car_for_api whose
# answer depends on a database, a file on disk, a process-wide cache loaded from
# either, or the wall clock.
ORACLES: tuple[tuple[str, str], ...] = (
    ("backend.enrichment.knowledge_engine_specs", "merge_verified_specs"),
    ("backend.enrichment.knowledge_engine_specs", "implausible_extended_spec_fields"),
    ("backend.enrichment.knowledge_engine_specs", "curated_zero_to_60_sec"),
    ("backend.enrichment.knowledge_engine", "decode_trim_logic"),
    ("backend.enrichment.catalog_lookup", "lookup_catalog_options_and_packages"),
    ("backend.utils.vpic_specs", "vpic_specs_for_vin"),
    ("backend.utils.price_plausibility", "implausible_price"),
    ("backend.utils.msrp_trust", "resolve_display_msrp"),
    ("backend.utils.first_seen", "first_seen_fields"),
    ("backend.utils.forced_induction", "classify_forced_induction_from_car_row"),
    ("backend.utils.transmission_normalize", "normalize_transmission_standard"),
    ("backend.utils.field_clean", "normalize_body_style_for_car"),
    ("backend.utils.market_price", "is_payment_shaped_price"),
    ("backend.utils.mileage_display", "mileage_not_listed"),
    ("backend.enrichment.knowledge_engine", "lookup_epa_master_by_id"),
    ("backend.enrichment.knowledge_engine", "lookup_vpic_from_cache"),
    ("backend.intelligence.deal_score_cache", "public_deal_score"),
    ("backend.intelligence.tco_fuel_estimates", "resolve_fuel_tank_gallons"),
    ("backend.intelligence.tco_fuel_estimates", "resolve_tco_avg_mpg"),
    ("backend.intelligence.tco_fuel_estimates", "resolve_tco_ev_efficiency"),
    ("backend.intelligence.ev_range_estimates", "resolve_factory_epa_range"),
    ("backend.parsers.vdp_urls", "resolve_vehicle_source_url"),
    ("backend.utils.car_serialize.engine", "build_engine_display"),
    ("backend.utils.car_serialize.engine", "_effective_fuel_type_for_display"),
    ("backend.utils.car_serialize.location_tco", "resolve_car_state_code"),
    ("backend.utils.car_serialize.location_tco", "resolve_car_fuel_requirement"),
)

# Oracles that mutate an argument in place; the recorded effect is the argument's
# state after the call, re-applied on replay.
_MUTATES_ARG: dict[str, int] = {}


# ── tagged JSON codec (keeps datetimes, tuples, Decimals, bytes exact) ──────────


def _enc(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        if v != v or v in (float("inf"), float("-inf")):
            return {"__t": "float", "v": repr(v)}
        return v
    if isinstance(v, _dt.datetime):
        return {"__t": "datetime", "v": v.isoformat()}
    if isinstance(v, _dt.date):
        return {"__t": "date", "v": v.isoformat()}
    if isinstance(v, decimal.Decimal):
        return {"__t": "decimal", "v": str(v)}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"__t": "bytes", "v": base64.b64encode(bytes(v)).decode()}
    if isinstance(v, tuple):
        return {"__t": "tuple", "v": [_enc(x) for x in v]}
    if isinstance(v, (set, frozenset)):
        return {"__t": "set", "v": sorted((_enc(x) for x in v), key=repr)}
    if isinstance(v, list):
        return [_enc(x) for x in v]
    if isinstance(v, dict):
        return {"__t": "dict", "k": [_enc(k) for k in v], "v": [_enc(x) for x in v.values()]}
    raise TypeError(f"golden codec cannot encode {type(v).__name__}")


def _dec(v: Any) -> Any:
    if isinstance(v, list):
        return [_dec(x) for x in v]
    if not isinstance(v, dict):
        return v
    t = v["__t"]
    if t == "dict":
        return {_dec(k): _dec(x) for k, x in zip(v["k"], v["v"])}
    if t == "float":
        return float(v["v"])
    if t == "datetime":
        return _dt.datetime.fromisoformat(v["v"])
    if t == "date":
        return _dt.date.fromisoformat(v["v"])
    if t == "decimal":
        return decimal.Decimal(v["v"])
    if t == "bytes":
        return base64.b64decode(v["v"])
    if t == "tuple":
        return tuple(_dec(x) for x in v["v"])
    if t == "set":
        return set(_dec(x) for x in v["v"])
    raise TypeError(t)


def _encode_out(d: dict[str, Any]) -> str:
    return json.dumps(_enc(d), separators=(",", ":"))


def _pack_outs(outs: dict[str, str]) -> dict[str, Any]:
    """``detail`` in full; every other combo as a diff against it (keeps the fixture small)."""
    base = _dec(json.loads(outs["detail"]))
    packed: dict[str, Any] = {"detail": outs["detail"]}
    for combo, enc in outs.items():
        if combo == "detail":
            continue
        cur = _dec(json.loads(enc))
        changed = {k: v for k, v in cur.items() if k not in base or _canon(base[k]) != _canon(v)}
        packed[combo] = {
            "keys": list(cur),
            "set": json.dumps(_enc(changed), separators=(",", ":")),
        }
        assert _unpack_out(packed, combo) == enc
    return packed


def _unpack_out(packed: dict[str, Any], combo: str) -> str:
    if combo == "detail":
        return packed["detail"]
    base = _dec(json.loads(packed["detail"]))
    entry = packed[combo]
    changed = _dec(json.loads(entry["set"]))
    full = {k: (changed[k] if k in changed else base[k]) for k in entry["keys"]}
    return _encode_out(full)


def _canon(v: Any) -> str:
    return json.dumps(_enc(v), sort_keys=True, separators=(",", ":"))


def _call_key(name: str, args: tuple, kwargs: dict) -> str:
    # dict key ORDER is irrelevant to a lookup's answer, so the key sorts it away
    payload = json.dumps(
        [name, _enc(list(args)), _enc(dict(sorted(kwargs.items())))],
        sort_keys=True,
        separators=(",", ":"),
        default=repr,
    )
    payload = json.dumps(json.loads(payload), sort_keys=True)
    return hashlib.sha1(_unorder(payload).encode()).hexdigest()[:20]


def _unorder(payload: str) -> str:
    """Canonicalize tagged dicts as sorted pairs so key order cannot change a key."""

    def fix(o: Any) -> Any:
        if isinstance(o, list):
            return [fix(x) for x in o]
        if isinstance(o, dict):
            if o.get("__t") == "dict":
                pairs = sorted(
                    ([fix(k), fix(x)] for k, x in zip(o["k"], o["v"])),
                    key=lambda p: json.dumps(p[0], sort_keys=True),
                )
                return {"__t": "dict", "p": pairs}
            return {k: fix(x) for k, x in o.items()}
        return o

    return json.dumps(fix(json.loads(payload)), sort_keys=True)


# ── patch every binding of an oracle ───────────────────────────────────────────


def _bindings(orig: Any) -> list[tuple[Any, str]]:
    found = []
    for mod_name, mod in list(sys.modules.items()):
        if mod is None or not mod_name.startswith("backend."):
            continue
        d = getattr(mod, "__dict__", None)
        if not d:
            continue
        for attr, val in list(d.items()):
            if val is orig:
                found.append((mod, attr))
    return found


def _import_everything() -> None:
    import backend.utils.car_serialize  # noqa: F401  (the package and every step module)

    pkg_dir = Path(backend.utils.car_serialize.__file__).parent
    for p in sorted(pkg_dir.glob("*.py")):
        if p.stem != "__init__":
            importlib.import_module(f"backend.utils.car_serialize.{p.stem}")
    for mod, _attr in ORACLES:
        importlib.import_module(mod)


def _install(wrapper_for: Callable[[str, Callable], Callable], setter: Callable) -> None:
    _import_everything()
    for mod_name, attr in ORACLES:
        orig = getattr(importlib.import_module(mod_name), attr)
        wrapped = wrapper_for(attr, orig)
        for mod, bound_attr in _bindings(orig):
            setter(mod, bound_attr, wrapped)


# ── test ───────────────────────────────────────────────────────────────────────


def _load_fixture() -> dict[str, Any]:
    with gzip.open(FIXTURE, "rt", encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def golden() -> dict[str, Any]:
    if not FIXTURE.exists():
        pytest.fail(f"golden fixture missing: {FIXTURE}")
    return _load_fixture()


def _replay_install(monkeypatch: pytest.MonkeyPatch, calls: dict[str, Any], misses: list[str]):
    def wrapper_for(name: str, orig: Callable) -> Callable:
        def replay(*args, **kwargs):
            key = _call_key(name, args, kwargs)
            rec = calls.get(key)
            if rec is None:
                misses.append(name)
                raise AssertionError(f"golden: unrecorded oracle call {name}")
            if "raise" in rec:
                raise Exception(rec["raise"])
            if name in _MUTATES_ARG:
                target = args[_MUTATES_ARG[name]]
                after = _dec(rec["arg_after"])
                target.clear()
                target.update(after)
            return _dec(rec["ret"])

        replay.__wrapped_oracle__ = orig  # type: ignore[attr-defined]
        return replay

    _install(wrapper_for, lambda mod, attr, val: monkeypatch.setattr(mod, attr, val))


def test_serialize_car_for_api_matches_golden(golden, monkeypatch):
    from backend.utils.car_serialize import serialize_car_for_api

    misses: list[str] = []
    _replay_install(monkeypatch, golden["calls"], misses)

    failures: list[str] = []
    checked = 0
    for case in golden["cases"]:
        raw = _dec(case["car"])
        vs = _dec(case["vs"])
        for combo in case["out"]:
            want = _unpack_out(case["out"], combo)
            kwargs = COMBOS[combo](vs)
            try:
                got = serialize_car_for_api(dict(raw), **kwargs)
            except AssertionError as exc:
                failures.append(f"car {case['id']} [{combo}]: {exc}")
                continue
            checked += 1
            got_enc = _encode_out(got)
            if got_enc != want:
                w = _dec(json.loads(want))
                diff_keys = [k for k in set(w) | set(got) if _canon(w.get(k, "<absent>")) != _canon(got.get(k, "<absent>"))]
                order = "" if list(w) == list(got) else " (key order differs)"
                failures.append(f"car {case['id']} [{combo}]: keys {sorted(diff_keys)[:8]}{order}")
        if len(failures) > 25:
            break
    assert not failures, f"{len(failures)} golden mismatches:\n" + "\n".join(failures[:25])
    assert checked == sum(len(c["out"]) for c in golden["cases"])
    assert checked >= 8000


def test_golden_covers_the_rules_it_pins(golden):
    """The sample must actually exercise the branches the split moves around."""
    seen: dict[str, int] = {}
    for case in golden["cases"]:
        out = _dec(json.loads(_unpack_out(case["out"], "detail")))
        for flag, hit in (
            ("vpic_transmission", out.get("transmission_source") == "NHTSA vPIC"),
            ("transmission_feed", out.get("transmission_feed") is not None),
            ("vpic_horsepower", out.get("horsepower_source") == "NHTSA vPIC"),
            ("trim_page_horsepower", out.get("horsepower_source") == "trim page"),
            ("hybrid_hp_note", out.get("horsepower_note") is not None),
            ("msrp_shown", out.get("msrp") is not None),
            ("payment_listed", bool(out.get("payment_listed"))),
            ("price_withheld", out.get("price") is None and _dec(case["car"]).get("price") is not None),
            ("ev_range", out.get("ev_range_miles") is not None),
            ("catalog_packages", out.get("catalog_packages") is not None),
            ("package_names", bool(out.get("package_names"))),
            ("price_history", bool(out.get("price_history"))),
            ("spin_frames", bool(out.get("spin_frames"))),
            ("deal_score", out.get("deal_score") is not None),
        ):
            if hit:
                seen[flag] = seen.get(flag, 0) + 1
    must = {"vpic_transmission", "transmission_feed", "vpic_horsepower", "msrp_shown",
            "ev_range", "package_names", "price_history"}
    missing = sorted(must - set(seen))
    assert not missing, f"golden sample never exercises: {missing} (seen {seen})"


# ── generator (read-only against the local fleet) ──────────────────────────────


def _generate(n: int = 2000) -> None:  # pragma: no cover - run by hand
    if "default_transaction_read_only=on" not in os.environ.get("PGOPTIONS", ""):
        raise SystemExit("set PGOPTIONS='-c default_transaction_read_only=on' (read-only fleet)")
    from backend.db.repositories.base_repo import db_conn
    from backend.db.repositories.cars_repo import get_cars_by_ids

    with db_conn() as conn:
        n_active = conn.execute(
            "SELECT count(*) FROM cars WHERE listing_removed_at IS NULL"
        ).fetchone()[0]
        stride = max(1, n_active // 1700)
        ids = [r[0] for r in conn.execute(
            "SELECT id FROM (SELECT id, row_number() OVER (ORDER BY id) AS rn "
            "FROM cars WHERE listing_removed_at IS NULL) s WHERE mod(rn, ?) = 0 ORDER BY id LIMIT 1700",
            (stride,),
        ).fetchall()]
        # targeted strata so the rare branches are pinned too
        for where in (
            "(packages LIKE '%%packages_normalized\": [{%%' "
            "OR packages LIKE '%%possible_packages\": [\"%%')",
            "lower(coalesce(fuel_type,'')) LIKE '%%electric%%'",
            "lower(coalesce(fuel_type,'')) LIKE '%%hybrid%%'",
            "transmission_type = 'CVT'",
            "msrp IS NOT NULL AND lower(coalesce(condition,'')) = 'new'",
            "price IS NOT NULL AND price < 1500",
            "spin_frames IS NOT NULL AND spin_frames NOT IN ('', '[]')",
            "lower(coalesce(body_style,'')) LIKE '%%pickup%%' AND drivetrain = 'FWD'",
            "packages IS NOT NULL AND packages NOT IN ('', '{}', '[]')",
            "make = 'BMW'",
            "price > 300000",
        ):
            ids += [r[0] for r in conn.execute(
                f"SELECT id FROM cars WHERE listing_removed_at IS NULL AND {where} "
                "ORDER BY md5(id::text) LIMIT 50"
            ).fetchall()]
    seen_ids: set[int] = set()
    ids = [i for i in ids if not (i in seen_ids or seen_ids.add(i))][:n]
    rows: list[dict[str, Any]] = []
    for i in range(0, len(ids), 200):
        rows.extend(get_cars_by_ids(ids[i : i + 200]))
    print(f"fetched {len(rows)} cars", file=sys.stderr)

    from backend.enrichment.knowledge_engine_specs import merge_verified_specs

    vs_by_id = {}
    for r in rows:
        try:
            vs_by_id[r["id"]] = merge_verified_specs(dict(r))
        except Exception:
            vs_by_id[r["id"]] = {}

    calls: dict[str, Any] = {}
    depth = [0]

    def wrapper_for(name: str, orig: Callable) -> Callable:
        def record(*args, **kwargs):
            if depth[0]:  # nested inside another oracle: replay never reaches it
                return orig(*args, **kwargs)
            key = _call_key(name, args, kwargs)
            depth[0] += 1
            try:
                ret = orig(*args, **kwargs)
            except Exception as exc:
                calls[key] = {"raise": repr(exc)}
                raise
            finally:
                depth[0] -= 1
            rec: dict[str, Any] = {"ret": _enc(ret)}
            if name in _MUTATES_ARG:
                rec["arg_after"] = _enc(args[_MUTATES_ARG[name]])
            prev = calls.get(key)
            if prev is not None and prev != rec:
                raise RuntimeError(f"oracle {name} answered one call two ways")
            calls[key] = rec
            return ret

        return record

    _install(wrapper_for, setattr)
    from backend.utils.car_serialize import serialize_car_for_api

    cases = []
    for r in rows:
        vs = vs_by_id[r["id"]]
        out = {}
        for combo, mk in COMBOS.items():
            got = serialize_car_for_api(dict(r), **mk(vs))
            out[combo] = _encode_out(got)
        out = _pack_outs(out)
        cases.append({"id": r["id"], "car": _enc(r), "vs": _enc(vs), "out": out})

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(FIXTURE, "wt", encoding="utf-8", compresslevel=9) as fh:
        json.dump({"generated": _dt.date.today().isoformat(), "cases": cases, "calls": calls}, fh,
                  separators=(",", ":"))
    print(f"wrote {len(cases)} cars, {len(calls)} oracle calls -> {FIXTURE}", file=sys.stderr)


if __name__ == "__main__":  # pragma: no cover
    if "--generate" in sys.argv:
        _generate(int(sys.argv[sys.argv.index("--generate") + 1]) if len(sys.argv) > sys.argv.index("--generate") + 1 else 2000)
