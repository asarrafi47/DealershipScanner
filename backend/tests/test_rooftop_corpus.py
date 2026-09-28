"""Rooftop attribution regression corpus.

One fixture per real case in ``backend/tests/fixtures/rooftop_corpus/<dealer_id>.json``,
cut from ``workspace/dealer_logs/<dealer_id>/`` and ``_learning/platform_playbook.md``
(2026-09-24 .. 2026-09-28). Each fixture carries the roster entry, the page evidence
(oem_code, JSON-LD street), the feed identity (identity source ids, Location facet) and
scenarios of 3-8 rows with the exact stamp shapes seen — ``dealer.location`` free text
(store name, ``street<br/>City, ST ZIP<br/>phone`` block, bare ``City, ST`` tag, lot tag
like ``TOW/JORGE R/367676``, single-word code), ``extra_fields.custom_location``,
``custom_text_N`` facets, ``source_id``, a bare street line, no stamp at all — each with the
decision the gate makes TODAY (``expected_today`` + ``tier_today`` / ``reject_today``).

Rows whose ideal decision differs carry ``expected_ideal`` + ``ideal_note``: a known open
bug or a decision only the synthesis-time store filter can take. Those scenarios run under
``test_rooftop_corpus_ideal`` as strict xfails, so the day the gate learns the rule the
xpass flags the fixture for promotion.

The corpus is the proof that a change to the gate preserves every existing case: it must
pass unchanged code, and it must pass with the scorer on and off once the scorer lands.
"""
from __future__ import annotations

import glob
import json
import os
import re

import pytest

import backend.parsers as parsers_mod
from backend.parsers import parse, resolve_rooftop_attribution

CORPUS_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "rooftop_corpus")

# Tiers _pick_target can answer with; the log line "matched this store by <tier>" is the
# only place the current gate reports the winning tier for kept rows.
PICK_TIERS = frozenset({
    "site_host", "name_exact", "name_tokens", "name_token_subset", "name_brand_alias",
    "name_brand_alias_subset", "name_contains", "roster_name_alias", "store_label_exact",
    "store_label_tokens", "store_label_token_subset", "store_label_alias", "dealer_id_host",
    "rooftop_slug_host", "rooftop_slug_name_tokens", "street_address", "street_address_partial",
    "street_address_suffix", "street_address_suffix_partial", "zip_code", "city_state",
})
# Labels for rows the gate keeps without picking a target, or keeps beside the target.
KEEP_LABELS = frozenset({
    "single_rooftop", "no_rooftop_evidence", "feed_scoped", "same_source_feed",
    "own_street_block", "unstamped_under_own_source", "identity_feed", "page_oem_code",
    "location_facet_value",
})
REJECT_REASONS = frozenset({
    "sibling_rooftop", "sibling_rooftop_weak_tier", "single_rooftop_is_not_this_store",
    "unstamped_row_in_group_feed", "target_rooftop_unidentified",
})


def _load_corpus() -> list[dict]:
    out = []
    for path in sorted(glob.glob(os.path.join(CORPUS_DIR, "*.json"))):
        with open(path, encoding="utf-8") as fh:
            fx = json.load(fh)
        assert fx["dealer_id"] == os.path.basename(path)[:-5], path
        out.append(fx)
    return out


CORPUS = _load_corpus()
SCENARIOS = [(fx, sc) for fx in CORPUS for sc in fx["scenarios"]]
IDS = [f"{fx['dealer_id']}::{sc['name']}" for fx, sc in SCENARIOS]
# The gate runs through backend/scanner/rooftop_match.py when SCANNER_ROOFTOP_SCORER is on
# (the default) and through the legacy ladder when it is 0. Both must decide every case alike.
MODES = ("legacy", "scorer")


def _ideal_params():
    params = []
    for mode in MODES:
        for fx, sc in SCENARIOS:
            if any("expected_ideal" in r for r in sc["rows"]):
                notes = sorted({r.get("ideal_note", "") for r in sc["rows"] if "expected_ideal" in r})
                params.append(pytest.param(mode, fx, sc, id=f"{mode}-{fx['dealer_id']}::{sc['name']}",
                                           marks=pytest.mark.xfail(strict=True, reason="; ".join(n for n in notes if n)[:200])))
    return params


# ── payload builders: the platform shapes the parsers read ─────────────────────

def _cc_listing(vin: str, typ: str, stamp: dict, account: str) -> dict:
    extra = dict(stamp.get("facets") or {})
    if stamp.get("custom_location"):
        extra["custom_location"] = stamp["custom_location"]
    ccid = int(re.sub(r"\D", "", account) or 0)
    return {
        "vin": vin, "type": typ, "year": 2025, "make": "Test", "model": "Model", "trim": "Base",
        "stock": vin[-6:], "mileage": 10, "source_id": stamp.get("source_id") or "",
        "pricing": {"price": 30000, "low_price": 30000},
        "dealer": {"id": "1", "ccid": ccid, "name": None, "website": None, "api_id": stamp.get("api_id"),
                   "location": stamp.get("location"), "city": None, "state": None, "address": None, "zipcode": None},
        "media": {"images": []}, "extra_fields": extra,
    }


def _ts_doc(vin: str, typ: str, stamp: dict, dealer_url: str) -> dict:
    return {"document": {
        "vin": vin, "yr": 2024, "make": "Jeep", "model": "Wrangler", "sellingPrice": 41995, "mileage": 5,
        "type": typ, "imageUrls": ["https://img.example/1.jpg"], "dealerName": stamp.get("dealerName"),
        "dealer": {"name": stamp.get("dealerName"), "address": stamp.get("address") or "", "city": stamp.get("city") or "",
                   "state": stamp.get("state") or "", "zip": stamp.get("zip") or "", "url": dealer_url},
    }}


def _plain_row(vin: str, typ: str, stamp: dict) -> dict:
    # Dealer eProcess JSON-LD cards carry no seller: the parsed row has no ``_rooftop``.
    return {"vin": vin, "condition": typ, "make": stamp.get("make") or "Test", "model": "Model", "year": 2024}


def _place_kwargs(fx: dict, sc: dict) -> dict:
    src = sc.get("place") if sc.get("place") is not None else fx["roster"]
    return {
        "dealer_address": src.get("street") or "",
        "dealer_city": src.get("city") or "",
        "dealer_state": src.get("state") or "",
        "dealer_zip": src.get("zip") or "",
        "dealer_address_source": src.get("street_source") or "",
    }


def run_scenario(fx: dict, sc: dict, monkeypatch, mode: str = "scorer") -> tuple[dict[str, dict], list[dict], list[dict]]:
    """Run the gate over one scenario. Returns ``(by_vin, kept, rejected)``."""
    monkeypatch.setenv("SCANNER_ROOFTOP_SCORER", "0" if mode == "legacy" else "1")
    dealer_id = fx["dealer_id"]
    dealer_url = "https://www." + dealer_id.replace("-com", ".com").replace("-net", ".net")
    aliases = tuple(sc.get("aliases") if "aliases" in sc else fx["roster"].get("aliases") or ())
    monkeypatch.setattr(parsers_mod, "roster_name_aliases", lambda d: aliases)
    # never touch the refusal ledger / DB from the corpus
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)
    kw = dict(base_url=dealer_url, dealer_id=dealer_id, dealer_name=fx["roster"]["name"], dealer_url=dealer_url, **_place_kwargs(fx, sc))
    rejected: list[dict] = []
    platform = fx["platform"]
    if platform == "carscommerce":
        payload = {"data": {"listings": [_cc_listing(r["vin"], r["type"], r["stamp"], fx["feed"]["account"]) for r in sc["rows"]]},
                   "meta": {"pagination": {"total": len(sc["rows"])}}}
        if sc["gate"] == "feed_scoped":
            payload["_feed_scoped"] = True  # the marker recipes.py sets on a store-scoped replay
        kept = parse("carscommerce", payload, rejected_out=rejected, **kw)
    elif platform == "typesense":
        payload = {"results": [{"found": len(sc["rows"]), "hits": [_ts_doc(r["vin"], r["type"], r["stamp"], dealer_url) for r in sc["rows"]]}]}
        kept = parse("typesense", payload, rejected_out=rejected, trust_feed_scope=(sc["gate"] == "feed_scoped"), **kw)
    else:
        rows = [_plain_row(r["vin"], r["type"], r["stamp"]) for r in sc["rows"]]
        kept, rejected = resolve_rooftop_attribution(rows, dealer_id=dealer_id, dealer_name=fx["roster"]["name"], dealer_url=dealer_url, **_place_kwargs(fx, sc))
    by_vin = {r["vin"]: r for r in list(kept) + list(rejected)}
    assert len(by_vin) == len(sc["rows"]), "a parser dropped a row"
    return by_vin, kept, rejected


def _decision(row: dict) -> str:
    return "refuse" if row.get("_rooftop_reject") else "keep"


# ── the corpus ────────────────────────────────────────────────────────────────

def test_corpus_fixtures_are_well_formed():
    assert len(CORPUS) >= 15
    for fx in CORPUS:
        assert fx["roster"]["name"] and fx["roster"]["city"] and fx["roster"]["state"]
        assert fx["evidence_files"] and fx["notes"]
        for sc in fx["scenarios"]:
            assert sc["gate"] in ("open", "feed_scoped")
            assert 3 <= len(sc["rows"]) <= 10, (fx["dealer_id"], sc["name"])
            vins = [r["vin"] for r in sc["rows"]]
            assert len(set(vins)) == len(vins) and all(len(v) == 17 for v in vins)
            for r in sc["rows"]:
                assert r["expected_today"] in ("keep", "refuse")
                if r["expected_today"] == "keep":
                    assert r["tier_today"] in PICK_TIERS | KEEP_LABELS, (fx["dealer_id"], r["vin"], r["tier_today"])
                else:
                    assert r["tier_today"] in REJECT_REASONS and r.get("reject_today") == r["tier_today"], (fx["dealer_id"], r["vin"])
                if "expected_ideal" in r:
                    assert r["expected_ideal"] != r["expected_today"] and r.get("ideal_note")


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("fx,sc", SCENARIOS, ids=IDS)
def test_rooftop_corpus_today(mode, fx, sc, monkeypatch, caplog):
    caplog.set_level("INFO", logger="backend.parsers")
    by_vin, kept, rejected = run_scenario(fx, sc, monkeypatch, mode)
    for r in sc["rows"]:
        got = by_vin[r["vin"]]
        assert _decision(got) == r["expected_today"], (r["vin"], r["tier_today"], got.get("_rooftop_reject"), r.get("note"))
        if r["expected_today"] == "refuse":
            assert got["_rooftop_reject"] == r["reject_today"], (r["vin"], got["_rooftop_reject"])
        elif sc["gate"] == "feed_scoped":
            assert got.get("_feed_scoped") is True
        if mode == "scorer" and sc["gate"] == "open":
            # the scorer names the winning signal on every row, kept or refused
            assert got.get("_rooftop_tier") == r["tier_today"], (r["vin"], got.get("_rooftop_tier"), r["tier_today"])
            assert isinstance(got.get("_rooftop_score"), float)
    # The current gate names its winning tier only in the log, and only when it refused
    # something on the same page: check it where it is available.
    logged = [m.group(1) for rec in caplog.records for m in [re.search(r"matched this store by (\w+)", rec.getMessage())] if m]
    pick_rows = [r for r in sc["rows"] if r["expected_today"] == "keep" and r["tier_today"] in PICK_TIERS]
    if logged and pick_rows:
        assert set(logged) == {pick_rows[0]["tier_today"]}, (logged, pick_rows[0]["tier_today"])


@pytest.mark.parametrize("mode,fx,sc", _ideal_params())
def test_rooftop_corpus_ideal(mode, fx, sc, monkeypatch):
    """Strict xfail: these scenarios document decisions the gate cannot take yet."""
    by_vin, _, _ = run_scenario(fx, sc, monkeypatch, mode)
    for r in sc["rows"]:
        want = r.get("expected_ideal", r["expected_today"])
        assert _decision(by_vin[r["vin"]]) == want, (r["vin"], r.get("ideal_note"))
