"""The street tier of the rooftop gate is only as strong as the roster street.

``dealerships.street_address`` is backfilled by
``backend/scripts/backfill_dealership_addresses.py``, which records WHERE each
value came from in ``street_address_source`` (migrations/V003): structured
sources (``site_jsonld``, ``site_jsonld_browser``, ``osm_website``) versus
free-text scrape guesses (``site_text``, ``site_text_browser``). V003 promised
a reader — "a value written by a weaker source can be re-checked or withdrawn"
— and the gate is that reader: a weak-provenance street still KEEPS this
store's rows (keeping can only add inventory), but its sibling refusals are
demoted to ``sibling_rooftop_weak_tier``, which
``backend.scanner.rooftop_disown.EVIDENCE_BACKED_REJECTS`` deliberately leaves
out — refuse the write, never un-list the car on a scraped guess.

Fixture style mirrors backend/tests/test_dealer_attribution.py (the unnamed
address-block rooftops of CarsCommerce account 5379783 / bmwofmurrieta.com).
"""
from __future__ import annotations

import inspect

from backend.parsers import parse, resolve_rooftop_attribution
from backend.scanner.rooftop_disown import EVIDENCE_BACKED_REJECTS, roster_place


def _vin(n: int) -> str:
    return f"WA1ANAFY4L20{n:05d}"


_MURRIETA = "41430 Auto Mall Pkwy<br/>Murrieta, CA 92562<br/>(951) 553-2000"
_CHARLOTTE = "1301 N Hendrick Dr<br/>Charlotte, NC 28206<br/>(704) 555-1212"


def _addr_rows(*labels: str) -> list[dict]:
    return [{"vin": _vin(200 + i), "_rooftop": {"key": lab}} for i, lab in enumerate(labels)]


def _resolve(**kwargs):
    return resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Pkwy", dealer_city="Murrieta",
        dealer_state="CA", dealer_zip="92562",
        **kwargs,
    )


# ── strong or unknown provenance: street stays the strong tier ───────────────

def test_strong_source_street_is_decisive_as_before():
    for source in ("site_jsonld", "site_jsonld_browser", "osm_website"):
        kept, rejected = _resolve(dealer_address_source=source)
        assert [r["vin"] for r in kept] == [_vin(200)]
        assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop"}
        assert all(r["_rooftop_reject"] in EVIDENCE_BACKED_REJECTS for r in rejected)


def test_missing_source_keeps_historical_strong_behaviour():
    """NULL/empty source reads as "unknown origin" (V003), not as "weak":
    hand-entered rows that predate provenance must not lose their teeth."""
    kept, rejected = _resolve()
    assert [r["vin"] for r in kept] == [_vin(200)]
    assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop"}


# ── weak provenance: street no longer decisive on its own ────────────────────

def test_weak_source_street_still_keeps_this_stores_rows():
    """Demotion must only make the gate more cautious about ACTING. The kept
    set is identical to the strong-source case — a weak street can never zero
    a dealer's inventory."""
    for source in ("site_text", "site_text_browser"):
        kept, _ = _resolve(dealer_address_source=source)
        assert [r["vin"] for r in kept] == [_vin(200)]


def test_weak_source_street_siblings_are_never_unlistable():
    """The refusal reason under weak provenance is the weak-tier marker, which
    EVIDENCE_BACKED_REJECTS excludes: the write is refused, but delta_scan's
    disown pass will never un-list on it."""
    for source in ("site_text", "site_text_browser"):
        _, rejected = _resolve(dealer_address_source=source)
        assert rejected, "the sibling rooftop must still be refused"
        assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop_weak_tier"}
        assert not [r for r in rejected if r["_rooftop_reject"] in EVIDENCE_BACKED_REJECTS]


def test_weak_source_demotes_the_suffix_street_tier_too():
    """Same street spelled with a different USPS suffix matches through the
    street_key tiers; those are street evidence too and demote the same way."""
    kept, rejected = resolve_rooftop_attribution(
        _addr_rows(_MURRIETA, _CHARLOTTE),
        dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Parkway",  # feed writes "Pkwy"
        dealer_city="Murrieta", dealer_state="CA", dealer_zip="92562",
        dealer_address_source="site_text",
    )
    assert [r["vin"] for r in kept] == [_vin(200)]
    assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop_weak_tier"}


def test_name_tier_matches_are_untouched_by_address_provenance():
    """Provenance only qualifies STREET evidence. A store identified by its
    feed-published name refuses siblings at full strength regardless."""
    rows = [
        {"vin": _vin(300), "_rooftop": {"name": "BMW of Murrieta"}},
        {"vin": _vin(301), "_rooftop": {"name": "Audi Fletcher Jones"}},
    ]
    _, rejected = resolve_rooftop_attribution(
        rows, dealer_id="bmwofmurrieta-com", dealer_name="BMW of Murrieta",
        dealer_url="https://www.bmwofmurrieta.com",
        dealer_address="41430 Auto Mall Pkwy", dealer_address_source="site_text",
    )
    assert {r["_rooftop_reject"] for r in rejected} == {"sibling_rooftop"}


# ── the plumbing: roster_place reads the provenance and the gate accepts it ──

def test_roster_place_returns_the_address_source(monkeypatch):
    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, sql, params):
            assert "street_address_source" in sql
            return self

        def fetchone(self):
            return ("41430 Auto Mall Pkwy", "Murrieta", "CA", "92562", "site_text")

    monkeypatch.setattr("backend.db.inventory_db.db_conn", lambda: _Conn())
    monkeypatch.setattr(
        "backend.listings.dealer_registry_match.resolve_car_dealership_registry_id",
        lambda car: 7,
    )
    place = roster_place("https://www.bmwofmurrieta.com")
    assert place["dealer_address_source"] == "site_text"
    assert place["dealer_address"] == "41430 Auto Mall Pkwy"


def test_roster_place_keys_splat_into_the_gate_and_into_parse():
    """Every caller passes ``**roster_place`` — each key the lookup can emit
    must be a keyword the gate and parse() accept, or scans crash."""
    keys = {"dealer_address", "dealer_city", "dealer_state", "dealer_zip",
            "dealer_address_source"}
    for fn in (resolve_rooftop_attribution, parse):
        params = set(inspect.signature(fn).parameters)
        assert keys <= params, f"{fn.__name__} missing {keys - params}"
