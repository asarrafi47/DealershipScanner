"""N8.11: DealerEProcess SRPs stamp a per-car ``_rooftop`` from
``filter_vehicle_data[<vehicle_id>].location_id`` and the page's ``site_locations``.

The fixture is the N1.2 DEP map's trimmed parksidekia used SRP (48 cards, 8 at the
site's own lot 15007, the rest at AMSI sister lots; 4 VINs appear twice, once at
the own lot and once as a pool twin). The HTML is rebuilt from those fields in the
live shape: JSON-LD Vehicle blocks plus ``depEventPublisher.setPageData({...})``.
"""
import json
import os

import pytest

from backend.parsers import dealer_eprocess, resolve_rooftop_attribution

_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "dep_rooftops", "parksidekia-com.json")


def _fixture() -> dict:
    with open(_FIXTURE, encoding="utf-8") as fh:
        return json.load(fh)


def _html(fx: dict, *, site_locations: bool = True) -> str:
    ld, fvd = [], {}
    for c in fx["cards"]:
        ld.append(
            '<script type="application/ld+json">'
            + json.dumps({
                "@type": "Vehicle",
                "name": f"{c['type']} {c['make']}",
                "vehicleIdentificationNumber": c["vin"],
                "brand": {"@type": "Brand", "name": c["make"]},
                "model": "X",
                "vehicleModelDate": "2018",
                "url": c["jsonld_url"],
                "offers": [{"@type": "Offer", "price": "9999", "sku": c["stock"]}],
            })
            + "</script>"
        )
        fvd[c["vehicle_id"]] = {
            "vin": c["vin"], "location_id": c["fvd_location_id"], "location_name": c["fvd_location_name"],
            "dealer_uuid": c["fvd_dealer_uuid"],
        }
    page = {"filter_vehicle_data": fvd}
    if site_locations:
        page["site_locations"] = fx["site_locations"]
    return "<html>" + "".join(ld) + "<script>depEventPublisher.setPageData(" + json.dumps(page) + ");</script></html>"


def _parse(html: str, **kw) -> list[dict]:
    args = dict(base_url="https://www.parksidekia.com", dealer_id="parksidekia-com", dealer_name="Parkside Kia", dealer_url="https://www.parksidekia.com")
    args.update(kw)
    return dealer_eprocess.parse(html, **args)


def test_every_card_is_stamped_and_the_own_lot_is_named_by_the_roster() -> None:
    fx = _fixture()
    rows = _parse(_html(fx))
    assert len(rows) == 48 - 4  # same-VIN twins collapse to one row
    assert all(r["_rooftop"]["lot_id"] for r in rows)
    own = [r for r in rows if r["_rooftop"].get("own_site")]
    assert {r["_rooftop"]["lot_id"] for r in own} == {"15007"}
    assert all(r["_rooftop"]["name"] == "Parkside Kia" and r["_rooftop"]["city"] == "Knoxville" for r in own)
    sib = [r for r in rows if not r["_rooftop"].get("own_site")]
    assert sib and all(r["_rooftop"]["lot_id"] != "15007" for r in sib)


def test_pool_twin_never_displaces_the_own_lot_record() -> None:
    fx = _fixture()
    own_vins = {c["vin"] for c in fx["cards"] if c["fvd_location_id"] == "15007"}
    twins = {c["vin"] for c in fx["cards"] if sum(1 for o in fx["cards"] if o["vin"] == c["vin"]) > 1}
    assert twins & own_vins
    rows = {r["vin"]: r for r in _parse(_html(fx))}
    for vin in twins & own_vins:
        assert rows[vin]["_rooftop"]["lot_id"] == "15007"
    # and in the other card order
    fx["cards"].reverse()
    rows = {r["vin"]: r for r in _parse(_html(fx))}
    for vin in twins & own_vins:
        assert rows[vin]["_rooftop"]["lot_id"] == "15007"


@pytest.mark.parametrize("mode", ["legacy", "scorer"])
def test_gate_keeps_only_the_dealers_own_cars(mode, monkeypatch) -> None:
    import backend.parsers as parsers_mod

    monkeypatch.setenv("SCANNER_ROOFTOP_SCORER", "0" if mode == "legacy" else "1")
    monkeypatch.setattr(parsers_mod, "roster_name_aliases", lambda d: ())
    monkeypatch.setattr("backend.scanner.rooftop_ledger.note_refusal", lambda *a, **k: None, raising=False)
    monkeypatch.setattr("backend.scanner.rooftop_ledger.flush", lambda *a, **k: None, raising=False)
    rows = _parse(_html(_fixture()))
    kept, refused = resolve_rooftop_attribution(
        rows, dealer_id="parksidekia-com", dealer_name="Parkside Kia", dealer_url="https://www.parksidekia.com",
        dealer_city="Knoxville", dealer_state="TN",
    )
    assert len(kept) == 8 and {r["_rooftop"]["lot_id"] for r in kept} == {"15007"}
    assert len(refused) == len(rows) - 8


def test_without_site_locations_or_hint_rows_stay_unstamped_and_log(monkeypatch, caplog) -> None:
    monkeypatch.setattr("backend.attribution.place.own_location_id", lambda d: "")
    dealer_eprocess._warned_unknown_location.discard("nohint-com")
    fx = _fixture()
    for c in fx["cards"]:
        c["fvd_location_name"] = ""  # no own-site card either
    with caplog.at_level("INFO", logger="backend.parsers.dealer_eprocess"):
        rows = _parse(_html(fx, site_locations=False), dealer_id="nohint-com")
    assert rows and all("_rooftop" not in r for r in rows)
    assert "rooftop not stamped" in caplog.text


def test_learned_location_hint_names_the_own_lot_without_site_locations(monkeypatch) -> None:
    monkeypatch.setattr("backend.attribution.place.own_location_id", lambda d: "15007")
    rows = _parse(_html(_fixture(), site_locations=False))
    own = [r for r in rows if r["_rooftop"].get("own_site")]
    assert own and {r["_rooftop"]["lot_id"] for r in own} == {"15007"}


def test_own_location_name_on_cards_identifies_the_site_without_hint(monkeypatch) -> None:
    monkeypatch.setattr("backend.attribution.place.own_location_id", lambda d: "")
    rows = _parse(_html(_fixture(), site_locations=False))
    assert {r["_rooftop"]["lot_id"] for r in rows if r["_rooftop"].get("own_site")} == {"15007"}


def test_pages_without_the_inline_map_parse_as_before() -> None:
    html = _html(_fixture()).replace("filter_vehicle_data", "other_data")
    rows = _parse(html)
    assert len(rows) == 44 and all("_rooftop" not in r for r in rows)


def test_own_location_id_hint_roundtrip(monkeypatch) -> None:
    from backend.attribution import place

    store: dict = {}
    monkeypatch.setattr("backend.scanner.recipe_store.get_scan_hints", lambda d: dict(store.get(d, {})))

    def _set(d, hints, merge=False):
        store.setdefault(d, {}).update(hints)
        return True

    monkeypatch.setattr("backend.scanner.recipe_store.set_scan_hints", _set)
    assert place.own_location_id("a-com") == ""
    assert place.learn_own_location_id("a-com", 16182) is True
    assert place.own_location_id("a-com") == "16182"
    assert place.learn_own_location_id("", "1") is False
