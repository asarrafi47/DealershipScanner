"""Unnamed stamps and unstamped rows under the matched store's own feed account are
that store (Honda of Huntersville, 2026-09-26: 141 of 389 kept by the name tier)."""
from __future__ import annotations

from backend.parsers import parse


def _listing(vin: str, typ: str, location: str | None, api_id: str, src: str = "") -> dict:
    return {"vin": vin, "type": typ, "year": 2024, "make": "Honda", "model": "Civic", "trim": "EX", "stock": vin[-6:],
            "source_id": src or api_id, "pricing": {"price": 25000},
            "dealer": {"id": "7070", "ccid": 6067306, "name": None, "api_id": api_id, "location": location, "city": None, "state": None},
            "media": {"images": []}, "extra_fields": {}}


def _payload(*rows):
    return {"data": {"listings": list(rows)}, "meta": {"pagination": {"total": len(rows)}}}


def _run(payload, **place):
    rej: list[dict] = []
    kept = parse("carscommerce", payload, base_url="https://www.hondahuntersville.com", dealer_id="hondahuntersville-com",
                 dealer_name="Honda Of Huntersville", dealer_url="https://www.hondahuntersville.com", rejected_out=rej, **place)
    return kept, rej


def test_street_block_and_unstamped_rows_under_the_store_source_are_kept():
    payload = _payload(
        _listing("1HGCV1F30PA000001", "New", "Honda of Huntersville", "MP23253"),
        _listing("1HGCV1F30PA000002", "New", "12815 Statesville Rd<br/>Huntersville, NC 28078<br/>(704) 875-3232", "MP23253"),
        _listing("1HGCV1F30PA000003", "Used", None, "MP23253"),
        _listing("1HGCV1F30PA000004", "New", "Hoover Toyota", "MP99999"),   # a real sibling, its own account
        _listing("1HGCV1F30PA000005", "Used", None, "MP99999"),             # unstamped under the sibling's account
    )
    # the street block is recognised as ours only when the store's street is known (registry / page JSON-LD)
    kept, rej = _run(payload, dealer_city="Huntersville", dealer_state="NC", dealer_address="12815 Statesville Rd")
    assert sorted(r["vin"][-1] for r in kept) == ["1", "2", "3"]
    assert {r["vin"][-1]: r["_rooftop_reject"] for r in rej} == {"4": "sibling_rooftop", "5": "unstamped_row_in_group_feed"}


def test_unnamed_stamp_under_a_foreign_source_still_refuses():
    payload = _payload(
        _listing("1HGCV1F30PA000001", "New", "Honda of Huntersville", "MP23253"),
        _listing("1HGCV1F30PA000002", "New", "1 Other St<br/>Charlotte, NC 28202<br/>(704) 000-0000", "MP55555"),
        _listing("1HGCV1F30PA000003", "New", "Hoover Toyota", "MP99999"),
    )
    kept, rej = _run(payload)
    assert [r["vin"][-1] for r in kept] == ["1"] and len(rej) == 2


def test_street_block_at_the_stores_own_street_is_kept_even_under_another_source():
    """Honda of Huntersville: 105 new cars under feed 209014 stamped with the store's
    street block; the page's JSON-LD gives the street (2026-09-26)."""
    payload = _payload(
        _listing("1HGCV1F30PA000001", "New", "Honda of Huntersville", "MP23253"),
        _listing("1HGCV1F30PA000002", "New", "12815 Statesville Rd<br/>Huntersville, NC 28078<br/>(704) 875-3232", "209014"),
        _listing("1HGCV1F30PA000003", "New", "99 Other Ave<br/>Huntersville, NC 28078<br/>(704) 111-1111", "209015"),
    )
    rej: list[dict] = []
    kept = parse("carscommerce", payload, base_url="https://www.hondahuntersville.com", dealer_id="hondahuntersville-com",
                 dealer_name="Honda Of Huntersville", dealer_url="https://www.hondahuntersville.com", rejected_out=rej,
                 dealer_city="Huntersville", dealer_state="NC", dealer_address="12815 Statesville Rd")
    assert sorted(r["vin"][-1] for r in kept) == ["1", "2"]
    assert [r["vin"][-1] for r in rej] == ["3"]


def test_group_wide_source_behind_other_rooftops_never_vouches():
    """Hendrick: "RHendrickUsed" backs nine rooftops; unstamped rows under it stay refused
    even when this store is matched (hendrickhonda-com kept 73 foreign rows, 2026-09-26)."""
    payload = _payload(
        _listing("1HGCV1F30PA000001", "New", "Honda of Huntersville", "MP23253"),
        _listing("1HGCV1F30PA000002", "Used", "Hendrick Kia of Cary", "RHendrickUsed"),
        _listing("1HGCV1F30PA000003", "Used", "Honda of Huntersville", "RHendrickUsed"),
        _listing("1HGCV1F30PA000004", "Used", None, "RHendrickUsed"),
    )
    kept, rej = _run(payload)
    assert sorted(r["vin"][-1] for r in kept) == ["1", "3"]
    assert {r["vin"][-1]: r["_rooftop_reject"] for r in rej} == {"2": "sibling_rooftop", "4": "unstamped_row_in_group_feed"}


def test_without_the_stores_street_an_unknown_street_block_makes_the_source_shared():
    payload = _payload(
        _listing("1HGCV1F30PA000001", "New", "Honda of Huntersville", "MP23253"),
        _listing("1HGCV1F30PA000002", "New", "12815 Statesville Rd<br/>Huntersville, NC 28078<br/>(704) 875-3232", "MP23253"),
        _listing("1HGCV1F30PA000003", "Used", None, "MP23253"),
    )
    kept, rej = _run(payload)
    assert [r["vin"][-1] for r in kept] == ["1"] and len(rej) == 2
