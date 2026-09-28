"""F15 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): the Chapman apiv2 feed
carries msrp (top-level and pricing.msrp) but chapman._map used it only to
compute price and never emitted it. Item shape from 09_platform_http_probes.txt."""
from __future__ import annotations

from backend.parsers.chapman import parse


def _doc(**over) -> dict:
    d = {"vin": "1C6SRFFT5SN123456", "year": 2025, "make": "Ram", "model": "1500", "trim": "Big Horn", "type": "New",
         "isCertified": False, "colorExt": "Bright White", "colorInt": "Black", "body": "Truck", "drive": "4WD",
         "fuel": "Gasoline", "engine": "5.7L V8", "transmission": "8-Speed Automatic", "stockNumber": "R123456",
         "mileage": 8, "mpgCity": 17, "mpgHwy": 22, "imageUrls": ["https://photos.chapmanchoice.com/1.jpg"],
         "msrp": 63230, "pricing": {"msrp": 63230, "markupsTotal": 0, "discountsTotal": 5000, "rebatesAppliedTotal": 1500},
         "isInTransit": False, "isMsrpRequired": False, "arkona": "CD1"}
    d.update(over)
    return d


def test_msrp_emitted_alongside_computed_price():
    rows = parse([_doc()], base_url="https://www.chapmandodge.com", dealer_id="chapmandodge-com")
    assert len(rows) == 1
    assert rows[0]["msrp"] == 63230
    assert rows[0]["price"] == 63230 - 5000 - 1500


def test_in_transit_zero_msrp_stays_null_and_pricing_fallback():
    rows = parse([_doc(msrp=0, pricing={"msrp": 0, "markupsTotal": 0, "discountsTotal": 0, "rebatesAppliedTotal": 0})],
                 base_url="https://www.chapmandodge.com", dealer_id="chapmandodge-com")
    assert rows[0].get("msrp") is None and not rows[0].get("price")
    rows = parse([_doc(msrp=None, pricing={"msrp": 41500, "markupsTotal": 0, "discountsTotal": 0, "rebatesAppliedTotal": 0})],
                 base_url="https://www.chapmandodge.com", dealer_id="chapmandodge-com")
    assert rows[0]["msrp"] == 41500


def test_detail_url_only_from_the_feed_never_guessed():
    rows = parse([_doc()], base_url="https://www.chapmandodge.com", dealer_id="chapmandodge-com")
    assert "source_url" not in rows[0] or rows[0]["source_url"] is None
    rows = parse([_doc(vdpUrl="/vehicle/1C6SRFFT5SN123456")], base_url="https://www.chapmandodge.com", dealer_id="chapmandodge-com")
    assert rows[0]["source_url"] == "https://www.chapmandodge.com/vehicle/1C6SRFFT5SN123456"
    assert rows[0]["_detail_url"] == rows[0]["source_url"]
