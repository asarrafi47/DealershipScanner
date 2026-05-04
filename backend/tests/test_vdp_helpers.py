"""Unit tests for VDP gallery URL batch merge and price hint selection (no Playwright)."""

import unittest

from backend.utils.vdp_gallery_urls import merge_https_url_batches
from backend.utils.vdp_price_merge import (
    merge_vdp_price_into_vehicle,
    parse_dom_price_text,
    pick_vdp_price_from_hints,
)


class VdpGalleryUrlMergeTest(unittest.TestCase):
    def test_merge_batches_respects_order_and_dedupe(self) -> None:
        ordered: list[str] = []
        seen: set[str] = set()
        self.assertEqual(
            merge_https_url_batches(ordered, seen, ["https://a.example/1.jpg", "https://a.example/1.jpg"]),
            1,
        )
        self.assertEqual(merge_https_url_batches(ordered, seen, ["ftp://b.example/x.jpg"]), 0)
        self.assertEqual(merge_https_url_batches(ordered, seen, ["ftp://b.example/x.jpg"], max_total=5), 0)
        n = merge_https_url_batches(
            ordered,
            seen,
            ["https://b.example/2.jpg", "https://c.example/3.jpg"],
            max_total=10,
        )
        self.assertEqual(n, 2)
        self.assertEqual(ordered[0], "https://a.example/1.jpg")
        self.assertEqual(len(ordered), 3)


class VdpPricePickMergeTest(unittest.TestCase):
    def test_pick_prefers_json_ld_offer_over_dom(self) -> None:
        hints = [
            {"value": 41000, "source": "dom_dealer:.price", "raw": "$41,000"},
            {"value": 39999, "source": "json_ld_offer", "raw": "39999"},
        ]
        val, meta = pick_vdp_price_from_hints(hints)
        self.assertEqual(val, 39999.0)
        self.assertEqual(meta.get("source"), "json_ld_offer")

    def test_merge_only_when_listing_empty(self) -> None:
        v = {"vin": "1M8GDM9AXKP042788", "price": 0}
        d = merge_vdp_price_into_vehicle(v, 42888.0, provenance_source="json_ld_offer", detail_url="https://x.example/vdp")
        self.assertTrue(d.get("updated"))
        self.assertEqual(v.get("price"), 42888)

    def test_merge_skips_when_listing_has_price(self) -> None:
        v = {"vin": "1M8GDM9AXKP042788", "price": 50000}
        d = merge_vdp_price_into_vehicle(v, 42888.0, provenance_source="json_ld_offer", detail_url="")
        self.assertFalse(d.get("updated"))
        self.assertEqual(v.get("price"), 50000)

    def test_parse_dom_price(self) -> None:
        self.assertEqual(parse_dom_price_text("Internet Price: $32,450"), 32450.0)
        self.assertIsNone(parse_dom_price_text("Call for price"))


if __name__ == "__main__":
    unittest.main()
