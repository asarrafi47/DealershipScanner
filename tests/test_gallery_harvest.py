"""Gallery URL harvest, dedupe, and merge policy (no network, no .env)."""

import unittest

from backend.parsers.base import (
    dedupe_urls_order_prefer_large,
    extract_gallery_urls,
    harvest_image_urls_from_json,
    normalize_image_url_https,
    strip_obvious_resize_query_params,
)
from backend.utils.gallery_merge import merge_inventory_row_galleries, merge_vdp_gallery_into_vehicle


class GalleryHarvestTest(unittest.TestCase):
    def test_normalize_http_to_https(self) -> None:
        self.assertEqual(
            normalize_image_url_https("http://cdn.example/a.jpg"),
            "https://cdn.example/a.jpg",
        )

    def test_strip_resize_query_params(self) -> None:
        u = "https://cdn.example/p/x.jpg?w=120&h=90&foo=bar"
        out = strip_obvious_resize_query_params(u)
        self.assertIn("foo=bar", out)
        self.assertNotIn("w=120", out)

    def test_dedupe_prefer_larger_over_thumb(self) -> None:
        thumb = "https://cdn.example/i/1.jpg?w=120&h=90"
        large = "https://cdn.example/i/1.jpg?w=1920&h=1440"
        out = dedupe_urls_order_prefer_large([thumb, large], max_len=10)
        self.assertEqual(len(out), 1)
        self.assertIn("1920", out[0])

    def test_harvest_nested_vehicle_media(self) -> None:
        obj = {
            "vin": "1M8GDM9AXKP042788",
            "vehicleMedia": {
                "photos": [
                    {"url": "https://cdn.example/a.jpg"},
                    {"uri": "https://cdn.example/b.jpg"},
                ]
            },
            "nested": {"spinImages": ["https://cdn.example/c.jpg"]},
        }
        urls = harvest_image_urls_from_json(obj, "https://dealer.example.com/", max_urls=20)
        self.assertGreaterEqual(len(urls), 3)
        self.assertTrue(all(u.startswith("https://") for u in urls))

    def test_extract_gallery_urls_merges_harvest(self) -> None:
        obj = {
            "images": [{"uri": "https://cdn.example/hero.jpg"}],
            "options": {"mediaGallery": ["https://cdn.example/extra.jpg"]},
        }
        urls = extract_gallery_urls(obj, "https://dealer.example.com/", max_images=20)
        self.assertIn("https://cdn.example/hero.jpg", urls)
        self.assertIn("https://cdn.example/extra.jpg", urls)

    def test_merge_vdp_replace_when_few_existing(self) -> None:
        v = {"vin": "1M8GDM9AXKP042788", "gallery": ["https://a.example/1.jpg"], "image_url": "https://a.example/1.jpg"}
        diag = merge_vdp_gallery_into_vehicle(
            v,
            [f"https://b.example/{i}.jpg" for i in range(6)],
            max_gallery=20,
        )
        self.assertEqual(diag.get("action"), "replace")
        self.assertGreaterEqual(len(v["gallery"]), 3)

    def test_merge_vdp_extend_when_enough_existing(self) -> None:
        existing = [f"https://keep.example/{i}.jpg" for i in range(4)]
        v = {"vin": "1M8GDM9AXKP042788", "gallery": list(existing), "image_url": existing[0]}
        diag = merge_vdp_gallery_into_vehicle(
            v,
            ["https://new.example/x.jpg"],
            max_gallery=10,
        )
        self.assertEqual(diag.get("action"), "extend")
        self.assertIn("https://new.example/x.jpg", v["gallery"])

    def test_merge_inventory_duplicate_rows(self) -> None:
        dst = {
            "vin": "1M8GDM9AXKP042788",
            "gallery": ["https://x.example/a.jpg"],
            "image_url": "https://x.example/a.jpg",
        }
        src = {
            "vin": "1M8GDM9AXKP042788",
            "gallery": ["https://x.example/a.jpg", "https://x.example/b.jpg"],
            "image_url": "https://x.example/a.jpg",
        }
        merge_inventory_row_galleries(dst, src)
        self.assertIn("https://x.example/b.jpg", dst["gallery"])


if __name__ == "__main__":
    unittest.main()
