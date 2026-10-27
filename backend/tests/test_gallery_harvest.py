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

    def test_merge_vdp_uncapped_when_max_gallery_none(self) -> None:
        v = {"vin": "1M8GDM9AXKP042788", "gallery": [f"https://keep.example/{i}.jpg" for i in range(4)]}
        candidates = [f"https://new.example/{i}.jpg" for i in range(60)]
        diag = merge_vdp_gallery_into_vehicle(v, candidates, max_gallery=None)
        self.assertEqual(diag.get("action"), "extend")
        self.assertGreaterEqual(len(v["gallery"]), 60)

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


# ---- N21.6: DEP extension-less photos, feed photoUrls, malformed CSS URLs ----

from backend.parsers import generic_json, sister_tv, team_velocity  # noqa: E402
from backend.parsers.base import (  # noqa: E402
    clean_gallery_url,
    dedupe_urls_order_prefer_large,
    gallery_urls_from_value,
    is_malformed_image_url,
)
from backend.scanner.utils.gallery_url_filter import is_junk_vdp_gallery_url  # noqa: E402
from backend.scanner.vdp.html_recovery import harvest_gallery_urls_from_html  # noqa: E402

_DEP = "https://cloudflareimages.dealereprocess.com/resrc/images/w_1024/dvp/site1/"


def _dep_urls(n: int) -> list[str]:
    return [f"{_DEP}{i:06d}-ID{i}-front.v1" for i in range(n)]


def test_dep_extensionless_gallery_yields_about_twenty_photos():
    photos = _dep_urls(20)
    html = (
        "<html><body><div class='gallery'>"
        + "".join(f"<img data-src='{u}'>" for u in photos)
        + "<img src='https://cloudflareimages.dealereprocess.com/resrc/images/site-logo'>"
        + "</div></body></html><!-- " + "x" * 600 + " -->"
    )
    got = harvest_gallery_urls_from_html(html, "https://www.example-dep.com/vdp/1")
    assert len(got) == 20
    assert all(u.startswith(_DEP) for u in got)


def test_dep_photo_is_media_url_and_dedupes_across_resize_prefixes():
    a = "https://cloudflareimages.dealereprocess.com/resrc/images/w_300/dvp/site1/000001-ID1-front"
    b = "https://cloudflareimages.dealereprocess.com/resrc/images/w_1600/dvp/site1/000001-ID1-front"
    assert len(dedupe_urls_order_prefer_large([a, b])) == 1


def test_malformed_css_urls_are_cleaned_or_rejected():
    assert clean_gallery_url('url("https://cdn.x.com/a.jpg")') == "https://cdn.x.com/a.jpg"
    assert clean_gallery_url("url(&quot;https://cdn.x.com/a.jpg&quot;);") == "https://cdn.x.com/a.jpg"
    assert clean_gallery_url("https://cdn.x.com/a.jpg);background:red") == ""
    assert clean_gallery_url("https://cdn.x.com/a b.jpg") == ""
    assert is_malformed_image_url("https://cdn.x.com/a.jpg)")
    assert not is_malformed_image_url("https://cdn.x.com/a.jpg?w=1&h=2")
    assert is_junk_vdp_gallery_url("https://cdn.x.com/cars/a.jpg)")
    assert not is_junk_vdp_gallery_url("https://cdn.x.com/cars/a.jpg")


def test_gallery_urls_from_value_shapes():
    joined = "https://a.com/1.jpg,https://a.com/2.jpg, https://a.com/3.jpg"
    assert gallery_urls_from_value(joined) == ["https://a.com/1.jpg", "https://a.com/2.jpg", "https://a.com/3.jpg"]
    assert gallery_urls_from_value([{"url": "//a.com/1.jpg"}, "http://a.com/1.jpg", "url(https://a.com/9.jpg)"]) == [
        "https://a.com/1.jpg",
        "https://a.com/9.jpg",
    ]
    assert gallery_urls_from_value(None) == []


_VIN = "1FTFW1E50NFA00001"


def test_generic_json_splits_joined_photo_urls_and_drops_malformed():
    item = {
        "vin": _VIN, "year": 2022, "make": "Ford", "model": "F-150", "price": 40000,
        "photoUrls": "https://a.com/1.jpg,https://a.com/2.jpg,https://a.com/3.jpg);x",
        "image": "url('https://a.com/1.jpg')",
    }
    rows = generic_json.parse([item], dealer_id="d")
    assert rows[0]["gallery"] == ["https://a.com/1.jpg", "https://a.com/2.jpg"]
    assert rows[0]["image_url"] == "https://a.com/1.jpg"


def test_team_velocity_feed_photourls_become_the_gallery():
    v = {"vin": _VIN, "year": 2022, "make": "Ford", "model": "F-150", "sellingPrice": 40000,
         "imageUrls": None, "photoUrls": "https://a.com/1.jpg,https://a.com/2.jpg"}
    row = team_velocity._map_vehicle(v, "https://d.com", "d", "D", "https://d.com")
    assert row["gallery"] == ["https://a.com/1.jpg", "https://a.com/2.jpg"]
    assert row["image_url"] == "https://a.com/1.jpg"
    v.pop("photoUrls")
    assert team_velocity._map_vehicle(v, "https://d.com", "d", "D", "https://d.com")["gallery"] == [team_velocity.FALLBACK_IMAGE_URL]


def test_team_velocity_vdp_attr_strips_css_debris():
    html = "<x :photoUrls=\"'https://a.com/1.jpg,https://a.com/2.jpg);,https://a.com/3.jpg'\">"
    assert team_velocity.extract_gallery(html) == ["https://a.com/1.jpg", "https://a.com/2.jpg", "https://a.com/3.jpg"]


def test_sister_tv_gallery_uses_shared_cleaner():
    src = {"display_pics": ["//a.com/1.jpg", "https://a.com/2.jpg\")"], "detail_pics": ["https://a.com/1.jpg"]}
    assert sister_tv._gallery(src) == ["https://a.com/1.jpg", "https://a.com/2.jpg"]
