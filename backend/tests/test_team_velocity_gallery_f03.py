"""F03 (docs/DATA_COMPLETENESS_FINDINGS_2026_09_28.md): Team Velocity galleries are on
the page (:photoUrls, 11-44 photos) but the generic harvester dropped every
cai-media photo ("common-vehicle-media" was a fluff signal), kept the OfferLogix
widget logo, never removed the seeded placeholder, and captured comma-joined
lists as one URL. Fixture URLs are the ones saved under h5/."""
from __future__ import annotations

import json

from backend.parsers import base
from backend.parsers.base import _is_placeholder_url, harvest_image_urls_from_json, split_joined_image_urls
from backend.parsers.team_velocity import _gallery_fillable
from backend.scanner.vdp.html_recovery import harvest_gallery_urls_from_html
from backend.scanner.vdp.prefetch import _apply_description_and_gallery

CAI = "https://assets.cai-media-management.com/resize/1024x1024/common-vehicle-media/"
PHOTOS = [CAI + f"{i:08x}-864e-4a57-9f7a-b6a691ec3267.jpg" for i in range(12)]
LOGO = "https://widget.buyercall.com/offerlogix/img/CD-full-dark-transp.png"
TOYOTA = [
    f"https://delivery.via.assetscs.toyota.com/adobe/assets/urn:aaid:aem:{i:08x}-1dd1-4457-905a-813dd9aac07f/as/image.png"
    "?fmt=png-alpha%2Crgb%2Cnone" for i in range(9)
]
PAD = "<!-- " + "x" * 600 + " -->"


def _tv_page(urls: list[str]) -> str:
    return (
        "<html><body><oem-gallery-component :photoUrls=\"'" + ",".join(urls) + "'\"></oem-gallery-component>"
        f"<img src='{LOGO}'><img src='https://www.example.com/static/logo.png'></body></html>" + PAD
    )


def test_cai_media_photos_are_not_fluff():
    assert "common-vehicle-media" not in base._FLUFF_URL_SIGNALS
    assert not _is_placeholder_url(PHOTOS[0])
    assert _is_placeholder_url(LOGO)


def test_harvester_returns_full_cai_gallery_without_widget_logo():
    got = harvest_gallery_urls_from_html(_tv_page(PHOTOS), "https://www.dthondachicago.com/viewdetails/x")
    assert len(got) == 12
    assert all(u.startswith(CAI) for u in got)
    assert LOGO not in got


def test_comma_joined_query_urls_split_into_separate_photos():
    got = harvest_gallery_urls_from_html(_tv_page(TOYOTA), "https://www.coronatoyota.com/viewdetails/x")
    assert len(got) == 9
    assert all("," not in u for u in got)
    assert split_joined_image_urls(",".join(TOYOTA[:3])) == TOYOTA[:3]
    assert split_joined_image_urls(PHOTOS[0]) == [PHOTOS[0]]
    # inline-JSON leaves carrying the joined list split too
    got_json = harvest_image_urls_from_json({"photoUrls": ",".join(PHOTOS[:4])}, "https://x.example", max_urls=50)
    assert got_json == PHOTOS[:4]


def test_prefetch_drops_placeholder_and_sets_hero(monkeypatch):
    monkeypatch.setenv("SCANNER_VDP_HTTP_FIRST_GALLERY_MIN", "8")
    v = {"vin": "2HGFG3B56DH533084", "gallery": ["/static/placeholder.svg", LOGO],
         "image_url": "/static/placeholder.svg", "description": "x" * 400}
    filled = _apply_description_and_gallery(v, _tv_page(PHOTOS), "https://www.dthondachicago.com/viewdetails/x")
    assert filled == 1
    assert v["gallery"] == PHOTOS
    assert "/static/placeholder.svg" not in v["gallery"]
    assert v["image_url"] == PHOTOS[0]


def test_gallery_fillable_ignores_widget_logo():
    assert _gallery_fillable(json.dumps(["/static/placeholder.svg", LOGO]))
    assert _gallery_fillable(json.dumps(["/static/placeholder.svg"]))
    assert not _gallery_fillable(json.dumps(["/static/placeholder.svg", PHOTOS[0]]))
