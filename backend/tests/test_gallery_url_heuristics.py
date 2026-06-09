"""Public gallery URL filtering for car detail pages."""

from __future__ import annotations

from backend.vision.url_heuristics import (
    filter_public_gallery_urls,
    heuristic_listing_gallery_fluff_url,
    prefer_full_gallery_url,
)


def test_heuristic_drops_transfer_badge_and_icons() -> None:
    junk = [
        "https://www.hendrickmini.com/sites/c/customwork/transferBadge/images/default.jpg",
        "https://www.hendrickmini.com/static/v9/media/images/mobile/directions-icon.png",
        "https://www.hendrickmini.com/static/v9/media/js/photoswipe/v4.1.3/dist/default-skin/default-skin.png",
        "https://cdn.gubagoo.io/gb1/735e328d611418e01fc085fa4e4374d4831bbb40.png",
        "https://pureinfluencer.idrove.it/imgs/6941d7ce96296253fadacc06/1765922767931-%24500+Incentive+.png",
    ]
    for u in junk:
        assert heuristic_listing_gallery_fluff_url(u), u


def test_heuristic_keeps_dealer_lot_photo() -> None:
    lot = "https://pictures.dealer.com/r/rickhendrickbuickgmcduluth/0659/a40135133caa335c9e74dfa71d7e1c82x.jpg"
    assert not heuristic_listing_gallery_fluff_url(lot)


def test_prefer_full_gallery_url_upgrades_carscommerce_thumb() -> None:
    thumb = (
        "https://vehicle-images.carscommerce.inc/ea40-110004659/"
        "WBXYJ1C07L5P62060/thumbnails/large/e849c95fee89f8bdd5fef7c07ed4e3f2.jpg"
    )
    full = prefer_full_gallery_url(thumb)
    assert "/thumbnails/" not in full
    assert full.endswith("e849c95fee89f8bdd5fef7c07ed4e3f2.jpg")


def test_filter_public_gallery_urls_car_352_style() -> None:
    raw = [
        "https://www.hendrickmini.com/sites/c/customwork/transferBadge/images/default.jpg",
        "https://pictures.dealer.com/r/rickhendrickbuickgmcduluth/0659/a40135133caa335c9e74dfa71d7e1c82x.jpg",
        "https://www.hendrickmini.com/static/v9/media/images/mobile/directions-icon.png",
    ]
    out = filter_public_gallery_urls(raw)
    assert len(out) == 1
    assert "pictures.dealer.com" in out[0]


def test_prepare_car_detail_filters_gallery() -> None:
    from backend.enrichment.knowledge_engine import prepare_car_detail_context

    car = {
        "gallery": [
            "https://www.hendrickmini.com/sites/c/customwork/transferBadge/images/default.jpg",
            "https://pictures.dealer.com/r/dealer/0659/photo.jpg",
        ],
        "image_url": "https://www.hendrickmini.com/sites/c/customwork/transferBadge/images/default.jpg",
    }
    ctx = prepare_car_detail_context(car)
    urls = ctx.get("gallery_images") or []
    assert len(urls) == 1
    assert "pictures.dealer.com" in urls[0]
