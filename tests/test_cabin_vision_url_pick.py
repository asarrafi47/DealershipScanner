"""URL heuristics for picking a cabin image before LLaVA interior color."""

from __future__ import annotations

from backend.scanner_post_pipeline import _url_suggests_interior_cabin_image


def test_url_heuristic_interior_needles() -> None:
    assert _url_suggests_interior_cabin_image("https://cdn.d.example/2024/foo-interior-02.jpg")
    assert _url_suggests_interior_cabin_image("https://host/v/_cabinView.jpg")
    assert not _url_suggests_interior_cabin_image("https://host/hero/exterior_front_01.jpg")
    assert not _url_suggests_interior_cabin_image("not-a-url")


def test_select_cabin_stops_on_first_path_hint() -> None:
    from backend.scanner_post_pipeline import select_url_for_cabin_vision

    urls = [
        "https://a/exterior-01.jpg",
        "https://b/vehicle/INTERIOR-04.jpg",
    ]
    assert select_url_for_cabin_vision(urls) == urls[1]
