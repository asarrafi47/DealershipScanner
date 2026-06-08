"""Tests for VDP HTML gallery recovery."""
from __future__ import annotations

from backend.scanner import vdp_html_recovery as rec


SAMPLE_HTML = """
<html><head>
<meta property="og:description" content="One-owner BMW with premium package and panoramic roof installed at delivery." />
<script type="application/ld+json">
{"@type":"Car","image":["https://cdn.dealer.com/photos/vin123/01.jpg","https://cdn.dealer.com/photos/vin123/02.webp"]}
</script>
<script id="__NEXT_DATA__" type="application/json">
{"props":{"vehicle":{"gallery":{"images":["https://cdn.dealer.com/photos/vin123/03.jpg"]}}}}
</script>
</head><body>
<img src="https://cdn.dealer.com/photos/vin123/04.png" />
<div class="vehicle-description">Clean Carfax one owner with navigation and HUD.</div>
</body></html>
"""


def test_harvest_gallery_urls_from_html() -> None:
    urls = rec.harvest_gallery_urls_from_html(SAMPLE_HTML, "https://dealer.example/vdp/1")
    assert len(urls) >= 3
    assert all(u.startswith("https://") for u in urls)


def test_recover_skips_when_gallery_full(monkeypatch) -> None:
    monkeypatch.setenv("SCANNER_GALLERY_RECOVERY_MIN", "3")
    big = [f"https://cdn.example.com/p{i}.jpg" for i in range(10)]
    out = rec.recover_from_detail_page("https://dealer.example/vdp/1", existing_gallery=big)
    assert out.get("skipped") == "gallery_sufficient"


def test_recover_merges_when_thin(monkeypatch) -> None:
    monkeypatch.setenv("SCANNER_GALLERY_RECOVERY_MIN", "14")

    def _fake_fetch(_url: str) -> str:
        return SAMPLE_HTML

    out = rec.recover_from_detail_page(
        "https://dealer.example/vdp/1",
        existing_gallery=["https://cdn.example.com/only.jpg"],
        fetch_html=_fake_fetch,
    )
    assert out.get("html_fetched") is True
    assert len(out.get("gallery_urls") or []) >= 2
    assert out.get("description")


CREVIER_STYLE_HTML = """
<html><body>
<h2>Description</h2>
<div class="description-block">
2024 CERTIFIED PRE-OWNED MINI COOPER SIGNATURE TRIM. NANUQ WHITE EXTERIOR WITH A CARBON BLACK INTERIOR.
THIS FUEL EFFICIENT MINI GETS A COMBINED 31 MPG. ALSO BACKED BY MINI'S 5 YEAR UNLIMITED MILE WARRANTY
GOOD THROUGH 05/20/2028! Come see our team at Crevier MINI to take advantage of this amazing deal today!
</div>
</body></html>
"""


def test_extract_description_from_description_heading() -> None:
    desc = rec.extract_description_from_html(CREVIER_STYLE_HTML)
    assert desc
    assert "MINI COOPER SIGNATURE" in desc
    assert "Crevier MINI" in desc
    assert len(desc) > 100


def test_should_extract_thin_gallery(monkeypatch) -> None:
    from backend.scanner import claude_vdp_extract as cve

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    monkeypatch.setenv("SCANNER_CLAUDE_VDP", "1")
    monkeypatch.setenv("SCANNER_CLAUDE_VDP_GALLERY_MIN", "20")
    v = {
        "vin": "1" * 17,
        "price": 25000,
        "condition": "Used",
        "exterior_color": "Black",
        "interior_color": "Black",
        "packages": "[]",
        "description": "ok",
        "transmission": "Auto",
        "drivetrain": "AWD",
        "fuel_type": "Gasoline",
        "cylinders": 4,
        "body_style": "SUV",
        "gallery": ["https://x.com/1.jpg"],
    }
    assert cve.should_extract_from_page(v) is True
