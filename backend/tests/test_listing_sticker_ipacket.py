"""iPacket / dealer listing MSRP sticker URL discovery and parsing."""

from backend.scanner.window_sticker import (
    _parse_msrp_from_sticker_text,
    _parse_options_from_sticker_text,
    extract_listing_sticker_urls_from_html,
    fetch_listing_sticker,
    group_sticker_options_for_display,
    is_listing_sticker_url,
    is_sticker_media_url,
    parse_sticker_option_items,
    pick_best_listing_sticker_url,
)


def test_is_listing_sticker_url_ipacket() -> None:
    url = (
        "https://djapi.autoipacket.com/v2/sticker-puller/download/W1K6G7GB6NA126084"
        "?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.test"
    )
    assert is_listing_sticker_url(url) is True
    assert is_sticker_media_url(url) is True


def test_ipacket_website_plugin_sticker_urls(monkeypatch) -> None:
    from backend.scanner import window_sticker as ws

    payload = {
        "modules": [
            {
                "label": "Original MSRP / Options Info",
                "url": (
                    "https://document-viewer.autoipacket.com/sticker/W1K6G7GB6NA126084"
                    "?token=eyJ0eXAi.test"
                ),
            }
        ]
    }

    class _Resp:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self):
            return payload

    monkeypatch.setattr("requests.get", lambda *a, **k: _Resp())
    urls = ws._ipacket_sticker_urls_from_website_plugin("W1K6G7GB6NA126084")
    assert len(urls) == 1
    assert "sticker-puller/download/W1K6G7GB6NA126084" in urls[0]
    assert "token=eyJ0eXAi.test" in urls[0]


def test_resolve_listing_sticker_fetch_url_document_viewer() -> None:
    from backend.scanner.window_sticker import resolve_listing_sticker_fetch_url

    viewer = (
        "https://document-viewer.autoipacket.com/sticker/W1K6G7GB6NA126084"
        "?token=abc123"
    )
    resolved = resolve_listing_sticker_fetch_url(viewer, "W1K6G7GB6NA126084")
    assert resolved == (
        "https://djapi.autoipacket.com/v2/sticker-puller/download/W1K6G7GB6NA126084?token=abc123"
    )


def test_extract_ipacket_url_from_listing_html() -> None:
    html = """
    <script>
      var x = "https://djapi.autoipacket.com/v2/sticker-puller/download/W1K6G7GB6NA126084?token=abc123";
    </script>
  """
    urls = extract_listing_sticker_urls_from_html(html, vin="W1K6G7GB6NA126084")
    assert len(urls) == 1
    assert "sticker-puller/download/W1K6G7GB6NA126084" in urls[0]


def test_pick_best_listing_sticker_url_prefers_ipacket_token() -> None:
    urls = [
        "https://cdn.dealer.com/monroney/thumb.jpg",
        "https://djapi.autoipacket.com/v2/sticker-puller/download/W1K6G7GB6NA126084?token=abc",
    ]
    best = pick_best_listing_sticker_url(urls, vin="W1K6G7GB6NA126084")
    assert best is not None
    assert "sticker-puller" in best


def test_parse_ipacket_msrp_text_options() -> None:
    text = """
MERCEDES-BENZ MSRP
Base $117,700.00
DG3 AMG Line $4,300
DU3 Warmth and Comfort Package $3,150
RVP 20" AMG Multi-Spoke Wheels - Gloss Black $1,150
810 Burmester Surround Sound System —
413 Panorama Roof —
Net Total $129,075.00
"""
    opts = _parse_options_from_sticker_text(text)
    blob = " ".join(opts).lower()
    assert "amg line" in blob or "warmth" in blob
    assert "burmester" in blob or "panorama" in blob or "amg multi" in blob
    assert _parse_msrp_from_sticker_text(text) == 129075


def test_parse_mercedes_monroney_added_options() -> None:
    text = """
Added Options
DC1 Night Package ($400): Includes Front Splitter and Fins in the air intakes in High Gloss Black
DG3 AMG Line ($4,300): Includes AMG Wheels and Sport Bodystyling
DU3 Warmth and Comfort Package ($3,150): Includes Heated Steering Wheel
RVP 20" AMG Multi-Spoke Wheels - Gloss Black $1,150
TOTAL SUGGESTED PRICE: $129,075
"""
    items = parse_sticker_option_items(text)
    opts = _parse_options_from_sticker_text(text)
    blob = " ".join(opts).lower()
    assert "night package" in blob
    assert "amg line" in blob
    assert "warmth and comfort" in blob
    assert "amg multi-spoke" in blob or "20" in blob
    prices = {item["name"].lower(): item.get("price") for item in items if item.get("name")}
    assert prices.get("night package") == 400 or any(v == 400 for v in prices.values())
    assert any(v == 4300 for v in prices.values())
    assert any(v == 1150 for v in prices.values())
    assert _parse_msrp_from_sticker_text(text) == 129075


def test_organize_sticker_option_sections_packages_and_options() -> None:
    from backend.scanner.window_sticker import organize_sticker_option_sections

    items = [
        {"name": "Base", "price": 117700, "code": None},
        {"name": "DC1 Night Package", "price": 400, "code": "DC1"},
        {"name": "P55 Night Package", "price": None, "code": "P55"},
        {"name": "DG3 AMG Line", "price": 4300, "code": "DG3"},
        {"name": "U26 AMG Floormats", "price": None, "code": "U26"},
        {"name": "RVP 20\" AMG Multi-Spoke Wheels - Gloss Black", "price": 1150, "code": "RVP"},
    ]
    sections = organize_sticker_option_sections(items)
    assert sections["base"] and sections["base"]["price"] == 117700
    assert len(sections["packages"]) >= 1
    assert any(p["name"] == "DC1 Night Package" for p in sections["packages"])
    night = next(p for p in sections["packages"] if p["name"] == "DC1 Night Package")
    assert night.get("features")
    assert any("Wheels" in o.get("name", "") for o in sections["options"])
    assert all(o.get("kind") == "line" for o in sections["options"])


def test_parse_ram_monroney_optional_equipment() -> None:
    from backend.scanner.window_sticker import (
        organize_sticker_option_sections,
        parse_sticker_option_items,
    )

    text = """
    OPTIONAL EQUIPMENT (May Replace Standard Equipment)
    Granite Crystal Metallic Clear-Coat Exterior Paint                    $295
    Deluxe Cloth Bucket Seat                                              $595
      2-Way Power Lumbar
      8-Way Power Driver Seat
    Big Horn Package 23Z
    Big Horn Level 1 Equipment Group                                    $1,995
      Handsfree Phone and Audio
      Front Fog Lamps
      Heated Front Seats
      Heated Steering Wheel
    Front and Rear Rubber Floor Mats by Mopar                             $160
    3.55 Rear Axle Ratio                                                  $145
  """
    items = parse_sticker_option_items(text)
    assert any(i.get("price") == 295 for i in items)
    assert any(i.get("price") == 1995 for i in items)
    sections = organize_sticker_option_sections(items)
    assert len(sections["packages"]) >= 2
    level1 = next(p for p in sections["packages"] if "Level 1" in p.get("name", ""))
    assert level1.get("price") == 1995
    assert len(level1.get("features") or []) >= 3
    assert any(o.get("price") == 295 for o in sections["options"])


def test_group_sticker_options_mercedes_packages() -> None:
    from backend.scanner.window_sticker import group_sticker_options_for_display

    items = [
        {"name": "Base", "price": 117700, "code": None},
        {"name": "DC1 Night Package", "price": 400, "code": "DC1"},
        {"name": "P55 Night Package", "price": None, "code": "P55"},
        {"name": "DG3 AMG Line", "price": 4300, "code": "DG3"},
        {"name": "U26 AMG Floormats", "price": None, "code": "U26"},
        {"name": "772 AMG Bodystyling", "price": None, "code": "772"},
        {"name": "Factory Code", "price": None, "code": None},
        {"name": "RVP 20\" AMG Multi-Spoke Wheels - Gloss Black", "price": 1150, "code": "RVP"},
    ]
    groups = group_sticker_options_for_display(items)
    assert groups[0]["kind"] == "line"
    assert groups[0]["name"] == "Base"
    night = next(g for g in groups if g.get("name") == "DC1 Night Package")
    assert night["kind"] in ("package", "line")
    assert night["name"] == "DC1 Night Package"
    amg = next(g for g in groups if g.get("name") == "DG3 AMG Line")
    assert amg["kind"] in ("package", "line")
    wheels = next(g for g in groups if "Wheels" in g.get("name", ""))
    assert wheels["kind"] == "line"
    assert all("factory code" not in (g.get("name") or "").lower() for g in groups)


def test_car_listing_may_have_sticker_with_source_url() -> None:
    from backend.scanner.window_sticker import car_listing_may_have_sticker, show_window_sticker_ui

    car = {
        "vin": "W1K6G7GB6NA126084",
        "make": "Mercedes-Benz",
        "source_url": "https://dealer.example.com/inventory/used-2022-mercedes-s580-vin-w1k6g7gb6na126084/",
    }
    assert car_listing_may_have_sticker(car) is True
    assert show_window_sticker_ui(car) is True


def test_fetch_listing_sticker_blocks_disallowed_url(monkeypatch) -> None:
    called: list[str] = []

    def _fake_get(*_a, **_k):
        called.append("get")
        raise AssertionError("requests.get should not run for disallowed URL")

    monkeypatch.setattr("requests.get", _fake_get)
    out = fetch_listing_sticker("https://evil.example.com/sticker.pdf", "W1K6G7GB6NA126084")
    assert out is None
    assert called == []
