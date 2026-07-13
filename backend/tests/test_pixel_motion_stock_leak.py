"""
Regression: PixelMotion (pm-motors-plugin) cards must not leak the stock code
into spec fields.

Every vlpm3VehicleRow card ships (a) live spec divs whose values are BARE TEXT
after ``</strong>`` (no ``<span>``) and (b) a commented-out legacy ``<ul>``
whose first ``</strong><span>...</span>`` pair is the STOCK NUMBER. The old
``_extract_span`` pattern scanned past the element boundary under ``re.S`` and
captured that stock code for exterior/interior color, drivetrain, transmission,
engine — and its digits for mileage (McPeek's CDJR Anaheim contamination).
"""

from __future__ import annotations

from backend.scanner.scrapers.pixel_motion import (
    _extract_span,
    _parse_mileage,
    parse_pixel_motion_inventory_html,
)

# Mirrors the live McPeek card markup (scratchpad/mcpeek_vdp.html, InfoStack +
# trailing commented-out legacy <ul> from the pm-motors-plugin template).
_CARD_TMPL = """
<div class="vlpm3VehicleRow panel panel-default" id="{vin}">
  <a class="view-vehicle btn" data-loc="vlp" href="/inventory/display/{cond_path}/{vin}/">
    <h2 class="condition text-uppercase">{title}</h2>
  </a>
  <div class="vlpm3VehicleImage__static">
    <img src="https://images.otf3.pixelmotiondemo.com/364x273/{vin}.jpg" />
  </div>
  <div class="vlpm3VehicleInfoStack no-extra">
      <!-- hide dummy Vins and Stock num -->
      {stock_div}
      <div class="vlp-item-vin"><strong>VIN: </strong><span>{vin}</span></div>
      <div class="vlp-item-mileage"><strong>Mileage:</strong> {mileage}</div>
      <div class="vlp-item-engine"><strong>Engine:</strong> 2.0L I4 DOHC DI Turbo</div>
      <div class="vlpm3VehicleInfoStack__extra" style="display:none;">
          <div class="vlp-item-color-exterior"><strong>Exterior Color&colon;</strong> {exterior}</div>
          <div class="vlp-item-color-interior"><strong>Interior Color&colon;</strong> Black</div>
          <div class="vlp-item-drive"><strong>Drive&colon;</strong> 4x4</div>
          <div class="vlp-item-transmission"><strong>Transmission&colon;</strong> Automatic</div>
      </div>
  </div>
  <div class="price-item-label'>McPeeks</div><div class='price-item-value'>&dollar;29,996</div>
  <!-- <ul class="list-unstyled stats-list">
      hide dummy Vins and Stock num
      <li class="vlp-item-stockNum"><strong>Stock #:</strong><span>{stock}</span></li>
      <li class="vlp-item-vin"><strong>VIN:</strong><span>{vin}</span></li>
      <li class="vlp-item-mileage"><strong>Mileage:</strong><span>5</span></li>
      <li class="vlp-item-color-exterior"><strong>Exterior Color:</strong><span>Cosmic Black</span></li>
  </ul> -->
</div>
"""


def _card(
    vin: str,
    stock: str,
    *,
    title: str = "New 2026 Jeep Compass Latitude",
    cond_path: str = "new/2026/Jeep/Compass",
    exterior: str = "Fathom Blue Pearl Coat",
    mileage: str = "7",
    visible_stock: bool = True,
) -> str:
    stock_div = (
        f'<div class="vlp-item-stockNum"><strong>Stock #: </strong><span>{stock}</span></div>'
        if visible_stock
        else ""
    )
    return _CARD_TMPL.format(
        vin=vin,
        stock=stock,
        stock_div=stock_div,
        title=title,
        cond_path=cond_path,
        exterior=exterior,
        mileage=mileage,
    )


def _parse(html: str) -> list[dict]:
    return parse_pixel_motion_inventory_html(
        html,
        "https://www.mcpeeks.com",
        "mcpeeks-com",
        "McPeek's Chrysler Dodge Jeep Ram Anaheim",
        "https://www.mcpeeks.com",
    )


def test_mcpeek_tokens_do_not_leak_into_spec_fields():
    """R1111 / FR040 / T0386 stay in stock_number; spec fields keep real values."""
    cards = [
        ("3C4NJDBN0TT214531", "R1111"),
        ("3C7WRMCL2RG204890", "FR040"),
        ("1C4RJKBG5S8714532", "T0386"),
    ]
    html = "\n".join(_card(vin, stock) for vin, stock in cards)
    rows = _parse(html)
    assert len(rows) == 3
    by_vin = {r["vin"]: r for r in rows}
    for vin, stock in cards:
        v = by_vin[vin]
        assert v["stock_number"] == stock
        assert v["exterior_color"] == "Fathom Blue Pearl Coat"
        assert v["interior_color"] == "Black"
        assert v["drivetrain"] == "4x4"
        assert v["transmission"] == "Automatic"
        assert v["engine_description"] == "2.0L I4 DOHC DI Turbo"
        assert v["mileage"] == 7
        # None of the six fields carries the stock code
        for key in (
            "exterior_color",
            "interior_color",
            "drivetrain",
            "transmission",
            "engine_description",
        ):
            assert v[key] != stock
        assert v["source_url"].endswith(f"/inventory/display/new/2026/Jeep/Compass/{vin}/")


def test_no_visible_stock_div_yields_none_not_commented_leak():
    """Historic markup (stock div hidden): never read the commented-out <ul>."""
    html = _card("4T1BF1FK0HU328802", "UK0005", visible_stock=False, mileage="189,645")
    rows = _parse(html)
    assert len(rows) == 1
    v = rows[0]
    assert v["stock_number"] is None  # not "UK0005" from the comment
    assert v["exterior_color"] == "Fathom Blue Pearl Coat"  # not "Cosmic Black"
    assert v["mileage"] == 189645


def test_extract_span_bounded_to_element():
    block = (
        '<div class="vlp-item-color-exterior"><strong>Exterior Color&colon;</strong> Bright White</div>'
        '<div class="vlp-item-stockNum"><strong>Stock #: </strong><span>T0386</span></div>'
    )
    assert _extract_span(block, "vlp-item-color-exterior") == "Bright White"
    assert _extract_span(block, "vlp-item-stockNum") == "T0386"


def test_extract_span_ignores_html_comments():
    block = (
        '<!-- <li class="vlp-item-color-exterior"><strong>Exterior Color:</strong>'
        "<span>Cosmic Black</span></li> -->"
        '<div class="vlp-item-drive"><strong>Drive&colon;</strong> 4x4</div>'
    )
    assert _extract_span(block, "vlp-item-color-exterior") is None
    assert _extract_span(block, "vlp-item-drive") == "4x4"


def test_parse_mileage_rejects_stock_code_tokens():
    for token in ("R1111", "FR040", "T0386", "UK0005", "U0105"):
        assert _parse_mileage(token) is None
    assert _parse_mileage("7") == 7
    assert _parse_mileage("189,645") == 189645
    assert _parse_mileage(None) is None
    assert _parse_mileage("") is None
