"""Tests for PixelMotion SSR inventory parser."""

from __future__ import annotations

from backend.scanner.scrapers.pixel_motion import (
    parse_pixel_motion_inventory_html,
)

_SAMPLE_ROW = """
<div class="vlpm3VehicleRow panel panel-default" id="3C4NJDBN0TT214531">
  <a class="view-vehicle" href="/inventory/display/new/2026/Jeep/Compass/3C4NJDBN0TT214531/">
    <h2 class="condition text-uppercase">New 2026 Jeep Compass Latitude Altitude 4x4</h2>
  </a>
  <div class="vlpm3VehicleImage__static">
    <img src="https://images.otf3.pixelmotiondemo.com/364x273/rjeVL.jpg" />
  </div>
  <div class="vlp-item-stockNum"><strong>Stock #: </strong><span>T0147</span></div>
  <div class="vlp-item-mileage"><strong>Mileage:</strong> 7</div>
  <div class="vlp-item-color-exterior"><strong>Exterior Color&colon;</strong> Blue</div>
  <div class="price-item-label'>McPeeks</div><div class='price-item-value'>&dollar;29,996</div>
</div>
"""


def test_parse_pixel_motion_row():
    html = f"<!-- pixelmotion -->\n{_SAMPLE_ROW}"
    rows = parse_pixel_motion_inventory_html(
        html,
        "https://www.mcpeeks.com",
        "mcpeeksdodgeanaheim-com",
        "McPeek",
        "https://www.mcpeeks.com",
    )
    assert len(rows) == 1
    v = rows[0]
    assert v["vin"] == "3C4NJDBN0TT214531"
    assert v["year"] == 2026
    assert v["make"] == "Jeep"
    assert v["price"] == 29996
    assert v["stock_number"] == "T0147"
