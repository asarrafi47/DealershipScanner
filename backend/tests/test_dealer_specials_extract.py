"""Network-free tests for the dealer-specials offer extractor.

Covers the two dominant SoCal platforms (dealer.com ``.special-offer`` cards and
DealerEProcess ``.offer-box`` cards) plus type classification and the empty case.
"""
from __future__ import annotations

from backend.scanner.specials.extract import extract_offers_from_html

# dealer.com "special-offer" card with an explicit .disclaimer fine-print block
# and a data-offer attribute carrying year/make/model (the mbontario pattern).
_DEALER_DOT_COM = """
<div class="special-offers-section">
  <div class="special-offer card" id="offer-1"
       data-offer="new.2026.Mercedes-Benz.GLA.250.Sport Utility">
    <div class="offer-content card-content">
      <span class="offertitle">Luxury Loaner 2026 GLA 250</span>
      <div class="offeritem">
        <span class="offerrate">Lease for $199</span>
        <span class="offerlabel">/mo. for 24 months</span>
      </div>
      <div class="offer-description"><p>Offer Expires: 07/31/2026</p></div>
      <div class="disclaimertoggle">View Disclaimer</div>
      <div class="disclaimer">
        Advertised 24 months lease payment based on MSRP of $46,185.00.
        $1,995.00 cash due at signing. 10,000 miles per year. Excludes taxes.
      </div>
    </div>
  </div>
</div>
"""

# DealerEProcess "offer-box" card where the whole card text carries the details.
_EPROCESS = """
<div class="special-offers">
  <div class="offer-box">
    <div class="offer">
      <div class="make">New 2026 Kia K4 LXS Sedan</div>
      <div class="offer-type-text">Lease for</div>
      <div class="offer-price">$256</div>
      <div class="offerbox-details-text2">Per month for 36 Months</div>
      <div class="offerbox-details-text3">Plus tax and license. $2719 Due At Signing</div>
      <div class="offerbox-details">
        <span>MSRP</span><span>$24,635</span>
        <span>Expires: 08/03/2026</span>
      </div>
    </div>
  </div>
</div>
"""

_FINANCE = """
<div class="special-offer card">
  <div class="offer-content">
    <span class="offertitle">New 2026 Toyota Tundra</span>
    <div class="disclaimer">$0 down and 2.99% APR for 72 months on approved credit.
      Offer expires 8/3/26.</div>
  </div>
</div>
"""


def test_dealer_dot_com_lease_offer():
    offers = extract_offers_from_html(_DEALER_DOT_COM, "https://x.com/specials")
    assert len(offers) == 1
    o = offers[0]
    assert o["type"] == "lease"
    assert o["payment"] == 199.0
    assert o["term_months"] == 24
    assert o["due_at_signing"] == 1995.0
    assert o["msrp"] == 46185.0
    assert o["mileage_per_year"] == 10000
    assert o["expires"] == "07/31/2026"
    assert o["vehicle_year"] == 2026
    assert o["vehicle_make"] == "Mercedes-Benz"
    assert o["vehicle_model"] == "GLA"
    assert "MSRP of $46,185.00" in o["fine_print"]
    assert o["source_url"] == "https://x.com/specials"
    assert o["offer_hash"]


def test_eprocess_offer_box_lease():
    offers = extract_offers_from_html(_EPROCESS, "https://y.com/specials")
    assert len(offers) == 1
    o = offers[0]
    assert o["type"] == "lease"
    assert o["payment"] == 256.0
    assert o["term_months"] == 36
    assert o["due_at_signing"] == 2719.0
    assert o["msrp"] == 24635.0
    assert o["vehicle_year"] == 2026
    assert o["vehicle_make"] == "Kia"
    assert o["vehicle_model"] == "K4"
    assert o["expires"] == "08/03/2026"


def test_finance_apr_classification():
    offers = extract_offers_from_html(_FINANCE, "https://z.com/specials")
    assert len(offers) == 1
    assert offers[0]["type"] == "finance"
    assert offers[0]["expires"] == "8/3/26"


def test_empty_and_garbage():
    assert extract_offers_from_html("", "u") == []
    assert extract_offers_from_html("<html><body><p>hi</p></body></html>", "u") == []


def test_dedupes_identical_cards():
    doubled = _DEALER_DOT_COM + _DEALER_DOT_COM
    offers = extract_offers_from_html(doubled, "u")
    assert len(offers) == 1
