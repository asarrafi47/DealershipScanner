"""
Visual review 2026-09-28, item group E:

* M1, M2, M4, M7, M8 -- CSS-only mobile fixes, asserted on the stylesheets.
* ES-4 / SA-10 -- no VIN on the recall page is an input state, not an outage.
* DC-5 / ES-5 -- the History tab promises a report only when carfax_url exists.
* DC-8 -- numeric dealer rating with source and fetch date; no emoji badge.
"""

import re
from pathlib import Path

from backend.utils.dealer_rating_display import dealer_rating_display

_ROOT = Path(__file__).resolve().parents[2]
_CSS = _ROOT / "frontend" / "static" / "css"


def _read(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


def test_m1_sidebar_pages_clear_the_toggle():
    css = (_CSS / "07-dealer-admin.css").read_text()
    for sel in (".compare-page-section", ".nhtsa-recalls-section", ".auth-section", ".dealer-research"):
        assert f"body:has(.app-sidebar) {sel}" in css


def test_m2_mobile_gallery_rule_follows_the_base_rule():
    css = (_CSS / "03-car-page.css").read_text()
    base = css.index("    height: 500px;\n    max-height: 500px;")
    mobile = css.index("aspect-ratio: 4 / 3;")
    assert mobile > base
    assert "height: min(56vw, 340px);" not in css


def test_m4_vdp_tabs_wrap_on_phones():
    css = (_CSS / "11-overrides.css").read_text()
    block = css[css.index("@media (max-width: 600px)"):]
    assert "flex-wrap: wrap;" in block and "min-height: 44px;" in block


def test_m7_filter_sheet_is_bottom_anchored():
    css = (_CSS / "09-components.css").read_text()
    sheet = css[css.index("M7 (visual review"):]
    sheet = sheet[: sheet.index("}")]
    assert "top: auto;" in sheet and "bottom: 0;" in sheet
    listings = (_CSS / "02-listings.css").read_text()
    assert "background: linear-gradient(180deg, rgba(255, 255, 255, 0.92) 0%, #fff 100%);" not in listings


def test_m8_landing_controls_are_44px_with_16px_text():
    css = (_CSS / "12-viewers.css").read_text()
    block = css[css.index("M8 (visual review"):]
    assert "min-height: 44px;" in block and "font-size: 16px;" in block


def test_recall_page_missing_vin_is_a_form_not_an_error():
    html = _read("frontend/templates/nhtsa_recalls.html")
    assert "lookup_error not in ('invalid_vin', 'missing_vin')" in html
    assert 'name="vin"' in html
    assert "({{ lookup_error }})" not in html
    assert 'role="alert"' in html


def test_recall_page_renders_vin_form_without_vin(monkeypatch, tmp_path):
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users.db"))
    from backend.main import app

    app.config["TESTING"] = True
    with app.test_client() as client:
        resp = client.get("/nhtsa-recalls")
    body = resp.get_data(as_text=True)
    assert resp.status_code == 200
    assert "nhtsa-recalls-form" in body
    assert "Could not reach NHTSA" not in body
    assert "missing_vin" not in body


def test_history_copy_branches_on_carfax_url():
    html = _read("frontend/templates/car.html")
    assert "Detailed history report available" not in html
    assert "No history report on file for this VIN" in html
    assert "may require purchase" in html
    # The paid lookup is never the primary button.
    paid = re.search(r'<a href="https://www\.carfax\.com/VehicleHistory[^>]*class="([^"]+)"', html)
    assert paid and "carfax-report-btn-secondary" in paid.group(1)


def test_dealer_rating_display():
    got = dealer_rating_display({"google_rating": 4.8, "google_review_count": 13610,
                                 "google_rating_fetched_at": "2026-07-18T10:00:00+00:00"})
    assert got == {"rating": "4.8", "fill_pct": 96.0, "reviews": "13,610 Google reviews", "as_of": "18 Jul 2026"}
    assert dealer_rating_display({"google_rating": None}) is None
    assert dealer_rating_display(None) is None
    one = dealer_rating_display({"google_rating": "4", "google_review_count": 1})
    assert one["reviews"] == "1 Google review" and one["as_of"] is None


def test_no_emoji_trophy_badge():
    js = _read("frontend/static/car_page.js")
    assert "Top Rated Dealer" not in js
    assert "\\uD83C\\uDFC6" not in js
    html = _read("frontend/templates/car.html")
    assert "dealer_reputation(dealer_rating" in html
    assert "(Loading reviews...)" not in html
