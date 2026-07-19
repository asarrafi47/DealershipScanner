"""Browser-free Carfax + factory packages/features capture per platform.

Covers the cosmos SRP card path (shared by DealerOn cosmos and Dealer.com cosmos
dealers) and the Typesense document mapper. Fixtures mirror the real feed shapes
captured live from longotoyota.com (cosmos) and toyotaoforange.com (Typesense).
"""

from __future__ import annotations

import json

from backend.parsers.typesense import parse as parse_typesense
from backend.scanner.scrapers.dealer_on import _extract_vehicles_from_srp_body


def _cosmos_body(card_overrides: dict | None = None) -> dict:
    card = {
        "VehicleVin": "JTDAAAAA6NA005560",
        "VehicleYear": 2022,
        "VehicleMake": "Toyota",
        "VehicleModel": "Mirai",
        "VehicleTrim": "XLE",
        "VehicleInternetPrice": 28998,
        "Mileage": "31,000",
        "VehicleCondition": "Used",
        "VehicleDetailUrl": "https://www.longotoyota.com/used/Toyota/2022-mirai.htm",
        "VehicleCarHistoryReport": (
            '<a class="reportImg" href="https://www.carfax.com/vehiclehistory/'
            'ar20/_6oMctfSf5MmnaI466-lk5yHBb8hRPx0" rel="noopener" target="_blank">'
            '<img src="https://partnerstatic.carfax.com/img/valuebadge/1own.svg"></a>'
        ),
        "Features": ["Bluetooth®", "Android Auto", "Apple CarPlay", "Navigation System"],
        "VehicleHighlightsModel": {
            "Highlights": ["Apple CarPlay", "Blind Spot Monitor", "Adaptive Cruise Control"],
        },
        "VehicleComments": (
            "TOYOTA CERTIFIED = 7 Year/100K Warranty. Navigation and heated seats. "
            "No haggle, stress-free shopping experience with a full vehicle history report."
        ),
    }
    if card_overrides:
        card.update(card_overrides)
    return {"DisplayCards": [{"VehicleCard": card}]}


def test_cosmos_carfax_from_history_report_anchor() -> None:
    rows = _extract_vehicles_from_srp_body(
        _cosmos_body(), "https://www.longotoyota.com", "17485", "Longo Toyota", "https://www.longotoyota.com"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["carfax_url"] == "https://www.carfax.com/vehiclehistory/ar20/_6oMctfSf5MmnaI466-lk5yHBb8hRPx0"


def test_cosmos_packages_features_and_highlights() -> None:
    rows = _extract_vehicles_from_srp_body(
        _cosmos_body(), "https://www.longotoyota.com", "17485", "Longo Toyota", "https://www.longotoyota.com"
    )
    pkg = rows[0]["packages"]
    assert isinstance(pkg, dict)
    assert "Apple CarPlay" in pkg["features"]
    # highlights only records phrases not already in features.
    assert "Blind Spot Monitor" in pkg["highlights"]
    assert "Apple CarPlay" not in pkg["highlights"]
    assert rows[0]["description"].startswith("TOYOTA CERTIFIED")


def test_cosmos_new_car_has_features_but_no_carfax() -> None:
    # New cars omit the history report anchor; features must still be captured.
    body = _cosmos_body({"VehicleCarHistoryReport": "", "VehicleCondition": "New"})
    rows = _extract_vehicles_from_srp_body(
        body, "https://x.com", "1", "X", "https://x.com"
    )
    assert "carfax_url" not in rows[0]
    assert rows[0]["packages"]["features"]


def test_cosmos_autocheck_history_report() -> None:
    body = _cosmos_body(
        {
            "VehicleCarHistoryReport": (
                '<a href="https://www.autocheck.com/vehiclehistory/report?vin=JTDAAAAA6NA005560">'
                "AutoCheck</a>"
            )
        }
    )
    rows = _extract_vehicles_from_srp_body(body, "https://x.com", "1", "X", "https://x.com")
    assert rows[0]["carfax_url"].startswith("https://www.autocheck.com/vehiclehistory/")


def _typesense_body(doc_overrides: dict | None = None) -> dict:
    doc = {
        "vin": "3TMLB5JN2SM171122",
        "yr": 2025,
        "make": "Toyota",
        "model": "Tacoma",
        "trim": "SR5",
        "finalPriceInt": 42000,
        "imageUrls": ["https://img.example.com/a.jpg"],
        "vdpUrl": "/inventory/2025-toyota-tacoma",
        "carfax": {
            "iconUrl": "https://partnerstatic.carfax.com/img/valuebadge/1own.svg",
            "snapshotKey": "Ub5uuoWMdEO6",
            "url": "https://www.carfax.com/vehiclehistory/ar20/Ub5uuoWMdEO6VDdBnevDTK",
        },
        "features": ["Cruise Control", "Backup Camera", "Blind Spot Monitor"],
        "packages": ["Premium Package"],
        "options": [],
        "vehicleHistory": ["CARFAX 1-Owner"],
    }
    if doc_overrides:
        doc.update(doc_overrides)
    return {"results": [{"hits": [{"document": doc}]}]}


def test_typesense_carfax_packages_history() -> None:
    rows = parse_typesense(
        _typesense_body(), base_url="https://www.toyotaoforange.com", dealer_id="toyotaoforange-com"
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["carfax_url"] == "https://www.carfax.com/vehiclehistory/ar20/Ub5uuoWMdEO6VDdBnevDTK"
    # clean_car_row_dict serializes the packages dict to JSON text.
    pkg = json.loads(row["packages"])
    assert "Cruise Control" in pkg["features"]
    assert pkg["factory_packages"] == ["Premium Package"]
    assert row["history_highlights"] == ["CARFAX 1-Owner"]


def test_typesense_new_car_without_carfax() -> None:
    body = _typesense_body({"carfax": None, "vehicleHistory": []})
    rows = parse_typesense(body, base_url="https://x.com", dealer_id="x")
    assert "carfax_url" not in rows[0] or not rows[0].get("carfax_url")
    # Features still captured for new cars.
    assert json.loads(rows[0]["packages"])["features"]
