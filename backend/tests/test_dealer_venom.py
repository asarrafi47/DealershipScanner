"""Dealer Venom / Typesense scraper helpers."""

from backend.scanner.scrapers.dealer_venom import (
    _extract_typesense_config_from_html,
    _map_typesense_document,
)

_SAMPLE_HTML = """
<script>
const indexName = "vehicles-TOY04247";
const typesenseAdapter = new TypesenseInstantSearchAdapter({
    server: {
        apiKey: "test-api-key-123",
        nodes: [{
            host: 'hjnrb3s21408ezpfp.a1.typesense.net',
            port: 443,
            protocol: 'https'
        }],
    },
});
</script>
"""

_SAMPLE_DOC = {
    "vin": "4T1G11AK6RU876395",
    "year": 2024,
    "make": "Toyota",
    "model": "Camry",
    "trim": "SE",
    "finalPriceInt": 28899,
    "mileage": "12,345",
    "condition": "Used",
    "exteriorColor": "Supersonic Red",
    "interiorColor": "Black",
    "engine": "2.5L I-4",
    "transmission": "8-Speed Automatic",
    "drivetrain": "FWD",
    "fuel": "Gasoline",
    "body": "Sedan",
    "stockNumber": "RU876395",
    "vdpUrl": "/vehicle/Used/2024/Toyota/Camry/4T1G11AK6RU876395/",
    "imageUrls": [
        "https://content.example.com/a.jpg",
        "https://content.example.com/b.jpg",
    ],
    "carfax": {"url": "https://www.carfax.com/vehiclehistory/example"},
}


def test_extract_typesense_config_from_html() -> None:
    cfg = _extract_typesense_config_from_html(_SAMPLE_HTML)
    assert cfg is not None
    assert cfg["host"] == "hjnrb3s21408ezpfp.a1.typesense.net"
    assert cfg["apiKey"] == "test-api-key-123"
    assert cfg["indexName"] == "vehicles-TOY04247"


def test_map_typesense_document() -> None:
    row = _map_typesense_document(
        _SAMPLE_DOC,
        "https://www.toyotaoforange.com",
        "toyotaoforange-com",
        "Toyota of Orange",
        "https://www.toyotaoforange.com",
    )
    assert row is not None
    assert row["vin"] == "4T1G11AK6RU876395"
    assert row["price"] == 28899
    assert row["mileage"] == 12345
    assert row["image_url"].startswith("https://")
    assert row["_detail_url"].endswith("/4T1G11AK6RU876395/")
    assert row["carfax_url"].startswith("https://www.carfax.com/")
