"""NHTSA recall lookup parsing and VIN-first YMM resolution (no network)."""

from __future__ import annotations

import io
import json
import urllib.error
from unittest.mock import MagicMock, patch

from backend.enrichment.vehicle_history_intelligence import (
    _parse_nhtsa_recall_http_payload,
    _fetch_recalls_by_ymm,
    fetch_nhtsa_recalls,
)


def test_parse_nhtsa_recall_http_payload_empty_400_body() -> None:
    raw = json.dumps({"Count": 0, "Message": "Results returned successfully", "results": []})
    recalls, err = _parse_nhtsa_recall_http_payload(raw)
    assert err is None
    assert recalls == []


def test_parse_nhtsa_recall_http_payload_with_rows() -> None:
    raw = json.dumps(
        {
            "Count": 1,
            "results": [
                {
                    "NHTSACampaignNumber": "24V123",
                    "Component": "AIR BAGS",
                    "Summary": "Example summary text.",
                }
            ],
        }
    )
    recalls, err = _parse_nhtsa_recall_http_payload(raw)
    assert err is None
    assert len(recalls) == 1
    assert recalls[0]["campaign"] == "24V123"
    assert recalls[0]["component"] == "AIR BAGS"


def test_fetch_recalls_by_ymm_treats_http_400_with_valid_json_as_success() -> None:
    payload = json.dumps({"Count": 0, "Message": "Results returned successfully", "results": []}).encode()
    err = urllib.error.HTTPError(
        url="https://api.nhtsa.gov/recalls/recallsByVehicle",
        code=400,
        msg="Bad Request",
        hdrs=None,
        fp=io.BytesIO(payload),
    )

    def fake_urlopen(req, timeout=20.0):
        raise err

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        recalls, api_err = _fetch_recalls_by_ymm("Volvo", "V60 Cross Country", 2026)
    assert api_err is None
    assert recalls == []


def test_fetch_nhtsa_recalls_prefers_vpic_model_over_listing_title() -> None:
    vpic_flat = {"Make": "VOLVO", "Model": "V60CC", "ModelYear": "2024"}
    recall_payload = json.dumps(
        {
            "Count": 1,
            "results": [
                {
                    "NHTSACampaignNumber": "24V999",
                    "Component": "ELECTRICAL",
                    "Summary": "Campaign detail.",
                }
            ],
        }
    ).encode()

    def fake_urlopen(req, timeout=20.0):
        url = req.full_url if hasattr(req, "full_url") else req.get_full_url()
        assert "model=v60cc" in url
        assert "make=volvo" in url
        resp = MagicMock()
        resp.read.return_value = recall_payload
        resp.__enter__ = lambda s: resp
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    with patch(
        "backend.enrichment.vehicle_history_intelligence.fetch_decode_vin_values_extended",
        return_value=(None, vpic_flat, None),
    ), patch("urllib.request.urlopen", side_effect=fake_urlopen):
        recalls, api_err = fetch_nhtsa_recalls(
            "YV4L12WK5T2170150",
            make="Volvo",
            model="V60 Cross Country",
            year=2026,
        )
    assert api_err is None
    assert len(recalls) == 1
    assert recalls[0]["campaign"] == "24V999"
