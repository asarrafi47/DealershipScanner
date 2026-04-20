"""NHTSA vPIC client (no network)."""

from __future__ import annotations

from backend.nhtsa_vpic import (
    decode_vpic_http_response,
    fetch_decode_vin_values_extended,
    flat_vpic_result_to_car_patch,
    looks_like_decode_vin,
)


def test_looks_like_decode_vin() -> None:
    assert looks_like_decode_vin("1HGBH41JXMN109185")
    assert not looks_like_decode_vin("UNKNOWN1234567890")
    assert not looks_like_decode_vin("SHORT")
    assert not looks_like_decode_vin(None)


def test_decode_and_flat_patch() -> None:
    body = {
        "Results": [
            {
                "Make": "HONDA",
                "Model": "Accord",
                "ModelYear": "2020",
                "TransmissionStyle": "Automatic",
                "TransmissionSpeeds": "10",
                "DriveType": "Front-Wheel Drive (FWD)",
                "FuelTypePrimary": "Gasoline",
                "EngineCylinders": "4",
                "BodyClass": "Sedan/Saloon",
                "DisplacementL": "1.5",
            }
        ]
    }
    flat = decode_vpic_http_response(body)
    assert flat is not None
    patch = flat_vpic_result_to_car_patch(flat)
    assert patch.get("make") == "Honda"
    assert patch.get("model") == "Accord"
    assert patch.get("year") == 2020
    assert patch.get("drivetrain") == "FWD"
    assert "Automatic" in (patch.get("transmission") or "")
    assert patch.get("body_style") == "Sedan/Saloon"


def test_fetch_decode_uses_injected_get_json() -> None:
    sample = {
        "Results": [
            {
                "Make": "HONDA",
                "Model": "Civic",
                "ModelYear": "2019",
                "ErrorText": "",
            }
        ]
    }

    def fake_get(_url: str) -> dict:
        return sample

    body, flat, err = fetch_decode_vin_values_extended("2HGFC2F59KH123456", get_json=fake_get)
    assert err is None
    assert body == sample
    assert flat is not None
    assert flat.get("Model") == "Civic"


def test_fetch_decode_http_error() -> None:
    import urllib.error

    def boom(_url: str) -> dict:
        raise urllib.error.HTTPError("url", 500, "msg", hdrs=None, fp=None)

    body, flat, err = fetch_decode_vin_values_extended("2HGFC2F59KH123456", get_json=boom)
    assert body is None and flat is None
    assert err == "http_500"
