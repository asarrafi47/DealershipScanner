"""NHTSA vPIC client (no network)."""

from __future__ import annotations

from backend.nhtsa_vpic import (
    _build_engine_description,
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
                "EngineConfiguration": "In-Line",
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
    assert patch.get("engine_description") == "1.5L I4"


def test_engine_description_durango_style_hemi() -> None:
    flat = {
        "DisplacementL": "5.7",
        "EngineConfiguration": "V-Shaped",
        "EngineCylinders": "8",
        "EngineModel": "HEMI MDS VVT eTorque",
    }
    assert _build_engine_description(flat) == "5.7L V8 HEMI MDS VVT eTorque"
    assert flat_vpic_result_to_car_patch(flat).get("engine_description") == "5.7L V8 HEMI MDS VVT eTorque"


def test_engine_description_v6_pentastar() -> None:
    flat = {
        "DisplacementL": "3.6",
        "EngineConfiguration": "V-shaped",
        "EngineCylinders": "6",
        "EngineModel": "Pentastar",
    }
    assert _build_engine_description(flat) == "3.6L V6 Pentastar"


def test_engine_description_i4_turbo_model_word() -> None:
    flat = {
        "DisplacementL": "2.0",
        "EngineConfiguration": "In-Line",
        "EngineCylinders": "4",
        "EngineModel": "Turbocharged DOHC",
    }
    assert _build_engine_description(flat) == "2.0L I4 Turbocharged DOHC"


def test_engine_description_v_shaped_missing_cylinders_keeps_raw_config() -> None:
    flat = {
        "DisplacementL": "3.0",
        "EngineConfiguration": "V-Shaped",
        "EngineModel": "DOHC 24V",
    }
    assert _build_engine_description(flat) == "3.0L V-Shaped DOHC 24V"


def test_engine_description_dedup_displacement_and_layout_in_model() -> None:
    flat = {
        "DisplacementL": "5.7",
        "EngineConfiguration": "V-Shaped",
        "EngineCylinders": "8",
        "EngineModel": "5.7L V8 OHV 16V",
    }
    assert _build_engine_description(flat) == "5.7L V8 OHV 16V"


def test_engine_description_ci_no_l_suffix() -> None:
    flat = {
        "DisplacementCI": "345",
        "EngineConfiguration": "V-Shaped",
        "EngineCylinders": "8",
        "EngineModel": "HEMI",
    }
    assert _build_engine_description(flat) == "345 CI V8 HEMI"


def test_engine_description_turbo_flag() -> None:
    flat = {
        "DisplacementL": "2.0",
        "EngineConfiguration": "In-Line",
        "EngineCylinders": "4",
        "Turbo": "Y",
    }
    assert _build_engine_description(flat) == "2.0L I4 Turbo"


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
