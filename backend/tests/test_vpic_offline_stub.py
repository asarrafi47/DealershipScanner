"""The autouse vPIC offline stub (backend/tests/conftest.py ``_vpic_offline``).

Pins three things: by default no vPIC call leaves the process and callers see
the unreachable-vPIC result; a ``get_json`` a test passes still wins; and
``@pytest.mark.real_vpic_client`` restores the real HTTP client so a test can
mock ``urllib.request.urlopen`` itself.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request

import pytest
import requests

from backend.enrichment import nhtsa_vpic, vpic_facts

VIN = "2HGFC2F59KH123456"


def test_default_decode_is_offline_and_reports_url_error() -> None:
    body, flat, err = nhtsa_vpic.fetch_decode_vin_values_extended(VIN)
    assert (body, flat, err) == (None, None, "url_error")


def test_injected_get_json_still_wins() -> None:
    body, flat, err = nhtsa_vpic.fetch_decode_vin_values_extended(
        VIN, get_json=lambda url: {"Results": [{"Make": "HONDA", "Model": "Civic", "ModelYear": "2019"}]}
    )
    assert err is None and flat["Make"] == "HONDA"


def test_batch_decode_is_offline() -> None:
    with pytest.raises(requests.exceptions.ConnectionError):
        vpic_facts.fetch_vpic_batch([VIN])


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.mark.real_vpic_client
def test_marker_restores_real_client(monkeypatch: pytest.MonkeyPatch) -> None:
    assert nhtsa_vpic.urllib is urllib
    seen: list[str] = []

    def fake_urlopen(req, timeout=None):
        seen.append(req.full_url)
        return _Resp(json.dumps({"Results": [{"Make": "HONDA", "Model": "Civic", "ModelYear": "2019"}]}).encode())

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    _body, flat, err = nhtsa_vpic.fetch_decode_vin_values_extended(VIN)
    assert err is None and flat["Model"] == "Civic"
    assert seen and "vpic.nhtsa.dot.gov" in seen[0] and VIN in seen[0]
