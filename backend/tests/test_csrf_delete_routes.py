"""DELETE routes that change account state require the CSRF header once logged in (2026-09-28)."""
from __future__ import annotations

import pytest

from backend.tests.test_hidden_dealers import CSRF, DEALER_ID, _login, make_account_env


@pytest.fixture
def env(monkeypatch, tmp_path, app_factory):
    return make_account_env(monkeypatch, tmp_path, app_factory)


def test_logged_in_delete_without_token_is_forbidden(env):
    client = env["app"].test_client()
    _login(client, env["uid"])
    assert client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}").status_code == 403
    assert client.delete("/api/profile/search-history").status_code == 403
    assert client.delete("/api/profile/search-history/1").status_code == 403


def test_logged_in_delete_with_token_reaches_the_view(env):
    client = env["app"].test_client()
    _login(client, env["uid"])
    hdrs = {"X-CSRF-Token": CSRF}
    # nothing hidden yet: the view answers 404, i.e. the hook let it through
    assert client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}", headers=hdrs).status_code == 404
    assert client.delete("/api/profile/search-history", headers=hdrs).status_code == 200


def test_anonymous_delete_still_gets_401(env):
    client = env["app"].test_client()
    assert client.delete(f"/api/profile/hidden-dealers/{DEALER_ID}").status_code == 401
    assert client.delete("/api/profile/search-history").status_code == 401
