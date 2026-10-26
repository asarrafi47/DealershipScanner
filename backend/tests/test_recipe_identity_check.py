"""recipe_identity_check (N8.5): shared endpoints and foreign detail-page hosts, read-only."""
from __future__ import annotations

import json

from backend.attribution import place
from backend.scripts import recipe_identity_check as ric


def _recipe(url, body=None, **kw):
    return {"url": url, "method": "POST" if body else "GET", "post_template": body, **kw}


def test_byte_copy_recipe_is_listed_with_both_dealers(tmp_path):
    cc = "https://www.mbontario.com/api/listings/2309549?x=1"
    (tmp_path / "mbontario-com.json").write_text(json.dumps([_recipe(cc)]))
    (tmp_path / "mbbeverlyhills-com.json").write_text(json.dumps({"recipes": [_recipe(cc)]}))
    (tmp_path / "other-com.json").write_text(json.dumps([_recipe("https://other.com/inv")]))
    (tmp_path / "gone-com.json").write_text(json.dumps([_recipe(cc, stale=True)]))
    (tmp_path / "_store.json").write_text("{}")
    rep = ric.run(tmp_path)
    assert rep["dealers"] == 3
    assert [f["dealers"] for f in rep["shared_endpoints"]] == [["mbbeverlyhills-com", "mbontario-com"]]


def test_same_url_with_different_body_is_not_shared(tmp_path):
    u = "https://x.com/api"
    (tmp_path / "a-com.json").write_text(json.dumps([_recipe(u, '{"s":"new"}')]))
    (tmp_path / "b-com.json").write_text(json.dumps([_recipe(u, '{"s":"used"}')]))
    assert ric.run(tmp_path)["shared_endpoints"] == []


def test_foreign_vdp_hosts_skip_own_alias_homepage_and_unreadable(monkeypatch):
    monkeypatch.setattr(place, "HOST_ALIASES", {"mbbeverlyhills-com": ("inventory.mbbh.net",)})
    monkeypatch.setattr(place, "_hinted_host_aliases", lambda d: ())
    rows = [
        ("mbbeverlyhills-com", "https://www.mbontario.com/used/vin-1.htm"),
        ("mbbeverlyhills-com", "https://www.mbontario.com/used/vin-2.htm"),
        ("mbbeverlyhills-com", "https://www.mbbeverlyhills.com/used/vin-3.htm"),
        ("mbbeverlyhills-com", "https://inventory.mbbh.net/vdp/4"),
        ("mbbeverlyhills-com", "https://www.mbontario.com/"),
        ("mbbeverlyhills-com", ""),
    ]
    assert ric.foreign_vdp_hosts(rows) == {"mbbeverlyhills-com": {"mbontario.com": 2}}
