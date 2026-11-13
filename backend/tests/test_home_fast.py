"""The signed-in /home page must not wait for the recommendation block (2026-11-13).

Pins: the shell renders without calling the slow build; the deferred fragment works and
degrades to "nothing" on failure; a stale value is served at once and rebuilt once; the
page's own queries are batched and cached; the car-page prefetch storm is gone.
"""
from __future__ import annotations

import re
import threading
from pathlib import Path

import pytest

from backend.tests.test_home_dashboard_routes import _insert_test_car, _make_app

ROOT = Path(__file__).resolve().parents[2]

ROWS = [{"id": 99, "title": "2020 Test Wagon", "price": 1000, "mileage": 5, "gallery": [], "image_url": ""}]
HEADING = {"eyebrow": "Based on your history", "title": "Recommended for You", "hint": ""}


@pytest.fixture
def home(monkeypatch, tmp_path, app_factory):
    app, save_user = _make_app(app_factory, monkeypatch, tmp_path)
    c = app.test_client()
    uid = save_user("fastuser", "fast@example.com", "longpassword123", role="general", org_id=None)
    with c.session_transaction() as sess:
        sess["user_id"] = uid
        sess["username"] = "fastuser"
        sess["mfa_ok"] = True
    car_id = _insert_test_car()
    from backend.db.user_history_db import record_car_view

    record_car_view(int(uid), car_id)
    from backend.routes import home_dashboard as hd

    yield c, int(uid), car_id, hd


def test_shell_renders_without_running_the_slow_build(home, monkeypatch):
    c, _uid, _car, hd = home

    def boom(*a, **k):
        raise AssertionError("the page must not run the recommendation build")

    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", boom)
    r = c.get("/home")
    html = r.data.decode()
    assert r.status_code == 200
    assert 'id="viewed-section"' in html  # the fast parts are there
    assert 'data-reco-src="/home?part=reco"' in html  # the slow block is announced
    assert "reco-track" not in html.split("<script", 1)[0]


def test_deferred_fragment_returns_the_section(home, monkeypatch):
    c, _uid, _car, hd = home
    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", lambda *a, **k: (list(ROWS), dict(HEADING)))
    r = c.get("/home?part=reco")
    assert r.status_code == 200
    assert 'id="reco-section"' in r.data.decode()
    assert "2020 Test Wagon" in r.data.decode()
    assert r.headers["Cache-Control"] == "private, no-store"
    # now cached: the next full page carries the section inline, no placeholder
    html = c.get("/home").data.decode()
    assert "2020 Test Wagon" in html and "data-reco-src=\"" not in html


def test_failing_recommendations_degrade_to_nothing(home, monkeypatch):
    c, _uid, _car, hd = home

    def boom(*a, **k):
        raise RuntimeError("scoring blew up")

    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", boom)
    assert c.get("/home").status_code == 200
    r = c.get("/home?part=reco")
    assert r.status_code == 204 and r.data == b""


def test_fragment_needs_a_session(home):
    c, _uid, _car, _hd = home
    with c.session_transaction() as sess:
        sess.clear()
    r = c.get("/home?part=reco", follow_redirects=False)
    assert r.status_code in (302, 303)


def test_stale_value_is_served_at_once_and_rebuilt_once(home, monkeypatch):
    _c, uid, _car, hd = home
    calls = []
    gate = threading.Event()

    def build(user_id, limit, serialize=True, **kw):
        calls.append(1)
        gate.wait(5)
        return [{"id": len(calls)}], dict(HEADING)

    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", build)
    gate.set()
    rows, _ = hd._recommendations_for_user(uid)
    assert rows == [{"id": 1}] and len(calls) == 1
    rows, _ = hd._recommendations_for_user(uid)  # fresh hit
    assert len(calls) == 1

    gate.clear()
    hd._invalidate_reco_cache(uid)  # a car view arrived: stale, not gone
    for _ in range(3):  # three visits while the rebuild runs
        rows, _ = hd._recommendations_for_user(uid)
        assert rows == [{"id": 1}]  # the old value, instantly
    ref = hd._reco_refreshers[hd._reco_key(uid, 20, {})]
    gate.set()
    assert ref.join(5)
    assert len(calls) == 2  # ONE rebuild for the three visits
    rows, _ = hd._recommendations_for_user(uid)
    assert rows == [{"id": 2}]
    assert len(calls) == 2


def test_failed_background_rebuild_keeps_the_old_value(home, monkeypatch):
    _c, uid, _car, hd = home
    state = {"fail": False}

    def build(user_id, limit, serialize=True, **kw):
        if state["fail"]:
            raise RuntimeError("db down")
        return [{"id": 1}], dict(HEADING)

    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", build)
    hd._recommendations_for_user(uid)
    state["fail"] = True
    hd._invalidate_reco_cache(uid)
    rows, _ = hd._recommendations_for_user(uid)
    hd._reco_refreshers[hd._reco_key(uid, 20, {})].join(5)
    assert rows == [{"id": 1}]
    rows, _ = hd._recommendations_for_user(uid)
    assert rows == [{"id": 1}]


def test_hidden_dealer_change_drops_the_recommendations(home, monkeypatch):
    _c, uid, _car, hd = home
    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", lambda *a, **k: ([{"id": 1}], dict(HEADING)))
    hd._recommendations_for_user(uid)
    hd._invalidate_reco_cache(uid, hard=True)
    assert hd._reco_lookup(hd._reco_key(uid, 20, {}))[0] == "miss"


def test_cold_concurrent_visits_share_one_build(home, monkeypatch):
    _c, uid, _car, hd = home
    calls = []
    gate = threading.Event()

    def build(user_id, limit, serialize=True, **kw):
        calls.append(1)
        gate.wait(5)
        return [{"id": 1}], dict(HEADING)

    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", build)
    out = []
    ts = [threading.Thread(target=lambda: out.append(hd._recommendations_for_user(uid)[0])) for _ in range(4)]
    for t in ts:
        t.start()
    gate.set()
    for t in ts:
        t.join(5)
    assert len(calls) == 1 and out == [[{"id": 1}]] * 4


def test_lists_are_cached_and_a_save_shows_at_once(home, monkeypatch):
    c, _uid, car_id, hd = home
    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", lambda *a, **k: ([], dict(HEADING)))
    reads = []
    real = hd.get_cars_by_ids
    monkeypatch.setattr(hd, "get_cars_by_ids", lambda ids: (reads.append(1), real(ids))[1])
    c.get("/home")
    c.get("/home")
    assert len(reads) == 1  # the second visit read nothing
    with c.session_transaction() as sess:
        sess["_csrf_token"] = "t" * 32
    r = c.post(f"/api/cars/{car_id}/save", headers={"X-CSRF-Token": "t" * 32})
    assert r.status_code == 200 and r.get_json()["saved"] is True
    html = c.get("/home").data.decode()
    assert len(reads) == 2
    assert "No saved cars yet" not in html


def test_home_lists_use_one_inventory_read_and_one_history_open(home, monkeypatch):
    c, _uid, _car, hd = home
    monkeypatch.setattr(hd, "_recommendations_for_user_uncached", lambda *a, **k: ([], dict(HEADING)))
    hist, reads, attrib = [], [], []
    rh, rc, rs = hd.get_recent_history_ids, hd.get_cars_by_ids, hd.serialize_cars_for_listings_grid
    monkeypatch.setattr(hd, "get_recent_history_ids", lambda *a, **k: (hist.append(1), rh(*a, **k))[1])
    monkeypatch.setattr(hd, "get_cars_by_ids", lambda ids: (reads.append(1), rc(ids))[1])
    monkeypatch.setattr(hd, "serialize_cars_for_listings_grid", lambda cs: (attrib.append(1), rs(cs))[1])
    c.get("/home")
    assert (len(hist), len(reads), len(attrib)) == (1, 1, 1)


def test_prefetch_never_fires_on_home_and_is_small():
    js = (ROOT / "frontend" / "static" / "nav_perf.js").read_text()
    m = re.search(r"var WARM_LIMIT = (\d+);", js)
    assert m and 1 <= int(m.group(1)) <= 4
    warm = js.split("var warm = function", 1)[1].split("};", 1)[0]
    assert 'p === "/home"' not in warm and 'p === "/dashboard"' not in warm
    assert 'p === "/listings"' in warm
    m = re.search(r"var WARM_IDLE_TIMEOUT_MS = (\d+);", js)
    assert m and int(m.group(1)) >= 2000
    assert "Math.min(limit || WARM_LIMIT, WARM_LIMIT)" in js
