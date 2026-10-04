"""
Golden for the car detail page (``/car/<id>``) and its view-context builder.

``backend.routes.cars_pages._build_car_detail_view_context`` is the 15-step
pipeline shared by the HTML VDP, ``GET /api/cars/<id>`` and the dev scan lab.
This test pins what it produces for ~300 real cars read from the local
inventory Postgres, as two personas (anonymous, paid member with billing off):

* every context key, and a hash of each JSON-serialisable value;
* a hash of the rendered ``car.html`` (CSRF token, CSP nonce and the static
  asset ``?v=`` stamp normalised out — those change per request / per deploy).

It is a refactor guard, not a hermetic unit test: it needs the local Postgres
(``CAR_DETAIL_GOLDEN_PG_DSN``, default ``postgresql://localhost:5432/cars``)
and is skipped when that is unreachable. The DSN is forced read-only
(``default_transaction_read_only=on``), so nothing here can write to it; the
app itself is imported under the SQLite test mode and only pointed at Postgres
afterwards (import-time DDL would fail on a read-only connection).

Run it with ``PYTHONHASHSEED=0`` (it skips otherwise): ``trim_ladder`` builds some
of its feature lists by iterating sets, so their order follows the per-process
string-hash seed (pre-existing, measured 2026-10-01 on car 873830 — "Skid
Plates"/"Hill Descent Control" swap between seeds).

Re-record (only when the inventory itself changed, never to paper over a diff)::

    PYTHONHASHSEED=0 CAR_DETAIL_GOLDEN_RECORD=1 .venv/bin/python -m pytest -q -p no:cacheprovider \\
        backend/tests/test_car_detail_context_golden.py

Recording writes one shard at a time, so it can be chunked with ``-k shard_0``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "car_detail_context_golden.json"
DEFAULT_DSN = "postgresql://localhost:5432/cars"
N_SHARDS = 6
PERSONAS = ("anon", "paid")
RECORD = os.environ.get("CAR_DETAIL_GOLDEN_RECORD") == "1"


def _ro_dsn() -> str:
    base = (os.environ.get("CAR_DETAIL_GOLDEN_PG_DSN") or DEFAULT_DSN).strip()
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}options=-c%20default_transaction_read_only%3Don"


def _pg_or_skip():
    try:
        import psycopg

        conn = psycopg.connect(_ro_dsn(), connect_timeout=3)
    except Exception as exc:  # noqa: BLE001 - any failure means "no local PG"
        pytest.skip(f"local inventory Postgres unreachable: {exc}")
    ro = conn.execute("SHOW default_transaction_read_only").fetchone()[0]
    assert ro == "on", "golden DSN must be read-only"
    return conn


_ACTIVE = "c.listing_removed_at IS NULL AND COALESCE(c.listing_active, 1) = 1"

_SELECTORS = (
    # (label, sql, n) — targeted buckets first so the stride fill cannot crowd them out.
    ("cdjr", f"SELECT c.id FROM cars c WHERE {_ACTIVE} AND lower(c.make) IN "
             "('chrysler','dodge','jeep','ram') ORDER BY c.id", 20),
    ("attribution", f"SELECT c.id FROM cars c JOIN car_attribution a ON a.car_id = c.id "
                    f"WHERE {_ACTIVE} ORDER BY c.id", 20),
    ("photo_sticker", f"SELECT c.id FROM cars c JOIN car_image_text t ON t.car_id = c.id "
                      f"WHERE {_ACTIVE} AND t.has_sticker ORDER BY c.id", 20),
    ("sticker_or_packages", f"SELECT c.id FROM cars c WHERE {_ACTIVE} AND "
                            "(c.window_sticker_url IS NOT NULL OR c.packages IS NOT NULL) "
                            "ORDER BY c.id", 20),
    ("no_registry", f"SELECT c.id FROM cars c WHERE {_ACTIVE} AND "
                    "c.dealership_registry_id IS NULL ORDER BY c.id", 20),
    ("stride", f"SELECT c.id FROM cars c WHERE {_ACTIVE} ORDER BY c.id", 300),
)


def _select_car_ids(conn, total: int = 300) -> list[int]:
    chosen: list[int] = []
    seen: set[int] = set()
    for _label, sql, n in _SELECTORS:
        try:
            ids = [int(r[0]) for r in conn.execute(sql).fetchall()]
        except Exception:  # noqa: BLE001 - a missing optional table just skips the bucket
            conn.rollback()
            continue
        if not ids:
            continue
        want = min(n, total - len(chosen))
        step = max(1, len(ids) // max(1, want))
        for cid in ids[::step]:
            if len(chosen) >= total or want <= 0:
                break
            if cid not in seen:
                seen.add(cid)
                chosen.append(cid)
                want -= 1
    return sorted(chosen)


def _canon(value) -> str:
    return json.dumps(value, sort_keys=True, default=repr, ensure_ascii=False)


def _h(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:20]


_HTML_NORMALISE = (
    (re.compile(r'(name="csrf-token" content=")[^"]*'), r"\1CSRF"),
    (re.compile(r'(nonce=")[^"]*'), r"\1NONCE"),
    (re.compile(r"([?&]v=)[0-9A-Za-z._-]+"), r"\1V"),
)


def _normalise_html(html: str) -> str:
    for pat, rep in _HTML_NORMALISE:
        html = pat.sub(rep, html)
    return html


@pytest.fixture
def golden_app(app_factory, monkeypatch):
    """backend.main reloaded under the SQLite test mode, then pointed at read-only PG."""
    if os.environ.get("PYTHONHASHSEED") != "0":
        pytest.skip("car detail golden needs PYTHONHASHSEED=0 (trim_ladder set order)")
    conn = _pg_or_skip()
    main = app_factory(BILLING_STRIPE_ENABLED="0", INVENTORY_SQLITE_TESTS="1")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", _ro_dsn())
    monkeypatch.setenv("DATABASE_URL", "")
    # The real schema is migrated; skip the lazy CREATE TABLE IF NOT EXISTS on
    # the read path (it would fail on the read-only connection).
    import backend.db.incomplete_listings_db as ild

    monkeypatch.setattr(ild, "_PG_INC_SCHEMA_OK", True)
    from backend.tests.conftest import _clear_inventory_derived_caches

    _clear_inventory_derived_caches()
    main.app.config["PROPAGATE_EXCEPTIONS"] = True
    try:
        yield main, conn
    finally:
        conn.close()
        _clear_inventory_derived_caches()


def _render(main, monkeypatch, car_ids: list[int]) -> dict:
    captured: dict = {}
    from backend.routes import cars_pages

    real = cars_pages._build_car_detail_view_context

    def recording(car_id, car_raw):
        ctx = real(car_id, car_raw)
        captured["ctx"] = dict(ctx)
        return ctx

    monkeypatch.setattr(cars_pages, "_build_car_detail_view_context", recording)
    out: dict = {}
    for persona in PERSONAS:
        with main.app.test_client() as client:
            if persona == "paid":
                with client.session_transaction() as sess:
                    sess["user_id"] = 1
                    sess["user_is_premium"] = True
            for cid in car_ids:
                captured.clear()
                resp = client.get(f"/car/{cid}")
                rec: dict = {"status": resp.status_code}
                if resp.status_code == 200:
                    ctx = captured["ctx"]
                    rec["keys"] = sorted(ctx)
                    rec["values"] = {k: _h(_canon(ctx[k])) for k in sorted(ctx)}
                    rec["html"] = _h(_normalise_html(resp.get_data(as_text=True)))
                out.setdefault(str(cid), {})[persona] = rec
    return out


def _load() -> dict:
    if not FIXTURE.is_file():
        return {}
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


@pytest.mark.parametrize("shard", range(N_SHARDS), ids=lambda i: f"shard_{i}")
def test_car_detail_context_matches_golden(shard, golden_app, monkeypatch):
    main, conn = golden_app
    data = _load()
    if RECORD:
        if not data.get("car_ids"):
            data = {
                "about": "see backend/tests/test_car_detail_context_golden.py",
                "car_ids": _select_car_ids(conn),
                "records": {},
            }
        ids = data["car_ids"][shard::N_SHARDS]
        data["records"].update(_render(main, monkeypatch, ids))
        FIXTURE.write_text(json.dumps(data, sort_keys=True, indent=1) + "\n", encoding="utf-8")
        return
    if not data:
        pytest.skip("golden fixture not recorded")
    ids = data["car_ids"][shard::N_SHARDS]
    got = _render(main, monkeypatch, ids)
    diffs = []
    for cid in ids:
        for persona in PERSONAS:
            want = data["records"][str(cid)][persona]
            have = got[str(cid)][persona]
            if want == have:
                continue
            if want.get("keys") != have.get("keys"):
                diffs.append((cid, persona, "keys", want.get("keys"), have.get("keys")))
                continue
            bad = [
                k for k in want.get("values", {}) if want["values"][k] != have["values"].get(k)
            ]
            diffs.append((cid, persona, "values", bad, want.get("html") == have.get("html")))
    assert not diffs, f"{len(diffs)} car/persona mismatches, first: {diffs[:5]}"
