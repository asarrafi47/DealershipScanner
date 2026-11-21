"""The search parser's vocabulary load: same output, a fraction of the work (2026-11-20).

``_load_inventory_keywords`` used to run the regex colour inference on every active row
(~200k: 23-55 s of GIL-bound work on the first ``/listings?q=`` after a start). It now
infers once per DISTINCT colour triple; ``_load_package_names`` parses each distinct
packages JSON once. Both must return exactly what the per-row versions returned: the
reference implementations below are the pre-change code, verbatim, run on the same
sample rows. The Postgres cache-key refresh (stale-while-revalidate) is pinned too.
"""

from __future__ import annotations

import json
import random
import sqlite3

import pytest

from backend.utils import query_parser as qp
from backend.utils.field_clean import is_effectively_empty
from backend.utils.interior_color_buckets import (
    infer_paint_color_buckets,
    parse_stored_buckets,
    sort_paint_family_ids,
)

_EXTERIORS = [
    "Agate Black Metallic", "Oxford White", "Rapid Red Metallic Tinted Clearcoat", "Iconic Silver",
    "Carbonized Gray", "Atlas Blue", "Star White Pearl", "Burgundy", "Champagne", "Bronze Fire",
    "Forged Green", "Sunset Orange", "Lunar Rock", "Yellow Jacket", "Purple Haze", "Tan", "Brown",
    "Gold Mist", "BLK", "WHT", "N/A", "", None, "unknown", "Red/Black", "  Silver  ", "Ruby Flare",
]
_INTERIORS = [
    "Ebony", "Black Leather", "Gray Cloth", "Beige", "Saddle Brown", "Red", "Ivory", "Tan Leatherette",
    "Charcoal", "Jet Black/Light Ash", "", None, "N/A", "Parchment",
]
_BUCKETS = [None, "", "[]", '["black"]', '["gray","black"]', '["red"]', "not json", '["nonsense"]', '["beige"]']
_PKG_NAMES = ["Premium Package", "premium package", "Tech Pkg", "Towing", "Sport Appearance", "Cold Weather"]


def _packages(rng: random.Random) -> str | None:
    kind = rng.randrange(7)
    if kind == 0:
        return None
    if kind == 1:
        return rng.choice(["{}", "[]", "null", "", "not json", '["a list"]'])
    names = rng.sample(_PKG_NAMES, rng.randrange(1, 4))
    return json.dumps({
        "packages_normalized": [{"canonical_name": names[0]}, {"name": " Tech Pkg "}, "junk"],
        "possible_packages": names[1:],
        "sticker_options_priced": [{"label": rng.choice(_PKG_NAMES)}],
        "sticker_options": [rng.choice(_PKG_NAMES), "  "],
    })


@pytest.fixture()
def sample_db(monkeypatch):
    rng = random.Random(20261120)
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.execute(
        "CREATE TABLE cars (id INTEGER PRIMARY KEY, make TEXT, model TEXT, trim TEXT, body_style TEXT, "
        "exterior_color TEXT, interior_color TEXT, interior_color_buckets TEXT, packages TEXT, "
        "listing_active INTEGER)"
    )
    rows = []
    for i in range(3000):
        rows.append((
            i + 1,
            rng.choice(["Toyota", "Honda", "BMW", None]),
            rng.choice(["Camry", "Civic", "X5", None]),
            rng.choice(["XLE", "Sport Touring", "xDrive40i", "LE", None, " "]),
            rng.choice(["SUV", "Sedan", "", None]),
            rng.choice(_EXTERIORS),
            rng.choice(_INTERIORS),
            rng.choice(_BUCKETS),
            _packages(rng),
            rng.choice([1, 1, 1, 0, None]),
        ))
    conn.executemany("INSERT INTO cars VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    conn.commit()

    class _NoClose:
        def __init__(self, c):
            self._c = c

        def cursor(self):
            return self._c.cursor()

        def close(self):
            pass

    monkeypatch.setattr(qp, "get_conn", lambda: _NoClose(conn))
    qp.clear_query_parser_caches()
    yield conn
    qp.clear_query_parser_caches()


# ── the pre-change implementations, verbatim (reference) ────────────────────────────


def _old_colours(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT exterior_color, interior_color, interior_color_buckets
        FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
        """
    )
    ext_ids: set[str] = set()
    int_ids: set[str] = set()
    for ext_raw, int_raw, int_bucks in cur.fetchall():
        if not is_effectively_empty(ext_raw):
            ext_ids.update(infer_paint_color_buckets(ext_raw, None))
        ib = parse_stored_buckets(int_bucks)
        if ib:
            int_ids.update(ib)
        elif not is_effectively_empty(int_raw):
            int_ids.update(infer_paint_color_buckets(int_raw, None))
    return sort_paint_family_ids(ext_ids), sort_paint_family_ids(int_ids)


def _old_package_names(conn):
    cur = conn.cursor()
    cur.execute(
        """
        SELECT packages FROM cars
        WHERE (COALESCE(listing_active, 1) = 1)
          AND packages IS NOT NULL
          AND packages NOT IN ('{}', '[]', 'null', '')
        """
    )
    seen: set[str] = set()
    names: list[str] = []
    for (pkg_raw,) in cur.fetchall():
        try:
            pj = json.loads(pkg_raw)
        except Exception:
            continue
        if not isinstance(pj, dict):
            continue
        row_names: list[str] = []
        for entry in pj.get("packages_normalized") or []:
            if isinstance(entry, dict):
                n = (entry.get("canonical_name") or entry.get("name") or "").strip()
                if n:
                    row_names.append(n)
        for n in pj.get("possible_packages") or []:
            if isinstance(n, str) and n.strip():
                row_names.append(n.strip())
        for priced in pj.get("sticker_options_priced") or []:
            if isinstance(priced, dict):
                n = (priced.get("name") or priced.get("label") or "").strip()
                if n:
                    row_names.append(n)
        for n in pj.get("sticker_options") or []:
            if isinstance(n, str) and n.strip():
                row_names.append(n.strip())
        for n in row_names:
            low = n.lower()
            if low not in seen:
                seen.add(low)
                names.append(n)
    names.sort(key=lambda s: len(s), reverse=True)
    return tuple(names)


# ── equivalence ──────────────────────────────────────────────────────────────────────


def test_keywords_match_the_per_row_implementation(sample_db):
    pairs, ext, inte, bodies, trims = qp._load_inventory_keywords("k1")
    old_ext, old_int = _old_colours(sample_db)
    assert ext == tuple(old_ext) and inte == tuple(old_int)
    assert ext and inte  # the sample exercises real buckets
    assert pairs and bodies and trims


def test_package_names_match_the_per_row_implementation(sample_db):
    assert qp._load_package_names("k1") == _old_package_names(sample_db)
    assert qp._load_package_names("k1")


def test_paint_ids_from_distinct_rows_equal_all_rows(sample_db):
    all_rows = sample_db.execute(
        "SELECT exterior_color, interior_color, interior_color_buckets FROM cars"
    ).fetchall()
    distinct = list(dict.fromkeys(all_rows))
    assert len(distinct) < len(all_rows)
    assert qp._paint_family_ids(distinct) == qp._paint_family_ids(all_rows)


def test_inference_runs_once_per_distinct_string(sample_db, monkeypatch):
    calls: list = []
    real = qp.infer_paint_color_buckets
    monkeypatch.setattr(qp, "infer_paint_color_buckets", lambda raw, make=None: calls.append(raw) or real(raw, make))
    qp._load_inventory_keywords("k2")
    assert len(calls) == len(set(calls)) <= len(_EXTERIORS) + len(_INTERIORS)


# ── the Postgres cache key (stale-while-revalidate) ──────────────────────────────────


@pytest.fixture()
def fresh_pg_key(monkeypatch):
    """Generation state reset for the test and restored afterwards."""
    monkeypatch.setattr("backend.db.inventory_pg.is_inventory_postgres", lambda: True)
    monkeypatch.setattr(qp, "_pg_vocab_generation", 0)
    monkeypatch.setattr(qp, "_pg_vocab_started_at", None)
    monkeypatch.setattr(qp, "_pg_vocab_refresh_thread", None)


def test_postgres_key_refreshes_in_the_background(monkeypatch, fresh_pg_key):
    monkeypatch.setenv("PARSER_VOCAB_TTL_S", "100")
    built: list[str] = []
    monkeypatch.setattr(qp, "_load_inventory_keywords", lambda key: built.append(key) or ((), (), (), (), ()))
    monkeypatch.setattr(qp, "_load_package_names", lambda key: ())
    clock = [1000.0]
    monkeypatch.setattr(qp.time, "monotonic", lambda: clock[0])

    assert qp._inventory_db_cache_key() == "pg:0"
    clock[0] += 50
    assert qp._inventory_db_cache_key() == "pg:0" and qp._pg_vocab_refresh_thread is None
    clock[0] += 60  # past the TTL: the stale key is still served, the next is built off-thread
    assert qp._inventory_db_cache_key() == "pg:0"
    qp._pg_vocab_refresh_thread.join(5)
    assert built == ["pg:1"]
    assert qp._inventory_db_cache_key() == "pg:1"


def test_postgres_key_never_refreshes_with_ttl_zero(monkeypatch, fresh_pg_key):
    monkeypatch.setenv("PARSER_VOCAB_TTL_S", "0")
    clock = [0.0]
    monkeypatch.setattr(qp.time, "monotonic", lambda: clock[0])
    qp._inventory_db_cache_key()
    clock[0] += 10**9
    assert qp._inventory_db_cache_key() == "pg:0"


def test_failed_refresh_keeps_the_current_vocabulary(monkeypatch, fresh_pg_key):
    monkeypatch.setenv("PARSER_VOCAB_TTL_S", "1")

    def boom(key):
        raise RuntimeError("db down")

    monkeypatch.setattr(qp, "_load_inventory_keywords", boom)
    clock = [0.0]
    monkeypatch.setattr(qp.time, "monotonic", lambda: clock[0])
    qp._inventory_db_cache_key()
    clock[0] += 5
    qp._inventory_db_cache_key()
    qp._pg_vocab_refresh_thread.join(5)
    assert qp._inventory_db_cache_key() == "pg:0"
