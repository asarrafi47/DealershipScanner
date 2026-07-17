"""Incomplete listings index, listings filter fail-closed, and dev dealership stats."""

from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from unittest.mock import patch

import bcrypt
import pytest

from backend.db import incomplete_listings_db as ild
from backend.db import inventory_db as inv_db


def _minimal_row(**kwargs):
    base = {
        "id": 1,
        "vin": "1HGBH41JXMN109186",
        "title": "2020 Honda Civic LX",
        "year": 2020,
        "make": "Honda",
        "model": "Civic",
        "trim": "LX",
        "price": 22000,
        "mileage": 5000,
        "transmission": "CVT",
        "drivetrain": "FWD",
        "fuel_type": "Gasoline",
        "exterior_color": "Crystal Black",
        "interior_color": "Gray",
        "image_url": "https://example.com/hero.jpg",
        "gallery": [],
        "body_style": "Sedan",
        "engine_description": "1.5L turbo I4",
        "condition": "Used",
        "cylinders": 4,
        "dealer_name": "Test Motors",
        "dealer_id": "test-motors",
    }
    base.update(kwargs)
    return base


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_filter_public_listings_fail_closed_when_index_unavailable(_mock_prep) -> None:
    inv_db.clear_inventory_listings_cache()
    incomplete = _minimal_row(id=10, transmission=None)
    complete = _minimal_row(id=11)
    with patch(
        "backend.db.incomplete_listings_db.get_incomplete_car_id_set",
        side_effect=RuntimeError("index unavailable"),
    ):
        out = inv_db._filter_public_listings_cars(
            [incomplete, complete],
            include_incomplete=False,
        )
    ids = {c["id"] for c in out}
    assert 11 in ids
    assert 10 not in ids


@patch(
    "backend.utils.listing_completeness.prepare_car_detail_context",
    return_value={"verified_specs": {}},
)
def test_invalid_car_id_uses_per_row_completeness(_mock_prep) -> None:
    inv_db.clear_inventory_listings_cache()
    bad_id_incomplete = _minimal_row(id=None, transmission=None)
    bad_id_incomplete.pop("id", None)
    snapshot = inv_db._IncompleteIndexSnapshot(ids=frozenset({99}))
    assert inv_db._car_is_publicly_incomplete(bad_id_incomplete, snapshot) is True


def test_listings_cache_token_includes_incomplete_db_mtime(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    inv_path = tmp_path / "inventory.db"
    inc_path = tmp_path / "incomplete_listings.db"
    monkeypatch.setattr(inv_db, "DB_PATH", str(inv_path))
    monkeypatch.setattr(ild, "DB_PATH", str(inc_path))
    inv_db.clear_inventory_listings_cache()

    inv_path.write_bytes(b"")
    inc_path.write_bytes(b"")
    t1 = inv_db._listings_cache_token()

    time.sleep(0.02)
    inc_path.write_bytes(b"x")
    t2 = inv_db._listings_cache_token()
    assert t2[1] != t1[1]


def test_dealership_issue_stats_uses_incomplete_index(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    inv_path = tmp_path / "inventory.db"
    inc_path = tmp_path / "incomplete_listings.db"
    monkeypatch.setattr(ild, "DB_PATH", str(inc_path))

    conn = sqlite3.connect(inv_path)
    conn.execute(
        """
        CREATE TABLE cars (
            id INTEGER PRIMARY KEY,
            dealer_name TEXT,
            dealer_id TEXT,
            marked_for_review INTEGER,
            data_quality_score REAL,
            price REAL
        )
        """
    )
    conn.execute(
        "INSERT INTO cars (id, dealer_name, dealer_id, marked_for_review, data_quality_score, price) VALUES (?,?,?,?,?,?)",
        (1, "Alpha", "alpha", 0, 0.8, 10000),
    )
    conn.execute(
        "INSERT INTO cars (id, dealer_name, dealer_id, marked_for_review, data_quality_score, price) VALUES (?,?,?,?,?,?)",
        (2, "Alpha", "alpha", 0, 0.7, 12000),
    )
    conn.commit()
    conn.close()

    inc = sqlite3.connect(inc_path)
    inc.execute(
        """
        CREATE TABLE incomplete_listings (
            car_id INTEGER PRIMARY KEY,
            vin TEXT,
            missing_fields_json TEXT,
            updated_at TEXT
        )
        """
    )
    inc.execute(
        "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (?,?,?,?)",
        (1, "VIN1", '["transmission"]', "2026-01-01T00:00:00+00:00"),
    )
    inc.commit()
    inc.close()

    @contextmanager
    def fake_db_conn(*, row_factory=None):
        c = sqlite3.connect(inv_path)
        if row_factory is not None:
            c.row_factory = row_factory
        try:
            yield c
        finally:
            c.close()

    monkeypatch.setattr(inv_db, "db_conn", fake_db_conn)

    stats = inv_db.get_dealership_issue_stats(limit=5)
    assert len(stats) == 1
    assert stats[0]["dealer_id"] == "alpha"
    assert stats[0]["incomplete_count"] == 1
    assert stats[0]["total_cars"] == 2


def test_fast_rebuild_is_atomic_on_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    inv_path = tmp_path / "inventory.db"
    inc_path = tmp_path / "incomplete_listings.db"
    monkeypatch.setattr(inv_db, "DB_PATH", str(inv_path))
    monkeypatch.setattr(ild, "DB_PATH", str(inc_path))

    conn = sqlite3.connect(inv_path)
    conn.execute(
        """
        CREATE TABLE cars (
            id INTEGER PRIMARY KEY,
            vin TEXT,
            title TEXT,
            year INTEGER,
            make TEXT,
            model TEXT,
            trim TEXT,
            price REAL,
            mileage INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            fuel_type TEXT,
            exterior_color TEXT,
            interior_color TEXT,
            image_url TEXT,
            gallery TEXT,
            body_style TEXT,
            engine_description TEXT,
            condition TEXT,
            cylinders INTEGER,
            listing_active INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO cars (id, vin, title, year, make, model, trim, price, mileage, transmission) "
        "VALUES (1, 'VIN', 't', 2020, 'Honda', 'Civic', 'LX', 1, 1, 'CVT')"
    )
    conn.commit()
    conn.close()

    inc = sqlite3.connect(inc_path)
    ild._ensure_schema(inc)
    inc.execute(
        "INSERT INTO incomplete_listings (car_id, vin, missing_fields_json, updated_at) VALUES (?,?,?,?)",
        (99, "OLD", '["trim"]', "2026-01-01T00:00:00+00:00"),
    )
    inc.commit()
    inc.close()

    with patch.object(ild, "listing_missing_field_codes", side_effect=RuntimeError("boom")):
        with pytest.raises(RuntimeError):
            ild.fast_rebuild_incomplete_listings_index()

    inc2 = sqlite3.connect(inc_path)
    n = inc2.execute("SELECT COUNT(*) FROM incomplete_listings").fetchone()[0]
    inc2.close()
    assert int(n) == 1


def test_rehash_does_not_overwrite_newer_password(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setenv("FLASK_ENV", "development")
    monkeypatch.setenv("USERS_DB_PATH", str(tmp_path / "users_race.db"))
    monkeypatch.delenv("USERS_DB_ENCRYPTION_KEY", raising=False)
    monkeypatch.setenv("BCRYPT_ROUNDS", "13")

    from backend.db import users_db

    users_db.init_users_db()
    uid = users_db.save_user("race_user", "race@example.com", "Original1!")
    weak = bcrypt.hashpw(b"Original1!", bcrypt.gensalt(rounds=12)).decode("utf-8")
    conn = users_db.get_conn()
    conn.execute("UPDATE users SET password = ? WHERE id = ?", (weak, uid))
    conn.commit()
    conn.close()

    users_db._schedule_password_rehash(uid, "Original1!", weak)
    from backend.db.password_hash import hash_password, verify_password

    new_hash = hash_password("Replacement1!")
    conn = users_db.get_conn()
    conn.execute("UPDATE users SET password = ? WHERE id = ?", (new_hash, uid))
    conn.commit()
    conn.close()

    deadline = time.time() + 2.0
    while time.time() < deadline:
        conn = users_db.get_conn()
        row = conn.execute("SELECT password FROM users WHERE id = ?", (uid,)).fetchone()
        conn.close()
        assert row is not None
        if row[0] == new_hash:
            break
        time.sleep(0.05)
    assert row[0] == new_hash
    assert verify_password("Replacement1!", row[0]) is True
