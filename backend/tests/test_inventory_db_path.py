"""Default inventory DB path resolution in backend.db.repositories.base_repo.

Regression tests for the repositories split: base_repo.py lives one directory
deeper than the original backend/db/inventory_db.py, so ``_REPO_ROOT`` needs
three ``os.pardir`` hops. Getting it wrong silently redirects the default DB
to ``backend/inventory.db`` unconditionally and kills the documented
``<repo>/inventory.db`` fallback.
"""

from __future__ import annotations

import os
import sqlite3

from backend.db.repositories import base_repo


def test_repo_root_is_the_repository_root() -> None:
    """_REPO_ROOT must be the directory that *contains* backend/, not backend/ itself."""
    assert os.path.basename(base_repo._REPO_ROOT) != "backend"
    assert os.path.isdir(os.path.join(base_repo._REPO_ROOT, "backend", "db", "repositories"))
    # The facade re-exports the same value.
    from backend.db import inventory_db

    assert inventory_db._REPO_ROOT == base_repo._REPO_ROOT


def test_default_path_prefers_backend_copy_with_cars_table(tmp_path, monkeypatch) -> None:
    (tmp_path / "backend").mkdir()
    backend_db = tmp_path / "backend" / "inventory.db"
    conn = sqlite3.connect(backend_db)
    conn.execute("CREATE TABLE cars (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

    monkeypatch.setattr(base_repo, "_REPO_ROOT", str(tmp_path))
    assert base_repo._default_inventory_db_path() == str(backend_db)


def test_default_path_falls_back_to_repo_root(tmp_path, monkeypatch) -> None:
    """No backend/inventory.db (or one without a cars table) => <repo>/inventory.db."""
    (tmp_path / "backend").mkdir()
    monkeypatch.setattr(base_repo, "_REPO_ROOT", str(tmp_path))
    assert base_repo._default_inventory_db_path() == str(tmp_path / "inventory.db")

    # A backend copy without a cars table is also skipped.
    backend_db = tmp_path / "backend" / "inventory.db"
    conn = sqlite3.connect(backend_db)
    conn.execute("CREATE TABLE not_cars (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    assert base_repo._default_inventory_db_path() == str(tmp_path / "inventory.db")
