"""Card L12, ``pg`` tier: the mirror export reads ``dealer_recipes`` in a read-only transaction."""

from __future__ import annotations

import json

import pytest

from backend.scripts import ops_publisher

pytestmark = pytest.mark.pg


def _row(dealer, url, **extra):
    row = {"dealer_id": dealer, "url": url, "method": "GET", "content_type": "application/json", "post_template": None,
           "auth_headers": {}, "pagination": "none", "vehicle_rows": 1, "total_count": 1, "provider_hint": "",
           "stale": False, "stale_reason": "", "saved_at": 5.0, "last_ok_at": 5.0, "field_coverage": {}}
    row.update(extra)
    return row


def test_export_is_stable_read_only_and_keeps_headers_and_stale_flags(pg_db, tmp_path):
    sets = {
        "a-com": [_row("a-com", "https://a.example/api", auth_headers={"x-api-key": "k"})],
        "b-com": [_row("b-com", "https://b.example/api", stale=True, stale_reason="401")],
        "c-com": [_row("c-com", "https://c.example/api")],
    }
    with pg_db.connect() as conn:
        for dealer, rows in sets.items():
            conn.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json, recipe_count, scan_hints) VALUES (%s, %s, %s, %s)",
                         (dealer, json.dumps(rows), len(rows), json.dumps({"price_source": "list", "x": 1}) if dealer == "a-com" else None))
        conn.commit()
    with pg_db.connect() as conn:
        read_sets, hints = ops_publisher.read_store(conn)
        with pytest.raises(Exception, match="read-only"):
            conn.execute("INSERT INTO dealer_recipes (dealer_id, recipes_json) VALUES ('z-com', '[]')")
        conn.rollback()
    assert set(read_sets) == set(sets) and hints == {"a-com": {"price_source": "list", "x": 1}}
    out = tmp_path / "m"
    ops_publisher.write_mirror(out, read_sets, hints)
    first = {p.name: p.read_bytes() for p in (out / "mirror").rglob("*") if p.is_file()}
    ops_publisher.write_mirror(out, read_sets, hints)
    assert first == {p.name: p.read_bytes() for p in (out / "mirror").rglob("*") if p.is_file()}
    assert b"x-api-key" in first["a-com.json"] and b'"stale": true' in first["b-com.json"]
    assert json.loads(first["scan_hints.json"]) == {"a-com": {"price_source": "list"}}


def test_pg_store_round_trip_caps_and_retention(pg_db):
    from backend.scanner.loop import store as ls

    s = ls.PgStore(connect=pg_db.connect)
    assert s.put("findings/a.md", b"one", "cloud") is True
    assert s.put("findings/a.md", b"one", "cloud") is False
    assert s.put("findings/a.md", b"two", "cloud") is True
    assert s.get("findings/a.md") == b"two" and s.exists("findings/a.md")
    for day in range(1, 5):
        s.put(f"runs/fleet_2026110{day}T090000Z/MANIFEST.json", b"{}")
    assert ls.prune_runs(s, 2) == 2
    assert [e.path for e in s.list("runs/")] == ["runs/fleet_20261103T090000Z/MANIFEST.json",
                                                 "runs/fleet_20261104T090000Z/MANIFEST.json"]
    assert s.list("findings/")[0].size == 3 and s.total_bytes() >= 3
    small = ls.PgStore(connect=pg_db.connect, max_total=s.total_bytes())
    with pytest.raises(ls.StoreError, match="full"):
        small.put("findings/b.md", b"x")
    assert s.delete_prefix("findings/") == 1
    with pytest.raises(Exception):  # the table's own check: a path the application would never write
        with pg_db.connect() as conn:
            conn.execute("INSERT INTO public.loop_files (path, body, sha256, size) VALUES ('', ''::bytea, 'x', 0)")
