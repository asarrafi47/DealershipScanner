"""Route-level guards for the comment API (``backend/routes/community_api.py``).

``test_comments_db.py`` covers the persistence layer. These exercise the HTTP
surface, which is where the authorization decisions actually live: who may post,
who may delete, what a reader is allowed to learn about an author, and how many
times one actor's report may count.

SQLite isolation mirrors ``test_comments_db.py`` — never touches prod Postgres.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from backend.db import comments_db
from backend.db.comments_db import SCOPE_CAR, create_comment, list_comments
from backend.main import app

CSRF = "community-api-test-csrf-token-32-chars-long"
CAR_ID = 101


@pytest.fixture()
def sqlite_inventory(tmp_path, monkeypatch):
    monkeypatch.setenv("INVENTORY_SQLITE_TESTS", "1")
    monkeypatch.setenv("INVENTORY_DATABASE_URL", "")
    monkeypatch.setenv("DATABASE_URL", "")
    # Volume limits are not what these tests are about, and the in-process
    # limiter is shared across the whole session.
    monkeypatch.setenv("COMMENT_FLAG_RPM", "500")
    monkeypatch.setenv("COMMENT_POST_RPM", "500")
    from backend.db import inventory_db

    monkeypatch.setattr(inventory_db, "DB_PATH", os.path.join(str(tmp_path), "inv.db"))
    return inventory_db


@pytest.fixture()
def seeded(sqlite_inventory):
    conn = sqlite_inventory.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS cars (id INTEGER PRIMARY KEY, vin TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS dealerships (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO cars (id, vin) VALUES ({CAR_ID}, 'TESTVIN0000000001')")
        conn.commit()
    finally:
        conn.close()
    return sqlite_inventory


def _client(user_id: int | None = None, ip: str = "203.0.113.10"):
    c = app.test_client()
    c.environ_base["REMOTE_ADDR"] = ip
    with c.session_transaction() as sess:
        sess.clear()
        sess["_csrf_token"] = CSRF
        if user_id is not None:
            sess["user_id"] = user_id
    return c


def _flag(client, comment_id: int):
    return client.post(
        f"/api/cars/{CAR_ID}/comments/{comment_id}/flag",
        headers={"X-CSRF-Token": CSRF},
    )


# ---------------------------------------------------------------------------
# Flag dedupe — one reporter cannot hide a comment alone
# ---------------------------------------------------------------------------

def test_repeat_flags_from_one_reporter_count_once(seeded):
    """Replaying the flag request must not drive a comment past the hide threshold.

    Before ``reporter_hash`` was threaded through, ``FLAG_HIDE_THRESHOLD`` (3)
    requests from a single unauthenticated visitor removed any comment on the
    site from every reader, and the per-IP limiter allows 20 a minute.
    """
    c = create_comment(SCOPE_CAR, CAR_ID, 1, "This dealer added a bogus $1995 package.")
    client = _client(ip="198.51.100.7")

    counts = []
    for _ in range(comments_db.FLAG_HIDE_THRESHOLD + 2):
        rv = _flag(client, c["id"])
        assert rv.status_code == 200
        counts.append(rv.get_json()["flag_count"])

    assert counts == [1] * len(counts), counts
    assert rv.get_json()["is_hidden"] is False
    # Still readable by everyone else — the point of the guard.
    assert len(list_comments(SCOPE_CAR, CAR_ID)) == 1


def test_distinct_reporters_still_reach_the_hide_threshold(seeded):
    """The dedupe must not break real moderation: N different actors still hide."""
    c = create_comment(SCOPE_CAR, CAR_ID, 1, "Buy cheap watches at spam dot example")

    last = None
    for i in range(comments_db.FLAG_HIDE_THRESHOLD):
        rv = _flag(_client(ip=f"198.51.100.{20 + i}"), c["id"])
        assert rv.status_code == 200
        last = rv.get_json()
        assert last["flag_count"] == i + 1

    assert last["is_hidden"] is True
    assert list_comments(SCOPE_CAR, CAR_ID) == []


def test_flagging_a_missing_comment_is_404_and_records_nothing(seeded):
    client = _client(ip="198.51.100.90")
    assert _flag(client, 999999).status_code == 404


# ---------------------------------------------------------------------------
# Author anonymity — the JSON must not carry the commenter's user_id
# ---------------------------------------------------------------------------

def test_list_does_not_expose_the_author_user_id(seeded):
    """Comments render as an anonymous "Shopper"; a stable id would undo that."""
    create_comment(SCOPE_CAR, CAR_ID, 4242, "Sat in it, the seats are cloth not leather.")

    rv = app.test_client().get(f"/api/cars/{CAR_ID}/comments")
    assert rv.status_code == 200
    payload = rv.get_json()
    assert len(payload["comments"]) == 1
    row = payload["comments"][0]
    assert "user_id" not in row
    assert row["is_mine"] is False
    assert "4242" not in rv.get_data(as_text=True)


def test_is_mine_is_true_only_for_the_author(seeded):
    create_comment(SCOPE_CAR, CAR_ID, 4242, "Sat in it, the seats are cloth not leather.")

    mine = _client(user_id=4242).get(f"/api/cars/{CAR_ID}/comments").get_json()
    assert mine["comments"][0]["is_mine"] is True

    theirs = _client(user_id=99).get(f"/api/cars/{CAR_ID}/comments").get_json()
    assert theirs["comments"][0]["is_mine"] is False
    assert "user_id" not in theirs["comments"][0]


def test_created_comment_is_serialized_the_same_way(seeded):
    client = _client(user_id=77)
    rv = client.post(
        f"/api/cars/{CAR_ID}/comments",
        data=json.dumps({"body": "Priced $2k over the others in town."}),
        content_type="application/json",
        headers={"X-CSRF-Token": CSRF},
    )
    assert rv.status_code == 201
    comment = rv.get_json()["comment"]
    assert "user_id" not in comment
    assert comment["is_mine"] is True


# ---------------------------------------------------------------------------
# Write authorization
# ---------------------------------------------------------------------------

def test_anonymous_cannot_post(seeded):
    rv = app.test_client().post(
        f"/api/cars/{CAR_ID}/comments",
        data=json.dumps({"body": "posting without an account"}),
        content_type="application/json",
        headers={"X-CSRF-Token": CSRF},
    )
    assert rv.status_code == 401
    assert rv.get_json()["error"] == "not_logged_in"
    assert list_comments(SCOPE_CAR, CAR_ID) == []


def test_post_without_the_csrf_header_is_rejected(seeded):
    rv = _client(user_id=5).post(
        f"/api/cars/{CAR_ID}/comments",
        data=json.dumps({"body": "no csrf header on this one"}),
        content_type="application/json",
    )
    assert rv.status_code == 403
    assert list_comments(SCOPE_CAR, CAR_ID) == []


def test_a_non_owner_cannot_delete_someone_elses_comment(seeded):
    c = create_comment(SCOPE_CAR, CAR_ID, 1, "The salesperson was straight with me.")

    rv = _client(user_id=2).delete(
        f"/api/cars/{CAR_ID}/comments/{c['id']}", headers={"X-CSRF-Token": CSRF}
    )
    assert rv.status_code == 403
    assert rv.get_json()["error"] == "not_your_comment"
    assert len(list_comments(SCOPE_CAR, CAR_ID)) == 1

    rv = _client(user_id=1).delete(
        f"/api/cars/{CAR_ID}/comments/{c['id']}", headers={"X-CSRF-Token": CSRF}
    )
    assert rv.status_code == 200
    assert list_comments(SCOPE_CAR, CAR_ID) == []


# ---------------------------------------------------------------------------
# Client/server contract
# ---------------------------------------------------------------------------

def test_frontend_sends_the_csrf_header_name_the_server_reads():
    """``validate_csrf_header`` reads ``X-CSRF-Token``; the widget must send that.

    ds_comments.js shipped with ``X-CSRFToken`` (Django's spelling), so every
    post, delete and flag from the UI was answered with 403 by
    ``backend/utils/csrf.py`` — the feature could not be used at all.
    """
    js = (
        Path(__file__).resolve().parents[2] / "frontend" / "static" / "ds_comments.js"
    ).read_text(encoding="utf-8")
    assert "X-CSRF-Token" in js
    assert "X-CSRFToken" not in js


def test_frontend_does_not_reconstruct_ownership_from_a_user_id():
    """The widget must use the server's ``is_mine``; there is no id to compare to."""
    js = (
        Path(__file__).resolve().parents[2] / "frontend" / "static" / "ds_comments.js"
    ).read_text(encoding="utf-8")
    assert "c.user_id" not in js
