"""The assess loop must not hold a transaction open across slow non-DB work (2026-09-28).

Production's idle_in_transaction_session_timeout killed three shards' assess
connections on the first Railway fleet run.
"""
from __future__ import annotations

from backend.scripts import dealer_pipeline as dp
from backend.tests.pipeline_patch import patch_pipeline


class _Raw:
    def __init__(self):
        self.autocommit = False
        self.closed = False


class _Conn:
    def __init__(self):
        self._raw = _Raw()


def test_assess_conn_is_autocommit(monkeypatch):
    patch_pipeline(monkeypatch, "get_conn", _Conn)
    conn = dp._assess_conn()
    assert conn._raw.autocommit is True


def test_conn_alive_detects_a_closed_connection():
    c = _Conn()
    assert dp._conn_alive(c)
    c._raw.closed = True
    assert not dp._conn_alive(c)


def test_sqlite_style_connection_without_autocommit_is_left_alone(monkeypatch):
    class _Plain:
        pass

    patch_pipeline(monkeypatch, "get_conn", _Plain)
    conn = dp._assess_conn()
    assert isinstance(conn, _Plain) and dp._conn_alive(conn)
