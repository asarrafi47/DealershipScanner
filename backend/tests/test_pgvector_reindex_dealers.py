"""B-034: _reindex_dealers must take state from the state column, not latitude."""

from __future__ import annotations

from backend.vector import pgvector_service as svc


class _Cur:
    def __init__(self, rows=None, log=None):
        self._rows = rows
        self.log = log if log is not None else []

    def execute(self, sql, params=None):
        self.log.append((sql, params))

    def fetchall(self):
        return self._rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _SqlConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cur(self._rows)

    def close(self):
        pass


class _PgConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return _Cur(log=self.log)

    def commit(self):
        pass


def test_reindex_dealers_float_latitude_does_not_crash(monkeypatch):
    rows = [(7, "Acme Honda", "https://a.example", "Charlotte", "NC", 35.2271, -80.8431)]
    monkeypatch.setattr(svc, "inventory_get_conn", lambda: _SqlConn(rows))
    monkeypatch.setattr(svc, "_model_name", lambda: "m")
    monkeypatch.setattr(svc, "_embed_texts", lambda docs: [[0.0] for _ in docs])
    pg = _PgConn()
    assert svc._reindex_dealers(pg) == 1
    insert = [p for s, p in pg.log if "INSERT" in s][0]
    assert insert[3].obj == {"dealer_id": 7, "state": "NC"}
