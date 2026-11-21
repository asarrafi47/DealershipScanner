"""B-033: a VIN with no decode yet must not stay empty after the decode lands."""
import json
from contextlib import contextmanager

from backend.utils import vpic_specs

VIN = "1HGCM82633A004352"


def test_miss_and_error_not_memoized(monkeypatch) -> None:
    vpic_specs.reset_cache()
    rows: dict = {}

    class _Conn:
        def execute(self, sql, params):
            class _R:
                def fetchone(_s):
                    return rows.get(params[0])
            return _R()

    @contextmanager
    def fake_conn():
        yield _Conn()

    monkeypatch.setattr("backend.db.repositories.base_repo.db_conn", fake_conn)
    assert vpic_specs.vpic_specs_for_vin(VIN) == {}
    rows[VIN] = (json.dumps({"Results": [{"EngineHP": "200"}]}),)
    assert vpic_specs.vpic_specs_for_vin(VIN).get("horsepower") == 200
    # hits are memoized
    rows.clear()
    assert vpic_specs.vpic_specs_for_vin(VIN).get("horsepower") == 200
    vpic_specs.reset_cache()


def test_db_error_not_memoized(monkeypatch) -> None:
    vpic_specs.reset_cache()
    state = {"fail": True}

    @contextmanager
    def fake_conn():
        if state["fail"]:
            raise RuntimeError("db down")
        class _C:
            def execute(self, *a):
                class _R:
                    def fetchone(_s):
                        return (json.dumps({"Results": [{"EngineHP": "150"}]}),)
                return _R()
        yield _C()

    monkeypatch.setattr("backend.db.repositories.base_repo.db_conn", fake_conn)
    assert vpic_specs.vpic_specs_for_vin(VIN) == {}
    state["fail"] = False
    assert vpic_specs.vpic_specs_for_vin(VIN).get("horsepower") == 150
    vpic_specs.reset_cache()
