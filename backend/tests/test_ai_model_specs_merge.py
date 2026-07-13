"""ai_model_specs merge into lookup_epa_extended_specs.

Real (epa_extended_specs) values always win; ai_model_specs only fills nulls.
Uses a fake cursor so no DB/network is required.
"""
import backend.enrichment.knowledge_engine as ke


class _FakeCursor:
    """Serves canned rows: first the epa_extended_specs query, then ai_model_specs."""

    def __init__(self, epa_row, ai_row):
        self._epa_row = epa_row
        self._ai_row = ai_row
        self._last = None

    def execute(self, sql, params=None):
        self._last = "ai" if "ai_model_specs" in sql else "epa"

    def fetchone(self):
        return self._ai_row if self._last == "ai" else self._epa_row


def _epa_dict(**kw):
    # Build a row tuple aligned to _EXTENDED_SPECS_COLUMNS from kwargs.
    return tuple(kw.get(c) for c in ke._EXTENDED_SPECS_COLUMNS)


def _ai_row(**kw):
    return tuple(kw.get(c) for c in ke._AI_SPEC_COLUMNS)


def test_ai_fills_only_missing_fields():
    # Real row has horsepower; ai has hp+torque+tow. Real hp must win, ai fills the rest.
    epa = _epa_dict(horsepower=300)
    ai = _ai_row(horsepower=999, torque_lb_ft=260, tow_capacity_lb=5000)
    cur = _FakeCursor(epa, ai)
    out = ke._merge_ai_model_specs(cur, 2020, "Jeep", "Compass", ke._extended_specs_row_to_dict(epa))
    assert out["horsepower"] == 300      # real value preserved, NOT overwritten by ai 999
    assert out["torque_lb_ft"] == 260    # ai filled the null
    assert out["tow_capacity_lb"] == 5000


def test_ai_row_alone_when_no_real_data():
    ai = _ai_row(horsepower=180, fuel_tank_gal=15.8)
    cur = _FakeCursor(None, ai)
    out = ke._merge_ai_model_specs(cur, 2019, "Honda", "Civic", {})
    assert out["horsepower"] == 180
    assert out["fuel_tank_gal"] == 15.8


def test_no_ai_row_leaves_result_unchanged():
    epa = _epa_dict(horsepower=290)
    cur = _FakeCursor(epa, None)
    base = ke._extended_specs_row_to_dict(epa)
    out = ke._merge_ai_model_specs(cur, 2021, "Ford", "Escape", dict(base))
    assert out == base


def test_missing_table_degrades_gracefully():
    class _Boom:
        def execute(self, *a, **k):
            raise RuntimeError("relation ai_model_specs does not exist")
        def fetchone(self):
            raise AssertionError("should not reach fetchone")

    base = {"horsepower": 250}
    out = ke._merge_ai_model_specs(_Boom(), 2022, "Kia", "Sportage", dict(base))
    assert out == base  # real data still returned when the AI table is absent
