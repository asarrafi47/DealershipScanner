"""Tests for backend.scripts.heal_stock_code_contamination (fake connection, no network)."""
from __future__ import annotations

from backend.scripts import heal_stock_code_contamination as heal


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 1

    def execute(self, sql, params=None):
        self._conn.executed.append((" ".join(sql.split()), tuple(params or ())))
        return self

    def fetchall(self):
        return self._conn.rows


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.executed: list[tuple[str, tuple]] = []
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1


def test_detect_full_signature():
    tok, carriers = heal.detect_contamination_token(
        {"drivetrain": "R1111", "transmission": "R1111",
         "exterior_color": "R1111", "interior_color": "R1111"}
    )
    assert tok == "R1111"
    assert carriers == ("drivetrain", "transmission", "exterior_color", "interior_color")


def test_detect_partial_signature_keeps_real_color():
    tok, carriers = heal.detect_contamination_token(
        {"drivetrain": "T0493", "transmission": "T0493",
         "exterior_color": "Cosmic Black", "interior_color": "T0493"}
    )
    assert tok == "T0493"
    assert carriers == ("drivetrain", "transmission", "interior_color")


def test_detect_rejects_genuine_matching_values():
    # Real colors equal across ext/int must NOT be treated as contamination.
    assert heal.detect_contamination_token(
        {"drivetrain": "AWD", "transmission": "Automatic",
         "exterior_color": "Black", "interior_color": "Black"}
    ) is None
    # A single stock-shaped value in one field only is not the signature either.
    assert heal.detect_contamination_token(
        {"drivetrain": "FR040", "transmission": None,
         "exterior_color": "", "interior_color": None}
    ) is None


def test_detect_handles_longer_token_shapes():
    for tok in ("T0417AA", "DTDTS1214", "FP164T", "UK0005"):
        hit = heal.detect_contamination_token(
            {"drivetrain": tok, "transmission": tok,
             "exterior_color": tok, "interior_color": tok}
        )
        assert hit is not None and hit[0] == tok


def test_apply_heal_updates_only_token_fields():
    plan = heal.HealPlan(
        car_id=42,
        vin="1C4PJXAG9TW336148",
        token="T0493",
        prior={"stock_number": None, "drivetrain": "T0493", "transmission": "T0493",
               "exterior_color": "Cosmic Black", "interior_color": "T0493"},
        fields_to_null=("drivetrain", "transmission", "interior_color"),
        set_stock_number=True,
    )
    conn = _FakeConn(rows=[])
    counts = heal.apply_heal(conn, [plan])
    assert counts == {
        "stock_number": 1, "drivetrain": 1, "transmission": 1,
        "exterior_color": 0, "interior_color": 1, "mileage": 0,
    }
    assert len(conn.executed) == 1
    sql, params = conn.executed[0]
    assert "stock_number = %s" in sql
    assert "exterior_color" not in sql  # genuine color untouched
    assert "mileage" not in sql  # null_mileage=False -> mileage untouched
    assert params == ("T0493", 42)
    assert conn.commits == 1


def test_token_digits_and_mileage_match():
    assert heal.token_digits("UK0005") == 5
    assert heal.token_digits("R1111") == 1111
    assert heal.token_digits("DTS1373") == 1373
    assert heal.token_digits("ABC") is None
    assert heal.mileage_matches_token(5, "UK0005")
    assert heal.mileage_matches_token(1111, "R1111")
    assert not heal.mileage_matches_token(189645, "UK0005")  # genuine odometer kept
    assert not heal.mileage_matches_token(None, "UK0005")


def test_apply_heal_nulls_token_derived_mileage():
    plan = heal.HealPlan(
        car_id=99,
        vin="4T1BF1FK0HU328802",
        token="UK0005",
        prior={"stock_number": None, "mileage": 5, "drivetrain": "UK0005",
               "transmission": "UK0005", "exterior_color": "UK0005", "interior_color": "UK0005"},
        fields_to_null=("drivetrain", "transmission", "exterior_color", "interior_color"),
        set_stock_number=True,
        null_mileage=True,
    )
    conn = _FakeConn(rows=[])
    counts = heal.apply_heal(conn, [plan])
    assert counts["mileage"] == 1
    sql, params = conn.executed[0]
    assert "mileage = NULL" in sql
    assert params == ("UK0005", 99)


def test_apply_heal_residual_mileage_only():
    # Row already healed by an earlier run: token in stock_number, fields NULL,
    # only the digit-matching mileage remains to be nulled.
    plan = heal.HealPlan(
        car_id=101,
        vin="3C7WRMCL2RG204890",
        token="R1111",
        prior={"stock_number": "R1111", "mileage": 1111, "drivetrain": None,
               "transmission": None, "exterior_color": None, "interior_color": None},
        fields_to_null=(),
        set_stock_number=False,
        null_mileage=True,
    )
    conn = _FakeConn(rows=[])
    counts = heal.apply_heal(conn, [plan])
    assert counts == {
        "stock_number": 0, "drivetrain": 0, "transmission": 0,
        "exterior_color": 0, "interior_color": 0, "mileage": 1,
    }
    sql, params = conn.executed[0]
    assert sql.startswith("UPDATE cars SET mileage = NULL")
    assert params == (101,)


def test_vpic_rederive_skips_mileage_only_plans():
    plan = heal.HealPlan(
        car_id=102, vin="3C7WRMCL2RG204890", token="R1111", prior={},
        fields_to_null=(), set_stock_number=False, null_mileage=True,
    )

    def fake_post(url, data):  # pragma: no cover - must not be called
        raise AssertionError("vPIC should not be queried for mileage-only plans")

    conn = _FakeConn(rows=[])
    counts = heal.apply_vpic_rederive(conn, [plan], post_json=fake_post, sleep_s=0)
    assert counts == {"drivetrain": 0, "transmission": 0, "vins_decoded": 0}
    assert conn.executed == []


def test_vpic_rederive_confident_only():
    plan = heal.HealPlan(
        car_id=7,
        vin="3C7WRMCL2RG204890",
        token="R1111",
        prior={},
        fields_to_null=("drivetrain", "transmission", "exterior_color", "interior_color"),
        set_stock_number=True,
    )

    def fake_post(url, data):
        assert "3C7WRMCL2RG204890" in data
        return {"Results": [{
            "VIN": "3C7WRMCL2RG204890",
            "DriveType": "4WD/4-Wheel Drive/4x4",
            "TransmissionStyle": "Automatic",
            "TransmissionSpeeds": "8",
        }]}

    conn = _FakeConn(rows=[])
    counts = heal.apply_vpic_rederive(conn, [plan], post_json=fake_post, sleep_s=0)
    assert counts["drivetrain"] == 1
    assert counts["transmission"] == 1
    assert plan.vpic_drivetrain == "4WD"
    assert plan.vpic_transmission == "8-Speed Automatic"
    sql, params = conn.executed[0]
    assert "drivetrain IS NULL" in sql and "transmission IS NULL" in sql
    assert params == ("4WD", "8-Speed Automatic", 7)


def test_vpic_rederive_skips_unconfident_drivetrain():
    plan = heal.HealPlan(
        car_id=8, vin="3C7WRMCL2RG204890", token="R1111", prior={},
        fields_to_null=("drivetrain", "transmission"), set_stock_number=True,
    )

    def fake_post(url, data):
        return {"Results": [{
            "VIN": "3C7WRMCL2RG204890",
            "DriveType": "Parallel",  # unrecognized layout -> not confident
            "TransmissionStyle": "",
        }]}

    conn = _FakeConn(rows=[])
    counts = heal.apply_vpic_rederive(conn, [plan], post_json=fake_post, sleep_s=0)
    assert counts == {"drivetrain": 0, "transmission": 0, "vins_decoded": 1}
    assert conn.executed == []
    assert "vpic:nothing_confident" in plan.notes
