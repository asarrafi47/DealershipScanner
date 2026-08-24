"""
Tests for the sticker-price reconciliation gate (backend/scripts/image_batch.py).

Two layers are covered:

  * ``_reconciles`` itself -- the arithmetic: a Monroney is an addition, so its parts
    either sum to its total or the reading is wrong somewhere. Only the impossible
    direction (parts EXCEED the whole) refuses; a shortfall is the normal case, because
    agents miss options in glare bands and destination is sometimes folded in.

  * the recording gate inside ``cmd_record`` -- when the arithmetic disproves the
    total, the itemization must be refused along with it, not persisted into
    car_image_text where the registry feed would push the disproven prices into the
    shared price book. A 10-car sticker re-read found 9 of 69 stored option prices
    wrong, all single-row column drift, all provable by the sum-check: this gate is
    the arithmetic backstop for exactly that class.

The real functions are imported; only I/O (the DB connection and the follow-up
corpus scrub, which needs one) is mocked.
"""

from __future__ import annotations

import json
import types

import backend.scripts.image_batch as image_batch
from backend.scripts.image_batch import _reconciles


# --- _reconciles: the arithmetic ---------------------------------------------------


def test_parts_that_sum_to_the_total_pass() -> None:
    ok, why = _reconciles(30_000, [{"name": "Premium Package", "price": 2_000.0}], 32_000.0)
    assert ok
    assert why == ""


def test_parts_exceeding_the_total_refuse_with_a_reason() -> None:
    # 60,000 + 5,000 = 65,000 against a claimed 62,000 total: a price is on the
    # wrong row somewhere. The reason must say so, and carry the numbers.
    ok, why = _reconciles(60_000, [{"name": "Premium Package", "price": 5_000.0}], 62_000.0)
    assert not ok
    assert "exceeds" in why
    assert "65,000" in why and "62,000" in why


def test_shortfall_is_deliberately_allowed() -> None:
    # Options get missed in glare bands and destination is sometimes folded in, so
    # parts summing to LESS than the total is the normal case, not a defect.
    ok, why = _reconciles(30_000, [], 40_000.0)
    assert ok
    assert why == ""


def test_tolerance_absorbs_rounding_and_one_unlisted_fee() -> None:
    # 1.5% of the total absorbs rounding and a single unlisted fee: a $400 overage
    # on a $30,000 total sits inside the band and must not refuse.
    ok, _ = _reconciles(30_000, [{"name": "x", "price": 400.0}], 30_000.0)
    assert ok


def test_missing_base_is_not_evidence_of_a_defect() -> None:
    ok, why = _reconciles(None, [{"name": "x", "price": 99_999.0}], 20_000.0)
    assert ok
    assert why == ""


def test_unparseable_base_is_not_evidence_of_a_defect() -> None:
    ok, _ = _reconciles("not a number", [{"name": "x", "price": 5_000.0}], 20_000.0)
    assert ok


def test_missing_total_is_not_evidence_of_a_defect() -> None:
    ok, why = _reconciles(30_000, [{"name": "x", "price": 5_000.0}], 0.0)
    assert ok
    assert why == ""


def test_unparseable_option_prices_are_skipped_not_fatal() -> None:
    ok, _ = _reconciles(
        30_000,
        [{"name": "x", "price": "??"}, {"name": "y", "price": 1_000.0}],
        32_000.0,
    )
    assert ok


# --- the recording gate ------------------------------------------------------------


class _FakeCursor:
    """Records every execute(); answers the reservation SELECT from a preset list."""

    def __init__(self, reserved: list[int]) -> None:
        self._reserved = reserved
        self.calls: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.calls.append((sql, params))

    def fetchall(self) -> list[tuple[int]]:
        return [(cid,) for cid in self._reserved]


class _FakeConn:
    def __init__(self, cur: _FakeCursor) -> None:
        self._cur = cur

    def cursor(self) -> _FakeCursor:
        return self._cur


def _agent_item(**overrides) -> dict:
    """A results-file entry that passes every MSRP gate before reconciliation."""
    item = {
        "car_id": 883394,
        "vin": "WBX73EF05R5Z12345",
        "vin_confirmed": True,
        "has_window_sticker": True,
        "msrp_read_directly": True,
        "sticker_is_original": True,
        "sticker_currency": "USD",
        "msrp": 62_000,
        "base_msrp": 60_000,
        # Package names on purpose: the accessory skew-guard exempts bundles, so these
        # reach the reconciliation gate rather than being filtered upstream of it.
        "options": [
            {"name": "Premium Package", "price": 5_000},
            {"name": "Driving Assistance Package", "price": 1_700},
        ],
        "equipment": ["Heated Front Seats"],
        "packages": ["Shadowline Package"],
        "images_read": 5,
        "gallery_size": 20,
    }
    item.update(overrides)
    return item


def _run_record(tmp_path, monkeypatch, item: dict) -> tuple[dict, tuple]:
    """Run the real cmd_record over one item; return (summary JSON, INSERT params)."""
    cur = _FakeCursor(reserved=[item["car_id"]])
    monkeypatch.setattr(image_batch, "_connect", lambda: _FakeConn(cur))
    # The post-record corpus scrub opens its own connection and reads the whole
    # table; it is corpus statistics, not the gate under test.
    monkeypatch.setattr(image_batch, "cmd_scrub_skew", lambda _args: 0)
    results = tmp_path / "results.json"
    results.write_text(json.dumps([item]))
    args = types.SimpleNamespace(results=str(results), allow_unreserved=False)
    assert image_batch.cmd_record(args) == 0
    inserts = [c for c in cur.calls if "INSERT INTO car_image_text" in c[0]]
    assert len(inserts) == 1
    _sql, params = inserts[0]
    return json.loads(params[3]), params


def test_refusal_empties_priced_options_and_keeps_the_reason(tmp_path, monkeypatch) -> None:
    # base 60,000 + options 6,700 = 66,700 against a claimed 62,000 total: refused.
    summary, params = _run_record(tmp_path, monkeypatch, _agent_item())
    assert summary["priced_options"] == []
    assert summary["priced_options_refused"]
    assert "exceeds" in summary["priced_options_refused"]
    # The raw note persists too -- an audit must tell "never read" from "read and refused".
    assert summary["msrp_reconcile_refused"]
    assert summary["sticker_msrp"] is None
    # The sticker_msrp COLUMN is refused as well, not just the summary field.
    assert params[8] is None


def test_refusal_keeps_option_names_as_unpriced_packages(tmp_path, monkeypatch) -> None:
    # The option being ON the car is still a true observation; only its number is
    # disproven. Names land in the existing packages list -- no new display path.
    summary, _ = _run_record(tmp_path, monkeypatch, _agent_item())
    assert "Premium Package" in summary["packages"]
    assert "Driving Assistance Package" in summary["packages"]
    assert "Shadowline Package" in summary["packages"]


def test_refusal_does_not_duplicate_names_already_in_packages(tmp_path, monkeypatch) -> None:
    item = _agent_item(packages=["Premium Package"])
    summary, _ = _run_record(tmp_path, monkeypatch, item)
    assert summary["packages"].count("Premium Package") == 1


def test_successful_reconciliation_records_everything_intact(tmp_path, monkeypatch) -> None:
    # 66,700 against a claimed 68,000 total: a shortfall, the normal case.
    summary, params = _run_record(tmp_path, monkeypatch, _agent_item(msrp=68_000))
    assert summary["priced_options"] == [
        {"name": "Premium Package", "price": 5_000.0},
        {"name": "Driving Assistance Package", "price": 1_700.0},
    ]
    assert summary["priced_options_refused"] is None
    assert summary["msrp_reconcile_refused"] is None
    assert summary["sticker_msrp"] == 68_000.0
    assert params[8] == 68_000.0
    assert summary["packages"] == ["Shadowline Package"]


def test_build_sheet_total_is_stored_with_its_document_type(tmp_path, monkeypatch) -> None:
    # 2026-08-18 policy: sticker_is_original no longer gates STORAGE. A dealer
    # build sheet's total is recorded, with the document type persisted so every
    # reader can label it as what it is.
    summary, params = _run_record(
        tmp_path, monkeypatch, _agent_item(msrp=68_000, sticker_is_original=False)
    )
    assert summary["sticker_msrp"] == 68_000.0
    assert params[8] == 68_000.0
    assert summary["sticker_is_original"] is False
    assert summary["msrp_document"] == "dealer_build_sheet"


def test_original_sticker_is_recorded_as_a_monroney_document(tmp_path, monkeypatch) -> None:
    summary, _ = _run_record(tmp_path, monkeypatch, _agent_item(msrp=68_000))
    assert summary["sticker_is_original"] is True
    assert summary["msrp_document"] == "monroney"


def test_build_sheet_total_still_fails_reconciliation(tmp_path, monkeypatch) -> None:
    # The arithmetic gate survives the policy change: a build-sheet total its own
    # parts disprove is refused exactly like a Monroney's would be.
    summary, params = _run_record(
        tmp_path, monkeypatch, _agent_item(sticker_is_original=False)
    )
    assert summary["sticker_msrp"] is None
    assert params[8] is None
    assert summary["msrp_reconcile_refused"]
    assert summary["msrp_document"] == "dealer_build_sheet"


def test_no_msrp_means_no_reconciliation_and_options_survive(tmp_path, monkeypatch) -> None:
    # With no accepted total there is nothing to reconcile against; refusing the
    # itemization on that basis would destroy real observations for no evidence.
    item = _agent_item(msrp_read_directly=False)
    summary, params = _run_record(tmp_path, monkeypatch, item)
    assert summary["sticker_msrp"] is None
    assert len(summary["priced_options"]) == 2
    assert summary["priced_options_refused"] is None


# ---------------------------------------------------------------------------
# _clean_equipment_list: record-time equipment normalization
# ---------------------------------------------------------------------------

from backend.scripts.image_batch import _clean_equipment_list


def test_flat_comma_blob_splits_into_features():
    out = _clean_equipment_list(
        ["AM/FM Radio, Android Auto, Apple CarPlay, Bluetooth Connection, WiFi Hotspot"]
    )
    assert "Android Auto" in out and "WiFi Hotspot" in out
    assert len(out) == 5


def test_structured_package_lines_kept_whole():
    pkg = "M Sport Package Pro (8-Speed Sport Automatic, M Sport Brakes with Red Calipers)"
    suite = "Honda Sensing: Adaptive Cruise Control, Collision Mitigation Braking"
    assert _clean_equipment_list([pkg]) == [pkg]
    assert _clean_equipment_list([suite]) == [suite]


def test_warranty_and_cpo_marketing_dropped():
    out = _clean_equipment_list(
        [
            "Mercedes-Benz Certified Pre-Owned program (12-month warranty, $0 deductible)",
            "Panoramic sunroof",
        ]
    )
    assert out == ["Panoramic sunroof"]


def test_short_lists_and_descriptions_not_split():
    # 3 comma segments = a coherent description, not a packed list
    desc = "Chevrolet Infotainment 3 Premium with Google built-in, 13.4-in touchscreen, wireless CarPlay"
    assert _clean_equipment_list([desc]) == [desc]


def test_dedupes_case_insensitively():
    out = _clean_equipment_list(["Heated Front Seat", "heated front seat", "Tow Hitch"])
    assert out == ["Heated Front Seat", "Tow Hitch"]
