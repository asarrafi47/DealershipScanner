"""N21.2 (D-N17b): 17-character IDs holding I, O or Q are kept, never decoded, marked
cannot_assess, and left out of the VIN-owner guard.

Pure tests run everywhere; the heal / write-path tests run on the Postgres harness
(``pg`` tier, skipped without ``TEST_PG_ADMIN_DSN``)."""
from __future__ import annotations

import json

import pytest

from backend.enrichment import vpic_facts as vf
from backend.parsers.base import is_preassignment_vin
from backend.scanner.upsert import guard as G

pg = pytest.mark.pg

IOQ = "1HGCY2F63TA0Q6331"   # Q in the serial
IOQ2 = "ZACCJBBB0JPI12345"  # I in the serial
REAL = "3TYLB5JNXRT022195"


def test_helper_flags_only_17_char_ids_with_i_o_q():
    assert is_preassignment_vin(IOQ) and is_preassignment_vin(IOQ2.lower())
    assert not is_preassignment_vin(REAL)
    assert not is_preassignment_vin("1HGCY2F63TA0Q633")      # 16 characters: D-N17 refuses it elsewhere
    assert not is_preassignment_vin("1HGCY2F63TA0Q63311")    # 18 characters
    assert not is_preassignment_vin(None) and not is_preassignment_vin("")
    assert not is_preassignment_vin("1HGCY2F63TA0-6331")     # not alphanumeric


class _Cur:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql, params=()):
        self.sql.append(sql)

    def fetchall(self):
        return []

    rowcount = 0


class _Conn:
    def __init__(self):
        self.c = _Cur()

    def cursor(self):
        return self.c

    def commit(self):
        pass


def test_decode_skips_ioq_ids_and_counts_them(monkeypatch):
    sent: list[list[str]] = []
    monkeypatch.setattr(vf, "fetch_vpic_batch", lambda chunk, timeout=90: sent.append(list(chunk)) or {})
    monkeypatch.setattr(vf, "_PAUSE_S", 0)
    stats = vf.decode_missing_vins(_Conn(), [IOQ, IOQ2, REAL, IOQ])
    assert stats["requested"] == 1 and stats["preassignment_skipped"] == 2
    assert sent == [[REAL]]  # the I/O/Q IDs never reach vPIC
    only_ioq = vf.decode_missing_vins(_Conn(), [IOQ])
    assert only_ioq["requested"] == 0 and only_ioq["preassignment_skipped"] == 1 and sent == [[REAL]]


def test_decode_stats_unchanged_without_ioq(monkeypatch):
    stats = vf.decode_missing_vins(_Conn(), [], dry_run=True)
    assert "preassignment_skipped" not in stats


def test_override_vehicles_marks_cannot_assess_and_never_looks_up(monkeypatch):
    looked: list[str] = []
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: None)
    monkeypatch.setattr("backend.enrichment.knowledge_engine.lookup_vpic_from_cache",
                        lambda vin: looked.append(vin) or {})
    cars = [{"vin": IOQ, "drivetrain": "RWD"}, {"vin": REAL, "drivetrain": "RWD"}]
    stats = vf.override_vehicles(cars)
    assert stats["preassignment"] == 1 and looked == [REAL]
    marker = json.loads(cars[0]["spec_source_json"])[vf.PREASSIGNMENT_KEY]
    assert marker["cannot_assess"] is True and "drivetrain" in marker["fields"]
    assert "spec_source_json" not in cars[1]
    assert cars[0]["drivetrain"] == "RWD"  # the feed's value is kept, only flagged
    # idempotent: a second pass over the stamped row adds nothing
    assert "preassignment" not in vf.override_vehicles(cars[:1])


def test_marker_is_not_a_field_provenance_entry():
    from backend.vehicle_facts import precedence as prec

    row = {}
    vf._mark_preassignment(row)
    assert prec.field_source("drivetrain", "RWD", row["spec_source_json"]) == prec.UNKNOWN


def test_guard_never_contests_an_ioq_id():
    """Another dealer's row for the same I/O/Q ID is not a conflict (fresh and active)."""
    class Cur:
        def execute(self, sql, params=()):
            self.params = params

        def fetchall(self):
            n = len(G.FACT_COLUMNS)
            base = ("owner-a", 1, "2099-01-01T00:00:00Z", None, "Owner A", None)
            return [(v, None) + base + (None,) * n for v in (IOQ, REAL)]

    by_vin = {IOQ: {"dealer_id": "claimant-b"}, REAL: {"dealer_id": "claimant-b"}}
    window = G.guard_window(24)
    _spec, conflicts = G.prefetch_existing(Cur(), by_vin, window, lambda *a, **k: True)
    assert IOQ not in conflicts
    assert REAL in conflicts


@pg
def test_heal_marks_ioq_rows_without_decoding_them(pg_db, monkeypatch):
    conn = pg_db.get_conn()
    for i, vin in enumerate((IOQ, REAL), start=1):
        conn.execute("INSERT INTO cars (id, vin, dealer_id, title, listing_active, drivetrain) "
                     "VALUES (?, ?, 'd1', 't', 1, 'RWD')", (i, vin))
    conn.commit()
    seen: list[list[str]] = []
    monkeypatch.setattr("backend.enrichment.knowledge_engine.prime_vpic_cache", lambda vins: seen.append(list(vins)))
    stats = vf.heal_rows(conn)
    assert stats["preassignment"] == 1 and stats["preassignment_marked"] == 1
    assert seen == [[REAL]]
    raw = conn.execute("SELECT spec_source_json FROM cars WHERE id=1").fetchone()[0]
    marker = (raw if isinstance(raw, dict) else json.loads(raw))[vf.PREASSIGNMENT_KEY]
    assert marker["cannot_assess"] is True
    again = vf.heal_rows(conn)
    assert again["preassignment_marked"] == 0  # already stamped
    dry = vf.heal_rows(conn, dry_run=True)
    assert dry["preassignment"] == 1


@pg
def test_write_path_lets_a_second_dealer_take_an_ioq_id_and_still_guards_real_vins(pg_db):
    from backend.scanner.database import upsert_vehicles

    def car(dealer, vin):
        return {"vin": vin, "title": "2023 Nissan Altima", "year": 2023, "make": "Nissan", "model": "Altima",
                "price": 30000, "dealer_id": dealer, "dealer_name": dealer, "dealer_url": f"https://{dealer}"}

    upsert_vehicles([car("owner-a", IOQ), car("owner-a", REAL)])
    stats: dict = {}
    n = upsert_vehicles([car("claimant-b", IOQ), car("claimant-b", REAL)], stats)
    conn = pg_db.get_conn()
    owner = dict(conn.execute("SELECT vin, dealer_id FROM cars WHERE vin IN (?, ?)", (IOQ, REAL)).fetchall())
    assert owner[IOQ] == "claimant-b"      # outside the guard
    assert owner[REAL] == "owner-a"        # a real VIN is still protected
    assert n == 1 and stats["vin_owner_conflict_vins"] == [REAL]
