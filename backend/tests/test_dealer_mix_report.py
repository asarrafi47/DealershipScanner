"""Dealer kind heuristic, the dealer mix report script and the fixture composition.

docs/independent_dealers/ANALYSIS.md quotes the fixture counts pinned here; if the heuristic
changes, these tests and that section change together.
"""
from __future__ import annotations

import ast
import gzip
import json
import re
from collections import Counter
from pathlib import Path

import pytest

from backend.discovery.dealer_kind import KINDS, classify_dealer, host_from_dealer_id
from backend.scripts import dealer_mix_report as rpt

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "backend" / "scripts" / "dealer_mix_report.py"
MODULE = REPO / "backend" / "discovery" / "dealer_kind.py"
FIXTURES = REPO / "backend" / "tests" / "fixtures" / "ops_publish"
SHARDS = ("scan_2026_11_03_s0", "scan_2026_11_03_s3", "scan_2026_11_03_s6")


def _fixture_dealers() -> list[dict]:
    out: list[dict] = []
    for s in SHARDS:
        out += json.load(gzip.open(FIXTURES / s / "triage.json.gz"))["dealers"]
    return out


def _kind(e: dict) -> str:
    return classify_dealer(dealer_id=e["dealer_id"], rows_new=e.get("rows_new"),
                           rows_used=e.get("rows_used"))[0]


# --- heuristic cases --------------------------------------------------------------------

@pytest.mark.parametrize("kw, kind", [
    ({"dealer_id": "longotoyota-com"}, "franchise"),
    ({"name": "Tindol Ford", "host": "https://www.tindolford.com/"}, "franchise"),
    ({"dealer_id": "mbzla-com"}, "franchise"),
    ({"dealer_id": "tuttleclickstustinjeep-com"}, "franchise"),
    ({"dealer_id": "coastlinecdjr-com"}, "franchise"),
    ({"dealer_id": "oxfordautosales-com"}, "independent"),  # "ford" inside a place name
    ({"dealer_id": "stanfordmotorcars-com"}, "independent"),
    ({"dealer_id": "quantumautosales-com"}, "independent"),
    ({"name": "ABC Auto Sales", "host": "abcautosales.net"}, "independent"),
    ({"name": "Smith Pre-Owned"}, "independent"),
    ({"dealer_id": "joescars-com", "rows_new": 0, "rows_used": 40}, "independent"),
    ({"dealer_id": "joescars-com", "rows_new": 0, "rows_used": 40,
      "makes": ["ford", "kia", "bmw", "honda", "audi"]}, "independent"),
    ({"dealer_id": "joescars-com", "rows_new": 0, "rows_used": 40, "makes": ["ford"]}, "unknown"),
    ({"dealer_id": "jasonlewisautomotive-com", "rows_new": 165, "rows_used": 169}, "franchise"),
    ({"dealer_id": "paradiseautos-com", "rows_new": 254, "rows_used": 76}, "franchise"),
    ({"dealer_id": "carmax-com"}, "national_retailer"),
    ({"name": "Carvana Vending Machine"}, "national_retailer"),
    ({"dealer_id": "vroom-com"}, "national_retailer"),
    ({"host": "https://www.cars.com/dealers/123/"}, "marketplace"),
    ({"host": "m.facebook.com"}, "marketplace"),
    ({"host": "www.carsforsale.com"}, "marketplace"),
    ({"dealer_id": "smithgroup-com"}, "unknown"),
    ({}, "unknown"),
])
def test_classify_cases(kw, kind):
    got, signal = classify_dealer(**kw)
    assert got == kind, (kw, got, signal)
    assert got in KINDS and signal


def test_host_from_dealer_id():
    assert host_from_dealer_id("davekirk-com") == "davekirk.com"
    assert host_from_dealer_id("northcountrycjdr-cars") == "northcountrycjdr.cars"
    assert host_from_dealer_id("cleveland-nissan-com") == "cleveland-nissan.com"


def test_module_is_stdlib_only():
    tree = ast.parse(MODULE.read_text(encoding="utf-8"))
    names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    names |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert names <= {"__future__", "re", "collections"}, names


# --- fixture composition (ANALYSIS.md section 3) -----------------------------------------

def test_fixture_composition_counts():
    dealers = _fixture_dealers()
    assert len(dealers) == 172
    kinds = Counter(_kind(e) for e in dealers)
    assert kinds == Counter({"franchise": 166, "independent": 6})
    verdicts = {k: Counter(e["verdict"] for e in dealers if _kind(e) == k) for k in kinds}
    assert verdicts["franchise"] == Counter({"ok": 100, "inaccurate": 54, "thin": 11, "no_rows": 1})
    assert verdicts["independent"] == Counter({"ok": 1, "thin": 2, "inaccurate": 1, "no_rows": 2})
    indep = sorted(e["dealer_id"] for e in dealers if _kind(e) == "independent")
    assert indep == ["autoboutiqueohio-com", "autosavvy-com", "lbmboaz-com", "overdrive-usa-com",
                     "premiumautosinc-com", "quantumautosales-com"]
    plat = Counter(e.get("provider") for e in dealers if _kind(e) == "independent")
    assert plat == Counter({"overfuel": 2, "dealer_dot_com": 2, "html_cards": 1, "dealer_eprocess": 1})
    signals = Counter(classify_dealer(dealer_id=e["dealer_id"], rows_new=e.get("rows_new"),
                                      rows_used=e.get("rows_used"))[1].split(":")[0] for e in dealers)
    assert signals == Counter({"host_brand": 159, "sells_new": 7, "lot_word": 4, "used_only": 2})


def test_fixture_every_dealer_had_a_recipe():
    # The fixtures hold only dealers already on the roster: no no_recipe or error verdicts.
    dealers = _fixture_dealers()
    assert all((e.get("recipe") or {}).get("had_recipes") for e in dealers)
    assert not [e for e in dealers if e["verdict"] in ("no_recipe", "error")]


# --- report script: read-only, SELECT only, no SQLite ------------------------------------

def _string_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


_WRITE_RE = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|UPSERT|CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COPY|VACUUM|"
    r"REINDEX|CLUSTER|COMMENT\s+ON|LOCK|CALL|DO|REFRESH)\b"
)


def test_script_has_no_write_or_ddl_statements():
    for s in _string_literals(SCRIPT):
        assert not _WRITE_RE.search(s.upper()), s
    for sql in rpt.QUERIES:
        assert sql.lstrip().upper().startswith("SELECT"), sql
        assert ";" not in sql


def test_script_sets_read_only_and_timeout():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "default_transaction_read_only=on" in text
    assert "statement_timeout=" in text
    assert "conn.read_only = True" in text
    assert "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY" in text
    assert "INVENTORY_DATABASE_URL" in text


def test_script_imports_and_no_sqlite():
    tree = ast.parse(SCRIPT.read_text(encoding="utf-8"))
    mods = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
    mods |= {(n.module or "").split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)}
    assert mods <= {"json", "os", "re", "sys", "collections", "backend", "psycopg"}, mods
    banned = "sql" + "ite"
    assert banned not in SCRIPT.read_text(encoding="utf-8").lower()
    assert banned not in MODULE.read_text(encoding="utf-8").lower()


def test_script_has_no_future_import_so_it_can_be_concatenated():
    # The Railway command sends dealer_kind.py followed by this script to ``python3 -``.
    assert "from __future__" not in SCRIPT.read_text(encoding="utf-8")
    combined = MODULE.read_text(encoding="utf-8") + "\n" + SCRIPT.read_text(encoding="utf-8")
    compile(combined, "<combined>", "exec")


def test_script_prints_no_vin_or_names():
    text = SCRIPT.read_text(encoding="utf-8")
    assert "vin" not in {m.lower() for m in re.findall(r"\b\w+\b", " ".join(rpt.QUERIES))}
    for col in ("phone", "street_address", "latitude", "email"):
        assert col not in " ".join(rpt.QUERIES)
    assert '"name"' not in text.split("def render", 1)[1]


def test_summarize_and_render_offline():
    cars = [
        {"dealer_id": "quantumautosales-com", "active": 12, "active_new": 0, "active_used": 12,
         "makes": ["ford", "bmw", "kia", "honda", "audi"], "name": "Quantum Auto Sales",
         "0-1d": 10, "61d+": 2, "age_unknown": 0},
        {"dealer_id": "longotoyota-com", "active": 900, "active_new": 700, "active_used": 200,
         "makes": ["toyota"], "name": "Longo Toyota", "0-1d": 900, "age_unknown": 0},
    ]
    recipes = [{"dealer_id": "quantumautosales-com", "recipe_count": 1, "provider": "html_cards",
                "scan_hints": json.dumps({"requires_browser": True})}]
    scans = [{"dealer_id": "quantumautosales-com", "has_error": False, "upserted": 12,
              "provider": "html_cards"}]
    registry = [
        {"name": "Longo Toyota", "website_url": "https://www.longotoyota.com", "state": "CA"},
        {"name": "Joe's Auto Sales", "website_url": "https://joesautosales.com/inv", "state": "TX"},
        {"name": "Cheap Cars", "website_url": "https://www.cars.com/dealers/1", "state": "TX"},
        {"name": "No Site Auto Sales", "website_url": "", "state": "tx"},
    ]
    hidden = [{"dealer_id": "quantumautosales-com", "hidden": 3}]
    rep = rpt.summarize(cars, recipes, scans, registry, hidden)
    assert rep["kinds"]["independent"]["hidden"] == 3
    assert rep["sets"]["dealers_with_active_cars"] == 2
    assert rep["sets"]["registry_ids_never_seen"] == 2
    ind = rep["kinds"]["independent"]
    assert ind["dealers"] == 1 and ind["small_lots"] == 1
    assert ind["hints"] == {"requires_browser": 1}
    assert ind["last_scan"] == {"under_20_rows": 1}
    assert ind["fresh"]["61d+"] == 2
    assert rep["kinds"]["franchise"]["last_scan"] == {"never_scanned": 1}
    never = rep["registry_never_seen"]
    assert never["independent"]["rows"] == 2 and never["independent"]["no_site"] == 1
    assert never["marketplace"]["site_is_marketplace"] == 1
    assert never["independent"]["top_states"] == {"TX": 2}
    out = rpt.render(rep)
    assert "Quantum" not in out and "Joe" not in out and "Longo Toyota" not in out
    assert "dealer_id=quantumautosales-com" in out
    assert out.endswith("END DEALER MIX REPORT")


def test_summarize_survives_missing_tables():
    rep = rpt.summarize(None, None, None, None)
    assert rep["tables"] == {"cars": False, "dealer_recipes": False, "scan_runs": False,
                             "dealerships": False, "incomplete_listings": False}
    assert "missing tables or columns" in rpt.render(rep)


def test_main_without_url(monkeypatch, capsys):
    monkeypatch.delenv("INVENTORY_DATABASE_URL", raising=False)
    assert rpt.main([]) == 2
    assert "not set" in capsys.readouterr().out


# --- the pg tier: the real migrated schema --------------------------------------------------

@pytest.mark.pg
def test_report_on_migrated_schema_and_session_refuses_writes(pg_db, capsys):
    with pg_db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dealerships (name, website_url, city, state) VALUES "
                "('Joe Auto Sales', 'https://www.joeautosales.com', 'Austin', 'TX')"
            )
            cur.execute("INSERT INTO scan_runs (dealer_id, finished_at, upserted) "
                        "VALUES ('quantumautosales-com', '2026-11-03T10:00:00', 165)")
        conn.commit()
    assert rpt.main([]) == 0
    out = capsys.readouterr().out
    assert "END DEALER MIX REPORT" in out
    assert "skip " not in out
    import psycopg

    conn = rpt.connect(pg_db.dsn)
    try:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE should_fail (x int)")
    finally:
        conn.close()
