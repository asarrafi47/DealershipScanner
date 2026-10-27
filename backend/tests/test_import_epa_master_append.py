"""P10B.3: ``import_epa_master --append-years`` is the single ingestion path (D-ER1 a).

vehicles.csv -> dictionary CSVs -> ``build_epa_master_pg`` (``--apply`` only). The wiring
tests run without a database; the provenance / re-run test needs Postgres (pg marker).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from backend.scripts import build_epa_master_pg as bld
from backend.scripts import import_epa_master as imp

FIXTURE = Path(__file__).parent / "fixtures" / "epa_vehicles_sample.csv"


@pytest.fixture
def builder_calls(monkeypatch):
    calls = []

    def fake_build(**kw):
        calls.append(kw)
        return {"insert": 0, "mode": "apply" if kw.get("apply") else "dry_run"}

    monkeypatch.setattr(bld, "build_epa_master", fake_build)
    return calls


def test_apply_writes_dictionary_then_runs_builder_apply(tmp_path, builder_calls):
    out = imp.append_years({2019}, str(FIXTURE), apply=True, dictionary_dir=tmp_path / "dict")
    files = sorted((tmp_path / "dict").glob("*_EPA.csv"))
    assert out["dictionary_files"] == len(files) > 0
    assert all(f.name.startswith("2019_") for f in files)
    assert builder_calls == [{"apply": True, "source_dir": tmp_path / "dict"}]
    assert "archive_missing" not in builder_calls[0]


def test_dry_run_writes_no_dictionary_and_runs_builder_dry_run(tmp_path, monkeypatch):
    seen = []

    def fake_build(**kw):
        d = Path(kw["source_dir"])
        seen.append((kw, sorted(p.name for p in d.rglob("*_EPA.csv"))))
        return {"insert": 0, "mode": "dry_run"}

    monkeypatch.setattr(bld, "build_epa_master", fake_build)
    out = imp.append_years({2019}, str(FIXTURE), dictionary_dir=tmp_path / "dict")
    assert out["mode"] == "dry_run" and out["dictionary_files"] > 0
    assert not (tmp_path / "dict").exists()
    assert seen[0][0]["dry_run"] is True and "apply" not in seen[0][0]
    # the builder sees the new-year files even though the real dictionary lacks them
    assert len(seen[0][1]) == out["dictionary_files"]


def test_dry_run_builder_sees_existing_dictionary_plus_new_year(tmp_path, monkeypatch):
    real = tmp_path / "dict"
    real.mkdir()
    (real / "2001_Acme_EPA.csv").write_text("x\n")
    seen = []

    def fake_build(**kw):
        seen.append(sorted(p.name for p in Path(kw["source_dir"]).glob("*_EPA.csv")))
        return {"insert": 0}

    monkeypatch.setattr(bld, "build_epa_master", fake_build)
    out = imp.append_years({2019}, str(FIXTURE), dictionary_dir=real)
    assert "2001_Acme_EPA.csv" in seen[0]
    assert len(seen[0]) == 1 + out["dictionary_files"]
    assert [p.name for p in real.glob("*_EPA.csv")] == ["2001_Acme_EPA.csv"]


def test_failed_rows_stop_before_the_builder(tmp_path, builder_calls, monkeypatch):
    from backend.scripts import import_epa_to_dictionary as to_dict

    def failing(csv_path, min_year, d, years=None, stats=None):
        stats["failed"] += 2
        return 0

    monkeypatch.setattr(to_dict, "process_epa_csv", failing)
    with pytest.raises(imp.BootstrapRefused, match="2 vehicles.csv row"):
        imp.append_years({2019}, str(FIXTURE), apply=True, dictionary_dir=tmp_path)
    assert builder_calls == []


def test_cli_append_is_dry_run_by_default(tmp_path, builder_calls, monkeypatch, capsys):
    seen = []
    monkeypatch.setattr(imp, "append_years", lambda y, p, apply=False: seen.append((y, p, apply)) or {"mode": "x"})
    assert imp.main(["--append-years", "2019", "--csv", str(FIXTURE)]) == 0
    assert imp.main(["--append-years", "2019", "--csv", str(FIXTURE), "--apply"]) == 0
    assert [s[2] for s in seen] == [False, True]
    assert "Dry run: nothing written" in capsys.readouterr().out


def test_cli_surfaces_builder_refusal_as_exit_2(monkeypatch, capsys):
    def refuse(*a, **k):
        raise bld.BuildRefused("guard")

    monkeypatch.setattr(imp, "append_years", refuse)
    assert imp.main(["--append-years", "2019", "--csv", str(FIXTURE), "--apply"]) == imp.EXIT_REFUSED
    assert "guard" in capsys.readouterr().err


class _Conn:
    def __init__(self, n):
        self.n = n

    def cursor(self):
        n = self.n

        class C:
            def execute(self, sql, params=None):
                pass

            def fetchone(self):
                return (n,)

        return C()


def test_yes_bootstrap_only_on_empty_catalog():
    imp.assert_catalog_empty(_Conn(0))
    with pytest.raises(imp.BootstrapRefused, match="holds 3 row"):
        imp.assert_catalog_empty(_Conn(3))


@pytest.mark.pg
def test_appended_rows_carry_provenance_and_rerun_inserts_nothing(pg_db, tmp_path, monkeypatch):
    """Not run in the draft (no scratch server). Seeds 40 dictionary_epa rows so the
    builder's 90% guard passes, appends 2019 through the path, re-runs."""
    from backend.scripts import import_epa_to_dictionary as to_dict

    monkeypatch.setattr(bld, "DEFAULT_MIN_FILES", 1)
    d = tmp_path / "dict"
    to_dict.process_epa_csv(FIXTURE, 1950, d, years={2019})
    with pg_db.connect() as conn:
        conn.execute("DELETE FROM public.epa_master")
        conn.commit()
    first = bld.build_epa_master(apply=True, source_dir=d, min_files=1, allow_large_update=True)
    assert first["applied"]["inserted"] > 0
    with pg_db.connect() as conn:
        bad = conn.execute("SELECT COUNT(*) FROM public.epa_master WHERE source IS DISTINCT FROM "
                           "'dictionary_epa' OR natural_key IS NULL").fetchone()[0]
    assert bad == 0
    again = bld.build_epa_master(apply=True, source_dir=d, min_files=1)
    assert again["insert"] == 0 and again["applied"]["inserted"] == 0
