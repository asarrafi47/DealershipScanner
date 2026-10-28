"""P10D.8: the root DICTIONARY/ content audit classifies files without writing."""
import importlib.util
from pathlib import Path

_SPEC = importlib.util.spec_from_file_location(
    "audit_root_dictionary", Path(__file__).resolve().parents[2] / "scripts" / "audit_root_dictionary.py"
)
audit_mod = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(audit_mod)


def _w(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_audit_classifies_each_status(tmp_path):
    root, target = tmp_path / "DICTIONARY", tmp_path / "backend_dictionary"
    hdr = "Year,Make,Model,mpg_city\n"
    _w(root / "a_EPA.csv", hdr + "2000,Acura,TL,17\n")
    _w(target / "epa" / "Acura" / "a_EPA.csv", hdr + "2000,Acura,TL,17\n")
    _w(root / "b_EPA.csv", hdr + "2000,Acura,RL,17\n")
    _w(target / "epa" / "Acura" / "B_EPA.csv", hdr + "2000,Acura,RL,17\n2000,Acura,RL,18\n")
    _w(root / "c_EPA.csv", hdr + "2000,Acura,NSX,17\n")
    _w(target / "epa" / "Acura" / "c_EPA.csv", hdr + "2000,Acura,NSX,19\n")
    _w(root / "d_EPA.csv", hdr + "2000,Acura,CL,17\n")
    before = sorted(p.name for p in tmp_path.rglob("*"))

    report = audit_mod.audit(root, target)

    by_name = {r["file"]: r["status"] for r in report["needs_review"]}
    assert by_name == {"c_EPA.csv": "root_unique_rows", "d_EPA.csv": "no_counterpart"}
    assert report["counts"] == {"identical": 1, "covered": 1, "root_unique_rows": 1, "no_counterpart": 1}
    assert sorted(p.name for p in tmp_path.rglob("*")) == before


def test_strict_exit_code(tmp_path):
    root, target = tmp_path / "DICTIONARY", tmp_path / "t"
    target.mkdir()
    _w(root / "x_EPA.csv", "Year\n2000\n")
    assert audit_mod.main(["--root", str(root), "--target", str(target)]) == 0
    assert audit_mod.main(["--root", str(root), "--target", str(target), "--strict"]) == 1
