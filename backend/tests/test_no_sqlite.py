"""N37.0: the all-Postgres guard (docs/ALL_POSTGRES_PLAN_2026_10.md section 6).

Scans git-tracked code for SQLite constructs. Existing offenders are frozen in
``sqlite_allowlist.txt`` (``path  construct  count``) and the list can only
shrink: a new hit, a higher count, or an entry that no longer matches fails.
Python files are parsed with ``ast`` so comments and docstrings are ignored;
string literals are still scanned because SQL lives in them. Docs and
CHANGELOG.md are exempt. This file is exempt; its detection fixtures are
constructed strings.

To regenerate after fixing files: ``python backend/tests/test_no_sqlite.py --write``.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ALLOWLIST = Path(__file__).with_name("sqlite_allowlist.txt")
SELF = "backend/tests/test_no_sqlite.py"

_SQLITE_MODULES = ("sqlite3", "sqlcipher3", "pysqlcipher3", "aiosqlite")
_CODE_SUFFIXES = (".py", ".sql", ".sh", ".yml", ".yaml", ".txt")
_BAD_SUFFIXES = (".db", ".sqlite", ".sqlite3")

# Built from parts so the guard file never matches itself, even if the SELF exemption is removed.
_J = "".join
_SQL_PATTERNS = {
    "insert_or": re.compile(r"INSERT\s+OR\s+(IGNORE|REPLACE)", re.I),
    "pragma": re.compile(r"\bPRAGMA\s", re.I),
    "autoincrement": re.compile(r"\bAUTOINCREMENT\b", re.I),
    "datetime_now": re.compile(r"datetime\(\s*'now'", re.I),
    "json_extract": re.compile(r"\bjson_extract\(", re.I),
    "sqlite_master": re.compile(r"\bsqlite_master\b", re.I),
    "ifnull": re.compile(r"\bIFNULL\(", re.I),
    "instr": re.compile(r"\bINSTR\(", re.I),
    "group_concat": re.compile(r"\bGROUP_CONCAT\(", re.I),
}
_ENV_PATTERN = re.compile(r"\bINVENTORY_SQLITE_TESTS\b|\bRATE_LIMIT_SQLITE_PATH\b|\b[A-Z][A-Z0-9_]*_DB_PATH\b")
_DBFILE_PATTERN = re.compile(r"[\w./-]+\.(?:db|sqlite3?)\b(?!\w)")
_QMARK_SQL = re.compile(r"\b(SELECT|INSERT|UPDATE|DELETE|WHERE|VALUES)\b", re.I)
_QMARK = re.compile(r"(?<![\w'\"?])\?(?![|&?\w'\"])")
_ATTR_CONSTRUCTS = {"executescript": "executescript", "lastrowid": "lastrowid"}


def _scan_text(text: str, comment: str | None) -> Counter:
    """Regex constructs over free text (sql/sh/yml/requirements and Python string literals)."""
    hits: Counter = Counter()
    for line in text.splitlines():
        if comment and line.lstrip().startswith(comment):
            continue
        for name, pat in _SQL_PATTERNS.items():
            if pat.search(line):
                hits[name] += 1
        if _ENV_PATTERN.search(line):
            hits["env_db_path"] += 1
        if _DBFILE_PATTERN.search(line):
            hits["db_path_literal"] += 1
        low = line.lower()
        if re.search(r"\bsqlite3\b|\bsqlcipher|\baiosqlite\b", low):
            hits["sqlite_ref"] += 1
    return hits


def _docstring_nodes(tree: ast.AST) -> set[int]:
    ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(getattr(body[0], "value", None), ast.Constant):
                ids.add(id(body[0].value))
    return ids


def scan_python(source: str) -> Counter:
    hits: Counter = Counter()
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return hits
    docs = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] in _SQLITE_MODULES:
                    hits["import_sqlite3"] += 1
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] in _SQLITE_MODULES:
                hits["import_sqlite3"] += 1
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in _SQLITE_MODULES:
                hits["sqlite3_attr"] += 1
            if node.attr in _ATTR_CONSTRUCTS:
                hits[_ATTR_CONSTRUCTS[node.attr]] += 1
            if node.attr == "row_factory":
                hits["row_factory"] += 1
        elif isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
            s = node.value
            for k, v in _scan_text(s, None).items():
                if k != "sqlite_ref":
                    hits[k] += v
            if _QMARK_SQL.search(s) and _QMARK.search(s):
                hits["qmark_placeholder"] += 1
    return hits


def scan_file(rel: str, text: str) -> Counter:
    if rel == SELF or rel.startswith("docs/") or rel == "CHANGELOG.md":
        return Counter()
    name = rel.rsplit("/", 1)[-1]
    hits: Counter = Counter()
    if rel.endswith(_BAD_SUFFIXES):
        hits["tracked_db_file"] += 1
    if rel.endswith(".py"):
        hits.update(scan_python(text))
    elif rel.endswith(".sql"):
        hits.update(_scan_text(text, "--"))
    elif rel.endswith(_CODE_SUFFIXES) or name.startswith("Dockerfile"):
        if rel.endswith(".txt") and not name.startswith("requirements"):
            return hits
        hits.update(_scan_text(text, "#"))
    return hits


def _tracked() -> list[str]:
    out = subprocess.run(["git", "ls-files", "-z"], cwd=REPO, capture_output=True, check=True).stdout
    return [p for p in out.decode().split("\0") if p]


def scan_repo() -> dict[tuple[str, str], int]:
    result: dict[tuple[str, str], int] = {}
    for rel in _tracked():
        path = REPO / rel
        if rel.endswith(_BAD_SUFFIXES):
            hits = scan_file(rel, "")
        else:
            if not (rel.endswith(_CODE_SUFFIXES) or rel.rsplit("/", 1)[-1].startswith("Dockerfile")):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError):
                continue
            hits = scan_file(rel, text)
        for construct, n in hits.items():
            result[(rel, construct)] = n
    return result


def read_allowlist() -> dict[tuple[str, str], int]:
    entries: dict[tuple[str, str], int] = {}
    if not ALLOWLIST.exists():
        return entries
    for line in ALLOWLIST.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        path, construct, count = line.split()
        entries[(path, construct)] = int(count)
    return entries


def write_allowlist(hits: dict[tuple[str, str], int]) -> None:
    head = (
        "# N37.0 frozen SQLite offenders: path  construct  count. May only shrink.\n"
        "# Each deletion unit removes its entries; target at N37.23 is empty.\n"
        "# Regenerate: python backend/tests/test_no_sqlite.py --write\n"
    )
    body = "".join(f"{p}  {c}  {n}\n" for (p, c), n in sorted(hits.items()))
    ALLOWLIST.write_text(head + body, encoding="utf-8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_no_new_sqlite_beyond_allowlist():
    allowed = read_allowlist()
    new = []
    for key, n in scan_repo().items():
        if n > allowed.get(key, 0):
            new.append(f"{key[0]}  {key[1]}  {n} (allowed {allowed.get(key, 0)})")
    assert not new, "new SQLite constructs (use Postgres, see docs/ALL_POSTGRES_PLAN_2026_10.md):\n" + "\n".join(new)


def test_allowlist_only_shrinks():
    actual = scan_repo()
    stale = []
    for key, n in read_allowlist().items():
        have = actual.get(key, 0)
        if have < n:
            stale.append(f"{key[0]}  {key[1]}: allowlist {n}, actual {have} (lower or remove the entry)")
    assert not stale, "allowlist entries no longer match:\n" + "\n".join(stale)


# --- detection fixtures: constructed strings so this file stays clean ------


def test_detects_import_and_attr():
    src = _J(["import ", "sqlite3\n", "c = sqlite3.", "connect('x')\n"])
    hits = scan_python(src)
    assert hits["import_sqlite3"] == 1 and hits["sqlite3_attr"] == 1


def test_detects_sql_constructs_in_strings_not_docstrings():
    src = '"""doc: ' + _J(["INSERT OR ", "IGNORE"]) + '"""\nq = "' + _J(["INSERT OR ", "REPLACE INTO t VALUES (?)"]) + '"\n'
    hits = scan_python(src)
    assert hits["insert_or"] == 1
    assert hits["qmark_placeholder"] == 1


def test_comments_ignored_in_python_and_shell():
    assert not scan_python("# " + _J(["import ", "sqlite3"]) + "\nx = 1\n")
    assert not scan_file("scripts/x.sh", "# " + _J(["PRAGMA", " foo"]) + "\n")


def test_detects_db_path_and_env_names():
    hits = scan_python('p = "data/' + _J(["users", ".db"]) + '"\ne = "' + _J(["RATE_LIMIT_", "SQLITE_PATH"]) + '"\n')
    assert hits["db_path_literal"] == 1 and hits["env_db_path"] == 1


def test_tracked_db_file_flagged():
    assert scan_file("backend/" + _J(["users", ".db"]), "")["tracked_db_file"] == 1


def test_attr_constructs():
    hits = scan_python("cur.executescript(s)\nx = cur.lastrowid\nc.row_factory = r\n")
    assert hits["executescript"] == hits["lastrowid"] == hits["row_factory"] == 1


if __name__ == "__main__":
    if "--write" in sys.argv:
        write_allowlist(scan_repo())
        print(f"wrote {ALLOWLIST}")
