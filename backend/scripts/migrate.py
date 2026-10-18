#!/usr/bin/env python3
"""
Apply the versioned schema migrations in ``migrations/`` to inventory Postgres.

Rebuilding this database used to mean replaying a July snapshot dump and hoping
the ALTER TABLEs that landed since were all reproduced by the runtime's
CREATE-TABLE-IF-NOT-EXISTS pass. This walks ``migrations/V<NNN>__*.sql`` in
version order instead, recording each file it applies in ``schema_migrations``
with a checksum, so "what schema is this database at" has an answer you can read.

The checksum is the point, not decoration: if an already-applied file's text
changes, two databases that both report the same version are no longer the same
schema, and the runner stops rather than paper over it. Applied files are
immutable; fix forward with a new higher-numbered file. See migrations/README.md.

What else the runner does (P4.2, 2026-10):

* **V019 is recorded, not run, only when that is safe** (``RECORD_ONLY``). V019
  re-creates 48 objects V001 already makes, without IF NOT EXISTS, so it can
  never run on a database built from V001. It is recorded when V001 runs earlier
  in the same invocation (a fresh build), or when all 48 already exist. Otherwise
  the runner refuses and names what is missing: a database on an old lineage
  (V001 baselined, V019's tables never created) must not be marked current while
  it lacks those tables.
* ``--record-only N[,M]`` records the named versions without running them. It is
  the operator's explicit call and skips the V019 check.
* ``--target N`` applies pending files up to and including VN; later files stay
  pending. One migration can reach prod on its own (the D-DB6 protocol).
* The target host, port and database are logged before anything is read. The
  user, password and connection options never are.
* Every file runs under ``SET LOCAL lock_timeout`` (5 s by default, also on the
  ``MIGRATE_ON_BOOT`` path). A file stuck behind a lock on ``cars`` fails and
  rolls back instead of queueing every reader behind it (the 2026-07-30 pile-up).
  A file that needs a longer wait sets its own ``SET LOCAL lock_timeout``.
* Every write transaction takes ``pg_advisory_xact_lock(MIGRATE_LOCK_KEY)`` and
  re-reads ``schema_migrations`` under it. Two boots (or a boot and an operator)
  serialize: the second waits up to ``--lock-wait``, then skips whatever the first
  already applied, or exits 1 if the wait runs out. No file runs twice.
* ``cars_cleanroom`` (``FROZEN_DATABASES``) is never written (P4.3b): it is the
  frozen clean-room baseline, held at V025 (D-N1). ``--apply`` there exits 2
  before any lock or write, and every write transaction re-checks
  ``current_database()`` before it takes the advisory lock. A dry run still
  reports. Scratch copies (``cars_cr_*``, scripts/cleanroom/build_db.sh) migrate
  as usual.

Usage:
  python -m backend.scripts.migrate                           # dry run (default), no writes
  python -m backend.scripts.migrate --apply
  python -m backend.scripts.migrate --apply --target 26       # stop after V026
  python -m backend.scripts.migrate --baseline 1 --apply      # record V001 without running it
  python -m backend.scripts.migrate --record-only 19 --apply  # record V019 without running it
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from backend.utils.project_env import load_project_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("migrate")

MIGRATIONS_DIR = _REPO_ROOT / "migrations"

_FILENAME_RE = re.compile(r"^V(\d+)__(.+)\.sql$")

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

# Per-file lock_timeout (SET LOCAL, so it ends with the file's transaction).
DEFAULT_LOCK_TIMEOUT = "5s"
# How long a second runner waits for the first one's advisory lock.
DEFAULT_LOCK_WAIT = "10min"
# pg_advisory_xact_lock key: the ASCII bytes of "migrate1" as a bigint
# (7883946363647714609 = 0x6d69677261746531, below 2**63). Every transaction
# that writes schema_migrations takes it.
MIGRATE_LOCK_KEY = int.from_bytes(b"migrate1", "big")

# Databases migrate never writes to. cars_cleanroom is the frozen clean-room
# baseline (D-N1): every clean-room comparison assumes it stays at V025.
# Exact names only, so cars_cr_* scratch copies still migrate.
FROZEN_DATABASES = frozenset({"cars_cleanroom"})

# Postgres interval-style values accepted for the two timeouts: "5s", "500ms",
# "10min", "0" (0 = wait forever). Validated here because they reach set_config.
_PG_DURATION_RE = re.compile(r"^\d+(?:\.\d+)?\s*(?:us|ms|s|min|h|d)?$")
_LOCK_NOT_AVAILABLE = "55P03"  # SQLSTATE for lock_timeout expiry

RUN, RECORD, REFUSE = "run", "record", "refuse"


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    path: Path
    checksum: str


@dataclass(frozen=True)
class Plan:
    pending: list[Migration]
    drifted: list[tuple[Migration, str]]  # (file, checksum recorded in the DB)
    missing: list[int]                    # applied versions with no file on disk
    out_of_order: list[Migration]         # pending, but older than something applied

    @property
    def blocked(self) -> bool:
        return bool(self.drifted or self.out_of_order)


@dataclass(frozen=True)
class RecordOnlyRule:
    """A file that re-creates objects an earlier file already makes."""

    created_by: int           # the version whose run creates ``objects``
    objects: tuple[str, ...]  # relations (to_regclass names) that must exist to record


@dataclass(frozen=True)
class Decision:
    """What the runner does with one pending file: run, record or refuse."""

    action: str
    reason: str = ""
    # Relations re-checked under the migration lock right before recording.
    verify: tuple[str, ...] = ()


# The objects V019 shares with V001: its 15 tables, 11 sequences and 22 primary
# key / unique constraints that V001 also creates (each constraint's index has
# the constraint's name, so to_regclass finds it). V019's other 5 objects
# (car_move_log, car_move_log_id_seq, car_move_log_pkey, cars_trim_quarantine,
# package_values_msrp_quarantine; plus idx_car_move_log_car) exist only in V019,
# so a fresh build that records V019 lacks them until V026 adopts them (P4.3).
# test_migrate.py re-derives this list from the two files.
V019_SHARED_WITH_V001: tuple[str, ...] = (
    # tables
    "public.ai_engine_specs", "public.ai_model_specs", "public.catalog_exterior_colors",
    "public.catalog_interior_colors", "public.catalog_options", "public.catalog_package_features",
    "public.catalog_packages", "public.catalog_trims", "public.dealer_recipes",
    "public.dealer_reviews", "public.dealer_specials", "public.dictionary_options",
    "public.lease_offer_matches", "public.market_price_stats", "public.model_generations",
    # sequences
    "public.dealer_reviews_id_seq", "public.dealer_specials_id_seq", "public.dictionary_options_id_seq",
    "public.exterior_colors_id_seq", "public.interior_colors_id_seq", "public.lease_offer_matches_id_seq",
    "public.model_generations_id_seq", "public.package_features_id_seq", "public.packages_id_seq",
    "public.standalone_options_id_seq", "public.vehicles_id_seq",
    # primary key / unique constraints (their indexes)
    "public.dealer_recipes_pkey", "public.dealer_reviews_dealer_id_user_id_key",
    "public.dealer_reviews_pkey", "public.dealer_specials_dealer_id_offer_hash_key",
    "public.dealer_specials_pkey", "public.dictionary_options_pkey", "public.exterior_colors_pkey",
    "public.exterior_colors_vehicle_id_color_name_key", "public.interior_colors_pkey",
    "public.interior_colors_vehicle_id_color_name_key", "public.lease_offer_matches_dealer_id_offer_hash_key",
    "public.lease_offer_matches_pkey", "public.market_price_stats_pkey",
    "public.model_generations_make_model_generation_key", "public.model_generations_pkey",
    "public.package_features_pkey", "public.packages_pkey", "public.packages_vehicle_id_package_name_key",
    "public.standalone_options_pkey", "public.standalone_options_vehicle_id_option_name_key",
    "public.vehicles_pkey", "public.vehicles_year_make_model_trim_engine_drivetrain_key",
)

# Files recorded without running when the rule says it is safe (see classify()).
RECORD_ONLY: dict[int, RecordOnlyRule] = {
    19: RecordOnlyRule(created_by=1, objects=V019_SHARED_WITH_V001),
}


class MigrationLockTimeout(RuntimeError):
    """Another migrate held the advisory lock for longer than ``--lock-wait``."""


class ConcurrentChecksumMismatch(RuntimeError):
    """A concurrent runner recorded this version with a different checksum."""


class RecordRefused(RuntimeError):
    """A conditional record-only file lost its preconditions under the lock."""


class FrozenDatabaseRefused(RuntimeError):
    """The connected database is in ``FROZEN_DATABASES``; migrate never writes to it."""


def compute_checksum(sql: str) -> str:
    """
    SHA-256 over the migration text, CRLF-normalized.

    Line endings are normalized because a Windows checkout or an editor that
    rewrites them would otherwise look identical to someone having edited an
    applied migration, and that is a hard stop.
    """
    normalized = sql.replace("\r\n", "\n").replace("\r", "\n")
    return "sha256:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def parse_filename(filename: str) -> tuple[int, str] | None:
    """``V007__add_foo.sql`` -> ``(7, 'add_foo')``; None for anything else."""
    m = _FILENAME_RE.match(filename)
    if not m:
        return None
    return int(m.group(1)), m.group(2)


def discover_migrations(directory: Path) -> list[Migration]:
    """
    Every migration file in ``directory``, ordered by version number.

    Sorted numerically, not lexically -- V010 sorts before V9 as text, which
    would apply the chain in an order no database has ever seen.
    """
    found: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        parsed = parse_filename(path.name)
        if not parsed:
            log.warning("ignoring %s (not V<NNN>__<description>.sql)", path.name)
            continue
        version, name = parsed
        if version in found:
            raise SystemExit(
                f"duplicate migration version {version}: "
                f"{found[version].path.name} and {path.name}. "
                "Two files cannot claim the same version -- renumber one."
            )
        found[version] = Migration(
            version=version,
            name=name,
            path=path,
            checksum=compute_checksum(path.read_text(encoding="utf-8")),
        )
    return [found[v] for v in sorted(found)]


def plan_migrations(available: list[Migration], applied: dict[int, str]) -> Plan:
    """
    Compare files on disk against the ``schema_migrations`` rows.

    ``applied`` maps version -> recorded checksum. Pure: no DB, no filesystem,
    so the interesting cases are testable without a live Postgres.
    """
    drifted = [
        (mig, applied[mig.version])
        for mig in available
        if mig.version in applied and applied[mig.version] != mig.checksum
    ]
    pending = [mig for mig in available if mig.version not in applied]
    missing = sorted(set(applied) - {mig.version for mig in available})

    # A file numbered below the high-water mark showing up as pending means two
    # databases would run the chain in different orders -- the rebuilt one runs
    # it in numeric order, production ran it interleaved. Refuse and renumber.
    high_water = max(applied, default=0)
    out_of_order = [mig for mig in pending if mig.version < high_water]

    return Plan(
        pending=pending, drifted=drifted, missing=missing, out_of_order=out_of_order
    )


def select_for_run(
    pending: Sequence[Migration], target: int | None
) -> tuple[list[Migration], list[Migration]]:
    """``(files this run handles, files above --target that stay pending)``."""
    if target is None:
        return list(pending), []
    return (
        [m for m in pending if m.version <= target],
        [m for m in pending if m.version > target],
    )


def classify(
    version: int,
    *,
    record_only: frozenset[int] = frozenset(),
    baseline_through: int = 0,
    runs_earlier: frozenset[int] = frozenset(),
    missing_objects: Sequence[str] | None = None,
    rules: Mapping[int, RecordOnlyRule] | None = None,
) -> Decision:
    """
    Run, record or refuse one pending file. Pure.

    ``runs_earlier``: versions this invocation runs (not records) before this
    one. ``missing_objects``: the rule's relations absent from the database, or
    None when they were not looked up (only needed when no earlier rule applies).
    """
    rules = RECORD_ONLY if rules is None else rules
    if version in record_only:
        return Decision(RECORD, "--record-only")
    if version <= baseline_through:
        return Decision(RECORD, f"--baseline {baseline_through}")
    rule = rules.get(version)
    if rule is None:
        return Decision(RUN)
    source = f"V{rule.created_by:03d}"
    n = len(rule.objects)
    if rule.created_by in runs_earlier:
        return Decision(
            RECORD,
            f"fresh build: {source} runs earlier in this run and creates the {n} objects it shares",
            verify=rule.objects,
        )
    if missing_objects is None:
        raise ValueError(f"V{version:03d}: look up which of its {n} {source} objects exist first")
    if not missing_objects:
        return Decision(RECORD, f"all {n} objects it shares with {source} already exist", verify=rule.objects)
    shown = ", ".join(missing_objects[:8]) + (" ..." if len(missing_objects) > 8 else "")
    return Decision(
        REFUSE,
        f"{len(missing_objects)} of the {n} objects it shares with {source} are missing ({shown}). "
        f"Recording it would mark this database current without them, and running it re-creates "
        f"objects without IF NOT EXISTS. Create the missing objects (schema_drift_report.py shows "
        f"the difference), then re-run; or, once checked by hand, --record-only {version}.",
    )


def describe_target(dsn: str | None) -> str:
    """Host, port and database of a DSN for the log; never the user, password or options."""
    if not dsn:
        return "(no inventory DSN)"
    try:
        from psycopg.conninfo import conninfo_to_dict

        params = conninfo_to_dict(dsn)
    except Exception:
        return "(unparseable DSN)"
    host = str(params.get("host") or params.get("hostaddr") or "default socket")
    if host.startswith("/"):
        host = f"unix-socket:{host}"
    return f"host={host} port={params.get('port') or 5432} db={params.get('dbname') or '(default)'}"


def frozen_database_reason(dbname: str | None) -> str | None:
    """Why migrate must not write to ``dbname``, or None when it may. Pure."""
    if dbname not in FROZEN_DATABASES:
        return None
    return (
        f"database {dbname} is the frozen clean-room baseline (D-N1, held at V025); "
        "migrate never writes to it. Migrate a cars_cr_* scratch copy instead "
        "(scripts/cleanroom/build_db.sh)"
    )


def current_database(cur) -> str | None:
    """The database this connection is on (the DSN may not name it: PGDATABASE, defaults)."""
    cur.execute("SELECT current_database()")
    row = cur.fetchone()
    return str(row[0]) if row and row[0] is not None else None


def refuse_frozen_database(cur) -> None:
    """Raise FrozenDatabaseRefused on a frozen database. Reads only; takes no lock."""
    reason = frozen_database_reason(current_database(cur))
    if reason:
        raise FrozenDatabaseRefused(reason)


def _connect():
    """
    Raw psycopg connection from the repo's own Postgres opener.

    Deliberately NOT ``inventory_db.db_conn()``: that wraps every statement in
    ``adapt_sql_for_postgres_execute``, which rewrites ``?`` to ``%s``, doubles
    bare ``%``, and rewrites IFNULL/datetime() for the SQLite-shaped call sites.
    Migration files are already Postgres SQL and must reach the server verbatim.
    """
    from backend.db import inventory_pg

    if not inventory_pg.is_inventory_postgres():
        raise SystemExit(
            "INVENTORY_DATABASE_URL (or DATABASE_URL) must point at postgresql:// "
            "to run migrations. Refusing to guess."
        )
    return inventory_pg.pg_connect()


def _inventory_dsn() -> str | None:
    from backend.db import inventory_pg

    return inventory_pg.inventory_postgres_dsn()


def _migrations_table_exists(cur) -> bool:
    cur.execute("SELECT to_regclass('public.schema_migrations')")
    row = cur.fetchone()
    return bool(row and row[0])


def read_applied(cur) -> dict[int, str]:
    """version -> checksum for everything already recorded, {} before first run."""
    if not _migrations_table_exists(cur):
        return {}
    cur.execute("SELECT version, checksum FROM public.schema_migrations")
    return {int(v): str(c) for v, c in cur.fetchall()}


def missing_relations(cur, names: Sequence[str]) -> list[str]:
    """The names ``to_regclass`` cannot resolve, in the given order. One round trip."""
    if not names:
        return []
    cur.execute(
        "SELECT n FROM unnest(%s::text[]) WITH ORDINALITY AS t(n, i) "
        "WHERE to_regclass(n) IS NULL ORDER BY i",
        (list(names),),
    )
    return [str(r[0]) for r in cur.fetchall()]


def plan_actions(
    cur,
    in_run: Sequence[Migration],
    *,
    record_only: frozenset[int] = frozenset(),
    baseline_through: int = 0,
    rules: Mapping[int, RecordOnlyRule] | None = None,
) -> list[tuple[Migration, Decision]]:
    """classify() every file of this run, looking objects up only where needed."""
    rules = RECORD_ONLY if rules is None else rules
    runs: set[int] = set()
    out: list[tuple[Migration, Decision]] = []
    for mig in in_run:
        rule = rules.get(mig.version)
        missing = None
        if (
            rule is not None
            and mig.version not in record_only
            and mig.version > baseline_through
            and rule.created_by not in runs
        ):
            missing = missing_relations(cur, rule.objects)
        decision = classify(
            mig.version,
            record_only=record_only,
            baseline_through=baseline_through,
            runs_earlier=frozenset(runs),
            missing_objects=missing,
            rules=rules,
        )
        if decision.action == RUN:
            runs.add(mig.version)
        out.append((mig, decision))
    return out


def _record(cur, mig: Migration) -> None:
    cur.execute(
        "INSERT INTO public.schema_migrations (version, name, checksum) VALUES (%s, %s, %s)",
        (mig.version, mig.name, mig.checksum),
    )


def _is_lock_timeout(exc: BaseException) -> bool:
    return getattr(exc, "sqlstate", None) == _LOCK_NOT_AVAILABLE


def _take_migration_lock(cur, lock_wait: str) -> None:
    """
    Transaction-scoped advisory lock; waits up to ``lock_wait`` for another runner.

    Every write transaction starts here, so the frozen-database refusal sits in
    front of the lock: it covers callers that skip main() (apply_migration).
    """
    refuse_frozen_database(cur)
    cur.execute("SELECT set_config('lock_timeout', %s, true)", (lock_wait,))
    try:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATE_LOCK_KEY,))
    except Exception as exc:
        if _is_lock_timeout(exc):
            raise MigrationLockTimeout(
                f"another migrate held the migration lock for more than {lock_wait} (--lock-wait)"
            ) from exc
        raise


def ensure_migrations_table(conn, *, lock_wait: str = DEFAULT_LOCK_WAIT) -> None:
    """Create ``schema_migrations`` under the migration lock (two first boots race otherwise)."""
    cur = conn.cursor()
    try:
        _take_migration_lock(cur, lock_wait)
        cur.execute(_CREATE_TABLE_SQL)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def run_one(
    conn,
    mig: Migration,
    *,
    record: bool = False,
    verify: Sequence[str] = (),
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    lock_wait: str = DEFAULT_LOCK_WAIT,
) -> str:
    """
    Run (or record) one file in one transaction, under the migration lock.

    Returns ``"applied"``, ``"recorded"`` or ``"skipped"`` (a concurrent runner
    recorded the same file while this one waited for the lock).

    The DDL and its bookkeeping row commit together or not at all; a crash
    halfway can never leave a migration applied but unrecorded (it would be
    re-applied on the next run) or recorded but unapplied (silently skipped).
    """
    cur = conn.cursor()
    try:
        _take_migration_lock(cur, lock_wait)
        cur.execute("SELECT checksum FROM public.schema_migrations WHERE version = %s", (mig.version,))
        row = cur.fetchone()
        if row is not None:
            conn.rollback()
            if str(row[0]) != mig.checksum:
                raise ConcurrentChecksumMismatch(
                    f"V{mig.version:03d} was recorded meanwhile with checksum {row[0]}, "
                    f"but this file is {mig.checksum}: two code versions are migrating at once"
                )
            return "skipped"
        if record:
            missing = missing_relations(cur, verify)
            if missing:
                raise RecordRefused(
                    f"{len(missing)} of the {len(verify)} objects that must exist before "
                    f"V{mig.version:03d} is recorded are missing: {', '.join(missing[:8])}"
                )
            _record(cur, mig)
            conn.commit()
            return "recorded"
        # set_config(..., true) is SET LOCAL: it ends with this file's transaction.
        cur.execute("SELECT set_config('lock_timeout', %s, true)", (lock_timeout,))
        cur.execute(mig.path.read_text(encoding="utf-8"))
        # pg_dump-generated files (V001) run set_config('search_path', '', false),
        # which outlives the file for the rest of the session: the unqualified
        # bookkeeping INSERT below then fails with "relation schema_migrations
        # does not exist" and every later file runs with an empty search_path.
        # Restore the session default after each file.
        cur.execute("SET search_path TO DEFAULT")
        _record(cur, mig)
        conn.commit()
        return "applied"
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def apply_migration(
    conn,
    mig: Migration,
    *,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    lock_wait: str = DEFAULT_LOCK_WAIT,
) -> None:
    """Run one file and record it, in a single transaction (see run_one)."""
    run_one(conn, mig, lock_timeout=lock_timeout, lock_wait=lock_wait)


def _report_blocked(plan: Plan, next_version: int) -> None:
    for mig, recorded in plan.drifted:
        log.error(
            "CHECKSUM DRIFT %s: applied as %s, file is now %s",
            mig.path.name, recorded, mig.checksum,
        )
    if plan.drifted:
        log.error(
            "An applied migration was edited. Every database that already ran it "
            "has the OLD schema, so re-running would silently diverge. Restore the "
            "file to its applied contents and put the change in a new V%03d__*.sql.",
            next_version,
        )
    for mig in plan.out_of_order:
        log.error(
            "OUT OF ORDER %s: version %d is below the highest applied version. "
            "Renumber it above the high-water mark.",
            mig.path.name, mig.version,
        )


def _positive_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a version number: {text!r}") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"versions start at 1, got {value}")
    return value


def _version_list(text: str) -> frozenset[int]:
    parts = [p.strip() for p in text.split(",")]
    if not all(parts):
        raise argparse.ArgumentTypeError(f"expected N or N,M, got {text!r}")
    return frozenset(_positive_int(p) for p in parts)


def _pg_duration(text: str) -> str:
    value = text.strip()
    if not _PG_DURATION_RE.match(value):
        raise argparse.ArgumentTypeError(f"expected a duration like 5s, 500ms or 10min, got {text!r}")
    return value


def _fmt(mig: Migration) -> str:
    return f"V{mig.version:03d}__{mig.name}.sql"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--apply", action="store_true",
                        help="write (default: dry run, no statement is executed)")
    parser.add_argument("--dry-run", action="store_true",
                        help="explicit no-op default; report the plan and exit")
    parser.add_argument("--dir", default=str(MIGRATIONS_DIR),
                        help=f"migrations directory (default: {MIGRATIONS_DIR})")
    parser.add_argument("--baseline", type=int, metavar="VERSION",
                        help="record migrations up to VERSION as applied WITHOUT "
                             "running them -- for a database that already has that "
                             "schema (production). Everything above still runs.")
    parser.add_argument("--record-only", type=_version_list, default=frozenset(), metavar="N[,M]",
                        help="record these pending versions as applied WITHOUT running them "
                             "(explicit; skips the V019 safety check). Other files still run.")
    parser.add_argument("--target", type=_positive_int, metavar="N",
                        help="handle pending files up to and including VN only; later ones stay pending")
    parser.add_argument("--lock-timeout", type=_pg_duration, default=DEFAULT_LOCK_TIMEOUT, metavar="DURATION",
                        help=f"SET LOCAL lock_timeout for each file (default {DEFAULT_LOCK_TIMEOUT}; 0 = no limit)")
    parser.add_argument("--lock-wait", type=_pg_duration, default=DEFAULT_LOCK_WAIT, metavar="DURATION",
                        help="how long to wait for another migrate's advisory lock before exiting 1 "
                             f"(default {DEFAULT_LOCK_WAIT}; 0 = no limit)")
    args = parser.parse_args(argv)

    load_project_dotenv()
    apply = args.apply and not args.dry_run

    directory = Path(args.dir)
    if not directory.is_dir():
        log.error("no migrations directory at %s", directory)
        return 2
    available = discover_migrations(directory)
    if not available:
        log.info("no migration files in %s", directory)
        return 0

    on_disk = {m.version for m in available}
    record_only: frozenset[int] = args.record_only
    unknown = sorted(record_only - on_disk)
    if unknown:
        log.error("--record-only names no migration file: %s",
                  ", ".join(f"V{v:03d}" for v in unknown))
        return 2
    if args.target is not None:
        if args.target > available[-1].version:
            log.error("--target %d is above the highest migration file (V%03d)",
                      args.target, available[-1].version)
            return 2
        above = sorted(v for v in record_only if v > args.target)
        if above:
            log.error("--record-only %s is above --target %d",
                      ", ".join(f"V{v:03d}" for v in above), args.target)
            return 2

    log.info("target: %s (%s)", describe_target(_inventory_dsn()),
             "apply" if apply else "dry run, no writes")
    try:
        conn = _connect()
    except Exception as exc:  # SystemExit (not a Postgres DSN) passes through
        log.error("could not connect to inventory Postgres: %s", exc)
        return 2
    try:
        cur = conn.cursor()
        frozen = frozen_database_reason(current_database(cur))
        if frozen and apply:
            # Before the plan and before any lock: --baseline and --record-only
            # write too, so every --apply is refused here, not just pending files.
            cur.close()
            conn.rollback()
            log.error("REFUSE --apply: %s", frozen)
            log.error("refused: nothing was written")
            return 2
        if frozen:
            log.warning("%s; --apply would be refused, this dry run only reports", frozen)
        table_exists = _migrations_table_exists(cur)
        applied = read_applied(cur)

        if not table_exists:
            # Creating it in a dry run would make the "no writes" promise a lie.
            log.info("schema_migrations does not exist yet%s",
                     "" if apply else " (would be created by --apply)")

        plan = plan_migrations(available, applied)
        log.info("%d migration file(s), %d already applied, %d pending",
                 len(available), len(applied), len(plan.pending))
        for version in plan.missing:
            # Not fatal: the DB is still the sum of what ran. But the chain can no
            # longer rebuild that database from scratch, which is the whole point.
            log.warning("version %d is recorded as applied but has no file on disk", version)
        if plan.blocked:
            cur.close()
            conn.rollback()
            _report_blocked(plan, max(max(applied, default=0), available[-1].version) + 1)
            return 1
        for version in sorted(record_only & set(applied)):
            log.info("V%03d is already applied; --record-only has nothing to do for it", version)

        in_run, held = select_for_run(plan.pending, args.target)
        if held:
            log.info("%d pending file(s) above --target %d stay pending: %s",
                     len(held), args.target, ", ".join(f"V{m.version:03d}" for m in held))
        if not in_run:
            cur.close()
            conn.rollback()
            if held:
                log.info("nothing pending at or below --target %d", args.target)
            else:
                log.info("database is up to date")
            return 0

        decisions = plan_actions(
            cur, in_run, record_only=record_only, baseline_through=args.baseline or 0
        )
        cur.close()
        conn.rollback()  # end the read-only planning transaction before any lock

        refused = [(m, d) for m, d in decisions if d.action == REFUSE]
        for mig, decision in refused:
            log.error("REFUSE %s: %s", _fmt(mig), decision.reason)
        if refused:
            log.error("refused: nothing was written")
            return 1

        if not apply:
            for mig, decision in decisions:
                verb = "apply " if decision.action == RUN else "record"
                log.info("would %s %s%s", verb, _fmt(mig),
                         f"  ({decision.reason})" if decision.reason else "")
            log.info("=== Summary (dry run) ===")
            log.info("  would apply %d and record %d migration(s) through V%03d",
                     sum(d.action == RUN for _, d in decisions),
                     sum(d.action == RECORD for _, d in decisions),
                     in_run[-1].version)
            log.info("  re-run with --apply to write")
            return 0

        if not table_exists:
            try:
                ensure_migrations_table(conn, lock_wait=args.lock_wait)
            except Exception as exc:
                log.error("could not create schema_migrations: %s", exc)
                return 1

        outcomes: Counter[str] = Counter()
        for mig, decision in decisions:
            record = decision.action == RECORD
            log.info("%s %s%s", "RECORD" if record else "APPLY", _fmt(mig),
                     f"  ({decision.reason})" if decision.reason else "")
            try:
                outcome = run_one(
                    conn, mig, record=record, verify=decision.verify,
                    lock_timeout=args.lock_timeout, lock_wait=args.lock_wait,
                )
            except Exception as exc:
                log.error("FAILED %s: %s", _fmt(mig), exc)
                if _is_lock_timeout(exc):
                    log.error(
                        "lock_timeout (%s) expired: another session holds a lock this file "
                        "needs (check pg_stat_activity). Retry in a quiet window, or pass a "
                        "longer --lock-timeout.", args.lock_timeout,
                    )
                log.error("rolled back; nothing at or above this version was applied")
                return 1
            if outcome == "skipped":
                log.info("  %s was recorded by a concurrent migrate meanwhile; skipped", _fmt(mig))
            outcomes[outcome] += 1

        log.info("=== Summary (applied) ===")
        log.info("  applied %d, recorded %d, skipped %d migration(s) through V%03d",
                 outcomes["applied"], outcomes["recorded"], outcomes["skipped"],
                 in_run[-1].version)
        if held:
            log.info("  %d file(s) above --target %d still pending", len(held), args.target)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
