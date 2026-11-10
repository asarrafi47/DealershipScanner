"""Merge one inventory database snapshot into another, dealer by dealer, never by replacing it.

Owner decision 2026-11-09: Railway's production Postgres gets this Mac's fresher scan data
(the 2026-11-03 fleet scan in ``cars_scan_20261103``) through a dealer-scoped MERGE that is
rehearsed on a local copy first; production changes only after the owner approves a runbook.

    # 1. freeze the scope (read-only on both databases)
    python -m backend.scripts.merge_dealer_snapshot --compute-scope --scope scope.json \\
        --source-dsn-env MERGE_SOURCE_DSN --target-dsn-env MERGE_TARGET_DSN \\
        --triage '<repo>/workspace/pipeline/scan_2026_11_03_s*/triage.json'
    # 2. exact counts, read-only (the default mode)
    python -m backend.scripts.merge_dealer_snapshot --dry-run --scope scope.json --plan-out plan.json \\
        --detail-out moves_retirements_skips.csv \\
        --source-dsn-env MERGE_SOURCE_DSN --target-dsn-env MERGE_TARGET_DSN
    # 3. one transaction, pre-image backup first
    python -m backend.scripts.merge_dealer_snapshot --apply --scope scope.json --expect-db <db> \\
        --expect-plan plan.json --source-dsn-env MERGE_SOURCE_DSN --target-dsn-env MERGE_TARGET_DSN
    # 4. undo exactly one run
    python -m backend.scripts.merge_dealer_snapshot --rollback <run id> --expect-db <db> \\
        --target-dsn-env MERGE_TARGET_DSN
    # 5. separate step: drop verified old backup tables (none today), VACUUM (ANALYZE) cars + grid cards
    python -m backend.scripts.merge_dealer_snapshot --cleanup --expect-db <db> --target-dsn-env MERGE_TARGET_DSN

Connection strings come only from the environment variables NAMED by --source-dsn-env and
--target-dsn-env, so no URL is in argv, ps, a log line, the scope file or the plan file
(connection errors print with host, user and password redacted). Against Railway:
``railway run --service Postgres -- sh -c 'MERGE_TARGET_DSN="$DATABASE_PUBLIC_URL" python -m ...'``.
The source is always opened READ ONLY, and so is the target for --compute-scope and --dry-run
(the server refuses writes in those sessions). Every writing mode (--apply, --rollback,
--cleanup) additionally refuses a target that is not on this machine unless --allow-remote-target
is given, so a stray environment variable cannot aim a rehearsal command at production.

Semantics (``cars``)
--------------------
* Columns: the intersection of the source and target ``cars`` columns. A column only one
  side has is never read or written (an insert leaves a target-only column at its default);
  a type mismatch on a shared column refuses. ``cars.id`` is never copied.
* Fresh source row: ``COALESCE(listing_active, 1) = 1`` and ``scraped_at >= since``, where
  ``since`` is the earliest ``started`` of the triage files (the scan's start; scraped_at is
  ISO-8601 UTC text, so text order is time order). Only fresh rows are evidence: a stale
  active row kept by a guard, or the scanner's own retirement, says nothing about the target.
* A dealer is SEEN holding a VIN when its fresh row has that VIN, or when the run's
  ``vin_owner_conflicts`` name it as the claimant (its feed listed the VIN and the VIN
  ownership guard refused the write).
* Matched VIN, fresh at in-scope dealer A, target row filed under A: the target row takes the
  source row's values and its ``cars.id`` never changes. ``first_seen_at`` becomes the earlier of
  the two. A target row scraped at or after the source row is never overwritten (a row never
  regresses), which is also why a second apply of the same scope writes nothing.
  Three refinements keep the merge from destroying target data:
  - KEEP RULE. A column's value is empty when it is NULL, or text that is blank or one of
    ``[]``, ``{}``, ``null`` (the empties the repo's own partial indexes use). An empty source
    value never replaces a non-empty target value, exactly the contract of the scanner's own
    upsert (backend/scanner/upsert/sql.py: "an empty incoming value never overwrites a stored
    one"). The state columns ``dealer_id``, ``dealer_name``, ``dealer_url``, ``scraped_at``,
    ``listing_active`` and ``listing_removed_at`` always take the source value (a reactivated
    row must lose its ``listing_removed_at``). Measured on the 2026-11-09 copy of production,
    a plain copy would have blanked a stored value in about 7,000 cells of the 90,877 matched
    rows (63 prices, 232 window-sticker URLs, 975 spec_source_json, 1,253 descriptions, 945
    msrp, ...) while filling far more than it blanks; the dry run and the apply print the
    exact count per column.
  - ADMIN EDITS. ``internal_notes`` and ``marked_for_review`` (written on the website by
    backend/dealer/admin/routes.py, and kept by the scanner upsert too) keep the target's values.
  - REFERENCES. ``dealership_registry_id``, and ``epa_master_id`` with ``epa_match_confidence``
    and ``epa_match_method``, are ids into tables whose numbering is per database. The source
    value is copied only when its id names the same row in both databases' ``dealerships`` /
    ``epa_master`` (identity columns compared, see REF_GUARDS); otherwise the target's values
    stay. A NULL source reference keeps the target's.
* New VIN (absent from the target): inserted with a target id from ``cars_id_seq`` (the apply
  refuses when the sequence is behind ``max(id)``); an unverifiable reference column is NULL.
* VIN ownership: a VIN the target files under a dealer OUTSIDE the scope (or under a blank
  dealer id) is never moved; it is counted as a skipped conflict
  (``target_owner_out_of_scope``). So is a VIN whose target owner is in scope but not part of
  this run (``--only-dealers``). Between two dealers of the run, a VIN the target lists
  ACTIVE under C moves to the source's owner A (same id, A's values) only when C's capture
  was strong enough to retire it (C's retire_unseen for the row's bucket) and C did not
  claim it in the run; otherwise it is skipped (``target_owner_capture_weak`` /
  ``contested_in_run``), because the source's owner of a group-feed VIN is whichever store
  wrote it first. A target row that is already inactive under C moves.
* Retirement: an ACTIVE target row of in-scope dealer C that C was not seen holding gets
  ``listing_active = 0, listing_removed_at = <apply time>`` only when the scope says
  retire_unseen for C and the row's condition bucket. A row whose VIN has a fresh row at ANY
  other dealer is never retired (the scan saw the car; it is moved or left where it is).
  Otherwise the row stays listed (``kept_unseen``) for the next real scan to decide.
  No row is ever deleted.
* Out-of-scope dealers' rows (both sides) and every user table (car_comments,
  dealer_comments, comment_attachments, user_search_history, user_hidden_dealers, saved_*,
  users, ...) are never written; ``car_comments`` reference ``cars.id`` and ids never change.
* ``nhtsa_vpic_cache``: rows the target lacks (by VIN) are inserted from the source; an
  existing target row is never overwritten.
* ``listings_grid_cards``: the cards of every updated, moved or retired car are deleted so
  they rebuild; run ``backend/scripts/build_listings_grid_cards.py`` after an apply.
* Nothing else is merged (dealerships, car_image_text, incomplete_listings,
  vin_owner_conflicts, car_move_log, package_observations, ...).

Scope (--compute-scope, written once to a frozen JSON file)
-----------------------------------------------------------
A dealer is in scope only when ALL of these hold: a non-blank id; rows in both databases;
not named by --exclude-dealer; exactly one triage entry, with verdict ok, thin or inaccurate
(no_rows and error dealers kept their stale rows and stay out); at least one fresh source
row; and no target row scraped at or after ``since`` (the target has not scanned it since).
Each dealer entry carries ``in_scope``, ``reason``, ``retire_unseen`` (yes when any bucket
retires) and ``retire_unseen_buckets`` with the numbers behind every bucket.

Retirement is decided per condition bucket (``new`` / ``used`` by the scanner's
``condition_bucket`` rule; ``unknown`` = blank condition retires only when new and used
both do) and is yes only when ALL of these hold:

1. the triage says ``reconcile.eligible`` and the pipeline did not keep that bucket
   (``reconcile.buckets.<bucket>.kept`` empty);
2. the target lists at least --min-bucket-rows (10) active cars in the bucket and the scan
   returned at least that many fresh rows of it;
3. size ratio = fresh rows / target active rows >= --min-size-ratio (0.8): a capture that is
   smaller than the lot Railway holds is not trusted to speak for the whole lot;
4. coverage = target active rows the dealer was seen holding / target active rows >=
   --min-coverage (0.7; it must be stricter than the scanner's 0.5 floor).

Why 0.7. The scanner retires every unseen row of a bucket once 50% of it was re-seen. On
2026-11-03 encinitasford-com re-saw 89 of its 171 pre-scan New cars (52%) with a capture of
normal size (164 rows, ratio 0.96) and the scanner retired the rest; that retirement was a
false one. Measured against Railway's own rows the same bucket is 101 of 170 (59.4%), so a
0.6 rule would clear it by 0.006. Coverage cannot tell a partial capture from turnover (36
days passed since Railway's last scan; median coverage is 0.56 for New and 0.33 for Used
buckets), so the rule limits the exposure instead: at 0.7 one merge can retire at most 30%
of a bucket, and the known bad case sits 2.8 binomial standard errors below the line
(sqrt(0.25 / 170) = 0.038) where the scanner's floor sits above it. Measured on the copy of
production, retirement by threshold (size ratio and the triage conditions held at their
defaults; rows = unseen rows retired, the rest stay listed for the next real scan):

    coverage >=   0.5      0.6      0.7      0.8
    new buckets   243      176      86       22
    used buckets  91       38       11       2
    rows retired  31,610   19,197   6,195    986

Every threshold is a flag and is written into the scope file next to each bucket's numbers.

Writes, backups and rollback
----------------------------
--apply needs --expect-db equal to the target's current_database() and refuses the
protected names (cars, cars_cleanroom, cars_scan_*, postgres, template*) and the source
database itself. It runs in ONE REPEATABLE READ transaction under an advisory lock: it
first writes ``merge_backup_<run id>`` (the full pre-image row of every car it will update,
move or retire), ``merge_inserted_<run id>`` (the ids it inserts), ``merge_vpic_<run id>``
(the VINs it adds to nhtsa_vpic_cache), ``merge_touched_<run id>`` (every car id it wrote,
with the md5 of the row after the write) and a ``merge_snapshot_runs`` row, then writes in
chunks, each chunk verified against the merged image it was built from, then verifies before
COMMIT: row counts equal the plan, the cells kept from the target equal the plan's, the rows
the transaction wrote (``xmin``) are exactly the planned ids, and target-owned and target-only
columns are unchanged. Any mismatch rolls the whole transaction back. With --expect-plan <the
--plan-out of a dry run> the apply also refuses unless its live counts equal the reviewed ones.
Disk: the backup table holds a full copy of every touched row and the merge writes a new
version of each, so the dry run prints an estimate of the extra space (check the volume first).

--rollback <run id> (one transaction) refuses when a row the run wrote has changed since
(md5 against merge_touched; --allow-drift restores anyway) or when a foreign key
(car_comments) references a car the run inserted. It then deletes the inserted cars,
restores every pre-image row byte for byte, deletes the vpic rows the run added and the
touched cars' grid cards, verifies, and marks the run rolled back. ``cars_id_seq`` is not
rewound (ids are never reused). The run's tables stay as evidence until the owner drops them.

--cleanup (separate flag, separate transaction) drops only the names in
CLEANUP_DROP_TABLES, each re-verified at run time to have no ``git grep -F`` hit outside this
file and its test (it fails closed without git), never with CASCADE, then runs VACUUM
(ANALYZE) on cars and listings_grid_cards. The list is EMPTY: on 2026-11-09 all 13 old backup
tables on Railway (cars_backup_capacity_20260718, cars_backup_mcpeek_prescan_20260712,
cars_backup_mileage_contam_20260712, cars_backup_modelname_20260712,
cars_backup_stock_contam_20260711_171841, cars_backup_stock_contam_20260711_171901,
cars_backup_vinrepair_20260713, cars_epa_link_backup_20260921, cars_owner_backup_20260929,
epa_extended_specs_bak_20260711_101806, epa_extended_specs_bak_20260712_083724,
epa_extended_specs_bak_badhp_20260712, epa_extended_specs_bak_badratio_20260712) had a git
grep hit. Eleven are created by migrations/V001__baseline.sql (dropping them diverges the
database from the chain), ``cars_epa_link_backup_20260921`` and ``cars_owner_backup_20260929``
are explained live-only objects in backend/scripts/schema_drift_report.py, and owner decision
D-DB1 (docs/NEXT_PHASES_PLAN_2026_10.md) already settles all of them: export, then drop through
a migration (P13B.4), with epa_extended_specs_bak_* and cars_epa_link_backup_20260921 kept as
EPA recovery sources until P10D.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

REPO_ROOT = Path(__file__).resolve().parents[2]
SELF_REL = "backend/scripts/merge_dealer_snapshot.py"
TEST_REL = "backend/tests/test_merge_dealer_snapshot.py"

SCOPE_FORMAT = "merge_dealer_snapshot.scope/1"
PLAN_FORMAT = "merge_dealer_snapshot.plan/1"

VERDICTS_IN_SCOPE = ("ok", "thin", "inaccurate")
BUCKETS = ("new", "used", "unknown")
SCANNER_COVERAGE_FLOOR = 0.5
DEFAULT_MIN_COVERAGE = 0.7
DEFAULT_MIN_SIZE_RATIO = 0.8
DEFAULT_MIN_BUCKET_ROWS = 10

REQUIRED_CAR_COLUMNS = ("id", "vin", "dealer_id", "condition", "listing_active", "scraped_at", "first_seen_at")
# Admin edits on the website; the scanner upsert keeps cars.* for them too (upsert/sql.py).
TARGET_OWNED_COLUMNS = ("internal_notes", "marked_for_review")
# State of the listing and of the scrape: the source value wins even when it is empty.
ALWAYS_SOURCE_COLUMNS = ("dealer_id", "dealer_name", "dealer_url", "scraped_at", "listing_active",
                         "listing_removed_at")
# What "empty" means for a text value besides NULL and blank (idx_cars_active_packages uses the same set).
EMPTY_TEXT_VALUES = ("", "[]", "{}", "null")


@dataclass(frozen=True)
class RefGuard:
    """A cars column holding another table's id, plus the columns that travel with it."""

    column: str
    travel: tuple[str, ...]
    table: str
    identity: tuple[str, ...]


REF_GUARDS: tuple[RefGuard, ...] = (
    RefGuard("dealership_registry_id", (), "dealerships", ("website_url", "name")),
    RefGuard("epa_master_id", ("epa_match_confidence", "epa_match_method"), "epa_master",
             ("epa_vehicle_id", "year", "make", "model", "trim")),
)

PROTECTED_TARGETS = frozenset({"cars", "cars_cleanroom", "postgres", "template0", "template1"})
PROTECTED_TARGET_RE = re.compile(r"^cars_scan_")
LOCAL_HOSTS = frozenset({"", "localhost", "127.0.0.1", "::1"})

RUNS_TABLE = "merge_snapshot_runs"
RUN_ID_RE = re.compile(r"^[0-9]{14}_[0-9a-f]{6}$")
ADVISORY_LOCK_KEY = 7_302_061_908_715  # merge_dealer_snapshot; one merge or rollback at a time

# Verified 2026-11-09 with ``git grep -F <name>``: none of the old backup tables qualifies
# (module docstring). A name added here is re-checked against the repo at run time.
CLEANUP_DROP_TABLES: tuple[str, ...] = ()
CLEANUP_VACUUM_TABLES = ("cars", "listings_grid_cards")

STAGE_CHUNK = 2000
MD5_CHUNK = 5000
FRESH_SQL = "COALESCE(c.listing_active, 1) = 1 AND replace(c.scraped_at, ' ', 'T') >= %(since)s"

EXIT_OK, EXIT_ERROR, EXIT_REFUSED, EXIT_ABORTED = 0, 1, 2, 3


class MergeRefused(Exception):
    """A precondition failed; nothing was written."""


class MergeAborted(Exception):
    """A check inside the write transaction failed; the transaction was rolled back."""


def _say(msg: str) -> None:
    print(f"[merge {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _cols(cols: Iterable[str], alias: str | None = None) -> str:
    pre = f"{alias}." if alias else ""
    return ", ".join(pre + _qi(c) for c in cols)


def _md5_expr(cols: Sequence[str], alias: str = "c") -> str:
    return f"md5(ROW({_cols(cols, alias)})::text)"


def _chunks(items: Sequence[Any], n: int) -> Iterator[list[Any]]:
    for i in range(0, len(items), n):
        yield list(items[i:i + n])


def _now_iso() -> str:
    """The scanner's ``listing_removed_at`` format (inventory_reconcile, pipeline reconcile)."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _mb(n: float) -> str:
    return f"{n / 1_048_576:,.0f} MB"


def condition_bucket(value: Any) -> str:
    """The scanner's bucket rule (backend/scanner/inventory_reconcile.condition_bucket).

    Copied so this script imports nothing that could load ``.env``; a test pins the two equal.
    """
    c = str(value or "").strip().lower()
    if not c:
        return "unknown"
    if c.startswith("new"):
        return "new"
    return "used"


def _parse_ts(value: Any) -> datetime | None:
    s = str(value or "").strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def least_first_seen(target: Any, source: Any) -> Any:
    """The earlier of two ``first_seen_at`` texts, as the text it was stored with.

    A tie or a missing source keeps the target's text (no write); an unparseable value loses
    to a parseable one; two unparseable values compare as text.
    """
    if source is None or not str(source).strip():
        return target
    if target is None or not str(target).strip():
        return source
    t, s = _parse_ts(target), _parse_ts(source)
    if t is not None and s is not None:
        return source if s < t else target
    if t is None and s is not None:
        return source
    if s is None and t is not None:
        return target
    return source if str(source) < str(target) else target


def at_least_as_fresh(target_scraped: Any, source_scraped: Any) -> bool:
    """True when the target row was scraped at or after the source row: it is never overwritten."""
    t, s = _parse_ts(target_scraped), _parse_ts(source_scraped)
    if s is None:
        return True  # nothing trustworthy to write over it
    if t is None:
        return False
    return t >= s


def normalize_since(value: str) -> str:
    """``2026-11-03T17:59:55+00:00`` -> ``2026-11-03T17:59:55`` (UTC), the text scraped_at sorts against."""
    dt = _parse_ts(value)
    if dt is None:
        raise MergeRefused(f"not an ISO-8601 time: {value!r}")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def _conninfo(dsn: str) -> dict[str, str]:
    try:
        from psycopg.conninfo import conninfo_to_dict

        return {k: str(v) for k, v in conninfo_to_dict(dsn).items() if v is not None}
    except Exception:  # noqa: BLE001 - an unparseable DSN has no host we can vouch for
        return {}


def is_local_host(host: str | None, hostaddr: str | None = None) -> bool:
    """True for a unix socket directory or a loopback name/address (not a remote server)."""
    names = [h.strip() for h in (host or "").split(",")] + [h.strip() for h in (hostaddr or "").split(",")]
    return all(h in LOCAL_HOSTS or h.startswith("/") for h in names)


def _redact(text: str, dsn: str) -> str:
    """*text* without the DSN or its host, user and password."""
    hide = {dsn}
    for key in ("password", "host", "hostaddr", "user"):
        val = _conninfo(dsn).get(key)
        if val:
            hide.update(str(val).split(","))
    for s in sorted((h for h in hide if h and len(h) >= 3), key=len, reverse=True):
        text = text.replace(s, "***")
    return text


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


@dataclass
class Db:
    conn: Any
    role: str
    name: str
    version: str
    system_id: str | None
    read_only: bool
    local: bool


def open_db(env_name: str | None, role: str, *, read_only: bool) -> Db:
    """Connect with the DSN held in environment variable *env_name* (never printed)."""
    import psycopg

    if not env_name:
        raise MergeRefused(f"--{role}-dsn-env is required")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env_name):
        raise MergeRefused(f"--{role}-dsn-env must be the NAME of an environment variable "
                           "(letters, digits, underscore); the value given is not printed")
    dsn = (os.environ.get(env_name) or "").strip()
    if not dsn:
        raise MergeRefused(f"{role}: environment variable {env_name} is unset or empty")
    kwargs: dict[str, Any] = {"autocommit": True, "application_name": f"merge_dealer_snapshot_{role}"}
    if "connect_timeout" not in dsn.lower():
        kwargs["connect_timeout"] = 20
    if "keepalives" not in dsn.lower():
        # A long write transaction over a proxy must not look idle to it.
        kwargs.update(keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=6)
    try:
        conn = psycopg.connect(dsn, **kwargs)
    except Exception as exc:  # noqa: BLE001
        raise MergeRefused(f"{role}: cannot connect: {type(exc).__name__}: {_redact(str(exc), dsn)}") from None
    try:
        # Float text output is part of the row hashes compared across the two servers; every
        # unqualified name (cars, the run tables) is public's, whatever the role's default.
        conn.execute("SET extra_float_digits = 1")
        conn.execute("SET search_path = public")
        if read_only:
            conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY")
        else:
            conn.execute("SET statement_timeout = 0")
            conn.execute("SET idle_in_transaction_session_timeout = 0")
            conn.execute("SET lock_timeout = '15s'")
        name, version = conn.execute(
            "SELECT current_database(), current_setting('server_version')").fetchone()
        system_id = None
        try:
            row = conn.execute("SELECT system_identifier::text FROM pg_control_system()").fetchone()
            system_id = row[0] if row else None
        except Exception:  # noqa: BLE001 - needs a privileged role; autocommit keeps the session clean
            system_id = None
        # The host the session really reached (PGHOST and service files included), not the DSN text.
        local = is_local_host(conn.info.host, conn.info.hostaddr)
        conn.autocommit = False
        conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        if read_only:
            conn.read_only = True
    except Exception:
        conn.close()
        raise
    return Db(conn, role, name, version, system_id, read_only, local)


def _close(db: Db | None) -> None:
    if db is None:
        return
    try:
        if not db.conn.closed:
            db.conn.rollback()
            db.conn.close()
    except Exception:  # noqa: BLE001
        pass


def table_columns(db: Db, table: str) -> list[tuple[str, str]]:
    """``[(column, type)]`` of ``public.<table>`` in column order; empty when the table is missing."""
    return [(r[0], r[1]) for r in db.conn.execute(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
        "WHERE a.attrelid = to_regclass(%s) AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        (f"public.{table}",),
    ).fetchall()]


def table_exists(db: Db, table: str) -> bool:
    return db.conn.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table}",)).fetchone()[0]


def check_pair(src: Db, tgt: Db) -> None:
    if src.name == tgt.name and (src.system_id is None or src.system_id == tgt.system_id):
        raise MergeRefused(f"source and target are the same database ({src.name}); refusing")


def check_expect_db(tgt: Db, expect: str | None, *, required: bool, what: str) -> None:
    if required and not expect:
        raise MergeRefused(f"{what} requires --expect-db <the target's current_database()>")
    if expect and expect != tgt.name:
        raise MergeRefused(f"--expect-db {expect!r} does not match the target database {tgt.name!r}; nothing written")


def check_writable_target(tgt: Db, src: Db | None = None, *, allow_remote: bool = False) -> None:
    if tgt.name in PROTECTED_TARGETS or PROTECTED_TARGET_RE.match(tgt.name):
        raise MergeRefused(f"refusing to write to protected database {tgt.name!r}")
    if src is not None and src.name == tgt.name:
        raise MergeRefused(f"refusing to write to a database named like the source ({tgt.name!r})")
    if not tgt.local and not allow_remote:
        raise MergeRefused(f"the target database {tgt.name!r} is not on this machine; writing to it needs "
                           "--allow-remote-target (and the owner's approval of the runbook)")


def advisory_lock(db: Db) -> None:
    if not db.conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,)).fetchone()[0]:
        raise MergeRefused("another merge_dealer_snapshot apply/rollback holds the advisory lock")


def check_id_sequence(tgt: Db) -> None:
    """Inserts take ids from cars' own sequence; refuse when it is behind the table."""
    seq = tgt.conn.execute("SELECT pg_get_serial_sequence('public.cars', 'id')").fetchone()[0]
    if not seq:
        raise MergeRefused("cars.id has no owned sequence; inserted rows could not get a target id")
    last, called = tgt.conn.execute(f"SELECT last_value, is_called FROM {seq}").fetchone()
    top = tgt.conn.execute("SELECT COALESCE(max(id), 0) FROM cars").fetchone()[0]
    if (last if not called else last + 1) <= top:
        raise MergeRefused(f"{seq} (last_value {last}) is not ahead of max(cars.id) {top}; "
                           "inserting would collide with existing ids")


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------


def _is_text_type(typ: str) -> bool:
    t = typ.lower()
    return t == "text" or t.startswith(("character", "varchar", "citext"))


def empty_sql(expr: str, text: bool) -> str:
    """SQL test for the module's notion of an empty value."""
    if not text:
        return f"({expr} IS NULL)"
    values = ", ".join("'" + v + "'" for v in EMPTY_TEXT_VALUES)
    return f"({expr} IS NULL OR btrim({expr}) IN ({values}))"


@dataclass
class ColumnPlan:
    common: list[str]  # shared columns in the target's order, id included
    stage: list[str]  # common minus id: what an insert carries
    update_set: list[str]  # stage minus vin and the target-owned columns: what an update writes
    copied: list[str]  # source values moved unmodified (always + keepable), hashed for transport
    always: list[str]  # the source value always wins
    keepable: list[str]  # an empty source value keeps a non-empty target value
    text_cols: set[str]
    groups: list[tuple[RefGuard, list[str], int]]
    ref_cols: list[str]
    source_only: list[str]
    target_only: list[str]
    target_all: list[str]


def plan_columns(src_cols: list[tuple[str, str]], tgt_cols: list[tuple[str, str]]) -> ColumnPlan:
    s, t = dict(src_cols), dict(tgt_cols)
    if not s or not t:
        raise MergeRefused("both databases need a public.cars table")
    missing = [c for c in REQUIRED_CAR_COLUMNS if c not in s or c not in t]
    if missing:
        raise MergeRefused(f"cars lacks required column(s) on one side: {missing}")
    common = [c for c, _ in tgt_cols if c in s]
    mismatched = [f"{c} ({s[c]} vs {t[c]})" for c in common if s[c] != t[c]]
    if mismatched:
        raise MergeRefused(f"cars column types differ between source and target: {mismatched}")
    groups: list[tuple[RefGuard, list[str], int]] = []
    ref_cols: list[str] = []
    for g in REF_GUARDS:
        if g.column in s and g.column in t:
            gcols = [g.column] + [c for c in g.travel if c in s and c in t]
            groups.append((g, gcols, len(ref_cols)))
            ref_cols.extend(gcols)
    special = {"id", "vin", "first_seen_at", *TARGET_OWNED_COLUMNS, *ref_cols}
    stage = [c for c in common if c != "id"]
    copied = [c for c in common if c not in special]
    always = [c for c in copied if c in ALWAYS_SOURCE_COLUMNS]
    return ColumnPlan(
        common=common,
        stage=stage,
        update_set=[c for c in stage if c != "vin" and c not in TARGET_OWNED_COLUMNS],
        copied=copied,
        always=always,
        keepable=[c for c in copied if c not in ALWAYS_SOURCE_COLUMNS],
        text_cols={c for c in common if _is_text_type(t[c])},
        groups=groups,
        ref_cols=ref_cols,
        source_only=[c for c, _ in src_cols if c not in t],
        target_only=[c for c, _ in tgt_cols if c not in s],
        target_all=[c for c, _ in tgt_cols],
    )


def mask_expr(cols: ColumnPlan, alias: str = "c") -> str:
    """One character per keepable column: 1 when the row's value is non-empty, 0 when empty."""
    if not cols.keepable:
        return "''::text"
    parts = [f"CASE WHEN {empty_sql(f'{alias}.{_qi(c)}', c in cols.text_cols)} THEN '0' ELSE '1' END"
             for c in cols.keepable]
    return "concat(" + ", ".join(parts) + ")"


# ---------------------------------------------------------------------------
# Light reads (no full rows)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class SrcRow:
    dealer: str
    bucket: str
    first_seen: Any
    scraped: Any
    refs: tuple
    mask: str
    md5: str | None


@dataclass(slots=True)
class TgtRow:
    id: int
    dealer: str
    active: bool
    bucket: str
    first_seen: Any
    scraped: Any
    refs: tuple
    mask: str


def load_source_fresh(db: Db, cols: ColumnPlan, since: str, *, with_md5: bool) -> dict[str, SrcRow]:
    sel = ["c.vin", "c.dealer_id", "c.condition", "c.first_seen_at", "replace(c.scraped_at, ' ', 'T')"]
    sel += [f"c.{_qi(x)}" for x in cols.ref_cols]
    sel.append(mask_expr(cols))
    if with_md5:
        sel.append(_md5_expr(cols.copied))
    n = len(cols.ref_cols)
    out: dict[str, SrcRow] = {}
    with db.conn.cursor(name="merge_source_fresh") as cur:
        cur.itersize = 5000
        cur.execute(f"SELECT {', '.join(sel)} FROM cars c WHERE {FRESH_SQL}", {"since": since})
        for row in cur:
            out[row[0]] = SrcRow((row[1] or "").strip(), condition_bucket(row[2]), row[3], row[4],
                                 tuple(row[5:5 + n]), row[5 + n], row[6 + n] if with_md5 else None)
    return out


def load_target(db: Db, cols: ColumnPlan) -> dict[str, TgtRow]:
    sel = ["c.id", "c.vin", "c.dealer_id", "c.condition", "COALESCE(c.listing_active, 1) = 1",
           "replace(c.scraped_at, ' ', 'T')", "c.first_seen_at"]
    sel += [f"c.{_qi(x)}" for x in cols.ref_cols]
    sel.append(mask_expr(cols))
    n = len(cols.ref_cols)
    out: dict[str, TgtRow] = {}
    with db.conn.cursor(name="merge_target_rows") as cur:
        cur.itersize = 5000
        cur.execute(f"SELECT {', '.join(sel)} FROM cars c")
        for row in cur:
            out[row[1]] = TgtRow(int(row[0]), (row[2] or "").strip(), bool(row[4]), condition_bucket(row[3]),
                                 row[6], row[5], tuple(row[7:7 + n]), row[7 + n])
    return out


def load_claims(db: Db, since: str) -> dict[str, set[str]]:
    """VINs each dealer's feed listed in the run but the ownership guard refused (claimant side)."""
    have = {c for c, _ in table_columns(db, "vin_owner_conflicts")}
    if not {"vin", "claimant_dealer_id", "seen_at"} <= have:
        return {}
    out: dict[str, set[str]] = defaultdict(set)
    for vin, claimant in db.conn.execute(
        "SELECT vin, claimant_dealer_id FROM vin_owner_conflicts WHERE replace(seen_at, ' ', 'T') >= %s",
        (since,),
    ).fetchall():
        out[(claimant or "").strip()].add(vin)
    return dict(out)


def load_source_dealers(db: Db) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for d, n, newest in db.conn.execute(
        "SELECT COALESCE(dealer_id, ''), count(*), max(replace(scraped_at, ' ', 'T')) FROM cars GROUP BY 1"
    ).fetchall():
        e = out.setdefault((d or "").strip(), {"rows": 0, "newest": None})
        e["rows"] += int(n)
        if newest and (e["newest"] is None or newest > e["newest"]):
            e["newest"] = newest
    return out


def vin_set(db: Db, table: str) -> set[str]:
    out: set[str] = set()
    with db.conn.cursor(name=f"merge_vins_{table}") as cur:
        cur.itersize = 20000
        cur.execute(f"SELECT vin FROM {_qi(table)}")
        for (vin,) in cur:
            out.add(vin)
    return out


def _vpic_columns(src: Db, tgt: Db) -> list[str]:
    s = dict(table_columns(src, "nhtsa_vpic_cache"))
    t = table_columns(tgt, "nhtsa_vpic_cache")
    cols = [c for c, typ in t if c in s and s[c] == typ]
    return cols if "vin" in cols else []


def vpic_missing(src: Db, tgt: Db) -> list[str]:
    if not _vpic_columns(src, tgt):
        return []
    return sorted(vin_set(src, "nhtsa_vpic_cache") - vin_set(tgt, "nhtsa_vpic_cache"))


def _identity_ids(src: Db, tgt: Db, guard: RefGuard, ids: set[Any]) -> set[Any]:
    """The ids whose identity columns are equal in both databases' *guard.table*."""
    if not ids:
        return set()
    s, t = dict(table_columns(src, guard.table)), dict(table_columns(tgt, guard.table))
    ident = [c for c in guard.identity if c in s and c in t]
    if "id" not in s or "id" not in t or not ident:
        return set()
    q = f"SELECT x.id, {_md5_expr(ident, 'x')} FROM {_qi(guard.table)} x WHERE x.id = ANY(%s::bigint[])"
    keys = sorted(int(i) for i in ids)
    fs: dict[int, str] = {}
    ft: dict[int, str] = {}
    for chunk in _chunks(keys, MD5_CHUNK):
        fs.update({int(i): h for i, h in src.conn.execute(q, (chunk,)).fetchall()})
        ft.update({int(i): h for i, h in tgt.conn.execute(q, (chunk,)).fetchall()})
    return {i for i in ids if int(i) in fs and fs[int(i)] == ft.get(int(i))}


def safe_reference_ids(src: Db, tgt: Db, cols: ColumnPlan, fresh: dict[str, SrcRow], run: set[str]) -> list[set[Any]]:
    out = []
    for guard, _gcols, start in cols.groups:
        ids = {s.refs[start] for s in fresh.values() if s.dealer in run and s.refs[start] is not None}
        out.append(_identity_ids(src, tgt, guard, ids))
    return out


def final_refs(src_refs: tuple, tgt_refs: tuple | None, cols: ColumnPlan, safe: list[set[Any]]) -> tuple:
    """The reference columns a written row ends with (module docstring, REFERENCES).

    An update keeps the target's group unless the source's id is non-NULL and names the same row
    in both databases. An insert has no target group: the source's stands when its id is NULL or
    verified, and is NULL otherwise.
    """
    out = list(src_refs)
    for gi, (_guard, gcols, start) in enumerate(cols.groups):
        sid = src_refs[start]
        n = len(gcols)
        if tgt_refs is None:
            if sid is not None and sid not in safe[gi]:
                out[start:start + n] = [None] * n
        elif sid is None or sid not in safe[gi]:
            out[start:start + n] = list(tgt_refs[start:start + n])
    return tuple(out)


def _guarded(src_refs: tuple, cols: ColumnPlan, safe: list[set[Any]]) -> list[str]:
    return [g.column for gi, (g, _c, start) in enumerate(cols.groups)
            if src_refs[start] is not None and src_refs[start] not in safe[gi]]


# ---------------------------------------------------------------------------
# Triage and scope
# ---------------------------------------------------------------------------


def load_triage(patterns: Sequence[str]) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[str]]:
    files: set[str] = set()
    for p in patterns:
        hits = glob.glob(p)
        if not hits:
            raise MergeRefused(f"--triage {p!r} matched no file")
        files.update(hits)
    entries: dict[str, list[dict[str, Any]]] = defaultdict(list)
    meta: list[dict[str, Any]] = []
    started: list[str] = []
    for f in sorted(files):
        raw = Path(f).read_bytes()
        try:
            doc = json.loads(raw)
        except ValueError as exc:
            raise MergeRefused(f"triage file {f} is not JSON: {exc}") from None
        if not isinstance(doc, dict) or not isinstance(doc.get("dealers"), list):
            raise MergeRefused(f"triage file {f} has no dealers[] list")
        if doc.get("started"):
            started.append(str(doc["started"]))
        for r in doc["dealers"]:
            if isinstance(r, dict) and str(r.get("dealer_id") or "").strip():
                entries[str(r["dealer_id"]).strip()].append(r)
        meta.append({"path": f, "sha256": hashlib.sha256(raw).hexdigest(),
                     "dealers": len(doc["dealers"]), "started": doc.get("started")})
    return dict(entries), meta, started


@dataclass
class Rule:
    min_coverage: float
    min_size_ratio: float
    min_bucket_rows: int


def bucket_decision(bucket: str, st: dict[str, Any], tri: dict[str, Any], rule: Rule) -> tuple[bool, str]:
    """(retire_unseen, why) for one new/used bucket (module docstring, conditions 1-4)."""
    rec = tri.get("reconcile") if isinstance(tri.get("reconcile"), dict) else {}
    if not rec.get("eligible"):
        return False, f"triage reconcile not eligible ({rec.get('reason') or 'no reconcile'})"
    kept = ((rec.get("buckets") or {}).get(bucket) or {}).get("kept")
    if kept:
        return False, f"triage kept the {bucket} bucket ({kept})"
    ta, fresh, seen = st["target_active"], st["fresh"], st["reseen"]
    if ta == 0:
        return False, "no target rows in this bucket"
    if ta < rule.min_bucket_rows:
        return False, f"{ta} target rows < {rule.min_bucket_rows}"
    if fresh < rule.min_bucket_rows:
        return False, f"{fresh} fresh rows < {rule.min_bucket_rows}"
    size, cov = fresh / ta, seen / ta
    if size < rule.min_size_ratio:
        return False, f"size ratio {size:.3f} < {rule.min_size_ratio}"
    if cov < rule.min_coverage:
        return False, f"coverage {cov:.3f} < {rule.min_coverage}"
    return True, f"coverage {cov:.3f} >= {rule.min_coverage}, size ratio {size:.3f} >= {rule.min_size_ratio}"


def _dealer_stats(fresh: dict[str, SrcRow], claims: dict[str, set[str]], tgt: dict[str, TgtRow],
                  since: str) -> dict[str, dict[str, Any]]:
    stats: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "target_rows": 0, "target_active": 0, "target_newest": None, "target_rows_since": 0, "fresh": 0,
        "buckets": {b: {"target_active": 0, "fresh": 0, "reseen": 0, "moved_elsewhere": 0} for b in BUCKETS},
    })
    for vin, t in tgt.items():
        st = stats[t.dealer]
        st["target_rows"] += 1
        if t.scraped and (st["target_newest"] is None or t.scraped > st["target_newest"]):
            st["target_newest"] = t.scraped
        if t.scraped and t.scraped >= since:
            st["target_rows_since"] += 1
        if not t.active:
            continue
        st["target_active"] += 1
        b = st["buckets"][t.bucket]
        b["target_active"] += 1
        s = fresh.get(vin)
        if (s is not None and s.dealer == t.dealer) or vin in claims.get(t.dealer, ()):
            b["reseen"] += 1
        elif s is not None:
            b["moved_elsewhere"] += 1
    for s in fresh.values():
        stats[s.dealer]["fresh"] += 1
        stats[s.dealer]["buckets"][s.bucket]["fresh"] += 1
    return stats


def build_scope(*, src: Db, tgt: Db, fresh: dict[str, SrcRow], claims: dict[str, set[str]],
                tgt_rows: dict[str, TgtRow], src_dealers: dict[str, dict[str, Any]],
                triage: dict[str, list[dict[str, Any]]], triage_meta: list[dict[str, Any]], since: str,
                rule: Rule, excluded: set[str]) -> dict[str, Any]:
    stats = _dealer_stats(fresh, claims, tgt_rows, since)
    all_ids = sorted(set(stats) | set(src_dealers) | set(triage))
    dealers: dict[str, Any] = {}
    reasons: Counter = Counter()
    for d in all_ids:
        st = stats[d]
        entries = triage.get(d) or []
        tri = entries[0] if len(entries) == 1 else {}
        verdict = tri.get("verdict") if tri else None
        if not d:
            reason = "blank_dealer_id"
        elif d not in src_dealers:
            reason = "not_in_source"
        elif st["target_rows"] == 0:
            reason = "not_in_target"
        elif d in excluded:
            reason = "excluded_by_operator"
        elif not entries:
            reason = "not_in_triage"
        elif len(entries) > 1:
            reason = "duplicate_triage_entries"
        elif verdict not in VERDICTS_IN_SCOPE:
            reason = f"verdict_{verdict}"
        elif st["fresh"] == 0:
            reason = "no_fresh_rows"
        elif st["target_rows_since"] > 0:
            reason = "target_newer"
        else:
            reason = "in_scope"
        in_scope = reason == "in_scope"
        reasons[reason] += 1
        rec = tri.get("reconcile") if isinstance(tri.get("reconcile"), dict) else {}
        buckets: dict[str, Any] = {}
        retire: dict[str, bool] = {}
        for b in BUCKETS:
            bs = dict(st["buckets"][b])
            ta = bs["target_active"]
            bs["unseen"] = ta - bs["reseen"] - bs["moved_elsewhere"]
            bs["coverage"] = round(bs["reseen"] / ta, 4) if ta else None
            bs["size_ratio"] = round(bs["fresh"] / ta, 4) if ta else None
            bs["triage_kept"] = ((rec.get("buckets") or {}).get(b) or {}).get("kept")
            buckets[b] = bs
        for b in ("new", "used"):
            ok, why = bucket_decision(b, buckets[b], tri, rule) if in_scope else (False, "dealer out of scope")
            buckets[b]["retire_unseen"], buckets[b]["why"] = ok, why
            retire[b] = ok
        retire["unknown"] = retire["new"] and retire["used"]
        buckets["unknown"]["retire_unseen"] = retire["unknown"]
        buckets["unknown"]["why"] = ("new and used both retire" if retire["unknown"]
                                     else "blank condition retires only when new and used both do")
        dealers[d] = {
            "in_scope": in_scope,
            "reason": reason,
            "verdict": verdict,
            "triage_reason": tri.get("reason") if tri else None,
            "reconcile_eligible": bool(rec.get("eligible")) if tri else None,
            "triage_vin_owner_conflicts": tri.get("vin_owner_conflicts") if tri else None,
            "claims_in_run": len(claims.get(d, ())),
            "source": {"rows": (src_dealers.get(d) or {}).get("rows", 0), "fresh": st["fresh"],
                       "newest": (src_dealers.get(d) or {}).get("newest")},
            "target": {"rows": st["target_rows"], "active": st["target_active"], "newest": st["target_newest"],
                       "rows_since_scan": st["target_rows_since"]},
            "buckets": buckets,
            "retire_unseen": any(retire.values()),
            "retire_unseen_buckets": retire,
        }
    in_ids = [d for d, e in dealers.items() if e["in_scope"]]
    totals = {
        "dealers": len(dealers),
        "in_scope": len(in_ids),
        "out_of_scope_by_reason": dict(sorted((k, v) for k, v in reasons.items() if k != "in_scope")),
        "in_scope_target_active": sum(dealers[d]["target"]["active"] for d in in_ids),
        "in_scope_fresh": sum(dealers[d]["source"]["fresh"] for d in in_ids),
        "retire_unseen_dealers": sum(1 for d in in_ids if dealers[d]["retire_unseen"]),
        "retire_buckets": {b: sum(1 for d in in_ids if dealers[d]["retire_unseen_buckets"][b]) for b in BUCKETS},
        "unseen_in_retire_buckets": sum(dealers[d]["buckets"][b]["unseen"] for d in in_ids for b in BUCKETS
                                        if dealers[d]["retire_unseen_buckets"][b]),
        "unseen_kept": sum(dealers[d]["buckets"][b]["unseen"] for d in in_ids for b in BUCKETS
                           if not dealers[d]["retire_unseen_buckets"][b]),
    }
    return {
        "format": SCOPE_FORMAT,
        "created_at": _now_iso(),
        "since": since,
        "rule": {
            "verdicts_in_scope": list(VERDICTS_IN_SCOPE),
            "min_coverage": rule.min_coverage,
            "min_size_ratio": rule.min_size_ratio,
            "min_bucket_rows": rule.min_bucket_rows,
            "scanner_coverage_floor": SCANNER_COVERAGE_FLOOR,
            "excluded_by_operator": sorted(excluded),
            "doc": "backend/scripts/merge_dealer_snapshot.py module docstring",
        },
        "source": {"database": src.name, "server_version": src.version, "cars_rows": sum(
            e["rows"] for e in src_dealers.values()), "fresh_rows": len(fresh),
            "claims": sum(len(v) for v in claims.values())},
        "target": {"database": tgt.name, "server_version": tgt.version, "cars_rows": len(tgt_rows),
                   "active_rows": sum(1 for t in tgt_rows.values() if t.active)},
        "triage": {"files": triage_meta, "dealers": len(triage)},
        "totals": totals,
        "dealers": dealers,
    }


def read_scope(path: str | None) -> tuple[dict[str, Any], str]:
    if not path:
        raise MergeRefused("--scope <file written by --compute-scope> is required")
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise MergeRefused(f"cannot read scope file: {exc}") from None
    try:
        scope = json.loads(raw)
    except ValueError as exc:
        raise MergeRefused(f"scope file is not JSON: {exc}") from None
    if not isinstance(scope, dict) or scope.get("format") != SCOPE_FORMAT or not isinstance(scope.get("dealers"), dict):
        raise MergeRefused(f"scope file is not a {SCOPE_FORMAT} file")
    for d, e in scope["dealers"].items():
        if e.get("in_scope") and not isinstance(e.get("retire_unseen_buckets"), dict):
            raise MergeRefused(f"scope entry {d!r} has no retire_unseen_buckets")
    return scope, hashlib.sha256(raw).hexdigest()


def run_dealers(scope: dict[str, Any], only: str | None) -> set[str]:
    in_scope = {d for d, e in scope["dealers"].items() if e.get("in_scope")}
    if not only:
        return in_scope
    picked = {d.strip() for d in only.split(",") if d.strip()}
    bad = sorted(picked - in_scope)
    if bad:
        raise MergeRefused(f"--only-dealers names dealer(s) that are not in scope: {bad[:20]}")
    return picked


# ---------------------------------------------------------------------------
# Plan
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Action:
    action: str  # update | move | insert | retire
    vin: str
    car_id: int | None
    dealer: str
    from_dealer: str | None = None
    bucket: str | None = None
    first_seen: Any = None
    refs: tuple | None = None
    md5: str | None = None


@dataclass
class Plan:
    actions: list[Action]
    dealers: dict[str, Counter]
    vpic_missing: list[str]
    kept: Counter
    grid_cards: int = 0
    disk: dict[str, int] | None = None
    # (kind, vin, car_id, dealer, other_dealer, bucket) of every move, retirement and skipped
    # conflict: the rows a person should be able to read before an apply (--detail-out).
    detail: list[tuple] | None = None

    def totals(self) -> dict[str, int]:
        tot: Counter = Counter()
        for c in self.dealers.values():
            tot.update({k: v for k, v in c.items() if k != "move_in"})
        tot["retire"] = sum(tot[f"retire_{b}"] for b in BUCKETS)
        tot["kept_unseen"] = sum(tot[f"kept_unseen_{b}"] for b in BUCKETS)
        tot["skipped_conflicts"] = sum(v for k, v in tot.items() if k.startswith("skip_"))
        tot["vpic_inserts"] = len(self.vpic_missing)
        tot["grid_cards_deleted"] = self.grid_cards
        return dict(sorted(tot.items()))

    def writes(self) -> int:
        return len(self.actions) + len(self.vpic_missing)


def build_plan(*, scope: dict[str, Any], run: set[str], fresh: dict[str, SrcRow], claims: dict[str, set[str]],
               tgt: dict[str, TgtRow], safe: list[set[Any]], cols: ColumnPlan) -> Plan:
    retire = {d: scope["dealers"][d]["retire_unseen_buckets"] for d in run}
    counts: dict[str, Counter] = defaultdict(Counter)
    actions: list[Action] = []
    kept: Counter = Counter()
    detail: list[tuple] = []

    def skip(reason: str, ca: Counter, vin: str, t: TgtRow, a_id: str) -> None:
        ca[f"skip_{reason}"] += 1
        detail.append((f"skip_{reason}", vin, t.id, a_id, t.dealer, t.bucket))

    def write(kind: str, vin: str, s: SrcRow, t: TgtRow, ca: Counter, **extra: Any) -> None:
        first = least_first_seen(t.first_seen, s.first_seen)
        refs = final_refs(s.refs, t.refs, cols, safe)
        for col in _guarded(s.refs, cols, safe):
            ca[f"ref_guarded_{col}"] += 1
        cells = [c for i, c in enumerate(cols.keepable) if s.mask[i] == "0" and t.mask[i] == "1"]
        if cells:
            ca["keep_rows"] += 1
            kept.update(cells)
        actions.append(Action(kind, vin, t.id, s.dealer, bucket=s.bucket, first_seen=first, refs=refs,
                              md5=s.md5, **extra))
        if not t.active:
            ca["reactivate"] += 1

    for vin in sorted(fresh):
        s = fresh[vin]
        a_id = s.dealer
        if a_id not in run:
            continue
        t = tgt.get(vin)
        ca = counts[a_id]
        if t is None:
            for col in _guarded(s.refs, cols, safe):
                ca[f"ref_guarded_{col}"] += 1
            actions.append(Action("insert", vin, None, a_id, bucket=s.bucket, first_seen=s.first_seen,
                                  refs=final_refs(s.refs, None, cols, safe), md5=s.md5))
            ca["insert"] += 1
            continue
        if t.dealer == a_id:
            if at_least_as_fresh(t.scraped, s.scraped):
                ca["unchanged"] += 1
                continue
            write("update", vin, s, t, ca)
            ca["update"] += 1
            continue
        c_id = t.dealer
        if c_id not in run:
            skip("target_owner_out_of_scope", ca, vin, t, a_id)
            continue
        if t.active and vin in claims.get(c_id, ()):
            skip("contested_in_run", ca, vin, t, a_id)
            continue
        if t.active and not retire[c_id][t.bucket]:
            skip("target_owner_capture_weak", ca, vin, t, a_id)
            continue
        if at_least_as_fresh(t.scraped, s.scraped):
            skip("target_row_newer", ca, vin, t, a_id)
            continue
        write("move", vin, s, t, ca, from_dealer=c_id)
        detail.append(("move", vin, t.id, a_id, c_id, t.bucket))
        ca["move_in"] += 1
        counts[c_id]["move_out"] += 1
    for vin in sorted(tgt):
        t = tgt[vin]
        c_id = t.dealer
        if c_id not in run or not t.active:
            continue
        s = fresh.get(vin)
        if s is not None:
            continue  # seen: at C (updated or unchanged above) or at another dealer (moved or left alone)
        if vin in claims.get(c_id, ()):
            counts[c_id]["seen_by_claim"] += 1
            continue
        if retire[c_id][t.bucket]:
            actions.append(Action("retire", vin, t.id, c_id, bucket=t.bucket))
            detail.append(("retire", vin, t.id, c_id, None, t.bucket))
            counts[c_id][f"retire_{t.bucket}"] += 1
        else:
            counts[c_id][f"kept_unseen_{t.bucket}"] += 1
    for d in run:
        counts[d]  # every dealer of the run gets a line, even an idle one
    return Plan(actions=actions, dealers=dict(counts), vpic_missing=[], kept=kept, detail=detail)


def plan_document(plan: Plan, scope_sha: str, src: Db, tgt: Db) -> dict[str, Any]:
    return {
        "format": PLAN_FORMAT,
        "created_at": _now_iso(),
        "scope_sha256": scope_sha,
        "source_database": src.name,
        "target_database": tgt.name,
        "totals": plan.totals(),
        "kept_target_cells": dict(sorted(plan.kept.items())),
        "disk_estimate_bytes": plan.disk,
        "dealers": {d: dict(sorted(c.items())) for d, c in sorted(plan.dealers.items())},
    }


_COLS = (("update", "upd"), ("reactivate", "react"), ("move_in", "mv_in"), ("move_out", "mv_out"),
         ("insert", "ins"), ("retire_new", "ret_n"), ("retire_used", "ret_u"), ("retire_unknown", "ret_?"),
         ("kept_unseen", "kept"), ("skip", "skip"), ("unchanged", "same"))


def print_plan(plan: Plan, *, label: str) -> None:
    head = f"{'dealer':<44}" + "".join(f"{h:>8}" for _, h in _COLS)
    print(head)
    for d in sorted(plan.dealers):
        c = plan.dealers[d]
        vals = []
        for key, _h in _COLS:
            if key == "kept_unseen":
                vals.append(sum(c[f"kept_unseen_{b}"] for b in BUCKETS))
            elif key == "skip":
                vals.append(sum(v for k, v in c.items() if k.startswith("skip_")))
            else:
                vals.append(c[key])
        print(f"{d:<44}" + "".join(f"{v:>8}" for v in vals))
    tot = plan.totals()
    print(f"{label} TOTAL: updates={tot.get('update', 0)} (reactivated={tot.get('reactivate', 0)}) "
          f"moves={tot.get('move_out', 0)} inserts={tot.get('insert', 0)} retirements={tot['retire']} "
          f"(new={tot.get('retire_new', 0)} used={tot.get('retire_used', 0)} unknown={tot.get('retire_unknown', 0)}) "
          f"kept_unseen={tot['kept_unseen']} skipped_conflicts={tot['skipped_conflicts']} "
          f"(target_owner_out_of_scope={tot.get('skip_target_owner_out_of_scope', 0)} "
          f"contested_in_run={tot.get('skip_contested_in_run', 0)} "
          f"target_owner_capture_weak={tot.get('skip_target_owner_capture_weak', 0)} "
          f"target_row_newer={tot.get('skip_target_row_newer', 0)}) "
          f"seen_by_claim={tot.get('seen_by_claim', 0)} unchanged={tot.get('unchanged', 0)} "
          f"vpic_inserts={tot['vpic_inserts']} grid_cards_deleted={tot['grid_cards_deleted']}", flush=True)
    guarded = {k: v for k, v in tot.items() if k.startswith("ref_guarded_")}
    if guarded:
        print(f"{label} reference columns kept from the target (or NULL on insert): {guarded}", flush=True)
    if plan.kept:
        top = ", ".join(f"{c}={n}" for c, n in plan.kept.most_common(12))
        print(f"{label} KEEP RULE: {tot.get('keep_rows', 0)} updated rows keep a non-empty target value where "
              f"the source is empty ({sum(plan.kept.values())} cells); most: {top}", flush=True)
    if plan.disk:
        d = plan.disk
        print(f"{label} DISK: backup table about {_mb(d['backup'])}, new row versions about {_mb(d['versions'])}, "
              f"vpic rows about {_mb(d['vpic'])}; cars is {_mb(d['cars'])} today. Estimates from the average row "
              f"size; indexes and WAL come on top and VACUUM does not shrink files: check the volume first.",
              flush=True)


def write_detail(path: str, plan: Plan) -> None:
    """CSV of every move, retirement and skipped conflict (updates and inserts are too many to read)."""
    import csv

    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["kind", "vin", "car_id", "dealer", "other_dealer", "bucket"])
        w.writerows(sorted(plan.detail or [], key=lambda r: (r[0], r[3], r[1])))


def estimate_disk(tgt: Db, plan: Plan, n_rows: int) -> dict[str, int]:
    cars = tgt.conn.execute("SELECT pg_table_size('public.cars')").fetchone()[0]
    avg = cars / max(n_rows, 1)
    kinds = Counter(a.action for a in plan.actions)
    vpic = 0
    if plan.vpic_missing and table_exists(tgt, "nhtsa_vpic_cache"):
        size, n = tgt.conn.execute("SELECT pg_table_size('public.nhtsa_vpic_cache'), "
                                   "(SELECT count(*) FROM nhtsa_vpic_cache)").fetchone()
        vpic = int(size / max(n, 1) * len(plan.vpic_missing))
    return {"cars": int(cars), "backup": int(avg * (kinds["update"] + kinds["move"] + kinds["retire"])),
            "versions": int(avg * (kinds["update"] + kinds["move"] + kinds["insert"])), "vpic": vpic}


def scope_drift(scope: dict[str, Any], run: set[str], fresh: dict[str, SrcRow], claims: dict[str, set[str]],
                tgt: dict[str, TgtRow], since: str) -> list[str]:
    live = _dealer_stats(fresh, claims, tgt, since)
    out = []
    for d in sorted(run):
        frozen = scope["dealers"][d]["buckets"]
        for b in BUCKETS:
            now = {k: live[d]["buckets"][b][k] for k in ("target_active", "fresh", "reseen")}
            was = {k: frozen[b].get(k) for k in now}
            if now != was:
                out.append(f"{d}/{b}: scope {was} now {now}")
    return out


def _plan_for(src: Db, tgt: Db, scope: dict[str, Any], run: set[str], *, with_cards: bool) -> tuple[
        Plan, ColumnPlan, dict[str, SrcRow], dict[str, set[str]], dict[str, TgtRow]]:
    since = scope["since"]
    cols = plan_columns(table_columns(src, "cars"), table_columns(tgt, "cars"))
    if cols.source_only or cols.target_only:
        _say(f"columns: {len(cols.common)} shared; source-only {cols.source_only}; target-only {cols.target_only}")
    _say("reading fresh source rows")
    fresh = load_source_fresh(src, cols, since, with_md5=True)
    claims = load_claims(src, since)
    _say(f"source: {len(fresh)} fresh rows since {since}, {sum(len(v) for v in claims.values())} run claims; "
         "reading target rows")
    tgt_rows = load_target(tgt, cols)
    _say(f"target: {len(tgt_rows)} rows; planning")
    safe = safe_reference_ids(src, tgt, cols, fresh, run)
    plan = build_plan(scope=scope, run=run, fresh=fresh, claims=claims, tgt=tgt_rows, safe=safe, cols=cols)
    plan.vpic_missing = vpic_missing(src, tgt)
    if with_cards and table_exists(tgt, "listings_grid_cards"):
        ids = [a.car_id for a in plan.actions if a.action != "insert"]
        n = 0
        for chunk in _chunks(ids, MD5_CHUNK):
            n += tgt.conn.execute("SELECT count(*) FROM listings_grid_cards WHERE car_id = ANY(%s::bigint[])",
                                  (chunk,)).fetchone()[0]
        plan.grid_cards = int(n)
    plan.disk = estimate_disk(tgt, plan, len(tgt_rows))
    return plan, cols, fresh, claims, tgt_rows


# Derived or advisory numbers a reviewed plan does not pin: the web refills grid cards while
# people browse, and the disk estimate is a rough size.
_UNPINNED_TOTALS = ("grid_cards_deleted",)


def compare_expected_plan(path: str, doc: dict[str, Any]) -> None:
    try:
        want = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise MergeRefused(f"cannot read --expect-plan file: {exc}") from None
    if want.get("format") != PLAN_FORMAT:
        raise MergeRefused(f"--expect-plan is not a {PLAN_FORMAT} file")
    diffs = []
    if want.get("scope_sha256") != doc["scope_sha256"]:
        diffs.append("scope file differs from the one the dry run used")
    w_tot = {k: v for k, v in (want.get("totals") or {}).items() if k not in _UNPINNED_TOTALS}
    d_tot = {k: v for k, v in doc["totals"].items() if k not in _UNPINNED_TOTALS}
    if w_tot != d_tot:
        keys = sorted(set(w_tot) | set(d_tot))
        diffs += [f"total {k}: reviewed {w_tot.get(k)} now {d_tot.get(k)}" for k in keys if w_tot.get(k) != d_tot.get(k)]
    if (want.get("kept_target_cells") or {}) != doc["kept_target_cells"]:
        diffs.append("cells kept from the target differ from the reviewed plan")
    if want.get("dealers") != doc["dealers"]:
        ids = sorted(set(want.get("dealers") or {}) | set(doc["dealers"]))
        diffs += [f"dealer {d}: reviewed {(want.get('dealers') or {}).get(d)} now {doc['dealers'].get(d)}"
                  for d in ids if (want.get("dealers") or {}).get(d) != doc["dealers"].get(d)]
    if diffs:
        raise MergeRefused("live plan differs from --expect-plan: " + "; ".join(diffs[:15])
                           + (f" (+{len(diffs) - 15} more)" if len(diffs) > 15 else ""))


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------


def new_run_id() -> str:
    return f"{datetime.now(timezone.utc):%Y%m%d%H%M%S}_{secrets.token_hex(3)}"


def run_tables(run_id: str) -> dict[str, str]:
    if not RUN_ID_RE.match(run_id or ""):
        raise MergeRefused("run id must look like 20261109153000_a1b2c3")
    return {k: f"merge_{k}_{run_id}" for k in ("backup", "inserted", "touched", "vpic")}


RUNS_DDL = f"""
CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    seq              bigserial NOT NULL UNIQUE,
    run_id           text PRIMARY KEY,
    applied_at       text NOT NULL,
    source_database  text NOT NULL,
    target_database  text NOT NULL,
    scope_sha256     text NOT NULL,
    since            text NOT NULL,
    totals_json      text NOT NULL,
    status           text NOT NULL,
    rolled_back_at   text
)"""


def _xid(cur: Any) -> str:
    """This transaction's 32-bit xid as ``xmin::text`` prints it."""
    full = int(cur.execute("SELECT pg_current_xact_id()::text").fetchone()[0])
    return str(full % (1 << 32))


def _expect(n: int, want: int, what: str) -> None:
    if n != want:
        raise MergeAborted(f"{what}: wrote {n}, plan says {want}")


def merged_stage_sql(cols: ColumnPlan) -> str:
    """INSERT that turns the raw source rows into the images the target rows must end with.

    ``c`` is the current target row. For a keepable column the target's value survives only when
    the source's is empty and the target's is not (the module docstring's KEEP RULE).
    """
    exprs = []
    for col in cols.stage:
        r, c = f"r.{_qi(col)}", f"c.{_qi(col)}"
        if col in cols.keepable:
            text = col in cols.text_cols
            exprs.append(f"CASE WHEN r.target_id IS NOT NULL AND {empty_sql(r, text)} "
                         f"AND NOT {empty_sql(c, text)} THEN {c} ELSE {r} END")
        else:
            exprs.append(r)
    return (f"INSERT INTO _merge_stage (target_id, action, {_cols(cols.stage)}) "
            f"SELECT r.target_id, r.action, {', '.join(exprs)} "
            "FROM _merge_raw r LEFT JOIN cars c ON c.id = r.target_id")


def kept_cells_sql(cols: ColumnPlan) -> str:
    """Per keepable column, how many updated/moved rows ended with the target's value."""
    if not cols.keepable:
        return "SELECT 1 WHERE false"
    parts = []
    for col in cols.keepable:
        r, s = f"r.{_qi(col)}", f"s.{_qi(col)}"
        parts.append(f"count(*) FILTER (WHERE {r} IS DISTINCT FROM {s})")
    return (f"SELECT {', '.join(parts)} FROM _merge_raw r JOIN _merge_stage s ON s.target_id = r.target_id "
            "WHERE r.action IN ('update', 'move')")


def apply_plan(src: Db, tgt: Db, cols: ColumnPlan, plan: Plan, scope: dict[str, Any], scope_sha: str) -> str:
    since = scope["since"]
    run_id = new_run_id()
    tb = run_tables(run_id)
    now = _now_iso()
    cur = tgt.conn.cursor()
    by_kind = Counter(a.action for a in plan.actions)
    planned_existing = [a for a in plan.actions if a.action != "insert"]
    writes = [a for a in plan.actions if a.action in ("update", "move", "insert")]

    _say(f"run {run_id}: writing pre-image backup and run tables")
    cur.execute("CREATE TEMP TABLE _merge_plan (car_id bigint, vin text NOT NULL, action text NOT NULL) ON COMMIT DROP")
    with cur.copy("COPY _merge_plan (car_id, vin, action) FROM STDIN") as cp:
        for a in planned_existing:
            cp.write_row((a.car_id, a.vin, a.action))
    cur.execute("CREATE INDEX ON _merge_plan (car_id)")
    cur.execute(RUNS_DDL)
    cur.execute(f"CREATE TABLE {_qi(tb['backup'])} AS SELECT c.* FROM cars c JOIN _merge_plan p ON p.car_id = c.id")
    _expect(cur.rowcount, len(planned_existing), "pre-image backup rows")
    cur.execute(f"ALTER TABLE {_qi(tb['backup'])} ADD PRIMARY KEY (id)")
    cur.execute(f"CREATE TABLE {_qi(tb['inserted'])} (car_id bigint PRIMARY KEY, vin text NOT NULL UNIQUE)")
    cur.execute(f"CREATE TABLE {_qi(tb['vpic'])} (vin text PRIMARY KEY)")
    cur.execute(f"CREATE TABLE {_qi(tb['touched'])} (car_id bigint PRIMARY KEY, vin text NOT NULL, "
                "action text NOT NULL, post_md5 text NOT NULL)")

    # Source rows for update / move / insert, staged chunk by chunk and applied set-based:
    # _merge_raw holds them as the source has them, _merge_stage the images the rows end with.
    head = "NULL::bigint AS target_id, NULL::text AS action"
    cur.execute(f"CREATE TEMP TABLE _merge_raw ON COMMIT DROP AS SELECT {head}, NULL::text AS src_md5, "
                f"{_cols(cols.stage, 'c')} FROM cars c WITH NO DATA")
    cur.execute(f"CREATE TEMP TABLE _merge_stage ON COMMIT DROP AS SELECT {head}, "
                f"{_cols(cols.stage, 'c')} FROM cars c WITH NO DATA")
    pos = {c: i for i, c in enumerate(cols.stage)}
    fs_pos = pos["first_seen_at"]
    ref_pos = [pos[c] for c in cols.ref_cols]
    fetch = (f"SELECT {_cols(cols.stage, 'c')}, {_md5_expr(cols.copied)} FROM cars c "
             f"WHERE c.vin = ANY(%(vins)s::text[]) AND {FRESH_SQL}")
    set_sql = ", ".join(f"{_qi(c)} = s.{_qi(c)}" for c in cols.update_set)
    upd_sql = (f"UPDATE cars AS c SET {set_sql} FROM _merge_stage s "
               "WHERE s.action IN ('update', 'move') AND c.id = s.target_id AND c.vin = s.vin")
    ins_sql = (f"WITH ins AS (INSERT INTO cars ({_cols(cols.stage)}) SELECT {_cols(cols.stage)} FROM _merge_stage "
               f"WHERE action = 'insert' RETURNING id, vin) INSERT INTO {_qi(tb['inserted'])} (car_id, vin) "
               "SELECT id, vin FROM ins")
    chk_raw = f"SELECT count(*) FROM _merge_raw r WHERE {_md5_expr(cols.copied, 'r')} IS DISTINCT FROM r.src_md5"
    chk_upd = (f"SELECT count(*) FROM cars c JOIN _merge_stage s ON c.id = s.target_id "
               f"WHERE s.action IN ('update', 'move') AND "
               f"{_md5_expr(cols.update_set, 'c')} IS DISTINCT FROM {_md5_expr(cols.update_set, 's')}")
    chk_ins = (f"SELECT count(*) FROM cars c JOIN _merge_stage s ON c.vin = s.vin WHERE s.action = 'insert' AND "
               f"{_md5_expr(cols.stage, 'c')} IS DISTINCT FROM {_md5_expr(cols.stage, 's')}")
    merge_sql = merged_stage_sql(cols)
    kept_sql = kept_cells_sql(cols)
    done = Counter()
    kept_done: Counter = Counter()
    for i, chunk in enumerate(_chunks(writes, STAGE_CHUNK)):
        want = {a.vin: a for a in chunk}
        cur.execute("TRUNCATE _merge_raw, _merge_stage")
        rows = src.conn.execute(fetch, {"vins": list(want), "since": since}).fetchall()
        if len(rows) != len(want):
            raise MergeAborted(f"source returned {len(rows)} of {len(want)} planned fresh rows: it changed since planning")
        with cur.copy(f"COPY _merge_raw (target_id, action, src_md5, {_cols(cols.stage)}) FROM STDIN") as cp:
            for row in rows:
                vals = list(row[:-1])
                a = want[vals[pos["vin"]]]
                if row[-1] != a.md5:
                    raise MergeAborted(f"source row {a.vin} changed since planning")
                vals[fs_pos] = a.first_seen
                for p, v in zip(ref_pos, a.refs or ()):
                    vals[p] = v
                cp.write_row([a.car_id, a.action, row[-1], *vals])
        bad = cur.execute(chk_raw).fetchone()[0]
        if bad:
            raise MergeAborted(f"{bad} staged row(s) differ from the source rows they were copied from")
        n_upd = sum(1 for a in chunk if a.action in ("update", "move"))
        n_ins = len(chunk) - n_upd
        cur.execute(merge_sql)
        _expect(cur.rowcount, len(chunk), "merged images")
        if cols.keepable and n_upd:
            kept_done.update({c: int(n) for c, n in zip(cols.keepable, cur.execute(kept_sql).fetchone()) if n})
        cur.execute(upd_sql)
        _expect(cur.rowcount, n_upd, "updated rows")
        cur.execute(ins_sql)
        _expect(cur.rowcount, n_ins, "inserted rows")
        bad = cur.execute(chk_upd).fetchone()[0] + cur.execute(chk_ins).fetchone()[0]
        if bad:
            raise MergeAborted(f"{bad} written row(s) do not match the merged image they were built from")
        done["update"] += n_upd
        done["insert"] += n_ins
        if (i + 1) % 25 == 0:
            _say(f"  staged {done['update'] + done['insert']} of {len(writes)} rows")
    _say(f"updated/moved {done['update']}, inserted {done['insert']}")
    if +kept_done != +plan.kept:
        raise MergeAborted(f"cells kept from the target differ from the plan: wrote {dict(kept_done)}, "
                           f"plan says {dict(plan.kept)}")

    cur.execute("UPDATE cars AS c SET listing_active = 0, listing_removed_at = %s FROM _merge_plan p "
                "WHERE p.action = 'retire' AND c.id = p.car_id AND COALESCE(c.listing_active, 1) = 1", (now,))
    _expect(cur.rowcount, by_kind["retire"], "retired rows")

    n_vpic = 0
    if plan.vpic_missing:
        vcols = _vpic_columns(src, tgt)
        cur.execute(f"CREATE TEMP TABLE _merge_vpic ON COMMIT DROP AS SELECT {_cols(vcols)} FROM nhtsa_vpic_cache WITH NO DATA")
        with cur.copy(f"COPY _merge_vpic ({_cols(vcols)}) FROM STDIN") as cp:
            for chunk in _chunks(plan.vpic_missing, STAGE_CHUNK):
                for row in src.conn.execute(f"SELECT {_cols(vcols)} FROM nhtsa_vpic_cache WHERE vin = ANY(%s::text[])",
                                            (chunk,)).fetchall():
                    cp.write_row(row)
        cur.execute(f"WITH ins AS (INSERT INTO nhtsa_vpic_cache ({_cols(vcols)}) SELECT {_cols(vcols)} FROM _merge_vpic "
                    f"ON CONFLICT (vin) DO NOTHING RETURNING vin) INSERT INTO {_qi(tb['vpic'])} (vin) SELECT vin FROM ins")
        n_vpic = cur.rowcount
        _expect(n_vpic, len(plan.vpic_missing), "nhtsa_vpic_cache rows inserted")

    n_cards = 0
    if table_exists(tgt, "listings_grid_cards"):
        cur.execute("DELETE FROM listings_grid_cards g USING _merge_plan p WHERE g.car_id = p.car_id")
        n_cards = cur.rowcount
    plan.grid_cards = n_cards

    # Verify before COMMIT.
    same_cols = [c for c in cols.target_all if c not in cols.update_set or c in TARGET_OWNED_COLUMNS]
    keep_cols = [c for c in cols.target_all if c not in ("listing_active", "listing_removed_at")]
    bad = cur.execute(
        f"SELECT count(*) FROM cars c JOIN {_qi(tb['backup'])} b ON b.id = c.id JOIN _merge_plan p ON p.car_id = c.id "
        f"WHERE (p.action IN ('update', 'move') AND {_md5_expr(same_cols, 'c')} IS DISTINCT FROM {_md5_expr(same_cols, 'b')}) "
        f"OR (p.action = 'retire' AND ({_md5_expr(keep_cols, 'c')} IS DISTINCT FROM {_md5_expr(keep_cols, 'b')} "
        "OR c.listing_active IS DISTINCT FROM 0))").fetchone()[0]
    if bad:
        raise MergeAborted(f"{bad} row(s) changed a column the merge must not touch")
    xid = _xid(cur)
    written = {int(r[0]) for r in cur.execute("SELECT id FROM cars WHERE xmin::text = %s", (xid,)).fetchall()}
    inserted = {int(r[0]) for r in cur.execute(f"SELECT car_id FROM {_qi(tb['inserted'])}").fetchall()}
    expected = {int(a.car_id) for a in planned_existing} | inserted
    if written != expected:
        raise MergeAborted(f"rows written by this transaction differ from the plan: {len(written - expected)} "
                           f"unplanned, {len(expected - written)} planned but unwritten")
    cur.execute(f"INSERT INTO {_qi(tb['touched'])} (car_id, vin, action, post_md5) "
                "SELECT c.id, c.vin, p.action, md5(c::text) FROM cars c JOIN _merge_plan p ON p.car_id = c.id")
    cur.execute(f"INSERT INTO {_qi(tb['touched'])} (car_id, vin, action, post_md5) "
                f"SELECT c.id, c.vin, 'insert', md5(c::text) FROM cars c JOIN {_qi(tb['inserted'])} i ON i.car_id = c.id")
    totals = plan.totals()
    totals["kept_target_cells"] = dict(sorted(kept_done.items()))
    cur.execute(f"INSERT INTO {RUNS_TABLE} (run_id, applied_at, source_database, target_database, scope_sha256, since, "
                "totals_json, status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'applied')",
                (run_id, now, src.name, tgt.name, scope_sha, since, json.dumps(totals, sort_keys=True)))
    tgt.conn.commit()
    _say(f"COMMITTED run {run_id}: {done['update']} updated/moved, {done['insert']} inserted, "
         f"{by_kind['retire']} retired, {n_vpic} vpic rows, {n_cards} grid cards deleted, "
         f"{sum(kept_done.values())} target cells kept ({dict(kept_done.most_common(8))})")
    _say(f"undo with: --rollback {run_id} --expect-db {tgt.name}; rebuild cards with build_listings_grid_cards")
    return run_id


def run_merge(args: argparse.Namespace, *, apply: bool) -> int:
    scope, scope_sha = read_scope(args.scope)
    src = tgt = None
    try:
        src = open_db(args.source_dsn_env, "source", read_only=True)
        tgt = open_db(args.target_dsn_env, "target", read_only=not apply)
        check_pair(src, tgt)
        check_expect_db(tgt, args.expect_db, required=apply, what="--apply")
        if apply:
            check_writable_target(tgt, src, allow_remote=args.allow_remote_target)
            advisory_lock(tgt)
        if scope.get("source", {}).get("database") != src.name:
            raise MergeRefused(f"the scope was computed from source {scope.get('source', {}).get('database')!r}, "
                               f"not {src.name!r}")
        run = run_dealers(scope, args.only_dealers)
        _say(f"{'APPLY' if apply else 'DRY RUN'}: source {src.name} -> target {tgt.name}; {len(run)} dealer(s); "
             f"scope sha256 {scope_sha[:12]}")
        if scope.get("target", {}).get("database") != tgt.name:
            _say(f"NOTE: the scope was computed against target {scope.get('target', {}).get('database')!r}; "
                 "its dealer decisions are applied as frozen, the bucket numbers below are re-checked")
        plan, cols, fresh, claims, tgt_rows = _plan_for(src, tgt, scope, run, with_cards=True)
        drift = scope_drift(scope, run, fresh, claims, tgt_rows, scope["since"])
        if drift:
            _say(f"NOTE: {len(drift)} bucket(s) differ from the frozen scope numbers (decisions stay frozen): "
                 + "; ".join(drift[:10]))
        print_plan(plan, label="APPLY PLAN" if apply else "DRY RUN")
        doc = plan_document(plan, scope_sha, src, tgt)
        if args.plan_out:
            Path(args.plan_out).write_text(json.dumps(doc, indent=1, sort_keys=True) + "\n", encoding="utf-8")
            _say(f"plan written to {args.plan_out}")
        if args.detail_out:
            write_detail(args.detail_out, plan)
            _say(f"moves, retirements and skipped conflicts written to {args.detail_out}")
        if not apply:
            return EXIT_OK
        if plan.writes() == 0:
            _say("nothing to apply: the target already holds this scope's merge (no tables created)")
            return EXIT_OK
        if args.expect_plan:
            compare_expected_plan(args.expect_plan, doc)
        if any(a.action == "insert" for a in plan.actions):
            check_id_sequence(tgt)
        apply_plan(src, tgt, cols, plan, scope, scope_sha)
        return EXIT_OK
    finally:
        _close(src)
        _close(tgt)


def compute_scope(args: argparse.Namespace) -> int:
    if not args.scope:
        raise MergeRefused("--compute-scope needs --scope <path to write>")
    out = Path(args.scope)
    if out.exists():
        raise MergeRefused(f"scope file {out} exists; a scope is frozen once written (choose a new path)")
    if not args.triage:
        raise MergeRefused("--compute-scope needs --triage <triage.json glob> (repeatable)")
    if not (SCANNER_COVERAGE_FLOOR < args.min_coverage <= 1.0):
        raise MergeRefused(f"--min-coverage must be stricter than the scanner's {SCANNER_COVERAGE_FLOOR} floor and <= 1")
    if not (0.0 < args.min_size_ratio <= 2.0) or args.min_bucket_rows < 1:
        raise MergeRefused("--min-size-ratio must be in (0, 2] and --min-bucket-rows >= 1")
    triage, meta, started = load_triage(args.triage)
    if args.since:
        since = normalize_since(args.since)
    else:
        if not started:
            raise MergeRefused("no triage file has 'started'; pass --since")
        since = min(normalize_since(s) for s in started)
    rule = Rule(args.min_coverage, args.min_size_ratio, args.min_bucket_rows)
    src = tgt = None
    try:
        src = open_db(args.source_dsn_env, "source", read_only=True)
        tgt = open_db(args.target_dsn_env, "target", read_only=True)
        check_pair(src, tgt)
        check_expect_db(tgt, args.expect_db, required=False, what="--compute-scope")
        cols = plan_columns(table_columns(src, "cars"), table_columns(tgt, "cars"))
        _say(f"SCOPE: source {src.name} -> target {tgt.name}; since {since}; {len(triage)} triage dealers")
        fresh = load_source_fresh(src, cols, since, with_md5=False)
        claims = load_claims(src, since)
        src_dealers = load_source_dealers(src)
        tgt_rows = load_target(tgt, cols)
        scope = build_scope(src=src, tgt=tgt, fresh=fresh, claims=claims, tgt_rows=tgt_rows, src_dealers=src_dealers,
                            triage=triage, triage_meta=meta, since=since, rule=rule,
                            excluded={d.strip() for d in args.exclude_dealer if d.strip()})
    finally:
        _close(src)
        _close(tgt)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "x", encoding="utf-8") as fh:
        fh.write(json.dumps(scope, indent=1, sort_keys=True) + "\n")
    t = scope["totals"]
    _say(f"scope written to {out}: {t['in_scope']} of {t['dealers']} dealers in scope; out: {t['out_of_scope_by_reason']}")
    _say(f"retire_unseen: {t['retire_unseen_dealers']} dealers, buckets {t['retire_buckets']}; unseen rows in "
         f"retire buckets {t['unseen_in_retire_buckets']}, kept listed {t['unseen_kept']}")
    return EXIT_OK


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def rollback(args: argparse.Namespace) -> int:
    tb = run_tables(args.rollback)
    tgt = None
    try:
        tgt = open_db(args.target_dsn_env, "target", read_only=False)
        check_expect_db(tgt, args.expect_db, required=True, what="--rollback")
        check_writable_target(tgt, allow_remote=args.allow_remote_target)
        advisory_lock(tgt)
        cur = tgt.conn.cursor()
        missing = [t for t in tb.values() if not table_exists(tgt, t)]
        if missing or not table_exists(tgt, RUNS_TABLE):
            raise MergeRefused(f"run {args.rollback} is unknown here (missing {missing or [RUNS_TABLE]})")
        row = cur.execute(f"SELECT status FROM {RUNS_TABLE} WHERE run_id = %s", (args.rollback,)).fetchone()
        if row is None or row[0] != "applied":
            raise MergeRefused(f"run {args.rollback} is {row[0] if row else 'not registered'}; only an applied run rolls back")
        later = [r[0] for r in cur.execute(
            f"SELECT run_id FROM {RUNS_TABLE} WHERE status = 'applied' "
            f"AND seq > (SELECT seq FROM {RUNS_TABLE} WHERE run_id = %s) ORDER BY seq",
            (args.rollback,)).fetchall()]
        if later:
            raise MergeRefused(f"later applied run(s) {later} sit on top of {args.rollback}; roll those back first")
        drift = [r[0] for r in cur.execute(
            f"SELECT t.car_id FROM {_qi(tb['touched'])} t LEFT JOIN cars c ON c.id = t.car_id "
            "WHERE c.id IS NULL OR md5(c::text) <> t.post_md5 ORDER BY 1").fetchall()]
        if drift and not args.allow_drift:
            raise MergeRefused(f"{len(drift)} row(s) the run wrote changed since (ids {drift[:20]}); "
                               "re-run with --allow-drift to restore the pre-image anyway")
        fk_hits = []
        for table, column in cur.execute(
            "SELECT c.conrelid::regclass::text, a.attname FROM pg_constraint c "
            "JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1] "
            "WHERE c.contype = 'f' AND c.confrelid = 'public.cars'::regclass").fetchall():
            n = cur.execute(f"SELECT count(*) FROM {table} x JOIN {_qi(tb['inserted'])} i ON i.car_id = x.{_qi(column)}"
                            ).fetchone()[0]
            if n:
                fk_hits.append(f"{table}.{column}: {n}")
        if fk_hits:
            raise MergeRefused(f"rows reference cars this run inserted ({fk_hits}); deleting them would delete user data")
        n_ins = cur.execute(f"SELECT count(*) FROM {_qi(tb['inserted'])}").fetchone()[0]
        cur.execute(f"DELETE FROM cars c USING {_qi(tb['inserted'])} i WHERE c.id = i.car_id")
        if cur.rowcount != n_ins and not args.allow_drift:
            raise MergeAborted(f"deleted {cur.rowcount} of {n_ins} inserted cars")
        bcols = [c for c, _ in table_columns(tgt, tb["backup"])]
        ccols = {c for c, _ in table_columns(tgt, "cars")}
        if set(bcols) - ccols:
            raise MergeRefused(f"cars lost column(s) since the run: {sorted(set(bcols) - ccols)}; cannot restore exactly")
        rest = [c for c in bcols if c != "id"]
        n_bak = cur.execute(f"SELECT count(*) FROM {_qi(tb['backup'])}").fetchone()[0]
        cur.execute(f"UPDATE cars AS c SET ({_cols(rest)}) = ROW({_cols(rest, 'b')}) FROM {_qi(tb['backup'])} b "
                    "WHERE c.id = b.id")
        restored = cur.rowcount
        if restored != n_bak:
            if not args.allow_drift:
                raise MergeAborted(f"restored {restored} of {n_bak} backed-up rows")
            cur.execute(f"INSERT INTO cars ({_cols(bcols)}) SELECT {_cols(bcols, 'b')} FROM {_qi(tb['backup'])} b "
                        "WHERE NOT EXISTS (SELECT 1 FROM cars c WHERE c.id = b.id)")
            restored += cur.rowcount
        cur.execute(f"DELETE FROM nhtsa_vpic_cache v USING {_qi(tb['vpic'])} m WHERE v.vin = m.vin")
        n_vpic = cur.rowcount
        n_cards = 0
        if table_exists(tgt, "listings_grid_cards"):
            cur.execute(f"DELETE FROM listings_grid_cards g USING {_qi(tb['touched'])} t WHERE g.car_id = t.car_id")
            n_cards = cur.rowcount
        bad = cur.execute(f"SELECT count(*) FROM {_qi(tb['backup'])} b LEFT JOIN cars c ON c.id = b.id "
                          f"WHERE c.id IS NULL OR {_md5_expr(bcols, 'c')} <> {_md5_expr(bcols, 'b')}").fetchone()[0]
        left = cur.execute(f"SELECT count(*) FROM cars c JOIN {_qi(tb['inserted'])} i ON i.car_id = c.id").fetchone()[0]
        if bad or left:
            raise MergeAborted(f"rollback verification failed: {bad} rows differ from the pre-image, {left} inserted remain")
        cur.execute(f"UPDATE {RUNS_TABLE} SET status = 'rolled_back', rolled_back_at = %s WHERE run_id = %s",
                    (_now_iso(), args.rollback))
        tgt.conn.commit()
        _say(f"ROLLED BACK run {args.rollback}: deleted {n_ins} inserted cars, restored {restored} rows byte for byte, "
             f"removed {n_vpic} vpic rows and {n_cards} grid cards (they rebuild); "
             f"{len(drift)} drifted row(s) overwritten. Run tables kept: {sorted(tb.values())}")
        return EXIT_OK
    finally:
        _close(tgt)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def repo_references(name: str) -> list[str]:
    """Tracked files (other than this script and its test) that contain *name*; raises when git cannot answer."""
    try:
        res = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "grep", "-l", "-I", "-F", "-e", name, "--", ".",
             f":(exclude){SELF_REL}", f":(exclude){TEST_REL}"],
            capture_output=True, text=True, check=False, timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise MergeRefused(f"cannot verify {name!r} with git grep ({type(exc).__name__}); refusing to drop") from None
    if res.returncode == 1:
        return []
    if res.returncode != 0:
        raise MergeRefused(f"git grep failed for {name!r} (exit {res.returncode}); refusing to drop")
    return [line for line in res.stdout.splitlines() if line.strip()]


def cleanup(args: argparse.Namespace) -> int:
    names = list(CLEANUP_DROP_TABLES)
    for n in names:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", n):
            raise MergeRefused(f"cleanup table name {n!r} is not a plain identifier")
    refs = {n: repo_references(n) for n in names}
    hit = {n: r for n, r in refs.items() if r}
    if hit:
        raise MergeRefused(f"refusing to drop tables the repo still references: {hit}")
    tgt = None
    try:
        tgt = open_db(args.target_dsn_env, "target", read_only=False)
        check_expect_db(tgt, args.expect_db, required=True, what="--cleanup")
        check_writable_target(tgt, allow_remote=args.allow_remote_target)
        cur = tgt.conn.cursor()
        dropped = []
        for n in names:
            if not table_exists(tgt, n):
                _say(f"{n}: absent, nothing to drop")
                continue
            size = cur.execute("SELECT pg_size_pretty(pg_total_relation_size(to_regclass(%s)))",
                               (f"public.{n}",)).fetchone()[0]
            cur.execute(f"DROP TABLE public.{_qi(n)}")
            dropped.append(f"{n} ({size})")
        tgt.conn.commit()
        _say(f"dropped {len(dropped)} table(s): {dropped}" if dropped else "no backup table to drop (CLEANUP_DROP_TABLES)")
        tgt.conn.autocommit = True
        for t in CLEANUP_VACUUM_TABLES:
            if not table_exists(tgt, t):
                continue
            t0 = time.time()
            tgt.conn.execute(f"VACUUM (ANALYZE) public.{_qi(t)}")
            size = tgt.conn.execute("SELECT pg_size_pretty(pg_total_relation_size(to_regclass(%s)))",
                                    (f"public.{t}",)).fetchone()[0]
            _say(f"VACUUM (ANALYZE) {t}: {time.time() - t0:.0f}s, total size {size}")
        return EXIT_OK
    finally:
        _close(tgt)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="merge_dealer_snapshot",
        description="Dealer-scoped merge of one inventory snapshot into another (module docstring).",
    )
    p.add_argument("--source-dsn-env", metavar="NAME", help="environment variable holding the source DSN")
    p.add_argument("--target-dsn-env", metavar="NAME", help="environment variable holding the target DSN")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--compute-scope", action="store_true", help="write the frozen scope file (read-only)")
    mode.add_argument("--dry-run", action="store_true", help="print the exact plan (read-only; the default)")
    mode.add_argument("--apply", action="store_true", help="apply the plan in one transaction")
    mode.add_argument("--rollback", metavar="RUN_ID", help="restore the target to before run RUN_ID")
    mode.add_argument("--cleanup", action="store_true",
                      help="drop CLEANUP_DROP_TABLES (verified), then VACUUM (ANALYZE) cars and listings_grid_cards")
    p.add_argument("--scope", metavar="PATH", help="scope file (written by --compute-scope, read otherwise)")
    p.add_argument("--triage", action="append", metavar="GLOB", help="triage.json file or glob (repeatable)")
    p.add_argument("--since", help="scan start (ISO-8601); default: earliest triage 'started'")
    p.add_argument("--exclude-dealer", action="append", default=[], metavar="ID", help="keep this dealer out of scope")
    p.add_argument("--min-coverage", type=float, default=DEFAULT_MIN_COVERAGE,
                   help="share of a bucket's target rows the scan must re-see before unseen rows retire "
                        f"(default {DEFAULT_MIN_COVERAGE}; must exceed the scanner's {SCANNER_COVERAGE_FLOOR})")
    p.add_argument("--min-size-ratio", type=float, default=DEFAULT_MIN_SIZE_RATIO)
    p.add_argument("--min-bucket-rows", type=int, default=DEFAULT_MIN_BUCKET_ROWS)
    p.add_argument("--only-dealers", metavar="A,B", help="merge only these in-scope dealers (a pilot)")
    p.add_argument("--plan-out", metavar="PATH", help="write the plan's counts as JSON")
    p.add_argument("--detail-out", metavar="PATH",
                   help="write every move, retirement and skipped conflict (vin, dealers, bucket) as CSV")
    p.add_argument("--expect-plan", metavar="PATH", help="--apply refuses unless its counts equal this plan file")
    p.add_argument("--expect-db", metavar="NAME", help="the target's current_database(); required to write")
    p.add_argument("--allow-remote-target", action="store_true",
                   help="allow a writing mode (--apply, --rollback, --cleanup) on a target that is not on this machine")
    p.add_argument("--allow-drift", action="store_true", help="--rollback: restore rows changed since the run")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.compute_scope:
            return compute_scope(args)
        if args.rollback:
            return rollback(args)
        if args.cleanup:
            return cleanup(args)
        return run_merge(args, apply=bool(args.apply))
    except MergeRefused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr, flush=True)
        return EXIT_REFUSED
    except MergeAborted as exc:
        print(f"ABORTED (rolled back, nothing changed): {exc}", file=sys.stderr, flush=True)
        return EXIT_ABORTED
    except Exception as exc:  # noqa: BLE001
        import psycopg

        if isinstance(exc, psycopg.Error):
            # Server errors carry SQL and values, never the DSN (connect errors are redacted in open_db).
            print(f"ERROR ({type(exc).__name__}; the open transaction was rolled back, nothing committed "
                  f"unless COMMITTED was printed): {exc}", file=sys.stderr, flush=True)
            return EXIT_ERROR
        raise


if __name__ == "__main__":
    sys.exit(main())
