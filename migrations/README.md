# Schema migrations

Versioned, forward-only DDL for the inventory Postgres database.

Before this directory existed, the only way to rebuild the database was to replay
`dump-dealership_scanner-202607012302.sql` -- a snapshot of one afternoon in July --
and then hope the ad-hoc `ALTER TABLE`s that landed since were all reproduced by
`init_postgres_inventory()`'s `CREATE TABLE IF NOT EXISTS` / `pg_add_columns` passes.
That code creates a schema that is *close to* production, not identical to it, and
nothing records which of the two a given database actually has. A migration chain
makes the schema a reviewable artifact in git instead of a property of whichever
box has been running longest.

## Convention

Modeled on the Flyway naming convention (this is a small self-contained runner,
not Flyway itself and not a port of anyone else's chain):

    V<NNN>__<short_description>.sql

* `NNN` is a zero-padded integer. Files apply in ascending numeric order, so
  `V002` runs before `V010`.
* Two files may not share a version number.
* `<short_description>` is snake_case and describes the change:
  `V004__add_cars_epa_master_id.sql`.

## Rules

**Applied files are immutable.** The runner stores a SHA-256 of every file it
applies. If a file's contents change afterwards it refuses to run at all, because
a database that ran the old text and a database that ran the new text are no
longer the same schema while both claim to be at the same version. To change
something you already shipped, add a new higher-numbered file.

**Forward-only.** There are no `down` scripts. A rollback is a new migration that
undoes the previous one, so the chain stays a truthful, replayable history.

**Additive by default.** A migration runs against a live production database.
Prefer `ADD COLUMN` / `CREATE INDEX IF NOT EXISTS` / backfill-then-swap over
anything that takes a long exclusive lock on `cars` (58k+ rows and a scanner
writing to it). Destructive changes (`DROP COLUMN`, `NOT NULL` on an existing
column) belong in their own file so they can be reviewed and timed on their own.

**Plain SQL only.** Each file is executed as a single statement batch inside one
transaction, by psycopg -- not by `psql`. Backslash meta-commands (`\i`,
`\connect`, `\copy`) will not work. Statements that cannot run inside a
transaction (`CREATE INDEX CONCURRENTLY`, `ALTER TYPE ... ADD VALUE` on older
servers) need their own file and a note; the runner will report the failure and
leave nothing half-applied.

**A new table is a new migration, never runtime DDL.** Do not add a
`CREATE TABLE IF NOT EXISTS` (or `ADD COLUMN`, or `CREATE INDEX`) to a module's
`ensure_*` helper or to `init_postgres_inventory()`. Runtime DDL is what made the
schema a property of whichever box ran longest, and its `CREATE INDEX` passes
caused the 2026-07-30 lock pile-up. A process on a database at the expected
version skips the legacy DDL entirely (`backend/db/schema_version.py`).

**Locks.** The runner sets `SET LOCAL lock_timeout = '5s'` before every file
(`--lock-timeout` changes it, `0` means no limit). A file that waits longer than
that for a lock on `cars` fails and rolls back instead of queueing every reader
behind it. A file that must wait longer (an index or a foreign key on `cars` in
a quiet window) sets its own `SET LOCAL lock_timeout` at the top; the file's own
setting wins.

## Running

    python -m backend.scripts.migrate                     # dry run (default): lists pending files, writes nothing
    python -m backend.scripts.migrate --apply             # apply pending files, one transaction each
    python -m backend.scripts.migrate --apply --target 26 # apply pending files up to V026 only
    python -m backend.scripts.migrate --record-only 19 --apply  # record V019 without running it

Bookkeeping lives in `schema_migrations (version, name, checksum, applied_at)` in
the same database, created on first `--apply`.

* The first log line names the target: `target: host=... port=... db=...`. The
  user, password and connection options are never logged. Read it before you
  trust a "database is up to date": a checkout's `.env` points at the local
  database, not prod.
* `--target N` handles pending files up to and including VN; later files stay
  pending and are listed. When only files above the target are pending, the run
  says `nothing pending at or below --target N`, never "database is up to date".
* `--record-only N[,M]` records the named pending versions without running them.
  It is the operator's explicit call, for a database that already has those
  objects; other pending files still run. `--baseline N` records every pending
  file up to VN the same way.
* Concurrent runs serialize. Every write transaction takes
  `pg_advisory_xact_lock` and re-reads `schema_migrations` under it, so a second
  `migrate` (another web boot, or an operator during a boot) waits up to
  `--lock-wait` (default 10min), then skips what the first one applied. If the
  wait runs out it exits 1 and writes nothing.
* A failed file is rolled back with its bookkeeping row and stops the run. Files
  before it stay applied.
* `cars_cleanroom`, the frozen clean-room baseline (D-N1), is never written. On it,
  `--apply` (with `--baseline` or `--record-only` too) logs `REFUSE --apply` and
  exits 2 before any lock or write; a dry run still reports. `cars_cr_*` scratch
  copies (`scripts/cleanroom/build_db.sh`) migrate as usual.

## Boot applies every pending file (`MIGRATE_ON_BOOT`)

The web container runs `python3 -m backend.scripts.migrate --apply` before it
serves (`scripts/docker-entrypoint-web.sh`, `MIGRATE_ON_BOOT`, default `1`), with
no `--target`. So every web deploy applies every migration merged into the tag it
ships, including those from earlier no-deploy releases, without a dump. A failure
there only logs a warning and boots on the old schema. Hence the protocol below:
a migration reaches prod by hand, before the deploy, and the boot step is a
no-op. Before every web deploy, a prod `migrate --dry-run` (the "Targeting prod"
form in `docs/RELEASING.md`) must log `database is up to date`.

## Migration protocol (D-DB6, REMEDIATION_PLAN Appendix C.1)

For every migration:

1. Apply it locally when it merges (`migrate --dry-run`, then `--apply`), before
   any local process runs the new code. Otherwise warn-mode processes fall back
   to the legacy runtime DDL.
2. Before any deploy that contains it, the owner takes a prod `pg_dump -Fc` with
   the PG18 client and verifies it (`pg_restore -l`), runs `migrate --dry-run`,
   then `migrate --apply --target N`.
3. Only then deploy. The boot-time `MIGRATE_ON_BOOT` step is then a no-op.
4. A migration that drops or constrains data merges only after its dumps or
   prerequisite repairs exist: every checkout's `--apply` and every boot apply all
   pending files.
5. A migration that locks `cars` (indexes, foreign keys) runs in a quiet window,
   after `pg_stat_activity` shows no idle-in-transaction session, with
   `SET LOCAL lock_timeout`.

Numbers follow merge order, from the registry in `docs/NEXT_PHASES_PLAN_2026_10.md`
Section 7.0 (and REMEDIATION_PLAN Appendix C.1). Only unmerged files renumber.

## V019 is recorded, not run

`V019__missing_reference_tables.sql` was cut from a local schema dump to give prod
the reference tables it lacked (prod ran it on 2026-09-06). 48 of its 53 objects (15 tables, 11
sequences, 22 primary key and unique constraints) are also in `V001__baseline.sql`,
and it creates them without `IF NOT EXISTS`, so it cannot run on any database
built from V001. Its other 5 objects (`car_move_log` with its sequence and primary
key, `cars_trim_quarantine`, `package_values_msrp_quarantine`, plus the
`idx_car_move_log_car` index) exist only in V019.

The runner (`RECORD_ONLY` in `backend/scripts/migrate.py`) therefore records V019
without running it, but only when that is safe:

* V001 runs earlier in the same invocation (a fresh build), or
* all 48 shared objects already exist (`to_regclass`), as on prod, local `cars`
  and `cars_cleanroom` (checked 2026-10-18).

Otherwise it refuses before writing anything and names the missing objects. That
protects a database on an old lineage (V001 recorded, V019's tables never
created) from being marked current while it lacks them. The check runs again
under the migration lock right before the row is written. `--record-only 19`
skips the check; use it only after checking the database by hand.

V026 (P4.3) creates those 5 objects and the index, each step guarded by a catalog
check, so a fresh build has them too and the databases that already hold them see
no change.

## V026 is chain completeness

After `V026__chain_completeness.sql` a fresh build has every object prod and local
`cars` hold, except the backup tables D-DB1 drops later (P13B.4). It adds the
V019-only objects, `review_reports` and `haiku_spec_cache` (copied exactly from the
runtime DDL in `backend/reviews/store.py` and `backend/enrichment/service.py`), the
4 untracked log tables (D-N30), and `uq_option_rejections_standing` as
`NULLS NOT DISTINCT` (D-DB7). On a database at V025 it creates the two runtime
tables and rebuilds that index on `option_rejections`, and does nothing else. It
stops on a duplicate group in `option_rejections`, never deduplicates, and never
touches `cars`. `backend/tests/fixtures/module_ddl_schema_golden.json` (captured by
`backend/scripts/capture_module_ddl_golden.py`) records what module and script DDL
builds, and `test_schema_from_migrations` checks the chain against it.

## Baselining an existing database

A populated database that predates the chain (it has `cars` but no
`schema_migrations`) already has V001's schema, so `V001__baseline.sql` must be
*recorded* there, not executed:

    python -m backend.scripts.dump_baseline_schema --write   # capture live schema (schema only, no data)
    python -m backend.scripts.migrate --baseline 1 --apply   # mark V001 applied without running it

V019 is then recorded by the rule above if its 48 shared objects exist. The
schema check (`describe_gap`) suggests this command for such a database.

A database rebuilt from scratch instead runs the whole chain normally (V001 runs,
V019 is recorded):

    createdb dealership_scanner
    python -m backend.scripts.migrate --apply
