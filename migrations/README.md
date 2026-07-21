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

## Running

    python -m backend.scripts.migrate                # dry run (default): lists pending files, writes nothing
    python -m backend.scripts.migrate --apply        # apply pending files, one transaction each

Bookkeeping lives in `schema_migrations (version, name, checksum, applied_at)` in
the same database, created on first `--apply`.

## Baselining an existing database

Production already has the schema, so `V001__baseline.sql` must be *recorded*
there, not executed:

    python -m backend.scripts.dump_baseline_schema --write   # capture live schema (schema only, no data)
    python -m backend.scripts.migrate --baseline 1 --apply   # mark V001 applied without running it

A database rebuilt from scratch instead runs the whole chain normally:

    createdb dealership_scanner
    python -m backend.scripts.migrate --apply
