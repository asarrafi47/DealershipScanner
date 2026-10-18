-- V026__chain_completeness.sql
--
-- Chain completeness (remediation P4.3, NEXT_PHASES_PLAN N5). After this file a
-- fresh `migrate --apply` builds every object that prod and local `cars` hold,
-- except the documented backup tables (D-DB1, dropped later in P13B.4).
--
-- What it adds, and why each object was missing from the chain:
--
--   * car_move_log (+ car_move_log_id_seq, car_move_log_pkey, idx_car_move_log_car),
--     cars_trim_quarantine, package_values_msrp_quarantine: created only by V019.
--     A fresh build records V019 without running it (migrate.RECORD_ONLY: 48 of its
--     53 objects are V001's), so before this file a fresh build lacked these 5.
--     Same column types as V019. car_move_log is live: cars_repo.py and
--     attribution_repo.py read it.
--   * review_reports: runtime DDL only, exactly reviews/store.py _DDL_REPORTS_PG.
--   * haiku_spec_cache: runtime DDL only, exactly enrichment/service.py
--     _HAIKU_CACHE_PG_DDL, so the runtime fallback and the chain can never build
--     two different tables. D-PR8 (a) keeps the table.
--   * image_summary_restore_log, make_canon_log, model_canon_log,
--     package_value_purge_log: on prod and local with no creator in git history.
--     D-N30 adopts them here; P13B.4 exports and drops them with the backup tables.
--     Definitions copied from local cars (pg_dump --schema-only, 2026-10-18).
--   * uq_option_rejections_standing: runtime DDL in
--     backend/scripts/feed_package_registry_from_vision.py. D-DB7 (a) adopts it
--     with NULLS NOT DISTINCT (prod 18.6, local 17.10), the same key the script's
--     dedupe uses (IS NOT DISTINCT FROM).
--   * No pgvector *_embeddings table: none exists on prod or local (P0A.2), so
--     D-DB7 has nothing to adopt there.
--
-- Guarded and additive: each step reads the catalog first and does nothing when
-- the object is already there. Nothing here deletes, updates or inserts a row, and
-- nothing touches `cars`. On prod and local cars (V025; 2026-10-08 census in
-- docs/db_layer/SCHEMA_LEDGER_2026_10.md) it therefore only:
--   1. creates review_reports and haiku_spec_cache (absent on both);
--   2. rebuilds uq_option_rejections_standing from NULLS DISTINCT to NULLS NOT
--      DISTINCT. That locks option_rejections (987 rows) for milliseconds and no
--      other table.
-- A duplicate group in option_rejections stops the file (RAISE) and rolls it back.
-- Deduplicating is a data repair (REMEDIATION_PLAN P4.4), never part of a
-- migration; P0A.2 found 0 groups on prod and local.
--
-- cars_cleanroom never receives this file: it is frozen at V025 (D-N1).
--
-- No lock_timeout of its own: the runner's per-file SET LOCAL (5 s by default)
-- caps every lock wait here, and `migrate --lock-timeout` can change it.

-- ---------------------------------------------------------------------------
-- V019-only objects
-- ---------------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS public.car_move_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.car_move_log (
    id bigint DEFAULT nextval('public.car_move_log_id_seq'::regclass) NOT NULL,
    car_id bigint NOT NULL,
    from_dealer_id text,
    from_dealer_name text,
    from_registry_id bigint,
    to_dealer_id text,
    to_dealer_name text,
    to_registry_id bigint,
    evidence text,
    moved_at timestamp with time zone DEFAULT now() NOT NULL,
    reverted_at timestamp with time zone
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE classid = 'pg_class'::regclass
          AND objid = 'public.car_move_log_id_seq'::regclass
          AND refclassid = 'pg_class'::regclass
          AND refobjid = 'public.car_move_log'::regclass
          AND deptype = 'a'
    ) THEN
        ALTER SEQUENCE public.car_move_log_id_seq OWNED BY public.car_move_log.id;
    END IF;
    -- ADD CONSTRAINT has no IF NOT EXISTS.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.car_move_log'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE ONLY public.car_move_log
            ADD CONSTRAINT car_move_log_pkey PRIMARY KEY (id);
    END IF;
    -- CREATE INDEX IF NOT EXISTS would still lock the table before it notices the
    -- index exists; to_regclass takes no lock.
    IF to_regclass('public.idx_car_move_log_car') IS NULL THEN
        CREATE INDEX idx_car_move_log_car ON public.car_move_log USING btree (car_id, moved_at DESC);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.cars_trim_quarantine (
    car_id integer,
    vin text,
    old_trim text,
    provenance_source text,
    title text,
    trim_provenance_json text,
    quarantined_at text
);

-- No primary key and no indexes, as in V019: the same row may be quarantined
-- more than once.
CREATE TABLE IF NOT EXISTS public.package_values_msrp_quarantine (
    id bigint NOT NULL,
    year integer,
    make text NOT NULL,
    model text NOT NULL,
    "trim" text NOT NULL,
    kind text NOT NULL,
    match_key text NOT NULL,
    name_display text NOT NULL,
    name_norm text NOT NULL,
    code text,
    msrp double precision,
    msrp_source text,
    msrp_authority integer NOT NULL,
    category text,
    observation_count integer NOT NULL,
    sticker_count integer NOT NULL,
    confidence double precision NOT NULL,
    first_seen_at text,
    last_seen_at text,
    quarantined_at timestamp with time zone DEFAULT now() NOT NULL,
    label text,
    reason text
);

-- ---------------------------------------------------------------------------
-- Runtime-DDL tables
-- ---------------------------------------------------------------------------

-- One row per (review, reporter): reviews/store.py _DDL_REPORTS_PG.
CREATE TABLE IF NOT EXISTS public.review_reports (
    review_id BIGINT NOT NULL,
    ip_hash TEXT NOT NULL,
    created_at TEXT,
    UNIQUE (review_id, ip_hash)
);

-- enrichment/service.py _HAIKU_CACHE_PG_DDL (D-PR8 (a): kept; the fallback that
-- fills it never writes fact columns and is opt-in, P8B.8).
CREATE TABLE IF NOT EXISTS public.haiku_spec_cache (
    cache_key TEXT PRIMARY KEY,
    make TEXT,
    model TEXT,
    year INTEGER,
    "trim" TEXT,
    engine_l TEXT,
    cylinders INTEGER,
    transmission TEXT,
    drivetrain TEXT,
    fuel_type TEXT,
    mpg_city INTEGER,
    mpg_highway INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------------------------
-- Untracked log tables (D-N30: adopted here, exported and dropped in P13B.4)
-- ---------------------------------------------------------------------------

CREATE SEQUENCE IF NOT EXISTS public.image_summary_restore_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.image_summary_restore_log (
    id bigint DEFAULT nextval('public.image_summary_restore_log_id_seq'::regclass) NOT NULL,
    car_id bigint NOT NULL,
    version integer NOT NULL,
    prior_summary jsonb NOT NULL,
    reason text,
    restored_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE SEQUENCE IF NOT EXISTS public.make_canon_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.make_canon_log (
    id bigint DEFAULT nextval('public.make_canon_log_id_seq'::regclass) NOT NULL,
    from_make text,
    to_make text,
    rows_changed integer,
    applied_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS public.model_canon_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.model_canon_log (
    id bigint DEFAULT nextval('public.model_canon_log_id_seq'::regclass) NOT NULL,
    make text,
    from_model text,
    to_model text,
    rows_changed integer,
    applied_at timestamp with time zone DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS public.package_value_purge_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

CREATE TABLE IF NOT EXISTS public.package_value_purge_log (
    id bigint DEFAULT nextval('public.package_value_purge_log_id_seq'::regclass) NOT NULL,
    source_table text NOT NULL,
    row_id bigint,
    name_display text,
    msrp numeric,
    observation_count integer,
    reason text,
    purged_at timestamp with time zone DEFAULT now() NOT NULL
);

DO $$
BEGIN
    -- image_summary_restore_log
    IF NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE classid = 'pg_class'::regclass
          AND objid = 'public.image_summary_restore_log_id_seq'::regclass
          AND refclassid = 'pg_class'::regclass
          AND refobjid = 'public.image_summary_restore_log'::regclass
          AND deptype = 'a'
    ) THEN
        ALTER SEQUENCE public.image_summary_restore_log_id_seq OWNED BY public.image_summary_restore_log.id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.image_summary_restore_log'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE ONLY public.image_summary_restore_log
            ADD CONSTRAINT image_summary_restore_log_pkey PRIMARY KEY (id);
    END IF;

    -- make_canon_log
    IF NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE classid = 'pg_class'::regclass
          AND objid = 'public.make_canon_log_id_seq'::regclass
          AND refclassid = 'pg_class'::regclass
          AND refobjid = 'public.make_canon_log'::regclass
          AND deptype = 'a'
    ) THEN
        ALTER SEQUENCE public.make_canon_log_id_seq OWNED BY public.make_canon_log.id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.make_canon_log'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE ONLY public.make_canon_log
            ADD CONSTRAINT make_canon_log_pkey PRIMARY KEY (id);
    END IF;

    -- model_canon_log
    IF NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE classid = 'pg_class'::regclass
          AND objid = 'public.model_canon_log_id_seq'::regclass
          AND refclassid = 'pg_class'::regclass
          AND refobjid = 'public.model_canon_log'::regclass
          AND deptype = 'a'
    ) THEN
        ALTER SEQUENCE public.model_canon_log_id_seq OWNED BY public.model_canon_log.id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.model_canon_log'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE ONLY public.model_canon_log
            ADD CONSTRAINT model_canon_log_pkey PRIMARY KEY (id);
    END IF;

    -- package_value_purge_log
    IF NOT EXISTS (
        SELECT 1 FROM pg_depend
        WHERE classid = 'pg_class'::regclass
          AND objid = 'public.package_value_purge_log_id_seq'::regclass
          AND refclassid = 'pg_class'::regclass
          AND refobjid = 'public.package_value_purge_log'::regclass
          AND deptype = 'a'
    ) THEN
        ALTER SEQUENCE public.package_value_purge_log_id_seq OWNED BY public.package_value_purge_log.id;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.package_value_purge_log'::regclass AND contype = 'p'
    ) THEN
        ALTER TABLE ONLY public.package_value_purge_log
            ADD CONSTRAINT package_value_purge_log_pkey PRIMARY KEY (id);
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- uq_option_rejections_standing (D-DB7 (a), NULLS NOT DISTINCT on PG 15+)
-- ---------------------------------------------------------------------------

DO $$
DECLARE
    dup_groups bigint;
    nulls_not_distinct_ok boolean := current_setting('server_version_num')::int >= 150000;
    idx regclass := to_regclass('public.uq_option_rejections_standing');
    is_expected boolean;
    is_nnd boolean;
BEGIN
    -- GROUP BY puts NULLs in one group: the IS NOT DISTINCT FROM key.
    SELECT count(*) INTO dup_groups FROM (
        SELECT 1 FROM public.option_rejections
        GROUP BY car_id, option_name, price, reason
        HAVING count(*) > 1
    ) AS g;
    IF dup_groups > 0 THEN
        RAISE EXCEPTION 'V026: option_rejections has % duplicate group(s) under (car_id, option_name, price, reason) with NULLs equal; remove them with a backed-up data repair first (REMEDIATION_PLAN P4.4), never inside a migration', dup_groups;
    END IF;

    IF idx IS NULL THEN
        IF nulls_not_distinct_ok THEN
            -- EXECUTE: a server before 15 must not even parse NULLS NOT DISTINCT.
            EXECUTE 'CREATE UNIQUE INDEX uq_option_rejections_standing ON public.option_rejections USING btree (car_id, option_name, price, reason) NULLS NOT DISTINCT';
        ELSE
            CREATE UNIQUE INDEX uq_option_rejections_standing ON public.option_rejections USING btree (car_id, option_name, price, reason);
        END IF;
        RETURN;
    END IF;

    IF NOT nulls_not_distinct_ok THEN
        RETURN;  -- nothing better to build on this server; keep what is there
    END IF;

    -- The runtime-DDL index (prod, local): replace it only when it is exactly the
    -- unique btree on these four columns; anything else under this name stops the file.
    SELECT i.indisunique
           AND i.indrelid = 'public.option_rejections'::regclass
           AND i.indpred IS NULL
           AND i.indexprs IS NULL
           AND am.amname = 'btree'
           AND ARRAY(
               SELECT a.attname::text
               FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
               JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
               ORDER BY k.ord
           ) = ARRAY['car_id', 'option_name', 'price', 'reason']
    INTO is_expected
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_am am ON am.oid = c.relam
    WHERE i.indexrelid = idx;
    IF NOT coalesce(is_expected, false) THEN
        RAISE EXCEPTION 'V026: public.uq_option_rejections_standing exists but is not the unique btree index on option_rejections (car_id, option_name, price, reason): %; resolve it by hand', pg_get_indexdef(idx);
    END IF;

    EXECUTE 'SELECT indnullsnotdistinct FROM pg_index WHERE indexrelid = $1' INTO is_nnd USING idx;
    IF NOT is_nnd THEN
        DROP INDEX public.uq_option_rejections_standing;
        EXECUTE 'CREATE UNIQUE INDEX uq_option_rejections_standing ON public.option_rejections USING btree (car_id, option_name, price, reason) NULLS NOT DISTINCT';
    END IF;
END $$;
