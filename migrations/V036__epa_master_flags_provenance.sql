-- V036__epa_master_flags_provenance.sql
--
-- N22 P10A.3 (plan registry: V032; renumbered at draft time because V032..V035 were taken by
-- N9, N13 and N18 on the integration head, so the next free number is V036).
--
-- EPA catalog foundation:
--   * epa_master gains the two EPA induction flags (t_charger, s_charger), the provenance of
--     the row (source), its permanent catalog key (natural_key, P10A.2) and when it was loaded.
--     A refreshed row stores '' rather than NULL for a blank flag, so "refreshed, naturally
--     aspirated" is distinguishable from "never refreshed" (NULL).
--   * epa_master_archive / epa_extended_specs_archive hold the rows a refresh or restore
--     replaces (the --restore round trip of P10A.8): the live columns plus archived_at,
--     archive_reason (and superseded_by for the master).
--   * idx_epa_master_vehicle_id: a per-id refresh would otherwise run one sequential scan per id.
--   * idx_cars_epa_master_id (partial): the dangling-link and relink checks read cars by catalog id.
--     This takes a SHARE lock on cars: apply in a quiet window, after pg_stat_activity shows no
--     idle-in-transaction session.
--
--   SELECT source, count(*) FROM epa_master GROUP BY 1;
--
-- Idempotent. Mirrors (SQLite tests, legacy runtime lists): schema_repo.py, scanner/database.py,
-- dictionary_schema.py, inventory_pg.py.

SET LOCAL lock_timeout = '5s';

ALTER TABLE public.epa_master ADD COLUMN IF NOT EXISTS t_charger text;
ALTER TABLE public.epa_master ADD COLUMN IF NOT EXISTS s_charger text;
ALTER TABLE public.epa_master ADD COLUMN IF NOT EXISTS source text;
ALTER TABLE public.epa_master ADD COLUMN IF NOT EXISTS natural_key text;
ALTER TABLE public.epa_master ADD COLUMN IF NOT EXISTS loaded_at timestamp with time zone;

CREATE TABLE IF NOT EXISTS public.epa_master_archive (
    id bigint NOT NULL,
    epa_vehicle_id integer,
    year integer,
    make text,
    model text,
    cylinders integer,
    displacement double precision,
    trany text,
    drive text,
    fuel_type text,
    city08 double precision,
    highway08 double precision,
    city_e double precision,
    highway_e double precision,
    atv_type text,
    "trim" text,
    body_style text,
    engine_description text,
    engine_display text,
    forced_induction text,
    t_charger text,
    s_charger text,
    source text,
    natural_key text,
    loaded_at timestamp with time zone,
    archived_at timestamp with time zone NOT NULL DEFAULT now(),
    archive_reason text,
    superseded_by bigint
);

CREATE TABLE IF NOT EXISTS public.epa_extended_specs_archive (
    epa_master_id bigint NOT NULL,
    year integer NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    "trim" text,
    horsepower integer,
    torque_lb_ft integer,
    torque_nm integer,
    curb_weight_lb integer,
    curb_weight_kg integer,
    zero_to_60_sec double precision,
    fuel_tank_gal double precision,
    ev_range_miles integer,
    battery_kwh double precision,
    body_style_detail text,
    specs_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_url text,
    source_host text,
    scrape_status text NOT NULL DEFAULT 'pending',
    scraped_at timestamp with time zone,
    last_error text,
    tow_capacity_lb integer,
    archived_at timestamp with time zone NOT NULL DEFAULT now(),
    archive_reason text
);

CREATE INDEX IF NOT EXISTS idx_epa_master_archive_id
    ON public.epa_master_archive (id);
CREATE INDEX IF NOT EXISTS idx_epa_extended_specs_archive_id
    ON public.epa_extended_specs_archive (epa_master_id);
CREATE INDEX IF NOT EXISTS idx_epa_master_vehicle_id
    ON public.epa_master (epa_vehicle_id);
CREATE INDEX IF NOT EXISTS idx_cars_epa_master_id
    ON public.cars (epa_master_id) WHERE epa_master_id IS NOT NULL;
