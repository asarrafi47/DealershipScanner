-- V038__epa_catalog_constraints.sql
--
-- N25 P10D.7 (D-ER4: RESTRICT, after the 0-dangling check). The plan registry's V033/V034 were
-- taken on integ/draft (listing_miss_strikes, dealer_recipes_rev) and V036/V037 are EPA
-- migrations, so this is the next free number.
--
--   * ux_epa_master_source_natural_key: the permanent catalog key (source, natural_key) is
--     unique. Rows with a NULL natural_key (legacy, not yet re-resolved) are not constrained.
--   * cars_epa_master_id_fkey: cars.epa_master_id -> epa_master(id) ON DELETE RESTRICT, added
--     NOT VALID so existing dangling links do not block the migration; new writes are checked.
--   * epa_extended_specs_epa_master_id_fkey: the CASCADE FK of the baseline is dropped and
--     re-added as RESTRICT NOT VALID. A catalog row with extended specs can no longer be
--     deleted out from under them; archive it instead.
--
-- V039 (VALIDATE CONSTRAINT on both FKs) ships in a later release, only after a read-only prod
-- check shows 0 dangling links:
--   SELECT count(*) FROM cars c WHERE c.epa_master_id IS NOT NULL
--     AND NOT EXISTS (SELECT 1 FROM epa_master m WHERE m.id = c.epa_master_id);
--
-- Rollback note: drop both FKs before any pg_restore of epa_master.
--
-- Idempotent: the index is IF NOT EXISTS; each FK is touched only when it is missing or still
-- the old shape.

SET LOCAL lock_timeout = '5s';

CREATE UNIQUE INDEX IF NOT EXISTS ux_epa_master_source_natural_key
    ON public.epa_master (source, natural_key);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.cars'::regclass AND conname = 'cars_epa_master_id_fkey'
    ) THEN
        ALTER TABLE public.cars
            ADD CONSTRAINT cars_epa_master_id_fkey
            FOREIGN KEY (epa_master_id) REFERENCES public.epa_master (id)
            ON DELETE RESTRICT NOT VALID;
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.epa_extended_specs'::regclass
          AND conname = 'epa_extended_specs_epa_master_id_fkey'
          AND confdeltype <> 'r'
    ) THEN
        ALTER TABLE public.epa_extended_specs
            DROP CONSTRAINT epa_extended_specs_epa_master_id_fkey;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'public.epa_extended_specs'::regclass
          AND conname = 'epa_extended_specs_epa_master_id_fkey'
    ) THEN
        ALTER TABLE public.epa_extended_specs
            ADD CONSTRAINT epa_extended_specs_epa_master_id_fkey
            FOREIGN KEY (epa_master_id) REFERENCES public.epa_master (id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END
$$;
