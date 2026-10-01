-- V025__cars_active_zip_index.sql
--
-- The one object the runtime DDL created that no migration did: the partial
-- zip index the listings zip/radius filter reads (runtime copy in
-- backend/db/repositories/schema_repo.py::ensure_cars_listings_indexes).
-- Found by diffing a database built from migrations/ against one built by
-- init_postgres_inventory + the other backend/db runtime ensure_* helpers
-- (backend/tests/test_schema_from_migrations.py). With this file the chain is a
-- superset of the runtime DDL, which is what lets that DDL be retired.
--
-- Existing databases (local, production) already have this index from the
-- runtime pass, so IF NOT EXISTS makes this a no-op there. On a fresh database
-- `cars` is empty and the build is instant. (Not CONCURRENTLY: the runner wraps
-- each file in a transaction.)

CREATE INDEX IF NOT EXISTS idx_cars_active_zip
    ON public.cars (zip_code)
    WHERE COALESCE(listing_active, 1) = 1 AND zip_code IS NOT NULL;
