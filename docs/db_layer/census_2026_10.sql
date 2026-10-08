SET default_transaction_read_only = on;
SET statement_timeout = '20s';
-- =============================================================================
-- docs/db_layer/census_2026_10.sql -- Phase 0A unit P0A.2, read-only DB census
-- (docs/REMEDIATION_PLAN_2026_10.md, "P0A.2: Read-only census").
--
-- READ ONLY. The two SETs above are the first statements on purpose: every
-- statement below runs in a read-only transaction with a 20 s timeout. Run it
-- with PGOPTIONS as a second guard, so even the connection default is read-only:
--
--   local MBP (DSN from INVENTORY_DATABASE_URL in .env; unix socket, db cars):
--     PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' \
--       psql 'postgresql:///cars?host=/tmp' -X -f docs/db_layer/census_2026_10.sql
--
--   prod (owner, or an agent authorized for P0A.2 only; never echo the DSN):
--     railway run -s Postgres -- sh -c 'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=20000" \
--       psql "$DATABASE_PUBLIC_URL" -X -f docs/db_layer/census_2026_10.sql' \
--       | sed -E "s/[a-z0-9-]+\.proxy\.rlwy\.net/<prod-host>/g" > /tmp/census_prod.txt
--
--   mini's own Postgres: only if D-DB5=(a) (plan Section 7). Not run in 0A.
--
-- Requires psql >= 10 (\gset/\if) and a server >= 16 (IS JSON predicate in Q10).
-- Sections are numbered as in the plan (Q1..Q14); Q15/Q16 are the chain-vs-live
-- object diff that P4.3 (V026) and D-DB7 need. The VALUES lists in Q2 and
-- Q15/Q16 were generated from migrations/ at commit dbdbf5cba (V001..V025);
-- regenerate them if a migration is added. Q15/Q16 are a name-level diff (a
-- static parse of CREATE/ADD CONSTRAINT names), not a definition diff: that is
-- P4.1's drift report.
-- Output contains no secret: no DSN, no client address, no user e-mail or seed.
-- =============================================================================
\set ON_ERROR_STOP off
\pset pager off
\pset null '(null)'
\pset footer off

\echo '=== Q0 session proof (must read on / on / 20s) ==='
SELECT current_setting('default_transaction_read_only') AS default_txn_read_only,
       current_setting('transaction_read_only')         AS txn_read_only,
       current_setting('statement_timeout')             AS statement_timeout,
       current_database()                               AS db,
       to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS run_at_utc;

SELECT to_regclass('public.schema_migrations')            IS NOT NULL AS has_schema_migrations,
       to_regclass('public.incomplete_listings_meta')     IS NOT NULL AS has_ilm,
       to_regclass('public.vin_owner_conflicts')          IS NOT NULL AS has_voc,
       to_regclass('public.cars_owner_backup_20260929')   IS NOT NULL AS has_owner_bak,
       to_regclass('public.dealer_recipes')               IS NOT NULL AS has_recipes,
       to_regclass('public.dealer_jobs')                  IS NOT NULL AS has_jobs,
       to_regclass('public.dealer_catalog')               IS NOT NULL AS has_catalog,
       to_regclass('public.epa_master')                   IS NOT NULL AS has_epa,
       to_regclass('public.epa_master_dump_id_map')       IS NOT NULL AS has_epa_map,
       to_regclass('public.epa_extended_specs')           IS NOT NULL AS has_epa_ext,
       to_regclass('public.option_rejections')            IS NOT NULL AS has_optrej,
       to_regclass('public.users')                        IS NOT NULL AS has_users
\gset

\echo '=== Q1 server, extensions, connections, timeouts ==='
SELECT current_setting('server_version') AS server_version,
       current_setting('server_version_num')::int AS server_version_num;
SELECT name, default_version, installed_version
  FROM pg_available_extensions WHERE name IN ('vector', 'dblink') ORDER BY name;
SELECT name, setting, unit, source
  FROM pg_settings
 WHERE name IN ('max_connections', 'superuser_reserved_connections',
                'idle_in_transaction_session_timeout', 'idle_session_timeout',
                'lock_timeout', 'statement_timeout', 'default_transaction_read_only')
 ORDER BY name;
-- database/role-level overrides of the timeout settings (ALTER DATABASE/ROLE ... SET)
SELECT coalesce(d.datname, '(all dbs)') AS db, coalesce(r.rolname, '(all roles)') AS role, cfg
  FROM pg_db_role_setting s
  LEFT JOIN pg_database d ON d.oid = s.setdatabase
  LEFT JOIN pg_roles r ON r.oid = s.setrole,
       unnest(s.setconfig) AS cfg
 WHERE cfg ~ '^(idle_|statement_timeout|lock_timeout|default_transaction_read_only)'
 ORDER BY 1, 2, 3;
SELECT count(*) AS backends_total,
       count(*) FILTER (WHERE backend_type = 'client backend') AS client_backends,
       count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'active') AS active,
       count(*) FILTER (WHERE backend_type = 'client backend' AND state = 'idle') AS idle,
       count(*) FILTER (WHERE backend_type = 'client backend' AND state LIKE 'idle in transaction%') AS idle_in_txn,
       current_setting('max_connections')::int AS max_connections
  FROM pg_stat_activity;
SELECT coalesce(datname, '-') AS db, coalesce(nullif(application_name, ''), '(none)') AS application_name,
       coalesce(state, '(hidden)') AS state, count(*) AS n
  FROM pg_stat_activity WHERE backend_type = 'client backend'
 GROUP BY 1, 2, 3 ORDER BY 4 DESC, 1, 2;

\echo '=== Q2 schema_migrations ledger, pending files, checksum drift ==='
\if :has_schema_migrations
SELECT version, name, checksum, to_char(applied_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') AS applied_at_utc
  FROM public.schema_migrations ORDER BY version;
-- repo chain at dbdbf5cba: version, name, checksum (backend.scripts.migrate.compute_checksum)
WITH repo(version, name, checksum) AS (VALUES
  (1, 'baseline', 'sha256:c7c44432cc66929a981c196c58402c5151539d18e020ea9db1bb06090eb0e6cd'),
  (2, 'community', 'sha256:f7b8690f59263793ebcb28d0717e51888d756f1edd4b421c802f8930429a12bc'),
  (3, 'dealership_address_provenance', 'sha256:ac1cd93bcb71380c275c913904d774b9ad0bfc3f741ea4299926ff2473636fc9'),
  (4, 'comment_flag_dedupe', 'sha256:132ca47a971d0b005f3e2029df114ffcfc8c49367db9fd74fc37b7113a204de7'),
  (5, 'comment_attachments', 'sha256:3dc51f4b8311ceecdbaa37dae1983e3c7a1498fdf26352e2493df7976036850b'),
  (6, 'car_image_text', 'sha256:6f50cef4496dc8af7400950f653dd88e672198c1dcab97ea71d88412aa0ec0e9'),
  (7, 'car_photo_attribution', 'sha256:5821f347dc9921a28f8511b40562c766c3a94527475c4cc709f405c651c4227b'),
  (8, 'dealer_scan_status', 'sha256:40a4f7edbdf692299fa11164f36e3d00cb2b6825a5e3f6fefcacff70d3c49287'),
  (9, 'rooftop_refusals', 'sha256:2af2faa656cfe6418cbe284effc20e51e6aaf587a3f055f875a1f4e4c92b7970'),
  (10, 'trim_msrp_bands', 'sha256:46f5d2c9b6484db47811513a6d74d7801b943adb173aa614a11707dbe822d594'),
  (11, 'car_attribution', 'sha256:37dde84ec06995df3e7f724a3013cc8f91ca07bc5b77dde7e9d5b6deb7134380'),
  (12, 'dealer_portal', 'sha256:b94ada57240a777b535cf59bbfb3352e9e7c8068095c4686c2f5a61f50d9dbb4'),
  (13, 'users', 'sha256:27c5c3289b1824a390987193e16fcf18d3318534a725f8effc2164ec6802d037'),
  (14, 'option_rejections', 'sha256:71e408b4a977ef5283280195dfec05f3b9a1da7624233f8ecc5d8245870382da'),
  (15, 'brochure_vision_facts', 'sha256:895fea8ffb648589af802604832c25c6532d44442101eff1f6d58ceaf3cfcfb1'),
  (16, 'brochure_local_extraction_failures', 'sha256:06e0046dc7f8fd816a42cb82437cf418f8689415d16a8bd9302235be25b725f4'),
  (17, 'car_image_vision_facts', 'sha256:001c54f02015566c6d3ca32384b604a53e5e1bf9a1c3a46d9723fa21a3b81f1b'),
  (18, 'saved_searches', 'sha256:9da13a44ea2ddc1e70f0190cd800f77f341ecfd19d9324e8216b27949666e0d9'),
  (19, 'missing_reference_tables', 'sha256:d74597f4bbf57959f1c3009e7678956cfe86431c6e3887c96123e1b6ff1e3e2e'),
  (20, 'dealerships_missing_columns', 'sha256:a1ab55f9c9e1b0e8b3c066250773b4454ce06d6da4bf5da3cf38184efc1b7b88'),
  (21, 'user_hidden_dealers', 'sha256:10b958316a9380317c5865757eed55146a21abab8cc9d7d52723ce3c97adc9a5'),
  (22, 'user_search_history', 'sha256:f9781307351059e1cbe1609e0109e4d2ed4e8b5692b96d64111d223cc8057260'),
  (23, 'listings_grid_cards', 'sha256:79a53ba970adf614d5ffd20061d8a763b566a7fc670b3526d2ae55866054c109'),
  (24, 'vin_owner_conflicts', 'sha256:829d72342ea0db2560939ed8363db94bc7397bcac66ee3691583c6133b4b8ec1'),
  (25, 'cars_active_zip_index', 'sha256:39118b9a789246cc6149ecbb9fc0737b2655ff6f24df639018d8f8ed74d3a50f')
)
SELECT coalesce(r.version, m.version) AS version,
       coalesce(r.name, m.name) AS name,
       CASE WHEN m.version IS NULL THEN 'PENDING (file not applied)'
            WHEN r.version IS NULL THEN 'APPLIED, NO FILE IN REPO'
            WHEN m.checksum <> r.checksum THEN 'CHECKSUM DRIFT'
            ELSE 'ok' END AS status
  FROM repo r FULL JOIN public.schema_migrations m ON m.version = r.version
 WHERE m.version IS NULL OR r.version IS NULL OR m.checksum <> r.checksum
 ORDER BY 1;
WITH repo(version) AS (SELECT generate_series(1, 25))
SELECT (SELECT max(version) FROM public.schema_migrations) AS db_high_water,
       (SELECT count(*) FROM public.schema_migrations) AS db_rows,
       (SELECT max(version) FROM repo) AS repo_high_water,
       (SELECT count(*) FROM repo r WHERE NOT EXISTS (SELECT 1 FROM public.schema_migrations m WHERE m.version = r.version)) AS pending;
\else
\echo 'schema_migrations: ABSENT (database was never put on the chain)'
\endif

\echo '=== Q3 out-of-chain / V019-only / runtime-only relations, *_embeddings, extensions ==='
SELECT o.name,
       to_regclass('public.' || o.name) IS NOT NULL AS present,
       coalesce((SELECT c.relkind::text FROM pg_class c WHERE c.oid = to_regclass('public.' || o.name)), '-') AS relkind
  FROM (VALUES ('review_reports'), ('haiku_spec_cache'), ('car_move_log'), ('car_move_log_id_seq'),
               ('cars_trim_quarantine'), ('package_values_msrp_quarantine')) AS o(name)
 ORDER BY 1;
-- car_move_log_pkey is the one constraint V019 alone creates
SELECT conname, conrelid::regclass AS on_table FROM pg_constraint WHERE conname = 'car_move_log_pkey';
-- every *_embeddings relation, and every column of type vector anywhere in public
SELECT c.relname, c.relkind, c.reltuples::bigint AS approx_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public' AND c.relname LIKE '%\_embeddings' ESCAPE '\'
 ORDER BY 1;
SELECT table_name, column_name, udt_name
  FROM information_schema.columns
 WHERE table_schema = 'public' AND udt_name = 'vector'
 ORDER BY 1, 2;
SELECT extname, extversion, n.nspname AS schema
  FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace ORDER BY 1;

\echo '=== Q4 indexes the code relies on; cars.zip_code ==='
SELECT o.name AS indexname,
       i.tablename,
       i.indexdef
  FROM (VALUES ('uq_option_rejections_standing'), ('idx_car_move_log_car'), ('idx_cars_active_zip')) AS o(name)
  LEFT JOIN pg_indexes i ON i.schemaname = 'public' AND i.indexname = o.name
 ORDER BY 1;
SELECT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'cars' AND column_name = 'zip_code') AS cars_has_zip_code;

\echo '=== Q5 option_rejections duplicate groups (feed_package_registry_from_vision.py:497-503 key) ==='
\if :has_optrej
-- GROUP BY treats NULLs as equal, i.e. the IS NOT DISTINCT FROM key the dedupe uses.
-- "groups_blocking_plain_unique" counts groups with no NULL in the key: the ones a
-- default (NULLS DISTINCT) unique index would refuse. A NULLS NOT DISTINCT index
-- (PG 15+) refuses every dup group.
SELECT (SELECT count(*) FROM option_rejections) AS total_rows,
       (SELECT count(*) FROM option_rejections WHERE car_id IS NULL OR price IS NULL) AS rows_with_null_key,
       count(*) AS dup_groups_not_distinct_key,
       coalesce(sum(n - 1), 0) AS surplus_rows,
       count(*) FILTER (WHERE has_null) AS dup_groups_with_null_key,
       count(*) FILTER (WHERE NOT has_null) AS groups_blocking_plain_unique
  FROM (SELECT car_id, option_name, price, reason, count(*) AS n,
               bool_or(car_id IS NULL OR price IS NULL) AS has_null
          FROM option_rejections GROUP BY 1, 2, 3, 4 HAVING count(*) > 1) g;
\else
\echo 'option_rejections: ABSENT'
\endif

\echo '=== Q6 backup/dump tables (public, name ~ bak|backup|dump) ==='
SELECT c.relname, c.relkind, c.reltuples::bigint AS approx_rows,
       pg_total_relation_size(c.oid) AS total_bytes,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'm') AND c.relname ~ '(bak|backup|dump)'
 ORDER BY c.relname;
SELECT count(*) AS backup_tables, pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS total_size
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'm') AND c.relname ~ '(bak|backup|dump)';

\echo '=== Q7 incomplete_listings_meta index_bootstrap_v1 ==='
\if :has_ilm
SELECT k, v FROM incomplete_listings_meta WHERE k = 'index_bootstrap_v1';
SELECT count(*) AS meta_rows FROM incomplete_listings_meta;
\else
\echo 'incomplete_listings_meta: ABSENT'
\endif

\echo '=== Q8 active cars by scraped_at day, last 14 days (never max() alone) ==='
SELECT count(*) AS cars_total,
       count(*) FILTER (WHERE coalesce(listing_active, 1) = 1) AS cars_active
  FROM cars;
SELECT left(scraped_at, 10) AS scraped_day, count(*) AS active_rows
  FROM cars
 WHERE coalesce(listing_active, 1) = 1
   AND scraped_at >= to_char((now() AT TIME ZONE 'UTC') - interval '14 days', 'YYYY-MM-DD')
 GROUP BY 1 ORDER BY 1;
-- everything older, by month, plus NULL/blank scraped_at
SELECT CASE WHEN scraped_at IS NULL OR scraped_at = '' THEN '(null/blank)' ELSE left(scraped_at, 7) END AS scraped_month,
       count(*) AS active_rows_older_than_14d
  FROM cars
 WHERE coalesce(listing_active, 1) = 1
   AND (scraped_at IS NULL OR scraped_at = ''
        OR scraped_at < to_char((now() AT TIME ZONE 'UTC') - interval '14 days', 'YYYY-MM-DD'))
 GROUP BY 1 ORDER BY 1;

\echo '=== Q9 vin_owner_conflicts; cars_owner_backup_20260929 ==='
\if :has_voc
SELECT count(*) AS conflict_rows,
       count(DISTINCT claimant_dealer_id) AS claimants,
       count(DISTINCT owner_dealer_id) AS owners,
       count(DISTINCT (owner_dealer_id, claimant_dealer_id)) AS pairs,
       max(seen_at) AS max_seen_at,
       min(seen_at) AS min_seen_at
  FROM vin_owner_conflicts;
SELECT left(seen_at, 10) AS seen_day, count(*) AS rows
  FROM vin_owner_conflicts GROUP BY 1 ORDER BY 1 DESC LIMIT 14;
SELECT owner_dealer_id, claimant_dealer_id, count(*) AS vins, max(seen_at) AS max_seen_at
  FROM vin_owner_conflicts GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 30;
\else
\echo 'vin_owner_conflicts: ABSENT'
\endif
\if :has_owner_bak
SELECT count(*) AS cars_owner_backup_20260929_rows FROM cars_owner_backup_20260929;
\else
\echo 'cars_owner_backup_20260929: ABSENT'
\endif

\echo '=== Q10 dealer_recipes ==='
\if :has_recipes
SELECT count(*) AS rows,
       count(*) FILTER (WHERE recipe_count > 0) AS rows_with_recipes,
       sum(recipe_count) AS recipes_total,
       max(updated_at) AS max_updated_at,
       count(*) FILTER (WHERE scan_hints IS NOT NULL AND scan_hints <> '' AND NOT (scan_hints IS JSON OBJECT)) AS invalid_hints_json,
       count(*) FILTER (WHERE NOT (recipes_json IS JSON ARRAY)) AS invalid_recipes_json
  FROM dealer_recipes;
-- recipe_status prefix (scan_hints.recipe_status = ok | stale:<status>:<iso> | blocked:<tag>:<status>:<iso> | rejected:<why> ...)
WITH h AS (SELECT dealer_id, CASE WHEN scan_hints IS JSON OBJECT THEN scan_hints::jsonb END AS j FROM dealer_recipes)
SELECT coalesce(split_part(j->>'recipe_status', ':', 1), '(no recipe_status)') AS recipe_status_prefix,
       CASE WHEN j->>'recipe_status' LIKE 'blocked:%' OR j->>'recipe_status' LIKE 'rejected:%'
            THEN split_part(j->>'recipe_status', ':', 2) ELSE '' END AS second_field,
       count(*) AS dealers
  FROM h GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2;
-- every scan_hints key and how many dealers carry it (lifecycle_* keys included)
WITH h AS (SELECT CASE WHEN scan_hints IS JSON OBJECT THEN scan_hints::jsonb END AS j FROM dealer_recipes)
SELECT k AS scan_hints_key, count(*) AS dealers FROM h, jsonb_object_keys(h.j) AS k GROUP BY 1 ORDER BY 2 DESC, 1;
WITH h AS (SELECT CASE WHEN scan_hints IS JSON OBJECT THEN scan_hints::jsonb END AS j FROM dealer_recipes)
SELECT count(*) FILTER (WHERE j ? 'lifecycle_last_attempt') AS with_lifecycle_last_attempt,
       min(j->>'lifecycle_last_attempt') AS min_lifecycle_last_attempt,
       max(j->>'lifecycle_last_attempt') AS max_lifecycle_last_attempt
  FROM h;
-- recipe sets with recipes but max_saved_at = 0 (synthesized sets; P1C saved_at backfill), by updated_at day
SELECT left(coalesce(updated_at, '(null)'), 10) AS updated_day, count(*) AS rows
  FROM dealer_recipes WHERE max_saved_at = 0 AND recipe_count > 0
 GROUP BY 1 ORDER BY 1;
SELECT count(*) AS rows_max_saved_at_0_with_recipes FROM dealer_recipes WHERE max_saved_at = 0 AND recipe_count > 0;
-- '+cascade' recipes (recipe_cascade.py:184 suffixes provider_hint)
SELECT count(*) FILTER (WHERE provider_hint LIKE '%+cascade%') AS rows_provider_hint_cascade,
       (SELECT count(*) FROM dealer_recipes r2, jsonb_array_elements(r2.recipes_json::jsonb) e
         WHERE r2.recipes_json IS JSON ARRAY AND e->>'provider_hint' LIKE '%+cascade%') AS cascade_recipes,
       (SELECT count(DISTINCT r2.dealer_id) FROM dealer_recipes r2, jsonb_array_elements(r2.recipes_json::jsonb) e
         WHERE r2.recipes_json IS JSON ARRAY AND e->>'provider_hint' LIKE '%+cascade%') AS dealers_with_cascade_recipe
  FROM dealer_recipes;
-- dealers whose every recipe is stale with stale_reason http_401/http_403
-- (unstale_host_blocked_recipes.candidates; 31 in the 10-04 prod dump)
WITH r AS (
  SELECT dealer_id, recipes_json::jsonb AS a,
         CASE WHEN scan_hints IS JSON OBJECT THEN scan_hints::jsonb END AS j
    FROM dealer_recipes WHERE recipes_json IS JSON ARRAY
), s AS (
  SELECT dealer_id, j, jsonb_array_length(a) AS n,
         (SELECT bool_and(coalesce(e->>'stale' IN ('true', '1'), false)) FROM jsonb_array_elements(a) e) AS all_stale,
         (SELECT bool_and(coalesce(e->>'stale_reason', '') IN ('http_401', 'http_403')) FROM jsonb_array_elements(a) e) AS all_auth
    FROM r
)
SELECT count(*) AS dealers_all_auth_staled FROM s WHERE n > 0 AND all_stale AND all_auth;
WITH r AS (
  SELECT dealer_id, recipes_json::jsonb AS a,
         CASE WHEN scan_hints IS JSON OBJECT THEN scan_hints::jsonb END AS j
    FROM dealer_recipes WHERE recipes_json IS JSON ARRAY
), s AS (
  SELECT dealer_id, j, jsonb_array_length(a) AS n,
         (SELECT bool_and(coalesce(e->>'stale' IN ('true', '1'), false)) FROM jsonb_array_elements(a) e) AS all_stale,
         (SELECT bool_and(coalesce(e->>'stale_reason', '') IN ('http_401', 'http_403')) FROM jsonb_array_elements(a) e) AS all_auth
    FROM r
)
SELECT dealer_id, n AS recipes, coalesce(j->>'recipe_status', '') AS recipe_status
  FROM s WHERE n > 0 AND all_stale AND all_auth ORDER BY dealer_id;
-- hints-only rows (no recipes, scan_hints only)
SELECT count(*) FILTER (WHERE recipes_json = '[]') AS hints_only_literal_empty,
       count(*) FILTER (WHERE recipes_json IS JSON ARRAY AND jsonb_array_length(recipes_json::jsonb) = 0) AS hints_only_any_empty_array
  FROM dealer_recipes;
\else
\echo 'dealer_recipes: ABSENT'
\endif

\echo '=== Q11 dealer_jobs and dealer_catalog ==='
\if :has_jobs
SELECT count(*) FILTER (WHERE status = 'queued')  AS queued,
       count(*) FILTER (WHERE status = 'running') AS running,
       count(*) AS total_jobs
  FROM dealer_jobs;
SELECT status, job_type, count(*) AS n, min(created_at) AS first_created, max(created_at) AS last_created,
       max(started_at) AS last_started, max(finished_at) AS last_finished
  FROM dealer_jobs GROUP BY 1, 2 ORDER BY 1, 2;
-- every queued/running row (P0A.4 needs these to be zero before stopping the worker)
SELECT id, dealer_id, job_type, status, coalesce(worker_id, '(none)') AS worker_id, created_at, started_at
  FROM dealer_jobs WHERE status IN ('queued', 'running') ORDER BY id LIMIT 100;
-- who has been claiming jobs (last 60 days)
SELECT coalesce(worker_id, '(none)') AS worker_id, count(*) AS jobs, max(started_at) AS last_started
  FROM dealer_jobs
 WHERE started_at >= to_char((now() AT TIME ZONE 'UTC') - interval '60 days', 'YYYY-MM-DD')
 GROUP BY 1 ORDER BY 3 DESC LIMIT 20;
\else
\echo 'dealer_jobs: ABSENT'
\endif
\if :has_catalog
-- dealer_catalog has no status column: counts by provider / inventory_mode, plus due rows
SELECT count(*) AS catalog_rows,
       count(*) FILTER (WHERE next_scan_at IS NOT NULL
                          AND next_scan_at <= to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS')) AS due_now,
       max(last_scan_at) AS max_last_scan_at, max(onboarded_at) AS max_onboarded_at, max(updated_at) AS max_updated_at
  FROM dealer_catalog;
SELECT coalesce(provider, '(null)') AS provider, coalesce(inventory_mode, '(null)') AS inventory_mode, count(*) AS n
  FROM dealer_catalog GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2 LIMIT 30;
\else
\echo 'dealer_catalog: ABSENT'
\endif

\echo '=== Q12 epa_master provenance, extended specs, cars links ==='
\if :has_epa
SELECT count(*) AS epa_master_rows, min(id) AS min_id, max(id) AS max_id,
       count(epa_vehicle_id) AS rows_with_vid,
       count(*) FILTER (WHERE id BETWEEN 1 AND 20006) AS rows_id_1_20006
  FROM epa_master;
\if :has_epa_map
SELECT count(*) AS dump_id_map_rows, count(DISTINCT live_id) AS distinct_live_ids FROM epa_master_dump_id_map;
SELECT count(*) FILTER (WHERE m.live_id IS NOT NULL) AS in_dump_id_map,
       count(*) FILTER (WHERE m.live_id IS NULL AND e.epa_vehicle_id IS NOT NULL) AS id_only_with_vid,
       count(*) FILTER (WHERE m.live_id IS NULL AND e.epa_vehicle_id IS NULL) AS rest_no_map_no_vid,
       count(*) FILTER (WHERE m.live_id IS NULL AND e.epa_vehicle_id IS NULL AND e.id BETWEEN 1 AND 20006) AS rest_in_1_20006
  FROM epa_master e
  LEFT JOIN (SELECT DISTINCT live_id FROM epa_master_dump_id_map) m ON m.live_id = e.id;
\else
\echo 'epa_master_dump_id_map: ABSENT'
\endif
SELECT count(*) AS dup_vid_groups, coalesce(sum(n), 0) AS rows_in_dup_vid_groups
  FROM (SELECT epa_vehicle_id, count(*) AS n FROM epa_master
         WHERE epa_vehicle_id IS NOT NULL GROUP BY 1 HAVING count(*) > 1) d;
\if :has_epa_ext
SELECT count(*) AS epa_extended_specs_rows,
       count(*) FILTER (WHERE e.id IS NULL) AS ext_rows_dangling
  FROM epa_extended_specs x LEFT JOIN epa_master e ON e.id = x.epa_master_id;
\endif
SELECT count(*) AS linked_all,
       count(*) FILTER (WHERE c.active) AS linked_active,
       count(*) FILTER (WHERE e.id IS NULL) AS dangling_all_listings,
       count(*) FILTER (WHERE e.id IS NULL AND c.active) AS dangling_active,
       count(*) FILTER (WHERE c.epa_master_id BETWEEN 1 AND 20006) AS links_to_ids_1_20006,
       count(*) FILTER (WHERE c.epa_master_id BETWEEN 1 AND 20006 AND c.active) AS active_links_to_ids_1_20006
  FROM (SELECT epa_master_id, coalesce(listing_active, 1) = 1 AS active
          FROM cars WHERE epa_master_id IS NOT NULL) c
  LEFT JOIN epa_master e ON e.id = c.epa_master_id;
\else
\echo 'epa_master: ABSENT'
\endif

\echo '=== Q13 active cars.forced_induction distribution ==='
SELECT coalesce(forced_induction, '(null)') AS forced_induction, count(*) AS active_rows
  FROM cars WHERE coalesce(listing_active, 1) = 1
 GROUP BY 1 ORDER BY 2 DESC, 1;

\echo '=== Q14 users (row count, non-null totp_secret; no values read) ==='
SELECT EXISTS (SELECT 1 FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'users' AND column_name = 'totp_secret') AS has_totp_col
\gset
\if :has_totp_col
SELECT count(*) AS users_rows,
       count(totp_secret) AS totp_secret_not_null,
       count(*) FILTER (WHERE totp_secret IS NOT NULL AND totp_secret <> '') AS totp_secret_non_empty
  FROM users;
\elif :has_users
SELECT count(*) AS users_rows, 'no totp_secret column' AS note FROM users;
\else
\echo 'users: ABSENT'
\endif

\echo '=== Q15/Q16 chain (V001..V025) vs live objects, by name ==='
-- direction = live_only : a live public table/index/sequence/view whose name no
--   migration CREATEs or ADD CONSTRAINTs (out-of-chain). Implicit objects of chain
--   tables are excluded: indexes backing a constraint on a chain table (inline
--   PRIMARY KEY/UNIQUE) and sequences owned by a chain-table column.
-- direction = chain_only : a name the chain creates that this database lacks.
-- direction = chain_count : how many objects of each kind the chain names.
-- Name-level only; definitions are compared by P4.1's drift report.
WITH chain(kind, name, versions) AS (VALUES
  ('CONSTRAINT_FOREIGN_KEY','dealerships_duplicate_of_id_fkey','1'),
  ('CONSTRAINT_FOREIGN_KEY','epa_extended_specs_epa_master_id_fkey','1'),
  ('CONSTRAINT_FOREIGN_KEY','exterior_colors_vehicle_id_fkey','1,19'),
  ('CONSTRAINT_FOREIGN_KEY','interior_colors_vehicle_id_fkey','1,19'),
  ('CONSTRAINT_FOREIGN_KEY','package_features_package_id_fkey','1,19'),
  ('CONSTRAINT_FOREIGN_KEY','packages_vehicle_id_fkey','1,19'),
  ('CONSTRAINT_FOREIGN_KEY','standalone_options_vehicle_id_fkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','car_move_log_pkey','19'),
  ('CONSTRAINT_PRIMARY_KEY','cars_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_catalog_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_geopoints_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_jobs_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_recipes_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_reviews_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_scan_profile_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_specials_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','dealer_vehicles_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dealerships_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','dictionary_options_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','epa_extended_specs_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','epa_master_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','exterior_colors_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','incomplete_listings_meta_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','incomplete_listings_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','interior_colors_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','lease_offer_matches_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','market_price_stats_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','model_generations_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','model_specs_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','nhtsa_vpic_cache_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','package_features_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','package_observations_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','package_values_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','packages_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','saved_cars_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','scan_runs_pkey','1'),
  ('CONSTRAINT_PRIMARY_KEY','standalone_options_pkey','1,19'),
  ('CONSTRAINT_PRIMARY_KEY','vehicles_pkey','1,19'),
  ('CONSTRAINT_UNIQUE','cars_vin_key','1'),
  ('CONSTRAINT_UNIQUE','dealer_reviews_dealer_id_user_id_key','1,19'),
  ('CONSTRAINT_UNIQUE','dealer_specials_dealer_id_offer_hash_key','1,19'),
  ('CONSTRAINT_UNIQUE','dealer_vehicles_user_id_vin_key','1'),
  ('CONSTRAINT_UNIQUE','exterior_colors_vehicle_id_color_name_key','1,19'),
  ('CONSTRAINT_UNIQUE','interior_colors_vehicle_id_color_name_key','1,19'),
  ('CONSTRAINT_UNIQUE','lease_offer_matches_dealer_id_offer_hash_key','1,19'),
  ('CONSTRAINT_UNIQUE','model_generations_make_model_generation_key','1,19'),
  ('CONSTRAINT_UNIQUE','package_observations_vin_kind_name_norm_source_key','1'),
  ('CONSTRAINT_UNIQUE','package_values_year_make_model_trim_kind_match_key_key','1'),
  ('CONSTRAINT_UNIQUE','packages_vehicle_id_package_name_key','1,19'),
  ('CONSTRAINT_UNIQUE','saved_cars_user_id_car_id_key','1'),
  ('CONSTRAINT_UNIQUE','standalone_options_vehicle_id_option_name_key','1,19'),
  ('CONSTRAINT_UNIQUE','vehicles_year_make_model_trim_engine_drivetrain_key','1,19'),
  ('INDEX','idx_brochure_local_failures_pending','16'),
  ('INDEX','idx_brochure_vision_facts_review','15'),
  ('INDEX','idx_brochure_vision_facts_source','15'),
  ('INDEX','idx_brochure_vision_facts_ymmt','15'),
  ('INDEX','idx_car_attribution_filed','11'),
  ('INDEX','idx_car_attribution_status','11'),
  ('INDEX','idx_car_comments_moderation','2'),
  ('INDEX','idx_car_comments_thread','2'),
  ('INDEX','idx_car_comments_user_recent','2'),
  ('INDEX','idx_car_image_text_sticker','6'),
  ('INDEX','idx_car_image_text_version','6'),
  ('INDEX','idx_car_image_vision_facts_car','17'),
  ('INDEX','idx_car_image_vision_facts_confirmed','17'),
  ('INDEX','idx_car_image_vision_facts_trim','17'),
  ('INDEX','idx_car_move_log_car','19'),
  ('INDEX','idx_car_photo_attr_pending','7'),
  ('INDEX','idx_car_photo_attr_resolved','7'),
  ('INDEX','idx_cars_active_facet_combo','1'),
  ('INDEX','idx_cars_active_make','1'),
  ('INDEX','idx_cars_active_packages','1'),
  ('INDEX','idx_cars_active_price','1'),
  ('INDEX','idx_cars_active_registry','1'),
  ('INDEX','idx_cars_active_zip','25'),
  ('INDEX','idx_cars_dealer_listing','1'),
  ('INDEX','idx_cch_user_time','13'),
  ('INDEX','idx_comment_attachments_comment','5'),
  ('INDEX','idx_comment_attachments_user','5'),
  ('INDEX','idx_comment_flags_comment','4'),
  ('INDEX','idx_compare_sessions_user_time','13'),
  ('INDEX','idx_cvh_user_time','13'),
  ('INDEX','idx_dealer_catalog_next_scan','1'),
  ('INDEX','idx_dealer_comments_moderation','2'),
  ('INDEX','idx_dealer_comments_thread','2'),
  ('INDEX','idx_dealer_comments_user_recent','2'),
  ('INDEX','idx_dealer_jobs_status','1'),
  ('INDEX','idx_dealer_ratings_fetched','2'),
  ('INDEX','idx_dealer_ratings_place','2'),
  ('INDEX','idx_dealer_reviews_dealer','1,19'),
  ('INDEX','idx_dealer_reviews_user','1,19'),
  ('INDEX','idx_dealer_scan_status_reason','8'),
  ('INDEX','idx_dealer_specials_dealer','1,19'),
  ('INDEX','idx_dealer_vehicles_user','1,12'),
  ('INDEX','idx_dealer_vehicles_vin','1,12'),
  ('INDEX','idx_dealerships_created','1'),
  ('INDEX','idx_dealerships_google_place','1'),
  ('INDEX','idx_dealerships_zip','1'),
  ('INDEX','idx_dgp_dealer_url','1'),
  ('INDEX','idx_dict_options_lookup','1,19'),
  ('INDEX','idx_dict_options_trim','1,19'),
  ('INDEX','idx_epa_extended_specs_ymm','1'),
  ('INDEX','idx_epa_master_lookup','1'),
  ('INDEX','idx_epa_master_trim','1'),
  ('INDEX','idx_exterior_colors_vehicle','1,19'),
  ('INDEX','idx_incomplete_listings_updated','1'),
  ('INDEX','idx_interior_colors_vehicle','1,19'),
  ('INDEX','idx_lease_matches_dealer','1,19'),
  ('INDEX','idx_option_rejections_lookup','14'),
  ('INDEX','idx_option_rejections_reason','14'),
  ('INDEX','idx_package_features_package','1,19'),
  ('INDEX','idx_package_obs_ymm','1'),
  ('INDEX','idx_package_values_ymm','1'),
  ('INDEX','idx_packages_vehicle','1,19'),
  ('INDEX','idx_rooftop_refusals_dealer','9'),
  ('INDEX','idx_rooftop_refusals_seen','9'),
  ('INDEX','idx_saved_searches_user','18'),
  ('INDEX','idx_scan_runs_dealer_time','1'),
  ('INDEX','idx_search_events_time','13'),
  ('INDEX','idx_search_events_user','13'),
  ('INDEX','idx_standalone_options_vehicle','1,19'),
  ('INDEX','idx_trim_msrp_bands_lookup','10'),
  ('INDEX','idx_user_hidden_dealers_user','21'),
  ('INDEX','idx_user_search_history_user','22'),
  ('INDEX','idx_users_apple_sub','13'),
  ('INDEX','idx_users_email_ci','13'),
  ('INDEX','idx_users_google_sub','13'),
  ('INDEX','idx_users_username_ci','13'),
  ('INDEX','idx_vehicles_embedding','1,19'),
  ('INDEX','idx_vehicles_fuel_type','1,19'),
  ('INDEX','idx_vehicles_make','1,19'),
  ('INDEX','idx_vehicles_search','1,19'),
  ('INDEX','idx_vehicles_year_make_model','1,19'),
  ('INDEX','idx_vin_owner_conflicts_pair','24'),
  ('INDEX','ux_ai_engine_specs','1,19'),
  ('INDEX','ux_ai_model_specs_ymm','1,19'),
  ('SEQUENCE','car_move_log_id_seq','19'),
  ('SEQUENCE','cars_id_seq','1'),
  ('SEQUENCE','dealer_jobs_id_seq','1'),
  ('SEQUENCE','dealer_reviews_id_seq','1,19'),
  ('SEQUENCE','dealer_specials_id_seq','1,19'),
  ('SEQUENCE','dealer_vehicles_id_seq','1'),
  ('SEQUENCE','dealerships_id_seq','1'),
  ('SEQUENCE','dictionary_options_id_seq','1,19'),
  ('SEQUENCE','epa_master_id_seq','1'),
  ('SEQUENCE','exterior_colors_id_seq','1,19'),
  ('SEQUENCE','interior_colors_id_seq','1,19'),
  ('SEQUENCE','lease_offer_matches_id_seq','1,19'),
  ('SEQUENCE','model_generations_id_seq','1,19'),
  ('SEQUENCE','package_features_id_seq','1,19'),
  ('SEQUENCE','package_observations_id_seq','1'),
  ('SEQUENCE','package_values_id_seq','1'),
  ('SEQUENCE','packages_id_seq','1,19'),
  ('SEQUENCE','saved_cars_id_seq','1'),
  ('SEQUENCE','scan_runs_id_seq','1'),
  ('SEQUENCE','standalone_options_id_seq','1,19'),
  ('SEQUENCE','vehicles_id_seq','1,19'),
  ('TABLE','ai_engine_specs','1,19'),
  ('TABLE','ai_model_specs','1,19'),
  ('TABLE','brochure_local_extraction_failures','16'),
  ('TABLE','brochure_vision_facts','15'),
  ('TABLE','car_attribution','11'),
  ('TABLE','car_comments','2'),
  ('TABLE','car_compare_history','13'),
  ('TABLE','car_image_text','6'),
  ('TABLE','car_image_vision_extraction_failures','17'),
  ('TABLE','car_image_vision_facts','17'),
  ('TABLE','car_move_log','19'),
  ('TABLE','car_photo_attribution','7'),
  ('TABLE','car_view_history','13'),
  ('TABLE','cars','1'),
  ('TABLE','cars_backup_capacity_20260718','1'),
  ('TABLE','cars_backup_mcpeek_prescan_20260712','1'),
  ('TABLE','cars_backup_mileage_contam_20260712','1'),
  ('TABLE','cars_backup_modelname_20260712','1'),
  ('TABLE','cars_backup_stock_contam_20260711_171841','1'),
  ('TABLE','cars_backup_stock_contam_20260711_171901','1'),
  ('TABLE','cars_backup_vinrepair_20260713','1'),
  ('TABLE','cars_trim_quarantine','19'),
  ('TABLE','catalog_exterior_colors','1,19'),
  ('TABLE','catalog_interior_colors','1,19'),
  ('TABLE','catalog_options','1,19'),
  ('TABLE','catalog_package_features','1,19'),
  ('TABLE','catalog_packages','1,19'),
  ('TABLE','catalog_trims','1,19'),
  ('TABLE','comment_attachments','5'),
  ('TABLE','comment_flags','4'),
  ('TABLE','compare_sessions','13'),
  ('TABLE','dealer_catalog','1'),
  ('TABLE','dealer_comments','2'),
  ('TABLE','dealer_feed_scope','11'),
  ('TABLE','dealer_geopoints','1'),
  ('TABLE','dealer_jobs','1'),
  ('TABLE','dealer_ratings','2'),
  ('TABLE','dealer_recipes','1,19'),
  ('TABLE','dealer_reviews','1,19'),
  ('TABLE','dealer_scan_profile','1'),
  ('TABLE','dealer_scan_status','8'),
  ('TABLE','dealer_specials','1,19'),
  ('TABLE','dealer_vehicles','1,12'),
  ('TABLE','dealerships','1'),
  ('TABLE','dictionary_options','1,19'),
  ('TABLE','epa_extended_specs','1'),
  ('TABLE','epa_extended_specs_bak_20260711_101806','1'),
  ('TABLE','epa_extended_specs_bak_20260712_083724','1'),
  ('TABLE','epa_extended_specs_bak_badhp_20260712','1'),
  ('TABLE','epa_extended_specs_bak_badratio_20260712','1'),
  ('TABLE','epa_master','1'),
  ('TABLE','epa_master_dump_id_map','1'),
  ('TABLE','incomplete_listings','1'),
  ('TABLE','incomplete_listings_meta','1'),
  ('TABLE','lease_offer_matches','1,19'),
  ('TABLE','listings_grid_cards','23'),
  ('TABLE','market_price_stats','1,19'),
  ('TABLE','model_generations','1,19'),
  ('TABLE','model_specs','1'),
  ('TABLE','nhtsa_vpic_cache','1'),
  ('TABLE','option_rejections','14'),
  ('TABLE','org_invites','13'),
  ('TABLE','orgs','13'),
  ('TABLE','package_observations','1'),
  ('TABLE','package_values','1'),
  ('TABLE','package_values_msrp_quarantine','19'),
  ('TABLE','rooftop_refusals','9'),
  ('TABLE','saved_cars','1'),
  ('TABLE','saved_searches','18'),
  ('TABLE','scan_runs','1'),
  ('TABLE','search_events','13'),
  ('TABLE','trim_msrp_bands','10'),
  ('TABLE','user_hidden_dealers','21'),
  ('TABLE','user_search_history','22'),
  ('TABLE','users','13'),
  ('TABLE','vin_owner_conflicts','24')
),
chain_tables AS (SELECT name FROM chain WHERE kind = 'TABLE'),
live AS (
  SELECT c.oid, c.relname AS name, c.relkind,
         CASE WHEN c.relkind IN ('i', 'I') THEN (SELECT t.relname FROM pg_index x JOIN pg_class t ON t.oid = x.indrelid WHERE x.indexrelid = c.oid)
              WHEN c.relkind = 'S' THEN (SELECT t.relname FROM pg_depend d JOIN pg_class t ON t.oid = d.refobjid
                                          WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid AND d.deptype IN ('a', 'i')
                                            AND d.refclassid = 'pg_class'::regclass LIMIT 1)
         END AS parent
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
   WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'i', 'I', 'S', 'v', 'm')
)
SELECT 'live_only' AS direction,
       CASE l.relkind WHEN 'r' THEN 'TABLE' WHEN 'p' THEN 'TABLE' WHEN 'i' THEN 'INDEX' WHEN 'I' THEN 'INDEX'
                      WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' END AS kind,
       l.name,
       coalesce(l.parent, '') AS parent,
       coalesce((l.parent IN (SELECT name FROM chain_tables))::text, '') AS parent_in_chain,
       CASE WHEN l.relkind IN ('r', 'p', 'm') THEN (SELECT reltuples::bigint FROM pg_class WHERE oid = l.oid)::text ELSE '' END AS approx_rows,
       pg_size_pretty(pg_total_relation_size(l.oid)) AS total_size,
       CASE WHEN l.relkind IN ('i', 'I') THEN pg_get_indexdef(l.oid) ELSE '' END AS detail
  FROM live l
 WHERE NOT EXISTS (SELECT 1 FROM chain ch WHERE ch.name = l.name)
   AND NOT (l.relkind IN ('i', 'I')
            AND EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = l.oid AND k.contype IN ('p', 'u', 'x'))
            AND l.parent IN (SELECT name FROM chain_tables))
   AND NOT (l.relkind = 'S' AND l.parent IN (SELECT name FROM chain_tables))
UNION ALL
SELECT 'chain_only', ch.kind, ch.name, '', '', '', '', 'created in V' || ch.versions
  FROM chain ch
 WHERE CASE WHEN ch.kind LIKE 'CONSTRAINT_%'
            THEN NOT EXISTS (SELECT 1 FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace
                              WHERE n.nspname = 'public' AND k.conname = ch.name)
            ELSE NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                              WHERE n.nspname = 'public' AND c.relname = ch.name)
       END
UNION ALL
SELECT 'chain_count', kind, count(*)::text, '', '', '', '', '' FROM chain GROUP BY kind
 ORDER BY 1, 2, 3;

\echo '=== Q99 session proof at end (must still read on) ==='
SELECT current_setting('transaction_read_only') AS txn_read_only,
       current_setting('default_transaction_read_only') AS default_txn_read_only;
