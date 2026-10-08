# Schema ledger, 2026-10 (P0A.2)

*P0A.2: Prod and local read-only census* (`docs/REMEDIATION_PLAN_2026_10.md`, Phase 0A).

- Date: 2026-10-08. Census runs: local 14:57:41Z (first) and 15:03:03Z (final); prod 14:58:45Z (first) and 15:03:14Z (final). `migrate --dry-run`: local 14:58:30Z, prod 14:59:22Z. The tables below come from the final runs. The final run repeated the first and added exact row counts to Q3 and Q6. A spot check of Q2, Q5, Q8, Q10 and Q12-Q14 found the same numbers in both runs.
- Run by: a Phase 0A workflow agent. The prod part was authorized by the owner (`docs/remediation/OWNER_DECISIONS_LOG.md`, 2026-10-07, "Phase 0A prod access": an agent runs the prod census in a READ ONLY session). The mini is not approved (no SSH), so its part is BLOCKED.
- Revision: detached worktree at `afa063a22` (feature/http-only-scans). `migrations/` and `backend/scripts/migrate.py` are byte-identical to `dbdbf5cba`, where the plan was written.
- Query file: `docs/db_layer/census_2026_10.sql` (Q0 to Q17 plus Q99). Its first two statements are `SET default_transaction_read_only = on; SET statement_timeout = '20s';`, and every session also ran with `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000'`.
- 10-04 reference: `workspace/backups/prod_20261004_pre150.dump` (600,428,043 bytes, file mtime 2026-10-04T20:21:13Z, sha256 prefix `91c4fa2fc7ee262f`, pg_dump 18.6, dbname `railway`). Its numbers were read by streaming `pg_restore -a -t <table> -f -` into a counting script, so nothing was restored into any database. The schema came from `pg_restore -s`.
- No secret appears in this file. Prod host and port are redacted, and so is every DSN. The users table appears as counts only. Dealer ids are dealership domains, not user data.

## Answers to the Accept items

1. **The ledger has one row per DB.** See "Ledger" below: prod `railway`, local MBP `cars`, and the mini's own Postgres (BLOCKED). The 10-04 dump is listed as the reference snapshot. The MBP's Homebrew server holds 10 other databases. None of them is an inventory DB: none has a `cars`, `schema_migrations` or `dealer_recipes` table.
2. **Prod is at V025.**
   - Prod `schema_migrations` has 25 rows, V001 to V025, with 0 pending, 0 checksum drift and 0 applied versions missing a file. `migrate --dry-run` against prod printed "25 migration file(s), 25 already applied, 0 pending / database is up to date".
   - This **overturns** the "prod at V018" docstrings, which P4.5 corrects:
     - `backend/db/schema_version.py:18-19`: "production's `schema_migrations` was copied from a local database at V018 on 2026-09-28 and has not been baselined past V019 yet".
     - `backend/db/inventory_pg.py:301-302`: "prod's `schema_migrations` is still at V018 until an operator baselines V019".
   - Prod's ledger is its own history. It was not copied from local:
     - V001-V018 recorded 2026-09-05 13:59Z (one baseline batch).
     - V019 2026-09-06 20:32Z; V020 2026-09-06 20:37Z.
     - V021-V023 2026-09-28 22:15Z; V024 2026-09-30 22:22Z.
     - V025 2026-10-04 20:34Z (the 1.5.0 deploy, as P0A.1 found in the boot logs).
     - The pre-1.4.1 prod dump `prod_cars_20260928_pre141.dump` already holds V001-V020 with the 09-05/09-06 stamps. The 09-28 `local_cars_20260928_for_prod.dump` carries no `schema_migrations` data. So no restore ever replaced prod's ledger.
3. **DBs that must receive V026:**
   - **prod `railway`**: by the owner, per D-DB6 (a) and P4.8.
   - **local MBP `cars`**: applied in the P4.3 merge (MAIN).
   - **the mini's own Postgres**: only if D-DB5 = (a). Its state is unknown (BLOCKED).
   - Fresh builds (CI, scratch DBs, compose) get V026 from the chain and need no ledger row.
   - On prod and local, V026 would do the same work:
     - create `review_reports` and `haiku_spec_cache`, which are absent on both;
     - adopt `uq_option_rejections_standing`, which is present on both. The P4.3 RAISE guard passes, because both DBs have 0 duplicate groups.
     - The V019-only objects already exist on both (`car_move_log`, its sequence, `car_move_log_pkey`, `idx_car_move_log_car`, `cars_trim_quarantine`, `package_values_msrp_quarantine`), so those parts are no-ops.
     - Neither DB has a `*_embeddings` table, so V026 has no pgvector table to adopt (D-DB7).
4. **Every out-of-chain object found:** see "Out-of-chain objects" below.
   - On prod: 7 tables, including the runner-owned `schema_migrations`, plus 1 index (`uq_option_rejections_standing`) and 4 owned sequences.
   - No out-of-chain extension, function, trigger, view, type, schema or explicitly named constraint on either DB.
   - No chain object is missing on either DB.
5. **Diff against the 10-04 dump:** every census number is identical, except that prod gained the `schema_migrations` row for V025 (2026-10-04 20:34Z, after the dump).
   - V025 was a no-op on prod: `idx_cars_active_zip` is already in the 10-04 schema. The runtime DDL had created it (`schema_repo.ensure_cars_listings_indexes`, `backend/db/repositories/schema_repo.py:127`).
   - The 31 auth-staled dealers are the same 31 ids.
   - Every aggregate the census reads is unchanged between the dump and today: counts, histograms, maxima and lists. This is not a row-by-row diff. Prod has had no scan since 2026-09-29.
6. **Every session ran READ ONLY.**
   - All four census sessions printed `default_transaction_read_only=on`, `transaction_read_only=on` and `statement_timeout=20s` at Q0, and `on/on` again at Q99.
   - The `migrate --dry-run` wrapper read those settings on its connection, and would have refused to continue unless both were `on`. It then let `migrate.main(['--dry-run'])` run.
   - `migrate.py` issues only `SELECT to_regclass(...)` and `SELECT version, checksum FROM schema_migrations` when `--apply` is absent. The `CREATE TABLE`, `INSERT` and per-file `execute` are all behind `apply` (`apply = args.apply and not args.dry_run` at :255; the writes at :297-321; read at `afa063a22`).
   - The probes of the other local databases used the same PGOPTIONS.

## Ledger (one row per DB)

| | prod `railway` | local MBP `cars` | mini's own Postgres | 10-04 prod dump (reference) |
|---|---|---|---|---|
| where | Railway service `Postgres`, image `ghcr.io/railwayapp-templates/postgres-ssl:18`, public TCP proxy `<prod-host>.proxy.rlwy.net` (port redacted) | Homebrew `postgresql@17`, unix socket `/tmp` port 5432 (DSN `postgresql:///cars?host=/tmp`, no credentials). Also the DB behind the mini's `localhost:15432` tunnel | the mini's local server, named by its `.env` | `workspace/backups/prod_20261004_pre150.dump` |
| census run | yes, READ ONLY (2 runs) | yes, READ ONLY (2 runs) | **BLOCKED**: no SSH to the mini (owner log 2026-10-07); D-DB5 unanswered. Command pack below | streamed with pg_restore |
| server_version | 18.6 (Debian 18.6-1.pgdg13+2) | 17.10 (Homebrew) | unknown | dumped from 18.6 |
| vector / dblink | 0.8.6 / 1.2 | 0.8.2 / 1.2 | unknown | both extensions present |
| DB size | 3,590 MB | 3,238 MB | unknown | 600 MB compressed archive |
| schema_migrations | 25 rows, V001-V025 | 25 rows, V001-V025 | unknown | 24 rows, V001-V024 |
| pending / checksum drift / missing file | 0 / 0 / 0 | 0 / 0 / 0 | unknown | V025 pending at the time |
| `migrate --dry-run` | host `<redacted>.proxy.rlwy.net`, db `railway`: "25 migration file(s), 25 already applied, 0 pending"; "database is up to date"; exit 0 | host `unix-socket:/tmp`, port 5432, db `cars`: same output; exit 0 | not run | n/a |
| at V025? | **yes** | yes | unknown | no (V024) |
| must receive V026 | **yes** (owner, P4.8) | **yes** (P4.3 merge, MAIN) | only if D-DB5 = (a) | n/a |
| V026 work on this DB | create review_reports, haiku_spec_cache; adopt uq_option_rejections_standing (exists, 0 dup groups) | same | unknown | n/a |
| out-of-chain objects | 7 tables (incl. schema_migrations), 1 index, 4 owned sequences | 6 tables (no `cars_owner_backup_20260929`), 1 index, 4 owned sequences | unknown | same as prod |
| max_connections / in use at census | 500 / 9 backends, 1 client backend (this psql) | 100 / 6 backends, 1 client backend | unknown | n/a |
| idle_in_transaction_session_timeout | 300000 ms (5 min), source `user`: `ALTER ROLE postgres SET idle_in_transaction_session_timeout=5min` (all DBs) | 0 (default) | unknown | n/a |
| idle_session_timeout | 0 (default) | 0 (default) | unknown | n/a |

The other databases on the MBP's Homebrew server are `Dealership`, `halfway_test`, `ninermatch`, `postgres`, `roster`, `stocks`, `stocks_test`, `studio`, `studio_test_auth` and `studio_test_core_db`. Each has 0 to 32 public tables, and none has `cars`, `schema_migrations` or `dealer_recipes`. `Dealership` has 1 public table and is not an inventory DB.

A second, separate PostgreSQL 18 server (EDB install, `/Library/PostgreSQL/18`, socket `/tmp/.s.PGSQL.5002`) runs on the MBP. It requires a password, the project's `.env` does not point at it, and it was not probed.

## Out-of-chain objects

"Out of chain" means no `migrations/V*.sql` file creates the name. The check is name-level only: Q15/Q16 for relations and constraints, Q17 for extensions, functions, triggers, views, types and schemas. Definitions are compared by P4.1.

| object | kind | prod | local | 10-04 dump | creator | for |
|---|---|---|---|---|---|---|
| `uq_option_rejections_standing` | UNIQUE INDEX on option_rejections (car_id, option_name, price, reason), NULLS DISTINCT | yes (104 kB) | yes (136 kB) | yes | runtime DDL, `backend/scripts/feed_package_registry_from_vision.py:506` (after the dedupe DELETE at :499) | D-DB7 (a) adopt in V026 (P4.3) |
| `cars_epa_link_backup_20260921` | table | 163,165 rows, 15 MB | 163,165 rows, 15 MB | yes | none in git history (`git log --all -S`); one-off | D-DB1 (P13B.4); EPA recovery source until P10D |
| `cars_owner_backup_20260929` | table | **6,961 rows, 984 kB (prod only)** | absent | 6,961 rows | none in git history; from the 09-29 owner-guard repair | **not in D-DB1's list**; add it there |
| `image_summary_restore_log` (+ `_pkey`, `_id_seq`) | table | 8 rows, 56 kB | 8 rows, 80 kB | 8 rows | none in git history | D-DB1 or V026 (owner) |
| `make_canon_log` (+ `_pkey`, `_id_seq`) | table | 59 rows | 59 rows | 59 rows | none in git history | same |
| `model_canon_log` (+ `_pkey`, `_id_seq`) | table | 0 rows | 0 rows | 0 rows | none in git history | same |
| `package_value_purge_log` (+ `_pkey`, `_id_seq`) | table | 5 rows | 5 rows | 5 rows | none in git history | same |
| `schema_migrations` (+ `_pkey`) | table | 25 rows | 25 rows | 24 rows | `backend/scripts/migrate.py` `_CREATE_TABLE_SQL` | runner-owned; expected, not drift |

Objects the chain names only in V019 (a fresh `migrate --apply` records V019 without running it, so a fresh build lacks them). Present on both DBs:

| object | prod | local | 10-04 dump |
|---|---|---|---|
| `car_move_log` (+ `car_move_log_id_seq`, `car_move_log_pkey`, `idx_car_move_log_car`) | 222 rows | 222 rows | 222 rows |
| `cars_trim_quarantine` | 5,405 rows | 5,405 rows | 5,405 rows |
| `package_values_msrp_quarantine` | 448 rows | 448 rows | 448 rows |

Runtime-only objects V026 must create, absent on both DBs:

| object | prod | local | creator |
|---|---|---|---|
| `review_reports` | absent | absent | lazily, on the first review report (`backend/reviews/store.py:289`, DDL at :69). Absent means no review has been reported on either DB |
| `haiku_spec_cache` | absent | absent | lazily, by the enrichment Haiku fallback (`backend/enrichment/service.py:140,198`). Never used on either DB |

Checked and clean on both DBs:
- Extensions: only `dblink`, `vector` (chain) and `plpgsql` (server default).
- Functions: only `refresh_vehicle_search_vector()` and `set_updated_at()` (chain).
- Triggers: only `trg_vehicles_search_vector` and `trg_vehicles_updated_at` on `catalog_trims` (chain).
- No views, materialized views or user types. One schema in use: `public` (prod 304 relations, local 303).
- No explicitly named live constraint outside the chain. No chain object (76 tables, 84 indexes, 21 sequences, 31 PK, 14 UNIQUE, 7 FK, 14 CHECK, 2 functions, 2 triggers, 2 extensions) is missing.
- No `*_embeddings` relation in any schema. The only `vector` column anywhere is `catalog_trims.embedding` (chain).

## Census results, side by side

Q numbers follow `census_2026_10.sql`. Δ is prod today minus the 10-04 dump.

| Q | item | prod 10-08 | local MBP | 10-04 dump | Δ prod vs dump |
|---|---|---|---|---|---|
| 1 | server_version | 18.6 | 17.10 | 18.6 | 0 |
| 1 | vector extversion | 0.8.6 | 0.8.2 | present | n/a |
| 1 | max_connections | 500 | 100 | n/a | n/a |
| 1 | idle_in_transaction_session_timeout | 5 min (role postgres) | 0 | dump header sets it to 0 for the dump session only | n/a |
| 2 | schema_migrations high water / rows / pending | 25 / 25 / 0 | 25 / 25 / 0 | 24 / 24 / (V025) | +1 row (V025) |
| 3 | review_reports / haiku_spec_cache | absent / absent | absent / absent | absent / absent | 0 |
| 3 | car_move_log / cars_trim_quarantine / package_values_msrp_quarantine rows | 222 / 5,405 / 448 | 222 / 5,405 / 448 | 222 / 5,405 / 448 | 0 |
| 3 | `*_embeddings` tables | 0 | 0 | 0 | 0 |
| 4 | uq_option_rejections_standing / idx_car_move_log_car / idx_cars_active_zip | present / present / present | present / present / present | present / present / present | 0 |
| 4 | cars.zip_code exists (D-DB8) | yes | yes | yes | 0 |
| 5 | option_rejections rows | 987 | 987 | 987 | 0 |
| 5 | rows with a NULL key column (car_id or price) | 0 | 0 | 0 | 0 |
| 5 | duplicate groups under the `IS NOT DISTINCT FROM` key / surplus rows | **0 / 0** | 0 / 0 | 0 / 0 | 0 |
| 6 | backup/dump tables (name ~ bak, backup, dump) | 14, 146 MB | 13, 145 MB | 14 | 0 |
| 7 | incomplete_listings_meta `index_bootstrap_v1` | `1` (1 meta row) | `1` (1 meta row) | `1` | 0 |
| 8 | cars total / active (`coalesce(listing_active,1)=1`) | 420,755 / 268,868 | 357,149 / 214,675 | 420,755 / 268,868 | 0 |
| 8 | active rows by scraped_at day, 09-24..10-08 | 09-24 413; 09-25 863; 09-26 3,316; 09-28 6,516; **09-29 257,337**; nothing later | 09-24 413; 09-25 3,435; 09-26 8,503; 09-28 201,899; nothing later | identical to prod | 0 |
| 8 | active rows scraped before 09-24 | 2026-07: 36; 2026-08: 387; nothing in 2026-09 before the 24th | 2026-07: 36; 2026-08: 389 | identical to prod | 0 |
| 9 | vin_owner_conflicts rows / claimants / owners / pairs | 12,903 / 146 / 144 / 212 | 0 / 0 / 0 / 0 | 12,903 / 146 / 144 / 212 | 0 |
| 9 | vin_owner_conflicts seen_at span | 2026-09-29T17:03:18Z to 19:32:01Z (all on 09-29) | none | same | 0 |
| 9 | cars_owner_backup_20260929 rows | 6,961 | absent | 6,961 | 0 |
| 10 | dealer_recipes rows / with recipes / recipes total | 687 / 594 / 1,158 | 687 / 594 / 1,137 | 687 / 594 / 1,158 | 0 |
| 10 | max(updated_at) | 2026-09-29T19:36:16Z | 2026-09-29T21:24:36Z | 2026-09-29T19:36:16Z | 0 |
| 10 | invalid scan_hints JSON / invalid recipes_json | 0 / 0 | 0 / 0 | 0 / 0 | 0 |
| 10 | recipe_status prefixes | none 608; ok 47; rejected:auth_needed 29; uncertain 2; stale 1 | none 687 (no dealer carries recipe_status) | identical to prod | 0 |
| 10 | dealers with `lifecycle_last_attempt` (min / max) | 65 (2026-09-29T01:27:45Z / 19:34:42Z) | 0 | 65, same span | 0 |
| 10 | rows with `max_saved_at=0 AND recipe_count>0` (P1C saved_at backfill) | **235** (231 updated 09-29, 4 on 09-28) | 201 (195 on 09-28, 4 on 09-29, 2 on 08-06) | 235 | 0 |
| 10 | `+cascade` rows / recipes | 0 / 0 | 0 / 0 | 0 / 0 | 0 |
| 10 | dealers whose every recipe is auth-staled (http_401/403) | **31** (list below) | 2 (`nissanorange-com`, `scottclarkhonda-com`) | 31, the same ids | 0 |
| 10 | hints-only rows (`recipes_json = '[]'`) | 93 | 93 | 93 | 0 |
| 11 | dealer_jobs queued / running / total | 0 / 0 / 0 | 0 / 0 / 0 | 0 rows | 0 |
| 11 | dealer_catalog rows / due now | 0 / 0 | 0 / 0 | 0 rows | 0 |
| 12 | epa_master rows / min id / max id | 70,496 / 1 / 70,496 | 70,496 / 1 / 70,496 | 70,496 / 1 / 70,496 | 0 |
| 12 | rows with an epa_vehicle_id / ids in 1..20006 | 50,394 / 20,006 | 50,394 / 20,006 | 50,394 / 20,006 | 0 |
| 12 | provenance: in epa_master_dump_id_map / id-only with a vid / rest (no map, no vid) | 50,029 / 461 / 20,006 (all 20,006 are ids 1..20006) | same | same | 0 |
| 12 | epa_master_dump_id_map rows (distinct live_id) | 50,029 (50,029) | 50,029 (50,029) | 50,029 | 0 |
| 12 | duplicate epa_vehicle_id groups / rows in them | 5,208 / 11,316 | 5,208 / 11,316 | 5,208 / 11,316 | 0 |
| 12 | epa_extended_specs rows / dangling epa_master_id | 49,912 / 0 | 49,912 / 0 | 49,912 / 0 | 0 |
| 12 | cars with epa_master_id: all / active | 373,864 / 238,582 | 313,454 / 186,521 | 373,864 / 238,582 | 0 |
| 12 | dangling cars.epa_master_id (all listings / active) | 0 / 0 | 0 / 0 | 0 / 0 | 0 |
| 12 | cars linked to ids 1..20006 (all / active) | 272,489 / 173,656 | 229,060 / 136,001 | 272,489 / 173,656 | 0 |
| 13 | active forced_induction: NULL / Turbocharged / Twin Turbocharged / Supercharged | 151,390 / 107,793 / 9,253 / 432 | 125,231 / 82,957 / 6,139 / 348 | 151,390 / 107,793 / 9,253 / 432 | 0 |
| 14 | users rows / non-null totp_secret | **0 / 0** | 0 / 0 | 0 / 0 | 0 |

Backup tables (Q6, exact rows): 

| table | prod | local |
|---|---|---|
| cars_backup_capacity_20260718 | 42,130 (127 MB) | 42,130 (127 MB) |
| cars_backup_mcpeek_prescan_20260712 | 419 | 419 |
| cars_backup_mileage_contam_20260712 | 412 | 412 |
| cars_backup_modelname_20260712 | 47 | 47 |
| cars_backup_stock_contam_20260711_171841 | 2 | 2 |
| cars_backup_stock_contam_20260711_171901 | 412 | 412 |
| cars_backup_vinrepair_20260713 | 53 | 53 |
| cars_epa_link_backup_20260921 (out of chain) | 163,165 (15 MB) | 163,165 (15 MB) |
| cars_owner_backup_20260929 (out of chain) | 6,961 (984 kB) | absent |
| epa_extended_specs_bak_20260711_101806 | 290 | 290 |
| epa_extended_specs_bak_20260712_083724 | 130 | 130 |
| epa_extended_specs_bak_badhp_20260712 | 839 | 839 |
| epa_extended_specs_bak_badratio_20260712 | 622 | 622 |
| epa_master_dump_id_map | 50,029 | 50,029 |
| total | 14 tables, 146 MB | 13 tables, 145 MB |

The dump's exact counts equal prod's for all 14 tables.

### Prod: the 31 dealers whose every recipe is auth-staled (Q10)

Same rule as `backend/scripts/unstale_host_blocked_recipes.candidates`: every recipe has `stale` true and `stale_reason` http_401 or http_403. The 10-04 dump yields the same 31 ids. P6B.4 diffs its list mode against this.

| dealer_id | recipes | recipe_status |
|---|---|---|
| 5starford-com | 1 | rejected:auth_needed |
| bmwofbloomfield-com | 2 | rejected:auth_needed |
| capitaltoyota-com | 1 | rejected:auth_needed |
| cartersubaruballard-com | 2 | rejected:auth_needed |
| cumberlandchryslercenter-com | 2 | rejected:auth_needed |
| daysrockmart-com | 1 | rejected:auth_needed |
| downeyhyundai-com | 2 | rejected:auth_needed |
| duvalford-com | 1 | rejected:auth_needed |
| easyhonda-com | 2 | rejected:auth_needed |
| fivestarforddallas-com | 1 | rejected:auth_needed |
| fremonttoyota-com | 1 | rejected:auth_needed |
| groovesubaru-com | 2 | rejected:auth_needed |
| groovetoyota-com | 2 | rejected:auth_needed |
| harperacura-net | 2 | rejected:auth_needed |
| hillsidetoyota-nyc | 1 | rejected:auth_needed |
| hondaofelcajon-com | 2 | rejected:auth_needed |
| hyundaiofcookeville-com | 2 | rejected:auth_needed |
| lambonb-com | 1 | rejected:auth_needed |
| lexusofchattanooga-com | 1 | rejected:auth_needed |
| lexusofknoxville-com | 1 | rejected:auth_needed |
| lindsaylexusofalexandria-com | 1 | rejected:auth_needed |
| mcgrathcityhonda-com | 2 | rejected:auth_needed |
| mclarennb-com | 2 | rejected:auth_needed |
| mymetrohonda-com | 2 | rejected:auth_needed |
| nissanofcookeville-com | 2 | rejected:auth_needed |
| nissanofirvine-com | 1 | stale:403:2026-09-29T01:21:55+00:00 |
| nissanorange-com | 1 | (none) |
| ourismanhondaoftysonscorner-com | 2 | rejected:auth_needed |
| overdrive-usa-com | 1 | rejected:auth_needed |
| pacificvolkswagen-com | 2 | rejected:auth_needed |
| parksidekia-com | 2 | rejected:auth_needed |

### Prod: vin_owner_conflicts, top 30 (owner, claimant) pairs (Q9)

All 12,903 rows were written on 2026-09-29 between 17:03Z and 19:32Z, during the Railway guarded fleet run. These are the guard's refused moves. They have no reader yet.

| owner_dealer_id | claimant_dealer_id | VINs | last seen_at |
|---|---|---|---|
| mbontario-com | mbbeverlyhills-com | 1,427 | 2026-09-29T19:24:19Z |
| ricartford-com | ricart-com | 1,339 | 2026-09-29T17:36:27Z |
| mblaguna-com | mbfoothill-com | 1,005 | 2026-09-29T19:32:01Z |
| mtnviewnissan-com | cleveland-nissan-com | 893 | 2026-09-29T19:26:36Z |
| crownlexus-com | bmwofmonrovia-net | 676 | 2026-09-29T18:14:53Z |
| universaltoyota-com | saford-com | 573 | 2026-09-29T18:25:19Z |
| toyotachulavista-com | pacifichonda-com | 487 | 2026-09-29T19:02:44Z |
| bentleygmc-com | bentleycadillac-com | 480 | 2026-09-29T19:13:17Z |
| bentleygmc-com | bentleyhyundai-com | 480 | 2026-09-29T18:31:26Z |
| toyotachulavista-com | kearnymesachevrolet-com | 479 | 2026-09-29T19:01:32Z |
| chapmandodge-com | chapmanfordaz-com | 436 | 2026-09-29T17:33:23Z |
| hughwhitehonda-com | hughwhitehonda-net | 382 | 2026-09-29T19:09:35Z |
| mossytoyota-com | mossyhondalemongrove-com | 353 | 2026-09-29T18:31:17Z |
| landersmclartyfordfortpayne-net | landersmclartychevrolet-com | 344 | 2026-09-29T17:52:46Z |
| parkscharlotte-com | parkschevrolethuntersville-com | 305 | 2026-09-29T17:21:16Z |
| mbhsv-com | landersmclartynissanhuntsville-com | 215 | 2026-09-29T19:07:45Z |
| lexusofgreenwoodvillage-com | kunilexusofgreenwoodvillage-com | 201 | 2026-09-29T19:15:18Z |
| hileyvwhuntsville-com | audihuntsville-com | 164 | 2026-09-29T19:21:53Z |
| crownlexus-com | bmwofbeverlyhills-com | 132 | 2026-09-29T18:05:12Z |
| mbofhenderson-com | fletcherjones-com | 130 | 2026-09-29T19:20:40Z |
| wisimonson-net | crownlexus-com | 113 | 2026-09-29T17:40:02Z |
| wisimonson-net | bmwofmonrovia-net | 104 | 2026-09-29T18:14:53Z |
| bmwbellevue-com | audibellevue-com | 100 | 2026-09-29T19:22:56Z |
| landersmclartyfordfortpayne-net | landersmclartytoyota-com | 99 | 2026-09-29T19:15:35Z |
| mbontario-com | fjmercedes-com | 99 | 2026-09-29T19:21:38Z |
| mbontario-com | audiofcostamesa-com | 90 | 2026-09-29T18:40:06Z |
| universaltoyota-com | redmccombstoyota-com | 89 | 2026-09-29T19:20:05Z |
| landersmclartysubaru-net | landersmclartydcjal-com | 88 | 2026-09-29T19:03:51Z |
| davekirk-com | easttennesseedodge-com | 84 | 2026-09-29T18:56:21Z |
| toyotacarson-com | mbbeverlyhills-com | 78 | 2026-09-29T19:24:19Z |

### scan_hints keys (Q10, dealers carrying each key)

| key | prod | local |
|---|---|---|
| hint_source | 641 | 641 |
| notes | 621 | 621 |
| timing | 581 | 464 |
| dealer_address | 106 | 64 |
| recipe_status | 79 | 0 |
| recipe_validation | 78 | 0 |
| lifecycle_last_attempt | 65 | 0 |
| dealer_address_source | 64 | 32 |
| dealer_city / dealer_state / place_source | 45 / 45 / 45 | 35 / 35 / 35 |
| rooftop_unidentified_rows | 36 | 36 |
| dealer_zip | 23 | 18 |
| skip_reason | 20 | 20 |
| requires_browser | 19 | 19 |
| needs_http_proxy | 18 | 18 |
| price_source | 17 | 17 |
| image_source | 8 | 8 |
| rooftop_name_aliases | 3 | 3 |
| price_requires_full_scan | 2 | 2 |

Local and prod share the dealer roster (687 rows) but have diverged since the 09-28 prod restore. Prod-only writes on 2026-09-29 (lifecycle stamps from 01:27Z to 19:34Z, a window that spans the Railway guarded fleet run of 16:43Z to 19:39Z) added recipe_status, recipe_validation and the lifecycle keys. Local recipe sets were last written 2026-09-29T21:24Z, without those keys.

## Facts for later units

- **P4.4 (option_rejections dedupe):** 0 duplicate groups and 0 NULL-key rows on prod and on local, so the script is a no-op on both. `uq_option_rejections_standing` already exists on both, which a duplicate would have prevented. P4.8 step 2.3 (P4.4 on prod) can be skipped, because P0A.2 found no groups.
- **D-DB7:** prod is PG 18 and local PG 17, so `NULLS NOT DISTINCT` (PG 15+) is available on both. Today's index is NULLS DISTINCT. No row has a NULL key yet, so switching costs nothing. Neither DB has a pgvector `*_embeddings` table to adopt.
- **D-TC3 / P15B.6:** prod is `18.6` with `vector 0.8.6` and `dblink 1.2`. The CI and compose image should be a pgvector/pgvector image at pg18 with vector 0.8.x. Local dev runs pg17 with vector 0.8.2, which is one major behind prod.
- **P4.5:** correct the two "prod at V018" docstrings (Answer 2).
- **Phase 6A entry gate / P11A.3 (connection headroom):** prod max_connections is 500, with 1 client backend at census time (this session). The web app held no idle connection then. N shards × 1 lock connection fits easily.
- **idle_in_transaction_session_timeout** is 5 min on prod (`ALTER ROLE postgres`, all DBs), as commit `68ce8433c` expects. It is 0 locally, so a local run cannot reproduce prod's idle-in-transaction kills. idle_session_timeout is 0 on both.
- **P0A.4 gate:** dealer_jobs is empty on prod (0 queued, 0 running, 0 total) and dealer_catalog is empty. This matches P0A.4's 09:03Z and 12:10Z reads.
- **P6B.4:** 31 auth-staled dealers, listed above.
- **P1C (saved_at backfill):** 235 prod rows and 201 local rows have recipes but `max_saved_at = 0`.
- **P16 (totp_secret DO block):** prod PG `users` has 0 rows and 0 non-null `totp_secret`, so the plan's raising DO block is allowed. The web's real user store is not this table: P0A.1 lists `USERS_DB_PATH` and `USERS_DB_ENCRYPTION_KEY` on web, which is a separate users DB.
- **D-DB1 / P13B.4:** the backup list is the 12 V001 tables, `cars_epa_link_backup_20260921`, and **`cars_owner_backup_20260929` (prod only, 6,961 rows)**. D-DB1 does not list the latter yet. The four untracked log tables (`image_summary_restore_log`, `make_canon_log`, `model_canon_log`, `package_value_purge_log`) need an owner call: adopt them in V026, or drop them with D-DB1.
- **D-DB8:** `cars.zip_code` and `idx_cars_active_zip` exist on both DBs.
- **EPA phases (10A-10D):** epa_master is identical on prod and local. It has 70,496 rows:
  - 50,029 rows are mapped from the dump (`epa_master_dump_id_map`);
  - 461 rows carry a vid but no map entry;
  - the 20,006 legacy rows are ids 1..20006, with no vid and no map entry.
  - 5,208 vids are duplicated (11,316 rows), and no extended-spec row or listing link dangles.
  - 173,656 of the 238,582 linked active prod listings (73%) point at the legacy ids 1..20006.
- **Scan staleness:** prod's newest active `scraped_at` day is 2026-09-29 (257,337 rows). Nothing has been scraped since. Local's newest is 2026-09-28.

## Mini's own Postgres: BLOCKED

The owner has not approved SSH to the mini (`OWNER_DECISIONS_LOG.md`, 2026-10-07), and D-DB5 is unanswered. The plan runs the census on the mini only if D-DB5 = (a). Mini commands normally use the `localhost:15432` tunnel, which reaches the MBP's `cars` DB (the local row above). The mini's own server is the one its `.env` names. Its version, chain state and contents are unknown.

Owner command pack, if D-DB5 = (a). Run on the mini from its checkout, after this census file is on its branch. Never paste the DSN anywhere.

```sh
# 1. DSN of the mini's OWN Postgres, from its .env (not the 15432 tunnel). Never echo it.
DSN=$(.venv/bin/python -c "from dotenv import dotenv_values; print(dotenv_values('.env')['INVENTORY_DATABASE_URL'])")
# 2. census, READ ONLY
PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' \
  psql "$DSN" -X -f docs/db_layer/census_2026_10.sql > /tmp/census_mini.txt 2>&1
grep -A3 'Q0 session' /tmp/census_mini.txt   # must read on | on | 20s
# 3. migration state, READ ONLY (dry run issues SELECTs only)
INVENTORY_DATABASE_URL="$DSN" PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' \
  .venv/bin/python -m backend.scripts.migrate --dry-run
unset DSN
```

Add a column to the ledger table from `/tmp/census_mini.txt`.

## What the census SQL review changed (partial file from the earlier agent)

The earlier agent left `census_2026_10.sql` unreviewed when the Mac slept. This review:
- regenerated the Q2/Q15 VALUES lists from `migrations/` at `afa063a22` with a static parser. Q2 checksums, tables, indexes, sequences, PK, UNIQUE and FK names all matched.
- **added the 14 CHECK constraints** the chain names, which were missing (car_comments_*, dealer_comments_*, comment_attachments_*, dealer_ratings_*, vehicles_year_check).
- added a live-only branch for explicitly named constraints. Default names (`<table>_..._pkey|key|fkey|check|excl`) are skipped, including names truncated at 63 bytes. Without that, V016's `brochure_local_extraction_fai_source_pdf_sha256_page_number_key` showed as a false positive.
- added Q17: extensions, non-extension functions, user triggers, views and materialized views, user types, and non-system schemas, each compared to the chain.
- added a list of every database on the server (names and sizes) to Q1.
- widened the `*_embeddings` and `vector`-column searches from `public` to every non-system schema (Q3).
- added exact row counts to Q3 and Q6, using `query_to_xml`, which is a SELECT and runs under READ ONLY.

Every other query was checked against the column definitions in `migrations/`. It also matches the logic it mirrors, the dedupe key at `feed_package_registry_from_vision.py:499-503` and `unstale_host_blocked_recipes.candidates` (:32-58), and needed no change.

## Reproducing

```sh
# local (DSN from .env has no credentials)
PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' \
  psql 'postgresql:///cars?host=/tmp' -X -f docs/db_layer/census_2026_10.sql
# prod (authorized agent or owner); redact the host on the way out
railway run -s Postgres -- sh -c 'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=20000" \
  psql "$DATABASE_PUBLIC_URL" -X -f docs/db_layer/census_2026_10.sql 2>&1' \
  | sed -E -e 's/[A-Za-z0-9.-]+\.proxy\.rlwy\.net/<prod-host>/g' -e 's#postgres(ql)?://[^ ]+#<dsn-redacted>#g'
# migrate dry run against prod, READ ONLY (run from a checkout; INVENTORY_DATABASE_URL overrides .env)
railway run -s Postgres -- sh -c 'INVENTORY_DATABASE_URL="$DATABASE_PUBLIC_URL" \
  PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=20000" \
  .venv/bin/python -m backend.scripts.migrate --dry-run 2>&1' | sed -E 's/[A-Za-z0-9.-]+\.proxy\.rlwy\.net/<prod-host>/g'
# 10-04 dump numbers: stream one table at a time, never restore
pg_restore -a -t dealer_recipes -f - workspace/backups/prod_20261004_pre150.dump | <count>
```

For this run, the `migrate --dry-run` invocations went through a small wrapper. It first logged the connection's redacted host, port and db and read the read-only settings, then called `migrate.main(['--dry-run'])` unchanged. `migrate.py` itself does not log its target. Logging the target before `--apply` is P4.2 item 4.
