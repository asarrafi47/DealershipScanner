# N8 Attribution fix wave - RESUME

Branch phase/N8 (head 6ad8d71c9), merged into integ/draft at 0b3058214, no conflicts. Draft only; no push, no scan, no DB write.

| Unit | Head | Status |
|---|---|---|
| N8.1, N8.2 | e5b39584d | DONE (fixtures) |
| N8.3 | 73971577b | DONE (fixtures) |
| N8.4, N8.9a | 48d9f3744 | DONE (fixtures; tool never run with --apply) |
| N8.5 | f61d64bd3 | PARTIAL (identity-check output and HOST_ALIASES data missing) |
| N8.6a | 2b9d5a62d | DONE (V030; PG apply not run) |
| N8.6b | a16ebd032 | PARTIAL (SQLite met; PG and third_rooftop strike deferred) |
| N8.7, N8.13 | b578a3d1a | DONE |
| N8.8 | 73971577b | PARTIAL (label chain, _feed_ccid, facet union; payload-dependent targets open) |
| N8.11 | 6326e2048 | DONE (stamping variant) |
| N8.14 | 6ad8d71c9 | DONE (PG case not run) |
| N8.9b, N8.10, N8.12 | - | SKIPPED, data_ops needing owner approval and a MAIN scan |

## Open after merge
- test_pg_harness.py::test_every_module_level_ready_flag_is_reset_or_reviewed: add backend.attribution.place._host_index_cache (N8.5).
- PG tier (test_upsert_pg, test_inventory_write_pg, upsert golden on PG, V030 apply) blocked: harness refuses without V028/V029 from N9. Re-run after N9 merges; the PG half of the upsert golden may need regeneration.
- Goldens re-recorded additively (dealer_run, upsert); the placeholder count assertion moved 50 to 59.
- Spec deviation in N8.5: weak tier needs roster street or town, not street alone (corpus cases Welborn Rome, Shottenkirk Canton).
- OneAudi left unstamped on purpose (stockCar.dealer is the publisher), contrary to plan text.
- Not done, follow-up units: M2 (run.provider vs result provider, dealer_run_steps/attribution.py:34), M5 (stale recipe on HTTP 404), M6 (learn own-store id at synthesis), M7 (real Motive VDP URL). learn_own_site_id and learn_own_location_id are never called automatically.
- N8.9a writes via recipe_store.db_save_recipes only; run import_recipes_to_db or reconcile_recipe_store after an apply. No --restore.

## Deferred checks (all N8.10, MAIN scan)
- mtnviewnissan CH rows >=300; bentleycadillac photo accounts; Landers don: ids; 80% of N1.2 expected own rows; bmwofmonrovia non-sonic rows = 0.
- 5starford VDP city vs North_Richland_Hills; Jazel paint words in interior_color; chapmanfordaz lots CAE/CAY/CAV; Lexus split within 5%; trim >=95% at 4 dealers; DEP pairs down 75%; 22 AutoSavvy dealer_ids on 22 registry rows; mbbeverlyhills rows on mbontario.com = 0, mbontario active >=1,200, fletcherjones Henderson = 0.
- Terry Greensboro 13 cars, 2309549 gate line, FP01 dealers (tedrussellford, kearnymesasubaru, hendricksubaruhoover, autosavvy, infinitiofcharlotte).

## Owner asks
- Approve the repair-set diff (N8.9a dry run) and the dealer-discovery fetches (N8.9b).
- Approve the MAIN scan and `git switch` for N8.10; approve the N8.12 local apply.
- Decide: manual-move detection (any car_move_log row not prefixed scanner_guard:), Jazel colour swap (unconditional), shared-host apply gate lift via --allow-gated, harness edit for _host_index_cache.
- Confirm Chapman store map (CAE/CAW) against the N1.2 map; fill HOST_ALIASES from identity-check output.
