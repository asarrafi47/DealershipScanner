# Monolith audit — enrichment / oem / parsers / vision / intelligence (186 files)
Read-only. Date 2026-10-01. Metrics: scratchpad/metrics.csv. fan_in = metrics fan_in unless "grep callers" stated.

## Working notes (appended as reviewed)

### knowledge_engine_specs.py (1488 lines, fan_in 9; 15 non-test importers of merge_verified_specs, 23 incl tests)
- merge_verified_specs L598-1087 (~489 lines). Jobs: (a) row clean + local helpers L633-662; (b) source fetch: decode_trim_logic L670, epa_master by id L675-698, vPIC L702, EPA aggregate + fuzzy engine-family rejection L719-744, extended specs L745-762, model_specs dictionary L763; (c) per-field precedence cascades: cylinders L769-803, drivetrain L804-824, gears/transmission L825-874, BEV/fuel-cell detection L875-935, display labels L936-966, body style incl Jeep Wrangler special-case L967-985, fuel economy L986-1000, engine string L1001, sources list L1003-1016, transmission normalize L1017-1034; (d) 40-key return dict L1036-1087 mixing storage values, display strings, provenance flags, generation info.
- Lazy-imports 10 private names from knowledge_engine (circular split: header says "mechanically split, no logic changed"). Private-name coupling (_is_na_spec, _drivetrain_ui_label, _transmission_has_gear_detail).
- Also: _sticker_specs_from_packages L15-201 (~186 lines, package-text spec mining), curated_zero_to_60_sec + _CURATED_ZERO_TO_60 table, plausibility guards L214-423, prepare_car_detail_context L1089+ (page-context assembly = presentation layer in enrichment).
- Why it hurts: each field's precedence policy (vPIC > sticker > dealer > catalog, per CLAUDE.md) is an inline if-cascade inside one function; can't unit-test "cylinders policy" alone; the return dict is consumed by serializer, incomplete-listings index, invariants, repair scripts -> any key change has 15-file blast radius.
- Split: enrichment/specs/sources.py (gather SpecSources dataclass: regex, epa_trim, epa_agg, vpic, dict_specs, sticker, extended) ; enrichment/specs/precedence/{cylinders,drivetrain,transmission,fuel,body}.py each `resolve(sources, car) -> FieldVerdict(value, display, verified, source)`; enrichment/specs/plausibility.py (L214-423 + curated 0-60); enrichment/specs/sticker_packages.py (L15-201); car-detail context -> backend/utils/car_serialize or web layer. merge_verified_specs becomes a ~40-line composer that keeps the same return dict (compat).
- Priority P1.

### knowledge_engine.py (1943 lines, fan_in 54)
- decode_trim_logic L164-435 (~270): one function of per-make regex blocks: generic xDrive/4MATIC L194-224, Dodge Daytona L225, BMW/MINI L234-297 (+ 6 _bmw_* helpers L56-163), Mazda L298, Mercedes L318, Jeep L330, Ford body L337 + Ford transmission year windows L388, Ram L421. Classic per-make if chain.
- _model_epa_fallbacks L1287-1503 (~217 lines) per-make model alias chain (Chevy/Ford/BMW/Mercedes ...), plus _bmw_epa_model_like_pattern/_ford_epa_pickup_like_pattern L1218-1278.
- Mixed jobs in one module: trim regex decoding; EPA catalog lookups (by trim / by id / aggregate / extended / engine_specs / dictionary CSV fallback L980-1071 / AI model specs merge L924-979); display formatting (format_transmission_display, build_master_engine_string, format_fuel_economy_display, _drivetrain_ui_label L1072-1215); vPIC cache reader + normalizer L1504-1686; NA detection.
- Hidden global state: 7 lru_caches (8192-16384 entries) + _VPIC_MEMO L1529 = UNBOUNDED module dict keyed by vin, primed in bulk by prime_vpic_cache -> grows with fleet inside the web worker (memory is a known Railway constraint, GUNICORN_WORKERS=1). Five clear_*_cache() functions callers must remember.
- Copy-paste: vPIC drivetrain normalization exists in >=4 places: knowledge_engine._VPIC_DRIVE_MAP/_normalize_vpic_response L1504-1686, enrichment/nhtsa_vpic._normalize_drivetrain L71 + flat_vpic_result_to_car_patch, utils/vpic_specs.py, intelligence/ai/agent._norm_drive_compare L245 (also scripts/scan_lab_report._drive_bucket). Given "vPIC values win" policy, divergent normalizers = divergent verdicts (4x2 vs RWD rule must hold in all).
- Bottom-of-file re-export from knowledge_engine_specs L1938 (E402) keeps the old import surface -> circular pair.
- Split: enrichment/trim_decode/{__init__ (dispatcher), generic.py, bmw.py, mercedes.py, ford.py, ram.py, mazda.py, jeep.py} with a registry dict make->decoder; enrichment/epa_lookup.py (all lookup_epa_* + caches + clear_all_caches()); enrichment/epa_model_aliases.py (_model_epa_fallbacks as data table per make); enrichment/vpic_cache.py (bounded LRU replacing _VPIC_MEMO; single normalizer shared with nhtsa_vpic); enrichment/spec_display.py (formatters). Keep knowledge_engine as a thin re-export facade for its 54 importers.
- Priority P1 (decode chain + vPIC duplication + unbounded memo).

### trim_ladder/ package (already split from a 3900-line module; __init__ 341 lines = 121 re-export lines, fan_in 56)
- build.py::_build_ladder_result L50-536 (~486, whole file). Jobs: rung-name gate L59-79; vehicle claim/exact rung match tiers L80-118; dictionary adds lookup L119; brochure engine bullets L140; per-rung loop L148-456 (~310 lines: curated adds sanitize L163, brochure key bind L168, brochure-overlay vs CSV fallback L190-273, merge display bullets L274, price-add stripping L281/L346, prose fallback L304, engine claim bullet L350-405, provenance gate L406-420, current/passed/ahead flags L421-456); citation re-alignment L457-466; source/order provenance tallies L467-503; result dict L504.
- Why: the per-rung loop is a pipeline of 8 gates written inline; every new gate lands here; comments (good ones) are the only structure. Tests can only exercise it through whole-ladder fixtures.
- Split: trim_ladder/rung_claim.py (L80-118: `claim_rung(steps, trim) -> index|-1`), trim_ladder/rung_bullets.py (`bullets_for_rung(step, ctx) -> RungBullets` holding L148-420 as named gate steps: curated -> brochure -> csv fallback -> strip prices -> engine claim -> provenance gate), trim_ladder/provenance.py (L457-503 tallies). _build_ladder_result becomes orchestration (<80 lines).
- Blast radius: low — only selection.py calls it (grep), fan_in 0 externally; output dict consumed by templates via resolve_trim_ladder.
- selection.py::_resolve_trim_ladder_inner L383-611 (~229): ladder-def pick, year gate, neighbour-year overlay gating L417-433, document-order evidence L434-516, 4 separate `<2 steps` fail-outs, plausibility + display gates L555-565. Medium god-function; split into `choose_steps()` and `apply_display_gates()`. P2.
- Facade __init__ re-exports underscore-private names that tests patch; its docstring warns patching facade doesn't reach callers — a trap. P3 (document/remove private re-exports).
- Trim-name normalization: canonical_trim_name lives in trim_ladder_knowledge/naming.py (L534, 836-line module, preserve_trim_label 186 lines); brochure_extract._canonical_trim_name delegates (fine). But separate key functions exist: trim_ladder/evidence._rung_evidence_key, trim_ladder/sticker_diffs._trim_key (_norm_token), trim_ladder/document_order._step_trim_keys, utils/market_price._trim_key, utils/spec_field_normalize.normalize_trim_text. 5 "trim identity" keys with different normalizations -> a trim can match in one gate and miss in another. Consolidate into one enrichment/trim_identity.py exposing `trim_key(label, make, model, strict=bool)`. P2.
- Priority overall: _build_ladder_result P2 (contained, fan_in 0, but 486 lines).

### brochure_extract.py (2403 lines, fan_in 52, 25 non-test importers)
- Six unrelated jobs in one module:
  1. PDF archive storage L29-357 (archive dir env, sha256, archive_source_pdf, meta writer, .gitignore text)
  2. PDF text/layout extraction L358-761 (extract_brochure_text_pdf L423-537 ~113, layout line clustering, payload/serialize/persist, rejection reasons)
  3. Equipment-matrix parsing L762-1133 (trim column detection, matrix tables, feature splitting, trim sections, adds-over-lower)
  4. Overlay/extract persistence + marketing CSV writer L1134-1346
  5. Provenance POLICY registry L1347-2207: LADDER_BULLET_STORES, LADDER_RUNG_STORES, LADDER_ORDER_STORES, env switches TRIM_RUNGS_REQUIRE_PROVENANCE / TRIM_ADDS_REQUIRE_PROVENANCE, citation keys, PUA readability, admissible_overlay_adds, apply_overlay_provenance_gate. This is what most of the 25 importers want (admissible_overlay_adds x8, verified_overlay_trim_names x4, load_brochure_trim_overlay x4, overlay_rung_order x3, ladder_bullet_store_admissible x3).
  6. Overlay loading + rung-order backfill L2208-2398 (lru_cache 512 on _rung_order_from_brochure_text).
- Why it hurts: consumers that only need the provenance gate (trim_ladder, web serialization) import a module that pulls pdfplumber-era PDF code + archive I/O; policy constants for three different gates are buried at L1347-1900 of a PDF parser. Circular lazy imports both ways with trim_ladder (_ladder_trim_order imports trim_ladder._pick_ladder_def; trim_ladder imports brochure_extract stores).
- DUPLICATE WITH DIVERGENT SEMANTICS: brochure_extract._marker_is_standard L762 treats "p"/"P" as STANDARD (_STANDARD_MARKERS={"p","s"} L135) while brochure_trim_candidates._marker_is_standard L782 maps "P" -> "neg" (_GRID_LETTER_MARKS L250). Same OEM glyph means opposite things depending on which parser read the book. Two independent brochure equipment-grid parsers (brochure_extract matrix L762-1133 vs brochure_trim_candidates grid/cell/coord/column parsers L766-1830).
- Split: enrichment/brochure/archive.py (job 1), brochure/pdf_text.py (job 2), brochure/matrix_parse.py (job 3, or delete if trim_candidates parsers superseded it — verify callers of extract_brochure_pdf), brochure/overlay_store.py (jobs 4+6), enrichment/provenance/ladder_stores.py (job 5: all *_STORES, admissible_*, env switches, citation keys). Keep brochure_extract as re-export facade one release.
- Priority P1 (high fan-in hub + divergent duplicate marker semantics).

### brochure_trim_candidates.py (2304 lines, fan_in 39)
- extract_trim_walk L2004-2247 (~243 per metrics 230): page loop dispatching to 4 page-format extractors (adds-to page L666, marker grid L996-1155 ~160, cell grid L1306-1444 ~140, column block L1503-1642 ~140, coord grid L1737-1829), then cross-grid merge L2135-2200, ranking, order_rungs_with_basis L1882-2003 (~120).
- Also: candidate draft/persist L34-249 (separate job: file I/O for derived/brochure_trim_candidates), failure classification L2248+.
- 6 module lru_caches (16384 each) keyed by (token, make, model) over _known_trim_vocabulary -> vocab changes in-process are invisible until restart; no clear function.
- Split: brochure/walk/{adds_to.py, marker_grid.py, cell_grid.py, column_block.py, coord_grid.py} each `extract(page, ctx) -> PageExtract`; brochure/walk/order.py (order_rungs_with_basis + _rank_bullets); brochure/trim_vocab.py (cached validators + cache_clear); brochure/candidates_store.py (L34-249). extract_trim_walk becomes dispatcher over a list of page extractors.
- Blast radius: 39 fan_in but mainly via extract_trim_walk / filter_spurious_brochure_trims / normalize_quoted_line — keep those names re-exported.
- Priority P2 (big but internally coherent; P1 element is the shared marker semantics above).

### generated_spec_sheet.py (1527 lines, fan_in 7)
- Jobs: formatting helpers L36-200; EV plug-in gate L198-332; attributable extended-spec SQL + bounds + 3 lru_caches L333-603 (DB access layer); cylinder consistency L604-655; catalog equipment / packages L656-803; sticker-photo findings with TTL-in-lru_cache trick (L840-911, cache key includes time bucket) L804-1024; package registry offerings L1025-1123; build_generated_spec_sheet L1124-1382 (~258, metrics says 238); unified options list L1383-1527 (a second, separate product: options tab).
- Duplication / divergence: _engine_display L143 picks dealer engine_description > epa_engine_description > master_engine_string > displacement+cyl and never reads vs["vpic_engine_l"]; utils/car_serialize/engine.build_engine_display ranks vPIC displacement above catalog (comment in knowledge_engine_specs L1053). Same car, two engine lines. _epa_mpg_display duplicates knowledge_engine.format_fuel_economy_display. A 5th SQL reader of epa_extended_specs (besides knowledge_engine lookup_epa_extended_specs*).
- Module state: 4 lru_caches (_extended_row_by_master_id, _extended_row_by_ymmt, _fields_shared_across_trims, _sticker_photo_summary_cached) + 2 clear_* functions.
- Split: enrichment/spec_sheet/build.py (builder only), spec_sheet/extended_attribution.py (L333-603 incl caches; or fold into the epa lookup module proposed for knowledge_engine), spec_sheet/equipment.py (L656-1123 catalog/sticker-photo/registry merges), enrichment/options_list.py (build_unified_options_list L1383+). Route engine line through car_serialize.engine.build_engine_display.
- Blast radius: fan_in 7 (car page serializer, options tab, tests). P2.

### enrichment/service.py (1415 lines, fan_in 5: scanner/post_scan/pipeline.py, dev/routes.py, window_sticker_service.py (private names!), 2 tests)
- Jobs: column DDL (ensure_enrichment_columns L93) + haiku_spec_cache table DDL/IO L104-179; Haiku spec Q&A via raw urllib POST to api.anthropic.com L180-258; gallery URL picking L259-331; image JPEG/b64 encode L337; vision-response JSON parsing/repair/refusal L356-480; candidate selection SQL L481-594; image prefetch thread pool class L602-667; _vision_analyze_car L683-856 (~172: prompt text L697-756, payload/headers, own 5x retry loop w/ 10s sleeps L794-828, parse); observation merge L857-951; InventoryEnricher class (batch write buffer, catalog apply, vision apply, run_all thread pool) L956-1368; CLI main L1369.
- Why: three different LLM transport paths in the area — raw urllib here (two places), anthropic SDK direct in window_sticker_service L705/L946 and agent.py L461 and oem/bmw.py L510-532, and the rate-limited wrapper vision/claude_rate_limit.anthropic_messages_create used by claude_vision.py. Retry/backoff, model ids (hard-coded "claude-haiku-4-5-20251001" in window_sticker_service L707), refusal accounting, and JSON repair diverge per path. Memory note "vision refusal counted ok" / "enrichment refusal half remaining" lives exactly in this divergence.
- window_sticker_service imports service._vision_analyze_car/_merge_vision_observations/_all_gallery_urls_ordered (private cross-module reuse) -> a sticker fetch pulls in the whole Nitro enricher.
- Split: backend/vision/llm_transport.py (ONE Anthropic call: SDK + 429 backoff + model registry + refusal/truncation detection + JSON repair; move L337-480 + claude_rate_limit here), vision/gallery_urls.py (L259-331, also used by window_sticker), vision/car_vision.py (_vision_analyze_car prompt+parse, observation merge), enrichment/haiku_spec_cache.py (L104-258), enrichment/enricher.py (InventoryEnricher + prefetch pool + CLI).
- Priority P1 (LLM transport duplication across 6+ call sites; refusal/retry policy divergence is a data-accuracy issue).

### window_sticker_service.py (1213 lines, fan_in 22)
- ensure_window_sticker_for_car L1036-1213 (~177): load car, resolve local path/txt upgrade L1061-1093, OEM fetch gate L1094-1132, storage key + write L1133-1149, text parse + Claude text fallback L1150-1180, package merge L1181, MSRP fill L1188-1195, provenance JSON L1196, vision fallback L1209+. Also storage paths L22-180, PNG preview rendering L181-233, package predicates L234-347, parsed->car field mapping L348-497, listing sticker URL discovery + fetch L629-843, two separate Claude calls (image L679, text L923) with hardcoded model.
- Split: stickers/storage.py (paths, keys, preview PNG), stickers/oem_fetch.py, stickers/listing_fetch.py (L629-843), stickers/parse_merge.py (L348-628, L857-922), stickers/llm_fallbacks.py (via shared transport). Orchestrator stays in window_sticker_service.
- Memory: "Leave OEM sticker fetch alone" — split must be behavior-preserving; downstream-only edits. Priority P2.

### html_spec_sources.py (1279 lines, fan_in 3)
- Cohesive "HTML spec page as citable source": table parser L154-527, citations+verification L528-776, storage/transcripts/overlay L777-1055, then a Honda newsroom crawler L1056-1230 (site-specific discovery — different job). build_citations is only 74 lines; no god function.
- Duplicate key fn with DIFFERENT normalization: html_spec_sources.catalog_key L777 = f"{year}|{make.lower()}|{normalized_model_token(make, model)}" vs dictionary_catalog.catalog_key L67 = f"{y}|{_norm_token(canonical_make(make))}|{_norm_token(model)}" (used by brochure_trim_candidates etc.). Same YMM can file under two keys (e.g. "Mercedes-Benz" vs canonical make) -> HTML overlay not found by brochure-side loaders. Also a 3rd standard/optional mark classifier (classify_mark L154) beside the two brochure ones.
- Split: move Honda crawler to enrichment/brochure_sources/honda_newsroom.py (brochure_sources already hosts per-host discovery); use dictionary_catalog.catalog_key. P3 (P2 for the key mismatch if confirmed by a data check).

### parsers/__init__.py (1239 lines in a package __init__, fan_in 149 — highest in area)
- Two unrelated jobs: (1) provider registry L17-88 + parse()/parse_kept()/_parse_rows L1054-1239; (2) the whole ROOFTOP ATTRIBUTION engine L89-1053 (~960 lines): name/token/brand normalization incl Stellantis initials L181-241, label/address heuristics L255-309, rooftop identity extraction L313-441, _Rooftop class L442, _pick_target L490-694 (~203: host tier, name tiers, alias tier L565, custom_location tier L579, dealer_id tier L598, slug tiers L607, street/locality tiers), department merge L753-801, _resolve_rooftop_attribution_inner L802-987 (~186).
- Dual implementation: _resolve_via_scorer L997-1053 routes the same decision through backend.scanner.rooftop_match.score_rows behind scorer_enabled() — two rooftop matchers for one verdict, kept in sync by hand ("same verdicts, same log lines"). Also rooftop_of() exists in team_velocity L250, typesense L304, carscommerce L279, parsers/__init__._rooftop_of L368 and scripts/attribute_feed_rooftops.py L89 (copy).
- Hidden state: _warned_unknown_providers module set L90; lru_cache(256) _cached_roster_place_items(dealer_url) L1193 -> roster address edits invisible to a long-running scanner/fleet process.
- Why it hurts: any `import backend.parsers.X` executes this 1239-line __init__ and imports all 15 parsers; fan_in 149 means every scanner test pays it. Attribution logic (a policy with memory notes "refuse to WRITE without evidence") hidden in an __init__ is hard to find and review.
- Split: parsers/registry.py (PARSERS, parse, parse_kept, _parse_rows, autowall/cosmos shims); backend/attribution/rooftop/{normalize.py (L181-309), identity.py (L313-441 + per-platform rooftop_of adapters), tiers.py (_pick_target as ordered list of tier functions, each `(rooftops, target) -> match|None`), resolve.py (inner resolver + department merge)}; pick ONE of _pick_target vs rooftop_match.score_rows and delete the other after a shadow-compare run. __init__.py keeps `from .registry import parse, parse_kept, PARSERS` + `resolve_rooftop_attribution` re-export.
- Priority P1.

### backend/oem/** — BMW intake island (bmw_locator_discovery 1891, bmw.py 804, bmw_pipeline 883, intake/cli 328, sqlite_store 413, normalize 211, bmw_keyword_sets/debug/parse_trace/discovery/http, crawl4ai_inventory 467, crawl4ai_discovery 426)
- bmw_locator_discovery.py: run_interaction_locator L186-505 (~318: Playwright session, zip input discovery, AI selector plan, clicks, result snapshots, network capture classification all inline) + 40 Playwright DOM helpers L506-1643 + run_deep_locator_discovery L1644. fan_in 0. Its ONLY caller is bmw.py L505 `from scrapers.oem.bmw_locator_discovery import ...` — package `scrapers` does not exist anywhere (no backend/scrapers, no top-level scrapers) -> the 1891 lines are unreachable; the import raises inside ingest_bmw_usa_playwright.
- bmw.py ingest_bmw_usa_playwright L475-708 (~232): Playwright + direct anthropic.Anthropic call L510-532 for selector planning + network JSON heuristics. Uses bare `from scraping.constants import USER_AGENT` (works only if backend/ is on sys.path — enrichment/service.py L42-43 does a sys.path insert for exactly this). Same pattern: oem/intake/bmw_pipeline.py L720-721 (`scraping.*`), oem/vehicle_reference/sources/epa_bmw_ingest.py L121 (`oem.vehicle_reference...`). Import-path dependent code = works from one cwd only.
- Reachability: whole backend/oem tree has ZERO tests and no importer outside backend/oem except backend/vector/pgvector_service.py (oem.intake.paths.BMW_DB_PATH + sqlite_store connect/init_schema), itself only lazily used by intelligence/ai/agent.py L1006. Dockerfile.web comment confirms crawl4ai_discovery "runs nowhere today" (scanner.js spawn path broken). This is a headless-browser path, contrary to the project goal "network-traffic scanning, no headless browsers".
- Recommendation (not a split): quarantine/delete. Keep only oem/intake/paths.py + sqlite_store.py (or move them to backend/vector/bmw_store.py) for pgvector_service; delete or move to archive/ the BMW Playwright scraper, locator discovery, crawl4ai_*, bmw_pipeline, intake/cli. Per memory "audit deletions before editing": grep confirms no consumer; check scanner.js:2363/2443 spawn before deleting crawl4ai_discovery.
- oem/vehicle_reference/** (cli 321, ingestion/bundle, structured, manifest, csv_export/flat_export, quality/*, sources/epa_*, core/*, utils/mpg) + 9 three-line legacy shims (db.py, export_csv.py, ingest*.py, mpg_format.py, paths.py, qa_report.py, validate.py) — self-contained CLI; no importer outside its own tree. Shims are dead compat layer. P3: confirm with owner, then delete shims (or the whole subtree if the reference DB is unused; epa_master is the live catalog).
- Priority: P2 (dead-code removal, ~6,000 lines; zero blast radius per grep). Not worth splitting.

### intelligence/ai/agent.py (1429 lines, fan_in 6: main.py L31, routes/ai_chat_bp.py (imports PRIVATE _is_prompt_probe, _PROMPT_REFUSAL, _guard_prompt_disclosure, _plain_chat_reply), tests)
- Jobs: prompt-safety (untrusted-data wrapping, probe detection, leak guard) L20-244; verify_car_data L267-395 (~128) — DEAD: zero callers anywhere (only docs/SCANNER_NODE.md mentions it), and it is a 2nd spec-verification path (decode_trim_logic + lookup_epa_aggregate, no vPIC) that would disagree with merge_verified_specs; web-research/price triggers L396-455; LLM transport _claude_reply L456 (direct SDK, hard-coded "claude-haiku-4-5-20251001") + _generate_reply provider chain L479 — duplicates utils/llm_client.py which already has its own anthropic call (L123-132); evidence/context line builders L520-863; run_car_page_chat L864-1122 (~257: probe short-circuit, context assembly L897-989, research branch pgvector + Playwright L990-1051, prompt assembly L1052-1097, provider call L1100+); compare chat L1123-1429 (separate feature: _compare_math, listing blocks, fence stripping, run_compare_chat).
- _norm_drive_compare L245 = yet another drivetrain normalizer.
- Why: chat feature, safety guard and transport in one file; ai_chat_bp reaching into private names means the guard can't move without breaking the route.
- Split: intelligence/ai/prompt_guard.py (L20-244 public API: is_prompt_probe, PROMPT_REFUSAL, guard_prompt_disclosure); intelligence/ai/car_context.py (L520-863 + context-assembly part of run_car_page_chat -> `build_car_chat_context(car, msg) -> ChatContext`); intelligence/ai/research.py (research branch); intelligence/ai/compare_chat.py (L1123-1429); transport -> utils/llm_client (single place). Delete verify_car_data.
- Priority P2 (fan_in small; the transport duplication is counted under the P1 LLM-transport finding).

### trim_ladder_knowledge/ (naming 836, bullets 872, tables 755 data-only, categories 358, drivetrain 169, adds 131, validation 129, year_windows 87, fallback 67, __init__ 318 re-exports fan_in 90)
- naming.py::preserve_trim_label L262-449 (~186): per-make if-chain (audi L269, mercedes L286/L361, bmw L299-352, mini L353, toyota L366/L428, jeep L395, volvo L412, gmc L416, tesla L420, porsche L424, landrover L432, cadillac L436-442). Plus per-make luxury sort keys (_bmw_luxury_sort_key L573, _mercedes_* L617-664). Same make-dispatch smell as knowledge_engine.decode_trim_logic.
- TWO EPA model-alias systems: trim_ladder_knowledge.naming._EPA_MODEL_SEARCH_NAMES/epa_model_search_name (L149-223; used by dictionary_catalog, epa_master_store, trim_ladder/citations, csv_ladders) vs knowledge_engine._model_epa_fallbacks (L1287-1503; used by knowledge_engine L662/L1831 and catalog/resolver.py L108). Listing model -> EPA model mapping decided twice; the catalog resolver and the dictionary loaders can land on different EPA rows for the same car.
- Split: enrichment/make_rules/<make>.py modules each exposing optional hooks {decode_trim, preserve_label, luxury_sort_key, epa_model_aliases}; dispatch tables in naming.py and knowledge_engine read from a registry. Merge the two EPA alias tables into one enrichment/epa_model_aliases.py returning ordered candidates.
- Priority P2 (alias-table merge P1-adjacent: grouped into the knowledge_engine P1).
- bullets.py (872, 25 fns, longest 56) — cohesive hygiene rules; fine. tables.py — pure data; fine. categories/drivetrain/adds/validation/year_windows/fallback — small, fine.

### Batch: trim_spec_extractor / dealer_ratings / dictionary_catalog / brochure_promote / package_registry
- trim_spec_extractor.py (959, fan_in 16, longest 79): two jobs — bullet/prose hygiene predicates L135-536 (is_junk_spec_text, is_narrative_prose, is_displayable_trim_bullet... overlapping trim_ladder_knowledge/bullets.py's "trim-add bullet hygiene") and CSV spec extraction L537-959. lru_cache(512) extract_trim_specs. Split hygiene into trim_ladder_knowledge/bullets.py (one bullet-hygiene module). P3.
- `_norm_token` copy-pasted in 7 enrichment modules (epa_master_store L40, trim_diff_engine L32, trim_ladder/_common L73, dictionary_catalog L38, trim_spec_extractor L79, trim_spec_sheets L18, dictionary_options_store L13) — identical regex today, but catalog_key / filename keys depend on all agreeing. Move to backend/utils/text_keys.py. P3 (cheap).
- dictionary_catalog.py (712, fan_in 39): manifest build + SQLite catalog DDL + lookup + legacy glob fallback + CSV column normalizer (a write-side migration tool, L661). 7 lru_caches incl lru_cache(maxsize=1) "is DB available" — a missing catalog DB at boot is cached as False forever; invalidate_catalog_cache() exists. Split: dictionary_catalog/build.py (L72-356 manifest+DDL+rebuild) vs lookup.py (L357-660); move normalize_csv_columns to scripts. P3.
- brochure_promote.py (636, fan_in 3, only scripts/promote_trim_candidates.py + scripts/analyze_brochure_with_llm.py): a THIRD brochure equipment parser (_parse_models_column_standards L157, _parse_single_trim_pages L212, _parse_inline_trim_bullet_rows L279, _parse_dot_feature_matrix L324, _parse_fca_standard_blocks L384 — FCA-specific) next to brochure_extract matrix + brochure_trim_candidates grid parsers. Fold into the brochure/walk page-extractor registry proposed above. P2 (grouped with brochure finding).
- dealer_ratings.py (777, fan_in 18; sync_dealer_ratings 120): Google Places fetch + verify + sync. Docstring justifies separation from discovery/google_place_rating.py. Cohesive; haversine/host helpers could live in utils. fine.
- package_registry.py (564; record_package_observations 128): cohesive (observe/lookup/price), 2 lru_caches + clear_lookup_cache. fine.

### parsers/ (per-platform modules)
- Copy-paste helpers: `_opt_str` defined in 11 parser modules (chapman, dealer_eprocess, dealermasters, dealer_dot_com, dealer_on, jazel, team_velocity, motive_ridemotive, overfuel, sister_tv, typesense); `_opt_label_str` in dealer_dot_com + team_velocity; `_norm_tracking_key` + tracking-attr-by-needle scanners in dealer_dot_com L438-492 and dealer_on L35-131; typesense._engine_liters/_cylinders L129-152 re-implement utils/engine_consistency.liters_from_engine_text/cylinders_from_engine_text (the module knowledge_engine_specs uses to arbitrate cylinders) -> feed-time and read-time can parse "V6" differently. rooftop_of in 4 parsers + __init__ (see above). parsers/base.py (812, fan_in 36) already hosts norm_str/norm_label_str/find_tracking_attr — the copies predate or ignore it.
- Split: move _opt_str/_opt_label_str -> base.norm_opt_str; tracking-attr needle scan -> base.tracking_value_by_needles; engine text -> import from utils/engine_consistency. P2 (cheap, removes 15+ dupes; risk low with parser fixture tests).
- team_velocity.py (554, fan_in 9): parser L106-298 PLUS a VDP recovery job L299-552 (HTTP fetch_vdp_html, ThreadPoolExecutor, request pacer class, SELECT/UPDATE cars via db_conn L385-529). Mixed layers: a parser module doing network + DB writes. Split: parsers/team_velocity.py (parse/detect/rooftop_of) + backend/scanner/recovery/team_velocity_vdp.py (recover_dealer, pacer, completion SQL). P2.
- dealer_dot_com.py (862, fan_in 6): _map_vehicle L649-799 (~149) — field extraction already factored into ~25 _extract_* helpers; acceptable. base.py: image URL utilities L278-760 (~480 lines of gallery/image dedupe) are a separate job from price/VIN list finding; could be parsers/images.py. P3.
- dealer_on.py (487), html_cards.py (429), carscommerce.py (392), typesense.py (350): single-platform, functions <110 lines. fine apart from dupes above.

### vision/ (image_text 768, claude_vision 506, equipment_vision 493, url_heuristics 344, vlm_ollama 217, analyze_images 214, monroney_merge 148, interior_vision_merge 130, claude_rate_limit 98, __init__ 2)
- Image-fetch duplication with DIFFERENT SSRF guards: claude_vision._fetch_image_b64 L152 uses its own _unsafe_fetch_url_reason L123 (DNS-resolving private/link-local/metadata IP check); enrichment/service._fetch_image_b64_optimized L608 relies on utils/safe_listing_url.normalize_listing_image_url (hostname-pattern check, no DNS resolution) — a DNS name pointing at 169.254.169.254 passes one guard and not the other; utils/outbound_url.py (getaddrinfo-based) is a third guard. Consolidate on outbound_url and one `fetch_image_b64(url, referer, max_dim)` in vision/image_fetch.py. P2 (security-adjacent).
- _is_sticker_url byte-identical in enrichment/service.py L301 and vision/equipment_vision.py L299. Refusal phrases already shared (good).
- image_text.py: 3 OCR backends (Apple Vision subprocess, tesseract, ollama) + text classification + sticker row parsing + gallery orchestration. Cohesive pipeline, longest fn 56. fine (could split ocr_backends.py, P3).
- claude_vision.py: blur/phash/SSRF/fetch + batch classify + equipment batch — 2 features; _classify_batch 95 lines. P3.
- equipment_vision/url_heuristics/vlm_ollama/analyze_images/monroney_merge (merge_monroney_parsed_into_vehicle 126 lines, single job)/interior_vision_merge/claude_rate_limit: fine.

### intelligence/ (dealer_score 536, lease_matcher 500, inventory_signals 495, ev_range_estimates 446, tco_fuel_estimates 440, market_pricing 380, deal_score_cache 187, pipeline/*)
- BEV/electrified detection re-implemented 9+ times: knowledge_engine_specs._is_battery_electric L295 (+ inline is_bev cascade in merge_verified_specs L875-935), generated_spec_sheet._can_be_plugged_in L198, tco_fuel_estimates._is_pure_battery_electric L428, ev_range_estimates._is_electrified_car L127, enrichment/service._is_ev_label L71, spec_backfill._is_ev_row L50, spec_structured_backfill._is_ev_fuel_hint L89, utils/engine_consistency.is_bev_fuel L30, scanner/post_scan/window_sticker.dodge_charger_daytona_is_bev, scripts/scan_lab_report._is_ev. Policy says vPIC electrification outranks the feed; only merge_verified_specs reads vpic electrification. Consolidate in backend/utils/electrification.py: `electrification(car, vpic=None) -> 'ev'|'phev'|'hybrid'|'fcev'|None`. P1 (data-accuracy: e.g. TCO tank/efficiency and spec sheet range gate can disagree with the drivetrain block on the same page).
- epa_extended_specs read by 7 modules with separate SQL + caches: knowledge_engine (3 lru_caches), generated_spec_sheet (3), tco_fuel_estimates._quoted_specs_* (2, L180-233), ev_range_estimates, trim_ladder/inventory, brochure_extract, utils/vpic_specs, car_serialize. Memory note: extended specs are model-level-contaminated and need read-time suppression — suppression logic (_fields_shared_across_trims, _extended_family_suspicious, quoted_extended_spec) exists in 3 variants. One reader module enrichment/extended_specs.py with the suppression rule applied once. P1 (grouped with knowledge_engine split).
- market_price_stats band index loaded into memory twice with different tuple key order: dealer_score.load_band_index/_band_key L324-402 (make,model,trim,year,cond,mi) and deal_score_cache._load_bands/band_for_car L66-155 (year,make,model,trim,cond,mi). Both use market_pricing primitives so currently consistent; dealer_score could just use deal_score_cache.band_for_car. P3.
- pipeline/orchestrator.run_hybrid_batch L64-254 (~189): dealer-discovery adjudication batch; uses bare `from intelligence.pipeline.eval_report import` L247 (sys.path-dependent). Only caller backend/scraping/cli.py. P3 (split run loop vs report writing).
- dealer_score, lease_matcher (match_cars 89), inventory_signals, ev_range_estimates, market_pricing, deal_score_cache, pipeline/{adjudication, evidence_builder, eval_report, review_queue}, agents/adjudicator_agent, llm/client: cohesive, functions <100 lines. fine.

### vPIC drivetrain normalizers — concrete divergence (evidence for the knowledge_engine P1)
- Input vPIC DriveType "4x2/2-Wheel Drive" (stamped on FWD Camrys and RWD trucks alike):
  - knowledge_engine._normalize_vpic_response L1633-1642 + _VPIC_DRIVE_MAP L1504 -> None (deliberate, comment L1507)
  - catalog/resolver._drive_bucket L47 -> "" (deliberate; vpic_facts._drive_bucket delegates here — good)
  - enrichment/nhtsa_vpic._normalize_drivetrain L71-85 -> falls through and returns the RAW string "4x2/2-Wheel Drive"; flat_vpic_result_to_car_patch L286-289 then puts it in the car patch as drivetrain (written when the slot is fillable via spec_structured_backfill).
  - intelligence/ai/agent._norm_drive_compare L245, scripts/scan_lab_report._drive_bucket: further variants.
- Fix: one backend/utils/drivetrain.py `normalize_drivetrain(raw) -> 'FWD'|'RWD'|'AWD'|'4WD'|None` (resolver semantics) used by all five. Low-risk, high-value.

### spec write-path backfillers (multiple implementations of "fill cylinders/drivetrain/transmission")
- spec_backfill.py (446): tier A decode+EPA aggregate L104, tier A master catalog L166, tier B VDP L230, tier C web search L245, conservative merge L285, run_spec_backfill_for_car L346.
- spec_structured_backfill.py (395): inventory_repair then vPIC (own vpic cache get/put L179-208 — a second nhtsa_vpic_cache accessor besides knowledge_engine.lookup_vpic_from_cache), apply_structured_spec_backfill_for_car L220-370 (~149).
- vpic_facts.py (372): vin_overrides / heal_rows / post_scan_vpic — the 2026-09-23 policy implementation.
- persist_enrichment.py (144), utils/inventory_repair.py (outside area) also fill the same columns.
- Four writers with four precedence orders for the same columns; read-side merge_verified_specs has a fifth. Memory note "audit the write path" applies. Recommendation: one enrichment/spec_writer.py `plan_spec_updates(car, sources) -> {col: (value, provenance)}` that reuses the read-side precedence modules proposed under knowledge_engine_specs; the four entry points become source-gatherers. P1 (part of the spec-precedence consolidation), but sequence AFTER read-side split.
- catalog/resolver.py (426, score_candidate 131): self-described "the ONE resolver"; scoring is long but single-purpose. fine-ish (P3: table-drive the score weights).
- catalog_lookup.py (176), catalog/linker.py, catalog/generations.py, catalog/__init__: fine.

### EPA model-alias logic: actually FOUR implementations (extends naming.py finding)
- knowledge_engine._model_epa_fallbacks L1287 (+ _bmw_epa_model_like_pattern/_ford_epa_pickup_like_pattern), consumed also by catalog/resolver._model_variants L89-115
- trim_ladder_knowledge/naming._EPA_MODEL_SEARCH_NAMES / epa_model_search_name L149-223
- epa_master_store._bmw_epa_model L44 / _mini_epa_model L57 / _model_search_variants L61-111 (uses epa_model_search_name plus its own BMW/MINI rules)
- model_specs_dictionary.iter_model_lookup_variants L171
-> One module enrichment/epa_model_aliases.py `candidates(make, model) -> list[str]` (ordered); all four call it. Keeps the catalog resolver, dictionary loaders and model_specs on the same EPA row. Part of P1 #knowledge_engine.

### Misc enrichment
- trim_spec_sheets.py (161) and trim_diff_engine.py (339) both load backend/data trim_spec_sheets JSON with separate loaders/normalizers (_load_all_sheets lru_cache(1) vs load_spec_sheet) + separate _norm_token/_norm_make/_norm_model. P3 merge loaders.
- epa_master_store._row_to_csv_dict L112 and dictionary_options_store._row_to_csv_dict L17 — same shape adapter pattern (DB row -> legacy CSV dict) per table; fine but shared base would help. P3.
- vehicle_history_intelligence (316; build 114), listing_packages_service (190), dictionary_paths (126, fan_in 62, pure path fns), model_specs_dictionary (303): fine.
- brochure_sources/ (already split from ~3850-line module; 17 files): facade __init__ (478, 166 re-export lines, fan_in 16) with the same "patch the submodule, not the facade" trap as trim_ladder. sha256_file duplicated with brochure_extract.sha256_file. Submodules cohesive (hosts.py/tiers.py data). fine / P3.

---------------------------------------------------------------------------
# FINAL SUMMARY

## Ranked table
| # | Pri | Target | Size | Problem | fan_in / callers |
|---|-----|--------|------|---------|------------------|
| 1 | P1 | enrichment/knowledge_engine_specs.py::merge_verified_specs | 489-line fn (1488 file) | every field's precedence cascade inline; 40-key mixed return dict | 15 non-test importers (23 incl tests) |
| 2 | P1 | enrichment/knowledge_engine.py (decode_trim_logic 270, _model_epa_fallbacks 217) | 1943 | per-make if-chains; EPA lookups+formatting+vPIC in one module; unbounded _VPIC_MEMO; 7 lru_caches | 54 |
| 3 | P1 | Cross-cutting: drivetrain/electrification/EPA-alias/extended-spec duplication | 5 drive normalizers, 9+ BEV detectors, 4 EPA alias tables, 7 extended-spec readers | divergent verdicts (nhtsa_vpic stores raw "4x2/2-Wheel Drive") | enrichment+intelligence+catalog |
| 4 | P1 | parsers/__init__.py (rooftop attribution engine in __init__; _pick_target 203, inner resolver 186) | 1239 | registry + 960-line attribution policy in a package __init__; dual matcher with scanner/rooftop_match | 149 |
| 5 | P1 | brochure_extract.py | 2403 | 6 jobs (archive, PDF text, matrix parse, persistence, provenance policy, overlay load); marker "P" = standard here, = negative in brochure_trim_candidates | 52 (25 non-test) |
| 6 | P1 | LLM transport (enrichment/service.py raw urllib x2, window_sticker_service SDK x2, agent.py SDK, oem/bmw.py SDK, vision/claude_rate_limit wrapper, utils/llm_client) | 6+ call paths | divergent retry/refusal/truncation/model-id handling | — |
| 7 | P1 | Spec write-path backfillers (spec_backfill, spec_structured_backfill, vpic_facts, persist_enrichment, utils/inventory_repair) | ~1,400 | four writers + one reader, five precedence orders for the same columns | post-scan pipeline |
| 8 | P2 | enrichment/service.py (InventoryEnricher + vision + Haiku cache + prefetch + CLI) | 1415 | 8 jobs; private names imported by window_sticker_service | 5 |
| 9 | P2 | trim_ladder/build.py::_build_ladder_result | 486-line fn | 8 inline gates in per-rung loop | 0 external (selection.py) |
| 10 | P2 | brochure_trim_candidates.py (+ brochure_promote.py third parser) | 2304 + 636 | 5 page-format parsers + order + persistence; 3 brochure grid parsers total | 39 / 3 |
| 11 | P2 | generated_spec_sheet.py | 1527 | builder + SQL + caches + options list; engine line ignores vPIC unlike car_serialize | 7 |
| 12 | P2 | backend/oem/** dead island (bmw_locator_discovery 1891 w/ 318-line fn, bmw.py 804, bmw_pipeline 883, crawl4ai_* 893, intake/cli, vehicle_reference shims) | ~6,000 | broken import `scrapers.oem...`, sys.path-dependent bare imports, no tests, headless browser | 0 outside oem except pgvector_service->intake.paths/sqlite_store |
| 13 | P2 | intelligence/ai/agent.py::run_car_page_chat (257) | 1429 | chat+guard+transport+compare in one file; dead verify_car_data (2nd spec verifier) | 6 (route imports privates) |
| 14 | P2 | window_sticker_service.py::ensure_window_sticker_for_car (177) | 1213 | storage/fetch/parse/LLM/merge in one module | 22 |
| 15 | P2 | parsers/* copy-paste (_opt_str x11, tracking-attr scanners x2, engine text parse in typesense) + team_velocity parser doing DB/HTTP recovery | — | dupes; mixed layers | 9 (team_velocity) |
| 16 | P2 | trim_ladder_knowledge/naming.py::preserve_trim_label (186) | 836 | per-make if-chain (13 makes) | 90 (package) |
| 17 | P2 | Image fetch with 3 different SSRF guards (claude_vision, service, utils/outbound_url) | — | DNS-rebinding/metadata IP passes service.py guard | — |
| 18 | P2 | trim_ladder/selection.py::_resolve_trim_ladder_inner (229) | 612 | choose+gate in one fn | via resolve_trim_ladder |
| 19 | P3 | html_spec_sources.py (Honda crawler; catalog_key with different normalization than dictionary_catalog.catalog_key) | 1279 | 2 jobs; key mismatch risk | 3 |
| 20 | P3 | dictionary_catalog.py (build vs lookup; lru_cache(1) availability flag) | 712 | 2 jobs | 39 |
| 21 | P3 | _norm_token x7, trim key fns x5, trim_spec_sheets/trim_diff_engine twin loaders, band index x2 (dealer_score/deal_score_cache), trim_spec_extractor hygiene vs bullets.py | — | copy-paste | — |
| 22 | P3 | trim_ladder/__init__ + brochure_sources/__init__ facades re-exporting private names (patch trap) | 341 / 478 | test-patching hazard | 56 / 16 |
| 23 | P3 | intelligence/pipeline/orchestrator.run_hybrid_batch (189; bare `intelligence.` import) | 293 | loop+report | 1 |

## P1 details (concise; full line ranges in working notes above)
1. merge_verified_specs — Split into enrichment/specs/sources.py (gather SpecSources), specs/precedence/{cylinders,drivetrain,transmission,fuel,body}.py (each returns value/display/verified/source), specs/plausibility.py (L214-545), specs/sticker_packages.py (L15-201); car-detail context (L1089+) to car_serialize. Keep merge_verified_specs as a ~40-line composer returning the identical dict (golden-test the dict on a fixture sample before/after). Blast radius: 15 importers read the dict only — no signature change needed.
2. knowledge_engine — trim_decode/<make>.py registry replacing decode_trim_logic's chain (BMW/MINI L234-297, Mazda, Mercedes, Jeep, Ford x2, Ram, Dodge); epa_lookup.py (all lookup_epa_* + one clear_all_caches); vpic_cache.py with bounded LRU instead of _VPIC_MEMO; spec_display.py formatters; knowledge_engine.py left as re-export facade (54 importers).
3. Cross-cutting normalizers — backend/utils/drivetrain.py (resolver semantics; fixes nhtsa_vpic L71-85 returning raw "4x2/2-Wheel Drive" into cars patch L286-289), backend/utils/electrification.py (replace 9+ detectors, vPIC first), enrichment/epa_model_aliases.py (merge 4 alias tables: knowledge_engine L1287, naming L149, epa_master_store L44-111, model_specs_dictionary L171), enrichment/extended_specs.py (one reader + the model-level contamination suppression applied once).
4. parsers/__init__ — parsers/registry.py (PARSERS/parse/parse_kept) + backend/attribution/rooftop/{normalize,identity,tiers,resolve}.py; tiers as an ordered list of functions; pick one of _pick_target vs scanner/rooftop_match.score_rows after a shadow-compare and delete the other; drop lru_cache on roster places or key it by roster version. __init__ re-exports public names only.
5. brochure_extract — brochure/{archive,pdf_text,matrix_parse,overlay_store}.py + provenance/ladder_stores.py (what 25 importers actually want). Reconcile the "P" marker meaning between brochure_extract L135/L762 and brochure_trim_candidates L250/L782 with a data check on held brochures before choosing.
6. LLM transport — one backend/vision/llm_transport.py (or extend utils/llm_client): SDK client, 429 backoff (from claude_rate_limit), model-id registry (removes 3 hard-coded "claude-haiku-4-5-20251001"), refusal + truncation + JSON repair (service.py L337-480). Callers: service._ask_haiku_specs L180 & _vision_analyze_car L757-828 (raw urllib, own 5x10s retry), window_sticker_service L705/L946, agent._claude_reply L456, claude_vision L243/L476, oem/bmw.py L510 (delete with island).
7. Spec writers — after #1, add enrichment/spec_writer.py plan_spec_updates(car, sources) reusing the precedence modules; spec_backfill / spec_structured_backfill / vpic_facts / persist_enrichment become source gatherers. Also removes the 2nd nhtsa_vpic_cache accessor (spec_structured_backfill L179-208).

## P2 details
8. enrichment/service.py — vision/gallery_urls.py (L259-331, de-dupe _is_sticker_url with equipment_vision L299), vision/car_vision.py (_vision_analyze_car + observation merge L857-951), enrichment/haiku_spec_cache.py (L104-258), enrichment/enricher.py (InventoryEnricher, _PrefetchCache, CLI). Callers: scanner/post_scan/pipeline.py L565, dev/routes.py L1339, window_sticker_service L967.
9. _build_ladder_result — trim_ladder/rung_claim.py (L80-118), rung_bullets.py (per-rung gates L148-420 as named steps), provenance.py (L457-503). Only selection.py calls it.
10. brochure_trim_candidates + brochure_promote — brochure/walk/{adds_to,marker_grid,cell_grid,column_block,coord_grid,fca_blocks}.py page-extractor registry; walk/order.py; trim_vocab.py with cache_clear; candidates_store.py.
11. generated_spec_sheet — spec_sheet/{build,extended_attribution,equipment}.py + enrichment/options_list.py; route engine line via car_serialize.engine.build_engine_display (vPIC-aware).
12. backend/oem — delete/archive (no split). Keep oem/intake/paths.py + sqlite_store.py (used by backend/vector/pgvector_service.py) or move them under backend/vector. Verify scanner.js:2363/2443 spawn (already broken per Dockerfile.web comment) before deleting crawl4ai_discovery.
13. agent.py — prompt_guard.py (public names for ai_chat_bp), car_context.py, research.py, compare_chat.py; delete verify_car_data (zero callers).
14. window_sticker_service — stickers/{storage,oem_fetch,listing_fetch,parse_merge,llm_fallbacks}.py; behavior-preserving only (memory: leave OEM sticker fetch alone).
15. parsers copy-paste — base.norm_opt_str, base.tracking_value_by_needles, typesense uses utils/engine_consistency; team_velocity recovery -> backend/scanner/recovery/team_velocity_vdp.py.
16. preserve_trim_label / luxury sort keys — enrichment/make_rules/<make>.py hooks shared with trim_decode registry.
17. Image fetch — vision/image_fetch.py on utils/outbound_url guard; service.py and claude_vision.py both call it.
18. _resolve_trim_ladder_inner — choose_steps() + apply_display_gates().

## Checked, fine (no monolith issue beyond notes above; one line each)
- backend/catalog/__init__.py (10L, longest 0) — package init / re-exports, fine
- backend/catalog/generations.py (220L, longest 27) — fine
- backend/catalog/linker.py (82L, longest 56) — fine
- backend/catalog/resolver.py (426L, longest 131) — single-purpose resolver (P3: table-drive weights)
- backend/enrichment/__init__.py (1L, longest 0) — package init / re-exports, fine
- backend/enrichment/brochure_overlay_backfill.py (167L, longest 44) — fine
- backend/enrichment/brochure_sources/agents.py (46L, longest 3) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/archive.py (157L, longest 31) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/archive_authenticity.py (456L, longest 74) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/discovery.py (140L, longest 22) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/hosts.py (522L, longest 0) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/http.py (104L, longest 21) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/identity.py (191L, longest 74) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/links.py (285L, longest 64) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/nameplates.py (277L, longest 63) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/naming.py (169L, longest 27) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/quality.py (324L, longest 105) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/quarantine.py (384L, longest 66) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/reachability.py (263L, longest 98) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/robots.py (164L, longest 31) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/storage.py (358L, longest 55) — post-split submodule, cohesive
- backend/enrichment/brochure_sources/tiers.py (137L, longest 13) — post-split submodule, cohesive
- backend/enrichment/catalog_lookup.py (176L, longest 48) — fine
- backend/enrichment/dealer_ratings.py (777L, longest 120) — cohesive Places sync
- backend/enrichment/dictionary_derived.py (206L, longest 63) — fine
- backend/enrichment/dictionary_options_store.py (199L, longest 77) — _norm_token/_row_to_csv_dict dup (#21)
- backend/enrichment/dictionary_paths.py (126L, longest 13) — fine
- backend/enrichment/listing_packages_service.py (190L, longest 48) — fine
- backend/enrichment/package_registry.py (564L, longest 128) — cohesive observe/lookup/price
- backend/enrichment/persist_enrichment.py (144L, longest 59) — one of the spec writers (#7)
- backend/enrichment/spec_search_client.py (189L, longest 60) — fine
- backend/enrichment/trim_ladder/_common.py (175L, longest 23) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/adds_filter.py (216L, longest 56) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/attribution.py (357L, longest 120) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/bullets.py (265L, longest 103) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/citations.py (252L, longest 59) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/claims.py (157L, longest 47) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/csv_ladders.py (175L, longest 62) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/document_order.py (152L, longest 77) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/engine_steps.py (185L, longest 72) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/epa.py (83L, longest 43) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/evidence.py (291L, longest 63) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/inventory.py (157L, longest 79) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/loaders.py (143L, longest 66) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/plausibility.py (278L, longest 104) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/steps.py (233L, longest 69) — post-split submodule, cohesive
- backend/enrichment/trim_ladder/sticker_diffs.py (202L, longest 77) — post-split submodule, cohesive
- backend/enrichment/trim_ladder_knowledge/__init__.py (318L, longest 0) — package init / re-exports, fine
- backend/enrichment/trim_ladder_knowledge/adds.py (131L, longest 39) — fine
- backend/enrichment/trim_ladder_knowledge/bullets.py (872L, longest 56) — cohesive hygiene rules
- backend/enrichment/trim_ladder_knowledge/categories.py (358L, longest 76) — fine
- backend/enrichment/trim_ladder_knowledge/drivetrain.py (169L, longest 81) — fine
- backend/enrichment/trim_ladder_knowledge/fallback.py (67L, longest 15) — fine
- backend/enrichment/trim_ladder_knowledge/tables.py (755L, longest 0) — pure data
- backend/enrichment/trim_ladder_knowledge/validation.py (129L, longest 7) — fine
- backend/enrichment/trim_ladder_knowledge/year_windows.py (87L, longest 19) — fine
- backend/enrichment/vehicle_history_intelligence.py (316L, longest 114) — fine
- backend/intelligence/__init__.py (2L, longest 0) — package init / re-exports, fine
- backend/intelligence/agents/__init__.py (7L, longest 0) — package init / re-exports, fine
- backend/intelligence/agents/adjudicator_agent.py (103L, longest 26) — fine
- backend/intelligence/ai/__init__.py (1L, longest 0) — package init / re-exports, fine
- backend/intelligence/deal_score_cache.py (187L, longest 51) — band index dup with dealer_score (#21)
- backend/intelligence/inventory_signals.py (495L, longest 61) — fine
- backend/intelligence/lease_matcher.py (500L, longest 89) — fine
- backend/intelligence/llm/__init__.py (4L, longest 0) — package init / re-exports, fine
- backend/intelligence/llm/client.py (52L, longest 18) — fine
- backend/intelligence/llm/providers/__init__.py (2L, longest 0) — package init / re-exports, fine
- backend/intelligence/market_pricing.py (380L, longest 86) — fine
- backend/intelligence/pipeline/__init__.py (18L, longest 0) — package init / re-exports, fine
- backend/intelligence/pipeline/adjudication.py (163L, longest 44) — fine
- backend/intelligence/pipeline/eval_report.py (164L, longest 37) — fine
- backend/intelligence/pipeline/evidence_builder.py (112L, longest 91) — fine
- backend/intelligence/pipeline/review_queue.py (52L, longest 11) — fine
- backend/oem/__init__.py (8L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/__main__.py (5L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/models.py (36L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/normalize.py (211L, longest 39) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/paths.py (16L, longest 3) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/raw_store.py (18L, longest 8) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/intake/sqlite_store.py (413L, longest 100) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/__init__.py (6L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/bmw_debug.py (29L, longest 5) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/bmw_keyword_sets.py (180L, longest 38) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/bmw_parse_trace.py (38L, longest 23) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/discovery.py (83L, longest 27) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/scraper/oem/http.py (20L, longest 11) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/__init__.py (17L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/cli.py (321L, longest 134) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/core/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/core/db.py (21L, longest 5) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/core/paths.py (21L, longest 4) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/csv_export/__init__.py (6L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/csv_export/flat_export.py (210L, longest 133) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/db.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/export_csv.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingest.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingest_manifest.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingest_structured.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingestion/__init__.py (30L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingestion/bundle.py (217L, longest 104) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingestion/manifest.py (67L, longest 26) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/ingestion/structured.py (159L, longest 97) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/mpg_format.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/parsers/__init__.py (7L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/parsers/bmw_ordering_guide.py (34L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/paths.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/qa_report.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/quality/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/quality/qa_report.py (102L, longest 76) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/quality/validate.py (92L, longest 77) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/sources/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/sources/epa_bmw_ingest.py (222L, longest 109) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/sources/epa_client.py (110L, longest 17) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/utils/__init__.py (2L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/utils/mpg.py (46L, longest 34) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/oem/vehicle_reference/validate.py (3L, longest 0) — part of dead oem island (#12) — delete/keep per #12, no split
- backend/parsers/carscommerce.py (392L, longest 76) — rooftop_of dup per #4 only
- backend/parsers/chapman.py (232L, longest 41) — fine
- backend/parsers/dealer_dot_com.py (862L, longest 149) — _map_vehicle 149 but extraction factored into helpers; dupes per #15
- backend/parsers/dealer_eprocess.py (302L, longest 41) — fine
- backend/parsers/dealer_on.py (487L, longest 65) — dupes per #15 only
- backend/parsers/dealermasters.py (124L, longest 24) — fine
- backend/parsers/generic_json.py (120L, longest 31) — fine
- backend/parsers/html_cards.py (429L, longest 94) — fine
- backend/parsers/inventory_carfax.py (88L, longest 28) — fine
- backend/parsers/inventory_condition.py (67L, longest 48) — fine
- backend/parsers/inventory_mpg.py (115L, longest 30) — fine
- backend/parsers/jazel.py (213L, longest 39) — fine
- backend/parsers/motive_ridemotive.py (189L, longest 38) — fine
- backend/parsers/oneaudi.py (147L, longest 55) — fine
- backend/parsers/overfuel.py (161L, longest 36) — fine
- backend/parsers/rooftop_aliases.py (89L, longest 18) — fine
- backend/parsers/sister_tv.py (228L, longest 48) — fine
- backend/parsers/vdp_urls.py (204L, longest 59) — fine
- backend/parsers/wp_vehicles_index.py (92L, longest 38) — fine
- backend/vision/__init__.py (2L, longest 0) — package init / re-exports, fine
- backend/vision/analyze_images.py (214L, longest 62) — fine
- backend/vision/claude_rate_limit.py (98L, longest 30) — fine
- backend/vision/equipment_vision.py (493L, longest 97) — _is_sticker_url dup per #8
- backend/vision/image_text.py (768L, longest 56) — cohesive OCR pipeline (P3: ocr_backends split optional)
- backend/vision/interior_vision_merge.py (130L, longest 77) — fine
- backend/vision/monroney_merge.py (148L, longest 126) — fine
- backend/vision/url_heuristics.py (344L, longest 118) — fine
- backend/vision/vlm_ollama.py (217L, longest 50) — fine

## Accounting
186 files in area_enrich.txt = 40 with findings (ranked table / working notes: knowledge_engine_specs, knowledge_engine, trim_ladder/{build,selection,__init__}, brochure_extract, brochure_trim_candidates, brochure_promote, generated_spec_sheet, service, window_sticker_service, html_spec_sources, parsers/{__init__,team_velocity,base,typesense}, oem/scraper/oem/{bmw_locator_discovery,bmw}, oem/intake/{bmw_pipeline,cli}, oem/scraper/{crawl4ai_inventory,crawl4ai_discovery}, intelligence/ai/agent, trim_ladder_knowledge/naming, trim_spec_extractor, dictionary_catalog, nhtsa_vpic, spec_backfill, spec_structured_backfill, vpic_facts, epa_master_store, model_specs_dictionary, tco_fuel_estimates, ev_range_estimates, vision/claude_vision, intelligence/dealer_score, intelligence/pipeline/orchestrator, trim_spec_sheets, trim_diff_engine, brochure_sources/__init__) + 146 in the checked list above.
Caveat: read-only static review; no tests run. The "P" marker divergence and the catalog_key mismatch are code-level observations — confirm with a data check before acting.
