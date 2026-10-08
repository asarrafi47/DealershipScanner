# Test-suite baseline, 2026-10 (P0A.6)

*P0A.6: Baseline: chunked offline suite counts, ruff, import sweep* (`docs/REMEDIATION_PLAN_2026_10.md`, Phase 0A).

- **Revision:** `dbdbf5cbac233470b3dedd57077654548cef8095` (`dbdbf5cba`, "docs: remediation plan 2026-10", 2026-10-07 14:06 -0400), the head of `feature/http-only-scans` when Phase 0 started. Every number below is for that revision. The branch has moved on since then (Phase 1B merges): `git diff dbdbf5cba HEAD -- backend/tests` at `afa063a22` shows 15 added and 3 modified test files. Those are not in this baseline.
- **Where:** the MBP, in detached git worktrees at `dbdbf5cba` under `/private/tmp/claude-501/phase0/`. Both worktrees are removed. No worktree had a `.env`.
- **Toolchain:** the repo `.venv`, which has Python 3.14.7, pytest 9.1.1 and ruff 0.15.22. CI (P2A.2) will run Python 3.12. P2A.6 class (b) covers any 3.12-only difference.
- **When:**
  - Suite: 2026-10-07, 20:01:09 to 21:01:50 -0400.
  - ruff: 19:47 that day, and again on 2026-10-08 at about 10:54 -0400. The two outputs are identical.
  - Import sweep and the failure re-checks: 2026-10-08, between about 10:54 and 11:05 -0400.
- **Power:** on battery. OWNER_DECISIONS_LOG.md (2026-10-07, "Battery") allows the running Phase 0A/0B work on battery. A first start at 19:48:40 ran no chunk; its progress file is `_progress_aborted_on_battery.tsv`. The suite was re-run at 20:01.
- **Prod, Railway, DB:** none. This unit made no prod or Railway call and opened no DB session on purpose. The suite ran in its SQLite-tests environment:
  - The shell exported no `INVENTORY_DATABASE_URL` or `DATABASE_URL` (checked by name).
  - The worktree had no `.env`, and the root conftest disables dotenv loading anyway.
  - `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000'` was forced on every command, so any libpq session would still have been read-only.
  - `OPENAI_API_KEY` and `ANTHROPIC_API_KEY` were unset for every command.

## Answers to the Accept items

1. **The doc holds the SHA and the counts.** The SHA is `dbdbf5cba` (in full above). Selected: **4,643** tests from **336** files in **56** chunks. Results: **4,577 passed, 3 failed, 0 errors, 33 skipped (8 of them asset-gated), 30 xfailed, 0 xpassed.** Also: 5 deselected by marker, 54 warnings, and 20 subtests passed. A separate `--collect-only` run at the same SHA reported "4643/4648 tests collected (5 deselected)". That matches the sum of the chunk outcomes exactly (4,577 + 3 + 33 + 30 = 4,643), so no test was lost between chunks. The per-chunk table is below, and Appendix B has per-file counts.
2. **Pre-existing failures are listed, not fixed.** There are 3, all in the list below.
   - No product code or test was changed.
   - All 3 come from one cause: a clean checkout lacks two gitignored dictionary index files.
   - With those two files copied into the worktree, all 3 pass at `dbdbf5cba`. They fail on any clean checkout, which includes CI. They should pass in the MBP main checkout, which has both files (built 2026-08-02), but this unit did not run the suite there.
   - They are class (a) in the P2A.6 ledger taxonomy.
3. **Change items.** Each is recorded below:
   - pass/fail/skip/xfail per chunk;
   - the ASSET-GATED SKIPS count, **8**;
   - `ruff check .`: exactly the expected 3x F811 at `backend/tests/test_csrf_delete_routes.py:7,15,24`;
   - `scripts/scanner_import_sweep.py` under a timeout: exit 0, "0 problem(s)", plus two import-time side effects it surfaced.

## Totals

| Outcome | Count |
|---|---|
| Collected | 4,648 |
| Deselected by `-m "not integration and not slow"` | 5 |
| Selected | 4,643 |
| Passed | 4,577 |
| Failed | 3 |
| Errors | 0 |
| Skipped | 33 |
| of which ASSET-GATED SKIPS | 8 |
| xfailed | 30 |
| xpassed | 0 |
| Warnings | 54 (every one a `datetime.utcnow()` DeprecationWarning; see Skips, xfails and warnings) |
| Subtests passed | 20 (chunk 21_fu) |
| Test files | 336 (325 `backend/tests/test_*.py` plus 11 in `backend/tests/trim_ladder/`) |
| Chunks | 56 (at most 10 files each) |
| Sum of pytest-reported chunk times | 337.8 s |

## Method

- **Chunks.** The 336 files were grouped by filename prefix after `test_`, at most 10 files per chunk. A prefix group larger than 10 was split one character deeper. `backend/tests/trim_ladder/` is its own chunk (56). The generator was `make_chunks.py <tests dir> 10`, and its logic is the paragraph above. Appendix A has the exact manifest.
- **Per-chunk command.** One pytest process per chunk, run serially by a background driver. The raw logs of the run are in `/private/tmp/claude-501/phase0/baseline_chunks/` (`_progress.tsv`, `<chunk>.log`, `<chunk>.xml`). It is ephemeral and will not survive a reboot.

  ```
  env -u OPENAI_API_KEY -u ANTHROPIC_API_KEY \
    PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=20000' \
    timeout <cap> .venv/bin/python -m pytest <chunk files> \
      -m "not integration and not slow" -q -p no:cacheprovider -rfEsxX \
      --junitxml=<chunk>.xml > <chunk>.log 2>&1 < /dev/null
  ```

- **Exit codes.**
  - Every chunk exited 0, except the two chunks holding the 3 failures, which exited 1.
  - The per-chunk hard cap never fired (no 124).
  - No chunk errored at collection.
- **Chunk time vs wall time.** The plan wants each chunk under 120 s. By pytest's own clock, the longest chunk is `48_se-sp` at 63.10 s, and the 56 chunks sum to 337.8 s.
  - The wall clock was much longer for 7 chunks (40, 42, 48, 49, 52, 53, 55), and over 120 s for 5 of them (40: 402 s, 48: 605 s, 52: 955 s, 53: 968 s, 55: 321 s).
  - The cause is that the MBP slept. `pmset -g log` shows the system asleep, between short dark wakes, at these times: 20:05:01 to 20:11:37, 20:11:49 to 20:12:48, 20:13:02 to 20:18:31, 20:18:44 to 20:18:56, 20:19:26 to 20:22:57, 20:23:04 to 20:23:56, 20:24:14 to 20:40:06, 20:40:08 to 20:56:17 and 20:56:19 to 21:01:40.
  - Those windows line up with each chunk's junit start `timestamp` and its log mtime. For example, `52_vd`'s session began at 20:40:06, right after the 20:24:14 sleep, and ran 4.14 s.
  - pytest's clock does not advance while macOS sleeps, so the pytest times are the real run times.
  - The sleep did not change any outcome. None of the 3 failures is timing-related, and all 3 reproduce on an awake machine (see Pre-existing failures).

## Per-chunk results

"pytest s" is pytest's reported duration; "Wall s" includes system sleep (see Method). Errors were 0 in every chunk and xpassed was 0 in every chunk, so those columns are omitted.

| # | Chunk | Files | Exit | pytest s | Wall s | Passed | Failed | Skipped | of which asset-gated | xfailed | Deselected | Warnings |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 01 | ad-an | 8 | 0 | 22.66 | 23 | 63 | 0 | 0 | 0 | 0 | 0 | 0 |
| 02 | ap-au | 10 | 0 | 5.83 | 7 | 307 | 0 | 0 | 0 | 0 | 0 | 0 |
| 03 | ba-bo | 6 | 0 | 17.32 | 18 | 51 | 0 | 0 | 0 | 0 | 0 | 0 |
| 04 | br-bz | 7 | 0 | 3.60 | 4 | 240 | 0 | 2 | 2 | 0 | 0 | 0 |
| 05 | cab | 1 | 0 | 0.31 | 1 | 2 | 0 | 0 | 0 | 0 | 0 | 0 |
| 06 | car_ | 9 | 0 | 3.36 | 4 | 98 | 0 | 6 | 0 | 0 | 0 | 0 |
| 07 | cars | 2 | 0 | 0.45 | 1 | 24 | 0 | 0 | 0 | 0 | 0 | 0 |
| 08 | cat | 1 | 0 | 0.42 | 1 | 21 | 0 | 0 | 0 | 0 | 0 | 0 |
| 09 | ch-cl | 5 | 0 | 2.72 | 3 | 19 | 0 | 1 | 0 | 0 | 0 | 0 |
| 10 | co | 9 | 0 | 2.69 | 3 | 126 | 0 | 0 | 0 | 0 | 0 | 0 |
| 11 | cs | 2 | 0 | 3.53 | 4 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| 12 | da-db | 3 | 0 | 0.60 | 1 | 76 | 0 | 0 | 0 | 0 | 1 | 0 |
| 13 | dealer_a-dealer_m | 9 | 0 | 1.28 | 2 | 126 | 0 | 0 | 0 | 0 | 0 | 0 |
| 14 | dealer_o-dealer_r | 8 | 0 | 3.23 | 4 | 88 | 0 | 0 | 0 | 0 | 0 | 0 |
| 15 | dealer_s-dealer_v | 6 | 0 | 0.51 | 1 | 43 | 0 | 0 | 0 | 0 | 0 | 0 |
| 16 | dealers | 3 | 0 | 1.41 | 2 | 66 | 0 | 0 | 0 | 0 | 0 | 0 |
| 17 | deb-dev | 8 | 0 | 4.03 | 4 | 41 | 0 | 0 | 0 | 0 | 0 | 0 |
| 18 | di-du | 3 | 1 | 3.97 | 5 | 18 | 2 | 1 | 1 | 0 | 0 | 0 |
| 19 | e | 10 | 0 | 3.33 | 4 | 135 | 0 | 0 | 0 | 0 | 0 | 0 |
| 20 | fe-fo | 7 | 0 | 1.28 | 2 | 61 | 0 | 0 | 0 | 0 | 0 | 0 |
| 21 | fu | 5 | 0 | 2.64 | 3 | 148 | 0 | 0 | 0 | 0 | 0 | 5 |
| 22 | g | 8 | 0 | 6.44 | 7 | 81 | 0 | 0 | 0 | 0 | 0 | 0 |
| 23 | h | 9 | 0 | 15.61 | 16 | 124 | 0 | 0 | 0 | 0 | 0 | 0 |
| 24 | im | 3 | 0 | 0.48 | 1 | 66 | 0 | 0 | 0 | 0 | 0 | 0 |
| 25 | in_-int | 4 | 0 | 9.14 | 10 | 27 | 0 | 0 | 0 | 0 | 0 | 0 |
| 26 | inventory_c-inventory_v | 10 | 0 | 0.81 | 1 | 63 | 0 | 0 | 0 | 0 | 0 | 0 |
| 27 | inventory_w | 1 | 0 | 0.37 | 1 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| 28 | j | 3 | 0 | 1.63 | 2 | 22 | 0 | 0 | 0 | 0 | 0 | 0 |
| 29 | k | 1 | 0 | 2.20 | 3 | 31 | 0 | 0 | 0 | 0 | 0 | 0 |
| 30 | le | 1 | 0 | 0.46 | 1 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| 31 | listing_ | 6 | 0 | 1.02 | 1 | 41 | 0 | 0 | 0 | 0 | 0 | 0 |
| 32 | listings | 6 | 0 | 5.53 | 6 | 79 | 0 | 0 | 0 | 0 | 2 | 1 |
| 33 | ll-lo | 5 | 0 | 8.53 | 9 | 167 | 0 | 1 | 0 | 0 | 0 | 0 |
| 34 | ma-mi | 10 | 1 | 9.88 | 11 | 51 | 1 | 0 | 0 | 0 | 1 | 3 |
| 35 | mo-ms | 6 | 0 | 19.81 | 20 | 115 | 0 | 0 | 0 | 0 | 0 | 0 |
| 36 | n | 9 | 0 | 4.10 | 5 | 132 | 0 | 0 | 0 | 0 | 0 | 0 |
| 37 | o | 3 | 0 | 0.36 | 1 | 10 | 0 | 0 | 0 | 0 | 0 | 0 |
| 38 | pa-ph | 8 | 0 | 37.50 | 38 | 48 | 0 | 0 | 0 | 0 | 0 | 2 |
| 39 | pi-pl | 8 | 0 | 1.36 | 2 | 61 | 0 | 0 | 0 | 0 | 0 | 0 |
| 40 | pr-pu | 5 | 0 | 12.90 | 402 | 31 | 0 | 0 | 0 | 0 | 0 | 0 |
| 41 | q | 1 | 0 | 2.73 | 7 | 12 | 0 | 0 | 0 | 0 | 0 | 0 |
| 42 | ra-re | 10 | 0 | 6.09 | 63 | 210 | 0 | 0 | 0 | 0 | 0 | 0 |
| 43 | ro | 6 | 0 | 0.64 | 1 | 131 | 0 | 0 | 0 | 30 | 0 | 0 |
| 44 | sa | 2 | 0 | 2.00 | 2 | 9 | 0 | 0 | 0 | 0 | 0 | 0 |
| 45 | scan_ | 7 | 0 | 0.98 | 2 | 48 | 0 | 0 | 0 | 0 | 0 | 0 |
| 46 | scann | 6 | 0 | 0.58 | 1 | 117 | 0 | 0 | 0 | 0 | 0 | 0 |
| 47 | sch-scr | 4 | 0 | 0.80 | 1 | 65 | 0 | 0 | 0 | 0 | 1 | 0 |
| 48 | se-sp | 10 | 0 | 63.10 | 605 | 141 | 0 | 17 | 0 | 0 | 0 | 4 |
| 49 | st | 8 | 0 | 1.32 | 52 | 62 | 0 | 0 | 0 | 0 | 0 | 0 |
| 50 | t | 9 | 0 | 3.14 | 4 | 153 | 0 | 0 | 0 | 0 | 0 | 0 |
| 51 | u | 8 | 0 | 13.71 | 15 | 29 | 0 | 0 | 0 | 0 | 0 | 39 |
| 52 | vd | 7 | 0 | 4.14 | 955 | 50 | 0 | 0 | 0 | 0 | 0 | 0 |
| 53 | ve-vl | 7 | 0 | 2.84 | 968 | 347 | 0 | 4 | 4 | 0 | 0 | 0 |
| 54 | vp | 5 | 0 | 0.90 | 1 | 26 | 0 | 0 | 0 | 0 | 0 | 0 |
| 55 | w | 7 | 0 | 2.69 | 321 | 56 | 0 | 0 | 0 | 0 | 0 | 0 |
| 56 | trim_ladder | 11 | 0 | 8.84 | 9 | 199 | 0 | 1 | 1 | 0 | 0 | 0 |
| | **Total** | **336** | | **337.8** | **3641** | **4577** | **3** | **33** | **8** | **30** | **5** | **54** |

## Pre-existing failures (listed, not fixed)

| # | Test | Chunk | Symptom | Cause |
|---|---|---|---|---|
| F1 | `backend/tests/test_dictionary_catalog.py::test_writing_the_real_manifest_is_blocked` | 18_di-du | `FileNotFoundError: .../backend/dictionary/index/manifest.json` at `dictionary_catalog.MANIFEST_PATH.read_bytes()` (test line 122) | `manifest.json` is gitignored (`.gitignore:109`), so a clean checkout lacks it. The test's skip gate (`:111`) checks `DICTIONARY_ROOT.is_dir()`, the tracked `backend/dictionary/` directory, not the manifest file, so it fails instead of skipping. |
| F2 | `backend/tests/test_dictionary_catalog.py::test_the_2026_07_31_incident_shape_is_now_stopped` | 18_di-du | Same `FileNotFoundError` (test line 154) | Same as F1. The skip gate is at `:140`. |
| F3 | `backend/tests/test_merge_verified_specs_golden.py::test_merge_verified_specs_golden_hermetic` | 34_ma-mi | `AssertionError: 2 golden mismatches`. Car `live:1239391`, for both `include_extended_specs=True` and `False`, gets `epa_engine_description` `'Hybrid 2.0L I4 (SIDI; Mild Hybrid)'` instead of `'2.0L I4 (SIDI)'`, `epa_city08` 26.0 instead of 22.0, `epa_highway08` 36.0 instead of 32.0, and `fuel_economy_display` `'26 City / 36 Hwy'` instead of `'22 City / 32 Hwy'`. | `dictionary_catalog.db` is gitignored (`.gitignore:110`), so a clean checkout lacks it. The golden was recorded with it present, and without it this car resolves to a different EPA row. The test has no asset gate at all (`:305`), and its docstring says it "Runs everywhere the dictionary tree is on disk", which is not true without the index DB. |

**How the cause was verified**, at `dbdbf5cba` in a fresh detached worktree, running only the two files under the same flags:

| Run | Result |
|---|---|
| Clean checkout (as in the baseline) | 3 failed, 9 passed, 1 skipped (the asset-gated `catalog db not built`), 1 deselected |
| With `index/manifest.json` and `index/dictionary_catalog.db` copied in from the MBP main checkout | **13 passed**, 1 deselected. The asset-gated test also runs and passes. |
| With `manifest.json` only | F3 still fails. F1 and F2 pass. |
| With `dictionary_catalog.db` only | F1 and F2 still fail. F3 passes. |

So F1 and F2 need `manifest.json`, and F3 needs `dictionary_catalog.db`. The copies were deleted before the worktree was removed. Nothing in the main checkout was written.

**What later units should know:**

- **CI.** All three will fail in CI (P2A.5 shakedown), because CI is a clean checkout. They are class (a) in P2A.6: they need a gitignored local asset but are not asset-gated skips. Two later units already touch these files:
  - P9.2 owns `test_dictionary_catalog.py`.
  - P8A.6 owns `test_merge_verified_specs_golden.py`.
- **F3 is more than a missing gate.** It shows that `merge_verified_specs` returns different EPA fuel-economy facts for the same car depending on whether the dictionary catalog DB exists.
  - The car is a 2026 Audi A5 Premium Plus 2.0 TFSI quattro: gasoline, dealer-reported 22/32 mpg, `epa_master_id` 19023 in the fixture.
  - With the DB, the result matches the dealer's 22/32. Without it, the result is the mild-hybrid row at 26/36.
  - P0A.3 found that prod web has never had `dictionary_catalog.db` and runs on the glob fallback.
  - So prod may take the "without" branch for cars like this one. This was not checked against prod, and not root-caused here. It is an input for Phase 9 (D-DC5) and P8A.6.
  - A P2A.7 fix that only adds a skip would hide this difference in CI. Recording a second expectation for the no-catalog branch would keep it visible.

## Skips, xfails and warnings

**ASSET-GATED SKIPS: 8.** This is the sum of the `ASSET-GATED SKIPS: N` sections the conftest prints per chunk. `REQUIRE_LOCAL_ASSETS` was unset, which is the default.

| Chunk | Test | Gate reason |
|---|---|---|
| 04_br-bz | `test_brochure_extract.py::test_extract_brochure_text_2016_grand_cherokee` | brochure PDF not present |
| 04_br-bz | `test_brochure_extract.py::test_extract_2016_grand_cherokee_trims` | brochure PDF not present |
| 18_di-du | `test_dictionary_catalog.py::test_real_catalog_db_is_readable_but_not_writable` | catalog db not built |
| 53_ve-vl | `test_verify_trim_citations.py::test_real_pdf_a_wrapped_grid_row_is_still_admitted` | 2026_Toyota_RAV4_Brochure.pdf not on disk |
| 53_ve-vl | `test_verify_trim_citations.py::test_real_pdf_a_head_chopped_quote_is_refused[Tex Leatherette ...]` | 2021_Volkswagen_Passat_Brochure.pdf not on disk |
| 53_ve-vl | `test_verify_trim_citations.py::test_real_pdf_a_head_chopped_quote_is_refused[Line\xae grille ...]` | 2021_Volkswagen_Passat_Brochure.pdf not on disk |
| 53_ve-vl | `test_verify_trim_citations.py::test_verify_overlay_stamps_the_fragment_false_and_the_whole_line_true` | 2021_Volkswagen_Passat_Brochure.pdf not on disk |
| 56_trim_ladder | `trim_ladder/test_citation_verifier.py::test_verifier_reads_a_wrapped_grid_row_off_the_real_pdf` | 2026 RAV4 brochure PDF not on disk |

**The gated tests pass when their assets are present.** The MBP main checkout has all three brochure PDFs under `backend/data/brochures/` (354 files there).

- With those three PDFs copied into the fresh worktree and `REQUIRE_LOCAL_ASSETS=1`, the three gated files passed in full: `test_brochure_extract.py`, `test_verify_trim_citations.py` and `trim_ladder/test_citation_verifier.py`, **60 passed, 0 skipped**.
- `test_real_catalog_db_is_readable_but_not_writable` passed with the catalog DB present (see the verification table above).
- So on the MBP main checkout the gated count should be 0. On a clean checkout or in CI it is 8 (P14C.3's budget input, together with P2A.7's CI count).

**Other skips: 25.** These are not asset-gated. Each one is opt-in or needs an environment setting.

| Count | File | Reason |
|---|---|---|
| 17 | `test_search_golden.py` | `SEARCH_GOLDEN_PG=1 not set (needs local Postgres)` |
| 6 | `test_car_detail_context_golden.py` | `car detail golden needs PYTHONHASHSEED=0 (trim_ladder set order)` |
| 1 | `test_llm_call_site_parity.py` | `set LLM_GOLDEN_REGEN=1 to recapture` |
| 1 | `test_charger_daytona_specs.py` | `car 3608 (Charger Daytona Scat Pack) absent from the test inventory` |

**xfailed: 30.** All of them are `test_rooftop_corpus.py::test_rooftop_corpus_ideal[...]`, the documented "ideal" corpus cases (chunk 43_ro). None xpassed.

**Deselected by marker: 5.**

- `test_data_quality_invariants.py::test_live_seeded_regression_in_a_temp_table`
- `test_listings_perf.py::test_search_cars_by_make_model_pairs_warm_under_200ms`
- `test_listings_perf.py::test_listings_grid_cold_build_under_one_second`
- `test_merge_verified_specs_golden.py::test_merge_verified_specs_golden_live_db`
- `test_schema_from_migrations.py::test_migrations_build_a_superset_of_the_runtime_ddl`

**Warnings: 54.** All are `DeprecationWarning: datetime.datetime.utcnow() is deprecated` on Python 3.14. They come from four lines:

- `backend/scanner/database.py:386`
- `backend/scanner/upsert/guard.py:111`
- `backend/tests/test_upsert_vin_owner_guard.py:88`
- `backend/tests/test_upsert_vin_owner_guard.py:103`

## ruff

- Command: `.venv/bin/ruff check --no-cache .` with ruff 0.15.22, at `dbdbf5cba`, from the worktree root.
- Result: exit 1, **Found 3 errors**, all `F811 Redefinition of unused 'env' from line 4`:
  - `backend/tests/test_csrf_delete_routes.py:7:54`
  - `backend/tests/test_csrf_delete_routes.py:15:55`
  - `backend/tests/test_csrf_delete_routes.py:24:42`
- The cause: line 4 re-exports the `env` fixture from `test_hidden_dealers` (`# noqa: F401`), and each test takes `env` as a parameter.
- This is exactly the expectation in the plan. It is identical (ignoring blank lines) to the earlier `_ruff.log` taken at the same SHA.
- No other rule fires. The fix is P2A.1.

## Import sweep (`scripts/scanner_import_sweep.py`)

- Command: `timeout 110 nice -n 10 .venv/bin/python scripts/scanner_import_sweep.py`, at `dbdbf5cba` in a fresh detached worktree, with API keys unset and `PGOPTIONS` read-only.
- The baseline run had not recorded a sweep, so this unit ran it.
- Result: **exit 0 in 2 s, "sweep done: 0 problem(s)"**.
  - All 13 entry modules print `ok`: `backend.scripts.dealer_pipeline`, `backend.scanner.cli`, `backend.scanner.orchestrator`, `backend.enrichment.vpic_facts`, `backend.scripts.compute_market_stats`, `backend.scripts.build_listings_grid_cards`, `backend.db.repositories.grid_cards_repo`, `backend.intelligence.market_pricing`, `backend.scripts.discovery_probe`, `backend.scripts.platform_candidates`, `backend.scanner.recipe_validation`, `backend.scanner.recipe_synth` and `backend.scripts.heal_from_vpic`.
  - The sweep walked 376 modules: `backend.scanner` 153, `backend.db` 50, `backend.enrichment` 99, `backend.utils` 74.
  - No module reported MISSING, allowed-missing or "other".
- **Limit:** this ran in the dev `.venv`, which installs every requirement. It proves the scan path imports cleanly at this SHA. It does not prove that `requirements-scanner.txt` is complete. That check needs the sweep inside the `Dockerfile.scanner` image, and no Docker build was run on battery. P15B.1 and P15B.2 own the requirements work.
- **Import-time side effects the sweep surfaced.** These were found by importing the swept modules one at a time and watching the cwd and the worktree after each import.
  1. Importing `backend.scanner.post_scan.job` changes the process's working directory. `backend/scanner/post_scan/job.py:26` runs `os.chdir(ROOT)` at module level, with `ROOT = Path(__file__).resolve().parents[2]`, which is `<repo>/backend`. Any process that imports it, whether a test, the sweep or a script, silently moves to `backend/`. No unit in the plan names this (`chdir` has 0 hits in REMEDIATION_PLAN_2026_10.md). It fits P13A's "side-effect-free import" goal.
  2. Importing `backend.db.search_analytics_db` creates `<repo>/users.db` (53,248 bytes), through the module-level `_bootstrap()` at `backend/db/search_analytics_db.py:53`. P13A.6 already covers this ("users.db bootstraps become lazy").

## Comparing a later run against this baseline

- Run the same selection: `-m "not integration and not slow" -q -p no:cacheprovider`, chunked, with no `.env`.
- Run it at the new SHA in a clean checkout, and compare per file using Appendix B. Chunk boundaries move as files are added, so compare files, not chunks.
- Expected differences:
  - files added after `dbdbf5cba` (15 by `afa063a22`);
  - tests deleted on purpose (the P17A exit gate);
  - the 3 failures above, once P2A.7, P9.2 or P8A.6 fix them;
  - the gated count, which depends on the checkout's local assets: 8 on a clean checkout, 0 expected on the MBP main checkout.

## Appendix A: chunk manifest

Each line is the chunk name, then its test files. Names drop the `backend/tests/test_` prefix and the `.py` suffix. Chunk 56 is the `backend/tests/trim_ladder/` directory (11 files).

```
01_ad-an: admin_operator_api admin_users ai_chat_bp ai_model_specs_merge ai_narrate_bp algolia_scope analyze_brochure_with_llm analyze_images_cli
02_ap-au: app_security_basics app_surface_golden apple_oauth assess_stamped_rows attribute_feed_rooftops attribution_destination attribution_golden_20261001 audit_fixes auto_heal autowall_http_recipe_20260924
03_ba-bo: backfill_extended_specs billing_catalog billing_gate billing_portal body_style_normalize bootstrap_site_admin
04_br-bz: brochure_acquisition_plan brochure_extract brochure_promote brochure_sources brochure_trim_candidates browser_gate_20260926 bz_woodland_split
05_cab: cabin_vision_url_pick
06_car_: car_attribution_overlay car_chat_policy car_detail_api car_detail_context_golden car_nhtsa_recalls_api car_page_perf car_price_history_vdp car_serialize_tco_fields car_serialize_urls
07_cars: carscommerce_description_f01 carscommerce_store_filter_20260924
08_cat: catalog_resolver
09_ch-cl: chapman_msrp_f15 charger_daytona_specs claude_rate_limit claude_vision_finalize client_ip
10_co: color_overlay comment_attachments comments_db community_api_routes compare_and_stats compare_features_transmission compare_specs condition_canonical_f14 condition_mileage_location
11_cs: csp_self_hosted_fonts csrf_delete_routes
12_da-db: data_quality_invariants db_admin_security db_connect
13_dealer_a-dealer_m: dealer_attribution dealer_com_bulk_fetch dealer_dot_com_engine_f05 dealer_dot_com_inventory_fields dealer_dot_com_msrp_f02 dealer_geo_radius dealer_location dealer_locator dealer_map
14_dealer_o-dealer_r: dealer_on_parser dealer_onboard_api dealer_pipeline_surface dealer_portal dealer_profile dealer_registry_match dealer_reviews dealer_run_golden_20261001
15_dealer_s-dealer_v: dealer_score dealer_site_url dealer_specials_extract dealer_specials_store dealer_sticker_provider dealer_venom
16_dealers: dealership_address_backfill dealership_discovery dealership_page
17_deb-dev: debug_audit delta_scan delta_scan_full_scan_parity dep_store_scope_20260926 dev_dealers_manifest dev_ip_allowlist dev_operator_premium dev_pipeline_import
18_di-du: dictionary_catalog discovery_fixes_20260924 dummy_vin_cleanup
19_e: egress_tag_blocked email_verification engine_consistency_catalog_row engine_display enrich_dictionary_heuristics enrich_dictionary_import_side_effects entitlements_revalidation epa_engine_resolve equipment_vision ev_range_estimates
20_fe-fo: fetch_oem_brochures_surface field_clean_spec_junk field_clean_stock_guard filter_options first_seen_line fleet_roster_rule forced_induction_display
21_fu: fuel_body_presets fuel_label_plausibility fuel_lookup fuel_type_canonical_f13 fuel_type_normalize
22_g: gallery_harvest gallery_url_filter gallery_url_heuristics generated_spec_sheet generic_json_parser_20260926 geocode_dealers google_oauth google_place_rating
23_h: heal_stock_code_contamination health_ready hermetic_test_env hidden_dealers home_dashboard_routes horsepower_hybrid_guard html_cards_20260926 html_jsonld_harvest html_spec_sources
24_im: image_batch_reconcile image_batch_upsize image_text
25_in_-int: in_transit incomplete_listings_index init_admin_db interior_color_buckets
26_inventory_c-inventory_v: inventory_card_location inventory_condition inventory_db_path inventory_filters inventory_mpg inventory_postgres_required inventory_recovery inventory_repair inventory_signals inventory_vin_merge
27_inventory_w: inventory_write
28_j: job_diagnosis job_queue js_unit
29_k: knowledge_engine_specs
30_le: lease_matcher
31_listing_: listing_completeness listing_description_extract listing_description_intro listing_gap_fill listing_packages_service listing_sticker_ipacket
32_listings: listings_indexes listings_packing listings_perf listings_scoped_grid listings_search_bounded listings_sort
33_ll-lo: llm_call_site_parity llm_client llm_client_transport local_llm_assistant login_session_fixes
34_ma-mi: manifest_roster market_price market_price_cache market_pricing merge_verified_specs_golden mfa_action_log mfa_qr mfa_totp migrate mileage_zero_f12
35_mo-ms: mobile_api_contract mobile_auth_api mobile_auth_register model_aliases monroney_merge msrp_trust
36_n: nearby_dealers_api nearby_dealers_listings network_observer next_data_inventory nhtsa_vpic nhtsa_vpic_drivetrain_b4 no_inline_style_elements non_dealer_filter not_found_page
37_o: oneaudi_recipe_20260924 outbound_url outbound_url_dns
38_pa-ph: paid_access_parity parser_base parsers_package partial_update_storage_vocabulary password_hash password_reset payment_listed_prices phev_fuel_bucket_f09
39_pi-pl: pipeline_assess_autocommit pipeline_reconcile_20260926 pixel_motion pixel_motion_stock_leak platform_carfax_packages platform_dashboard platform_fingerprint platform_registry
40_pr-pu: premium_checkout price_drop_badge price_plausibility production_security public_listings_count_cache
41_q: query_parser_condition
42_ra-re: rarity_score recipe_lifecycle recipe_synth recipe_synth_surface recipe_validation recipe_walk_fixes_20260925 recipes reconcile_condition_guard registration_email registration_password_limits
43_ro: rooftop_address_provenance rooftop_corpus rooftop_match rooftop_refusals_report rooftop_same_source_20260926 rooftop_weak_name_and_department
44_sa: safe_listing_url saved_cars_api
45_scan_: scan_efficiency scan_efficiency_vdp scan_lab scan_lab_provider_hint scan_lock_path_env scan_only_mode scan_timing
46_scann: scanner_http_characterization scanner_http_fetch scanner_intercept_filter scanner_inventory_reconcile scanner_shard scanner_vision_env_defaults
47_sch-scr: schema_from_migrations scrape_confidence scraper_chain scraping_cli_package_imports
48_se-sp: search_golden search_history serialize_car_for_api_golden smart_search spec_backfill spec_field_normalize spec_guardrails spec_placeholders_f10 spec_structured_backfill spin_assets_roundtrip
49_st: static_assets sticker_preview_quality sticker_reconciliation sticker_skip_vision sticker_trim_diffs store_admin store_scoped_recipe_gate_20260925 stripe_premium_verify
50_t: tally_exemptions_section7 tco_fuel_estimates team_velocity_gallery_f03 team_velocity_parser transmission_normalize trim_diff_engine trim_invariant trim_ladder_audit trim_spec_sheets
51_u: upsert_data_quality_score upsert_deadlock_retry upsert_keeps_vdp_only_fields upsert_last_price_change_at upsert_lot_location upsert_vehicles_golden upsert_vin_owner_guard ux_premium_gates
52_vd: vdp_extras_parse vdp_helpers vdp_html_recovery vdp_prefetch vdp_recipes vdp_spec_gap vdp_urls
53_ve-vl: vehicle_facts vehicle_history_nhtsa_recalls vehicle_narrator verify_trim_citations vision_refusal_accounting visual_review_mobile_history_recalls vlm_ollama
54_vp: vpic_facts vpic_heal_derived_fields_f06 vpic_offline_stub vpic_override_cylinders vpic_specs
55_w: web_researcher window_sticker_parse window_sticker_price_filter window_sticker_scan window_sticker_service window_sticker_ui_visibility window_sticker_urls
56_trim_ladder: trim_ladder/ (directory)
```

## Appendix B: per-file counts at `dbdbf5cba`

These come from the 56 junit XML files. The columns are passed, failed, skipped and xfailed. Errors and xpassed were 0 for every file. Deselected tests are not counted. The column totals are 4,577, 3, 33 and 30.

<details>
<summary>336 files</summary>

```
file                                                     pass  F skip xfl
admin_operator_api                                         11  0   0   0
admin_users                                                10  0   0   0
ai_chat_bp                                                 10  0   0   0
ai_model_specs_merge                                        4  0   0   0
ai_narrate_bp                                               4  0   0   0
algolia_scope                                               9  0   0   0
analyze_brochure_with_llm                                  12  0   0   0
analyze_images_cli                                          3  0   0   0
app_security_basics                                         7  0   0   0
app_surface_golden                                          5  0   0   0
apple_oauth                                                 3  0   0   0
assess_stamped_rows                                         3  0   0   0
attribute_feed_rooftops                                     9  0   0   0
attribution_destination                                    35  0   0   0
attribution_golden_20261001                               227  0   0   0
audit_fixes                                                 7  0   0   0
auto_heal                                                   7  0   0   0
autowall_http_recipe_20260924                               4  0   0   0
backfill_extended_specs                                    22  0   0   0
billing_catalog                                             9  0   0   0
billing_gate                                                7  0   0   0
billing_portal                                              3  0   0   0
body_style_normalize                                        7  0   0   0
bootstrap_site_admin                                        3  0   0   0
brochure_acquisition_plan                                   2  0   0   0
brochure_extract                                           35  0   2   0
brochure_promote                                            3  0   0   0
brochure_sources                                          183  0   0   0
brochure_trim_candidates                                    2  0   0   0
browser_gate_20260926                                       7  0   0   0
bz_woodland_split                                           8  0   0   0
cabin_vision_url_pick                                       2  0   0   0
car_attribution_overlay                                    22  0   0   0
car_chat_policy                                             8  0   0   0
car_detail_api                                              4  0   0   0
car_detail_context_golden                                   0  0   6   0
car_nhtsa_recalls_api                                       3  0   0   0
car_page_perf                                              44  0   0   0
car_price_history_vdp                                       4  0   0   0
car_serialize_tco_fields                                    7  0   0   0
car_serialize_urls                                          6  0   0   0
carscommerce_description_f01                                5  0   0   0
carscommerce_store_filter_20260924                         19  0   0   0
catalog_resolver                                           21  0   0   0
chapman_msrp_f15                                            3  0   0   0
charger_daytona_specs                                       0  0   1   0
claude_rate_limit                                           2  0   0   0
claude_vision_finalize                                      1  0   0   0
client_ip                                                  13  0   0   0
color_overlay                                               7  0   0   0
comment_attachments                                        27  0   0   0
comments_db                                                44  0   0   0
community_api_routes                                       11  0   0   0
compare_and_stats                                           4  0   0   0
compare_features_transmission                               9  0   0   0
compare_specs                                               3  0   0   0
condition_canonical_f14                                    15  0   0   0
condition_mileage_location                                  6  0   0   0
csp_self_hosted_fonts                                       2  0   0   0
csrf_delete_routes                                          3  0   0   0
data_quality_invariants                                    51  0   0   0
db_admin_security                                           3  0   0   0
db_connect                                                 22  0   0   0
dealer_attribution                                         59  0   0   0
dealer_com_bulk_fetch                                       3  0   0   0
dealer_dot_com_engine_f05                                   6  0   0   0
dealer_dot_com_inventory_fields                             3  0   0   0
dealer_dot_com_msrp_f02                                     9  0   0   0
dealer_geo_radius                                           7  0   0   0
dealer_location                                             8  0   0   0
dealer_locator                                             28  0   0   0
dealer_map                                                  3  0   0   0
dealer_on_parser                                            5  0   0   0
dealer_onboard_api                                          9  0   0   0
dealer_pipeline_surface                                     7  0   0   0
dealer_portal                                               5  0   0   0
dealer_profile                                              5  0   0   0
dealer_registry_match                                       5  0   0   0
dealer_reviews                                             11  0   0   0
dealer_run_golden_20261001                                 41  0   0   0
dealer_score                                               23  0   0   0
dealer_site_url                                             5  0   0   0
dealer_specials_extract                                     5  0   0   0
dealer_specials_store                                       1  0   0   0
dealer_sticker_provider                                     7  0   0   0
dealer_venom                                                2  0   0   0
dealership_address_backfill                                21  0   0   0
dealership_discovery                                       19  0   0   0
dealership_page                                            26  0   0   0
debug_audit                                                 3  0   0   0
delta_scan                                                  5  0   0   0
delta_scan_full_scan_parity                                 4  0   0   0
dep_store_scope_20260926                                    3  0   0   0
dev_dealers_manifest                                        9  0   0   0
dev_ip_allowlist                                            4  0   0   0
dev_operator_premium                                        1  0   0   0
dev_pipeline_import                                        12  0   0   0
dictionary_catalog                                          9  2   1   0
discovery_fixes_20260924                                    5  0   0   0
dummy_vin_cleanup                                           4  0   0   0
egress_tag_blocked                                          5  0   0   0
email_verification                                          6  0   0   0
engine_consistency_catalog_row                             12  0   0   0
engine_display                                             29  0   0   0
enrich_dictionary_heuristics                                5  0   0   0
enrich_dictionary_import_side_effects                       1  0   0   0
entitlements_revalidation                                  11  0   0   0
epa_engine_resolve                                          8  0   0   0
equipment_vision                                            6  0   0   0
ev_range_estimates                                         52  0   0   0
fetch_oem_brochures_surface                                14  0   0   0
field_clean_spec_junk                                       5  0   0   0
field_clean_stock_guard                                    15  0   0   0
filter_options                                              3  0   0   0
first_seen_line                                             5  0   0   0
fleet_roster_rule                                          10  0   0   0
forced_induction_display                                    9  0   0   0
fuel_body_presets                                           8  0   0   0
fuel_label_plausibility                                    47  0   0   0
fuel_lookup                                                 6  0   0   0
fuel_type_canonical_f13                                    33  0   0   0
fuel_type_normalize                                        54  0   0   0
gallery_harvest                                             9  0   0   0
gallery_url_filter                                          4  0   0   0
gallery_url_heuristics                                      5  0   0   0
generated_spec_sheet                                       40  0   0   0
generic_json_parser_20260926                                2  0   0   0
geocode_dealers                                             8  0   0   0
google_oauth                                                8  0   0   0
google_place_rating                                         5  0   0   0
heal_stock_code_contamination                              11  0   0   0
health_ready                                                9  0   0   0
hermetic_test_env                                          12  0   0   0
hidden_dealers                                             15  0   0   0
home_dashboard_routes                                       3  0   0   0
horsepower_hybrid_guard                                     7  0   0   0
html_cards_20260926                                         7  0   0   0
html_jsonld_harvest                                         7  0   0   0
html_spec_sources                                          53  0   0   0
image_batch_reconcile                                      21  0   0   0
image_batch_upsize                                         19  0   0   0
image_text                                                 26  0   0   0
in_transit                                                  7  0   0   0
incomplete_listings_index                                   8  0   0   0
init_admin_db                                               3  0   0   0
interior_color_buckets                                      9  0   0   0
inventory_card_location                                     2  0   0   0
inventory_condition                                         4  0   0   0
inventory_db_path                                           3  0   0   0
inventory_filters                                          14  0   0   0
inventory_mpg                                               2  0   0   0
inventory_postgres_required                                 3  0   0   0
inventory_recovery                                         12  0   0   0
inventory_repair                                            3  0   0   0
inventory_signals                                          16  0   0   0
inventory_vin_merge                                         4  0   0   0
inventory_write                                             5  0   0   0
job_diagnosis                                               2  0   0   0
job_queue                                                  19  0   0   0
js_unit                                                     1  0   0   0
knowledge_engine_specs                                     31  0   0   0
lease_matcher                                              10  0   0   0
listing_completeness                                        9  0   0   0
listing_description_extract                                10  0   0   0
listing_description_intro                                   3  0   0   0
listing_gap_fill                                            3  0   0   0
listing_packages_service                                    4  0   0   0
listing_sticker_ipacket                                    12  0   0   0
listings_indexes                                            3  0   0   0
listings_packing                                           11  0   0   0
listings_perf                                              16  0   0   0
listings_scoped_grid                                       40  0   0   0
listings_search_bounded                                     4  0   0   0
listings_sort                                               5  0   0   0
llm_call_site_parity                                       48  0   1   0
llm_client                                                  9  0   0   0
llm_client_transport                                       39  0   0   0
local_llm_assistant                                        64  0   0   0
login_session_fixes                                         7  0   0   0
manifest_roster                                             3  0   0   0
market_price                                               11  0   0   0
market_price_cache                                          5  0   0   0
market_pricing                                             13  0   0   0
merge_verified_specs_golden                                 0  1   0   0
mfa_action_log                                              1  0   0   0
mfa_qr                                                      1  0   0   0
mfa_totp                                                    3  0   0   0
migrate                                                    10  0   0   0
mileage_zero_f12                                            4  0   0   0
mobile_api_contract                                        35  0   0   0
mobile_auth_api                                             5  0   0   0
mobile_auth_register                                        3  0   0   0
model_aliases                                              17  0   0   0
monroney_merge                                              6  0   0   0
msrp_trust                                                 49  0   0   0
nearby_dealers_api                                          3  0   0   0
nearby_dealers_listings                                     5  0   0   0
network_observer                                           50  0   0   0
next_data_inventory                                         2  0   0   0
nhtsa_vpic                                                 12  0   0   0
nhtsa_vpic_drivetrain_b4                                   18  0   0   0
no_inline_style_elements                                    4  0   0   0
non_dealer_filter                                          36  0   0   0
not_found_page                                              2  0   0   0
oneaudi_recipe_20260924                                     4  0   0   0
outbound_url                                                5  0   0   0
outbound_url_dns                                            1  0   0   0
paid_access_parity                                         13  0   0   0
parser_base                                                 4  0   0   0
parsers_package                                             2  0   0   0
partial_update_storage_vocabulary                          11  0   0   0
password_hash                                               8  0   0   0
password_reset                                              3  0   0   0
payment_listed_prices                                       5  0   0   0
phev_fuel_bucket_f09                                        2  0   0   0
pipeline_assess_autocommit                                  3  0   0   0
pipeline_reconcile_20260926                                 4  0   0   0
pixel_motion                                                1  0   0   0
pixel_motion_stock_leak                                     5  0   0   0
platform_carfax_packages                                    6  0   0   0
platform_dashboard                                          3  0   0   0
platform_fingerprint                                       17  0   0   0
platform_registry                                          22  0   0   0
premium_checkout                                            2  0   0   0
price_drop_badge                                            6  0   0   0
price_plausibility                                         11  0   0   0
production_security                                         8  0   0   0
public_listings_count_cache                                 4  0   0   0
query_parser_condition                                     12  0   0   0
rarity_score                                                6  0   0   0
recipe_lifecycle                                           34  0   0   0
recipe_synth                                               66  0   0   0
recipe_synth_surface                                        3  0   0   0
recipe_validation                                          47  0   0   0
recipe_walk_fixes_20260925                                  4  0   0   0
recipes                                                    43  0   0   0
reconcile_condition_guard                                   3  0   0   0
registration_email                                          3  0   0   0
registration_password_limits                                1  0   0   0
rooftop_address_provenance                                  8  0   0   0
rooftop_corpus                                             85  0   0  30
rooftop_match                                              17  0   0   0
rooftop_refusals_report                                    11  0   0   0
rooftop_same_source_20260926                                5  0   0   0
rooftop_weak_name_and_department                            5  0   0   0
safe_listing_url                                            7  0   0   0
saved_cars_api                                              2  0   0   0
scan_efficiency                                             8  0   0   0
scan_efficiency_vdp                                         6  0   0   0
scan_lab                                                    4  0   0   0
scan_lab_provider_hint                                      2  0   0   0
scan_lock_path_env                                          5  0   0   0
scan_only_mode                                              2  0   0   0
scan_timing                                                21  0   0   0
scanner_http_characterization                              70  0   0   0
scanner_http_fetch                                         12  0   0   0
scanner_intercept_filter                                   14  0   0   0
scanner_inventory_reconcile                                12  0   0   0
scanner_shard                                               2  0   0   0
scanner_vision_env_defaults                                 7  0   0   0
schema_from_migrations                                     19  0   0   0
scrape_confidence                                           4  0   0   0
scraper_chain                                              39  0   0   0
scraping_cli_package_imports                                3  0   0   0
search_golden                                               6  0  17   0
search_history                                             16  0   0   0
serialize_car_for_api_golden                                2  0   0   0
smart_search                                               21  0   0   0
spec_backfill                                              13  0   0   0
spec_field_normalize                                       18  0   0   0
spec_guardrails                                            37  0   0   0
spec_placeholders_f10                                      17  0   0   0
spec_structured_backfill                                    8  0   0   0
spin_assets_roundtrip                                       3  0   0   0
static_assets                                              10  0   0   0
sticker_preview_quality                                     2  0   0   0
sticker_reconciliation                                     22  0   0   0
sticker_skip_vision                                         5  0   0   0
sticker_trim_diffs                                          4  0   0   0
store_admin                                                13  0   0   0
store_scoped_recipe_gate_20260925                           5  0   0   0
stripe_premium_verify                                       1  0   0   0
tally_exemptions_section7                                   6  0   0   0
tco_fuel_estimates                                         42  0   0   0
team_velocity_gallery_f03                                   5  0   0   0
team_velocity_parser                                       13  0   0   0
transmission_normalize                                     27  0   0   0
trim_diff_engine                                            6  0   0   0
trim_invariant                                             22  0   0   0
trim_ladder_audit                                           1  0   0   0
trim_spec_sheets                                           31  0   0   0
upsert_data_quality_score                                   2  0   0   0
upsert_deadlock_retry                                       5  0   0   0
upsert_keeps_vdp_only_fields                                2  0   0   0
upsert_last_price_change_at                                 3  0   0   0
upsert_lot_location                                         1  0   0   0
upsert_vehicles_golden                                      1  0   0   0
upsert_vin_owner_guard                                      9  0   0   0
ux_premium_gates                                            6  0   0   0
vdp_extras_parse                                            6  0   0   0
vdp_helpers                                                 5  0   0   0
vdp_html_recovery                                           6  0   0   0
vdp_prefetch                                               19  0   0   0
vdp_recipes                                                 8  0   0   0
vdp_spec_gap                                                1  0   0   0
vdp_urls                                                    5  0   0   0
vehicle_facts                                             278  0   0   0
vehicle_history_nhtsa_recalls                               4  0   0   0
vehicle_narrator                                           17  0   0   0
verify_trim_citations                                      11  0   4   0
vision_refusal_accounting                                  18  0   0   0
visual_review_mobile_history_recalls                       10  0   0   0
vlm_ollama                                                  9  0   0   0
vpic_facts                                                  5  0   0   0
vpic_heal_derived_fields_f06                                7  0   0   0
vpic_offline_stub                                           4  0   0   0
vpic_override_cylinders                                     2  0   0   0
vpic_specs                                                  8  0   0   0
web_researcher                                              4  0   0   0
window_sticker_parse                                       13  0   0   0
window_sticker_price_filter                                 8  0   0   0
window_sticker_scan                                         2  0   0   0
window_sticker_service                                     11  0   0   0
window_sticker_ui_visibility                                8  0   0   0
window_sticker_urls                                        10  0   0   0
trim_ladder/adds_ranking                                   23  0   0   0
trim_ladder/bullet_wellformedness                          11  0   0   0
trim_ladder/citation_verifier                               7  0   1   0
trim_ladder/encyclopedia_contamination                     14  0   0   0
trim_ladder/epa_csv_revoked                                 5  0   0   0
trim_ladder/epa_file_identity                               7  0   0   0
trim_ladder/every_store_gate                                4  0   0   0
trim_ladder/provenance_gate                                12  0   0   0
trim_ladder/rung_match                                     89  0   0   0
trim_ladder/rung_name_gate                                 22  0   0   0
trim_ladder/rung_order_provenance                           5  0   0   0
```

</details>
