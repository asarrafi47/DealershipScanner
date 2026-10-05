# Owner decisions from the 2026-10-05 audit follow-ups

These came out of workflow wf_c6536bac-5aa. Every step was a no-behavior-change refactor, so each item is current behavior the agents flagged and deliberately did NOT change. Nothing here is fixed yet.

## 1. Which rule decides that a recipe works

Commit: `787546e21`.

Recommendation: make `validate_recipe_set` (verdict ok/reject/uncertain, replayed through the scan's own `_replay_request`) the single gate, and reduce `validate_recipe` to a VIN counter that feeds it, because the set check matches what the scan will actually see. Line numbers below predate 4 added import lines and may be off by 4-21 lines (verifier note).

- How the minimum VIN count is applied differs by caller. pipeline/recipes.py:68 keeps a recipe only if it alone yields 5 or more VINs (n >= 5). scripts/synthesize_recipes.py:223-235 keeps any recipe with more than 0 VINs and then applies --min-vins (default 5) to the total across all kept recipes. Example: a used feed with 3 VINs and a new feed with 3 VINs is dropped by the pipeline but saved by the CLI. recipe_cascade.py:240 uses vins >= min_vins (DEFAULT_MIN_VINS=5, line 51) per adapted recipe, and that module never calls gate_recipes. Which rule should apply everywhere, and should cascade results go through the set gate?
- Pages walked: validate_recipe walks up to 40 pages (_VALIDATE_MAX_PAGES, recipe_validation.py:960), while validate_recipe_set reads only PAGES_REPLAYED=2 (recipe_validation.py:83). So validate_recipe's count is close to the whole lot, but the set verdict judges coverage from 2 pages plus the site's own total (COVERAGE_FLOOR 0.50 at line 86, SHORT_PAGE_TOLERANCE 0.95 at line 89, WHOLE_LOT_SEEN 0.90 at line 92). Which count should go into vehicle_rows and decide pass or fail?
- Fetch path: validate_recipe sends cosmos and Team Velocity JSON through synth.http._cosmos_get_json (recipe_validation.py:1046, 1137) and DEP and HTML walks through _dep_fetch_html (lines 1078, 1112). _check_recipe sends every recipe through the scan's own recipes._replay_request (line 667 `fetch = fetch or _replay_request`). Its own comment at line 82 says validation should clear the same edge the scan will. As a result, a cosmos or Team Velocity recipe can count VINs through the impersonating synth fetch and still be rejected for auth_needed or zero_rows by the set gate when replayed (or the reverse). Should there be one fetcher, and which one?
- Parser and rooftop gate: validate_recipe calls parse_kept(..., **(place or {})) with no roster place items (recipe_validation.py:991-997). _parse_rows (line 575) calls parse() with _cached_roster_place_items(base_url) merged with place. A group feed whose rooftops are named only in the roster can therefore score 0 in validate_recipe and score more than 0 in the set check.
- Stop condition: validate_recipe stops when the stored recipe.total_count is reached (line ~1027) or, for cosmos, at the page's TotalCount (line ~1149). _check_recipe stops at the site_total taken from page 1 by SITE_TOTAL_EXTRACTORS (line ~652). For DEP and HTML walks with a stale total_count, the two disagree about where to stop.
- What counts as a pass: validate_recipe returns a bare VIN count, and the caller picks the threshold. It has no notion of condition, section scoping, short pages or auth. validate_recipe_set returns ok, reject or uncertain. It rejects on one_condition (only when the other side is shown to have at least OTHER_SIDE_MIN_VINS=5 VINs, line 100), section_scoped (all recipes), short_page or coverage below 0.50, and zero_rows. Example: a carscommerce recipe scoped to one section that yields 40 VINs passes validate_recipe (40 >= 5) but is rejected by the set check. Should validate_recipe be reduced to a helper inside the set check, or keep its own pass/fail meaning?
- POST template handling: when post_template is bad JSON, validate_recipe quietly treats it as None and, for a GET, still replays (recipe_validation.py ~980-985). _check_recipe records an error 'post_template is not JSON' and stops (line ~600). The same GET recipe with a junk post_template yields VINs in one check and an error in the other.

## 2. Scanner HTTP behavior that differs per fetcher

Commit: `5986b498b`.

Recommendation, in order of payoff: (a) send replay through `scanner_proxies()` so SCANNER_HTTP_PROXY covers the main feed path (the 09-29 Railway 403s); (b) add challenge detection to VDP prefetch so a Cloudflare page is never parsed as a vehicle; (c) one fingerprint-block status set and one profile rotation for every curl_cffi path. Each is now an option change at its call site into `backend/scanner/net/client.py`, and `test_scanner_http_characterization.py` pins today's behavior, so a change shows up as an intended test edit.

- backend/scanner/recipes.py:731-744 and :786 — Recipe replay (the main scan path) passes no proxies= to curl_cffi or requests, so SCANNER_HTTP_PROXY is never used. Only HTTPS_PROXY/HTTP_PROXY can apply, through libcurl's and requests' own environment lookup. Every other curl_cffi fetcher passes scanner_proxies(). Should replay use the scanner proxy?
- backend/scanner/chain.py:272-280 — RequestsFetcher passes no proxies=, so it also ignores SCANNER_HTTP_PROXY (requests still honors the standard proxy variables through trust_env). ImpersonatingFetcher in the same file does pass the proxy.
- backend/scanner/vdp/prefetch.py:363 — VDP prefetch does no challenge detection: a 200 HTML Cloudflare 'Just a moment' page is returned as the VDP and parsed. synth/http rejects such pages (looks_like_challenge plus a 2000-byte minimum).
- backend/scanner/vdp/prefetch.py:365 vs backend/scanner/recipes.py:711 — the fingerprint-block status sets differ. Prefetch treats {403, 429, 503} as a block and skips the requests fallback; replay and synth escalate on {403, 405, 429}. A 405 in prefetch falls back to plain requests, and a 503 in replay does not escalate to impersonation.
- backend/scanner/vdp/prefetch.py:360 and backend/scanner/vdp/vdp_recipes.py:266 — Prefetch and VDP JSON use the single profile 'chrome' with no rotation. Chain, synth and replay rotate chrome, chrome124 and safari17_0 (29 of 30 cleared, versus about half for chrome alone).
- backend/scanner/vdp/vdp_recipes.py:278-281 — _fetch_json never falls back to plain requests on a curl_cffi error (it returns (0, None)); it falls back only when curl_cffi is not installed. Prefetch does fall back on errors.
- Timeouts differ per site: synth 25s (synth/http.py fetch_dealer_html, _dep_fetch_page, _cosmos_get_json), chain ImpersonatingFetcher 25s / RequestsFetcher 20s (chain.py:219, 268), replay 20s (recipes.py:508), prefetch 15s (vdp/prefetch.py:49), vdp_recipes 15s (vdp/vdp_recipes.py:39).
- Retries and pacing differ: only synth/http.fetch_dealer_html retries (429/5xx, sleeps of 2s then 4s, synth/http.py:184), and only synth paces (the _pace hook, SCANNER_SYNTH_FETCH_DELAY). Replay, prefetch, vdp_recipes and chain have neither.
- User agents and headers differ: chain sends UA only, with Chrome/124.0 (chain.py:59-61). synth sends the full navigation header set with Chrome/124.0.0.0 and Accept-Encoding: identity (synth/http.py:52). Replay sends Chrome/124.0.0.0 with Origin and Referer. Prefetch and vdp_recipes send Chrome/126.0.0.0. Chain ImpersonatingFetcher sends no extra headers, only curl_cffi's profile defaults.
- Other curl_cffi call sites outside the shared layer, left alone because they were not in this step's list: backend/scanner/pipeline/recipes.py:39 (homepage status probe, no proxy, timeout 20), backend/scanner/synth/platforms/wp_vehicles.py:27 (no proxy, no pacing, timeout 30), backend/scripts/discovery_probe.py:58, backend/scripts/audit_unscannable_dealers.py:122.

## 3. Brochure acquisition CLI

Commit: `d096c2221`.

Recommendation: keep the ledger provenance strings (readers match on them); subcommands are optional polish.

- Ledger provenance strings still say "fetcher": "backend.scripts.fetch_oem_brochures" (backend/enrichment/brochure_acquisition/corpus.py:89) and "backend.scripts.fetch_oem_brochures --html-specs" (html_specs.py:337, html_specs.py:407). I kept them so existing ledger readers see the same value. Should they name the new modules?
- Subcommands (plan|download|quarantine|audit|html-specs|probe), which the audit recommends, were not added in this step. Do you want them, with the old flags kept as aliases?
- --probe-reachability uses only the first --brand (backend/enrichment/brochure_acquisition/reachability.py:74), while --archive-verify-overlap and the default mode use every --brand given. This is how it already worked, unchanged. Is it intended?
- In default and --html-specs mode, --user-agent both silently falls back to 'browser' (backend/enrichment/brochure_acquisition/plan.py:129, and the same in html_specs_mode). This is how it already worked. Should it be rejected instead?
- --quarantine-unidentified always goes on to run quarantine_derived (backend/enrichment/brochure_acquisition/quarantine.py:142/164/208), so one flag sweeps the derived stores too, including under --apply. This is how it already worked, unchanged. Is that confirmed as intended?

## 4. Test-suite findings

Commit: `39d7241db`.

Recommendation: block unresolvable hostnames in the SSRF guard (fail closed).

- backend/utils/outbound_url.py:45 - destination_host_blocked_after_dns allows hostnames that cannot be resolved (it returns False when the lookup fails). The guard is there to stop requests to private addresses, but this lets through a name that fails to resolve at check time and resolves privately later (DNS rebinding). Should unresolvable names be blocked? The new test in test_outbound_url.py pins the current allow behavior.
- backend/enrichment/nhtsa_vpic.py:342 - the vPIC HTTP getter is a closure inside fetch_decode_vin_values_extended, so a test can only stub it by swapping the `urllib` name inside the module. Moving it to a module-level function would make it easier to stub. That is a small production refactor, so I did not do it in this tests-only step.
