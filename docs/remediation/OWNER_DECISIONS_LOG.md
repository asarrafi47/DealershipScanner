# Owner decisions log (remediation plan 2026-10)

Answers to the decisions in `docs/REMEDIATION_PLAN_2026_10.md` Section 7, in the order given.

| Date | Decision | Answer | Notes |
|---|---|---|---|
| 2026-10-07 | D-REL1 legacy scanner-worker / scanner-scheduler | (a) stop + delete | Record variable names, settings, volumes first. Volumes dropped only after a separate owner OK on the dry-run list. |
| 2026-10-07 | D-IF7 data-quality / rooftop-refusal report jobs | (a) unload on the MBP now, re-home later (P18A.6) | Plist files kept. |
| 2026-10-07 | Phase 0A prod access | An agent runs the read-only parts (Railway audit, prod census in a READ ONLY session, web container file list) | No secret values written. Service deletion waits for the owner's OK on the dry-run list. |
| 2026-10-07 | Phase 0A/0B approvals | Approved: P0A.6 offline-suite baseline (AC power); 0B network measurements from the MBP on AC; D-SEC1 stopgap `CAR_CHAT_WEB_RESEARCH=0` on Railway web + redeploy | NOT approved: SSH to the mini. The mini parts of P0A.5 and P0B.5 stay BLOCKED. |
| 2026-10-07 | Battery | Owner allows the running Phase 0A/0B and 1A work, incl. the P0B.3 Chromium measurement, to continue on battery | Overrides the "AC only" condition above for this run. |
| 2026-10-08 | D-RS5 audit/attribution scripts write recipe state? | Yes, keep writing (owner overrode the "No" recommendation) | P1B.5 (persist=False) DROPPED. Geocode, rooftop attribution and coverage audit keep marking stale / updating coverage. |
| 2026-10-08 | D-SR7 test-leak artifacts | Delete after a tarball backup (dry-run first) | P1C.1 |
| 2026-10-08 | Phase 1C local data repairs | Dry-run first; owner OKs each apply after seeing the counts | P1C.3, P1C.4 |
| 2026-10-08 | Mini SSH for Phase 1C | Not now | Mini steps of P1C.1/P1C.4 BLOCKED; the mini must not scan until repaired. |
| 2026-10-08 | D-REL3 VERSION bump scope | (a) every push bumps; CI release-guard judges only pushes/PRs to main; ci/* shakedown branches may push without a bump during P2A.5; a release needs a CHANGELOG section | P2A.4, P2A.5, P2B.1, P2B.7 |
| 2026-10-08 | D-TC5 CI dependency install | Full requirements.txt with CPU torch installed first | P2A.2 |
| 2026-10-08 | Phase 1C applies | Approved on the MBP: P1C.1 leak cleanup, P1C.3 backfill (200 rows), P1C.4 reconcile (re-dry-run after the backfill), P1C.5 report regen | Mini still BLOCKED. |
| 2026-10-08 | P1C.4 saved_at=0 rule | A saved_at=0 recipe counts as written at its newest last_ok_at (keep the 09-29 synthesized sets) | Departs from the literal plan text on purpose. |
| 2026-10-08 | P1C.4 overlapping recipes (7 dealers) | Keep both now; clean up in Phase 7 | andymohrford, dchparamushonda, stevenscreektoyota, huntersvilleford, bmwofmtlaurel, kunilexusofgreenwoodvillage, tuttleclickmazda |
| 2026-10-08 | Phase 1 release step | Approved: bump_version.sh patch (1.5.3), secrets scan, push feature/http-only-scans to origin (no deploy, nothing to main) | |
| 2026-10-08 | D-REL2 branching | (a) trunk: phase/* branches fast-forwarded into main; retire feature/http-only-scans after the first release | |
| 2026-10-08 | D-REL4 unmerged remotes | (a) archive/* tag then delete; tell ksarrafi first | owner tells ksarrafi |
| 2026-10-08 | D-REL5 repo visibility | (a) keep PUBLIC | D-HY2 (7 history accounts) still owed |
| 2026-10-08 | D-REL6 main protection | (a) ruleset: no delete/force-push, 4 required checks pinned to Actions, owner bypass | |
| 2026-10-08 | D-REL7 retro tags | yes, 1.3.2..1.5.2 | |
| 2026-10-08 | D-REL8 first deploy | (b) web on the first release; scanner-nightly at P6B.1 | |
| 2026-10-08 | D-REL9 first release level | patch | |
| 2026-10-08 | D-REL10 scanner-nightly deploys from a tag | (a) yes, with loud ALLOW_UNRELEASED_DEPLOY=1 override | |
| 2026-10-08 | CLAUDE.md release wording (P2B.1/P2B.7 proposals) | Add both lines (main release guard; RELEASING.md pointer) | |
| 2026-10-08 | Web GitHub source | Owner disconnects web Source in Railway | removes the 09-11 rebuild-from-main hazard |
| 2026-10-09 | D-SEC1 Chromium in web | (a) remove from web; WebResearcher HTTP-only by default; operator trim script keeps browser mode | P3.8; based on P0B.3 |
| 2026-10-09 | D-SEC4 OAuth linking | (a) no auto-link; link only after a password login; email-only lookup | P3.1 |
| 2026-10-16 | Next-phases plan (`docs/NEXT_PHASES_PLAN_2026_10.md`) gating minimum | Owner: "go ahead on the rest" (all recommendations accepted except D-N4, below) | Asked in plain terms on 10-16; rows below record each answer. |
| 2026-10-16 | D-N25 approvals bundle (through N12) | Approved as recommended | Paced read-only live fetches from the home IP (≤3 pages per dealer for dossiers, ≤30 per host for platform maps, ≥2 s apart, no browser); the listed scratch-DB scans (`cars_cr_test`, `cars_cr_n6`, `cars_cr_n8_<unit>`, `cars_cr_n8`, `cars_cr_n9`, `cars_cr_rc`, `cars_cr_rc_b`, `cars_cr_conv`, N11.13 into `cars_cr_rc`, P5A.8 shadow into `cars_rehearsal` with `SCANNER_RECONCILE=dry`); the P5B.8 rescans into `cars`; one multi-GB restore of `cars` into `cars_rehearsal`. Docker builds stay a per-run ask. |
| 2026-10-16 | D-N9 the 263 unanalysed dealers | C+D | Group dossiers now (N1.2), mechanical audit of all 458 (N1.3), the ≤46 signal dealers on rehearsal data (N11.10); the 8 pause-killed dealers rescanned, not analysed. Option A (~120 agents) dropped. |
| 2026-10-16 | D-N1 rehearsal DBs | As recommended | `cars_cleanroom` frozen at V025 as the read-only baseline; re-runs into new `cars_cr_*` DBs (`build_db.sh` or `createdb -T`); `cars` stays the prod-shaped repair rehearsal DB. |
| 2026-10-16 | D-N2 mini role | (b) no scanning role for now | MBP is the home host and canonical log host. Implies D-DB5 (b), D-IF10 = MBP, D-IF1 host = MBP. The mini returns only via N4.4 after SSH approval and the P1C repairs. |
| 2026-10-16 | D-N3 group-pool ownership | As recommended | (1) A store whose own feed stamps the car at a strong tier owns it; (2) otherwise the designated primary in tracked `backend/config/group_pools.json`, `dealer_feed_scope='group'`, site shows "listed by N group stores; location unconfirmed"; (3) pairs not in the file keep their owner and are flagged. Multi-store schema deferred. |
| 2026-10-16 | D-N4 rooftops missing from the roster | **Changed by owner: profile and scan them** | Owner: "we should build a profile for these and scan them when we see cases like this." Each missing rooftop becomes its own dealer: full NETWORK_SCAN_PROCESS loop (discovery, location verify, recipe, test scan, vPIC verify, five dealer_logs files) and a roster entry, so its cars are scanned and filed under it. Standing rule: whenever a scan sees cars from a rooftop not on the roster, that rooftop is queued for profiling (N8.13). Until its profile exists, its cars are held (refused at the gate with the rooftop recorded), never filed under a sister store. New units: N1.11 (first profiling batch), N8.13 (detector and queue). |
| 2026-10-16 | D-RC1 retirement writer | A: the scanner writes, the pipeline reports | P5A.4 |
| 2026-10-16 | D-RC2 one miss or two strikes | Two-strike, ≥20 h apart | P5B.4 |
| 2026-10-16 | N1 run approvals | Owner "go ahead" | Run N1 as a workflow: `phase/N1` from `origin/main`, unit worktrees, `git switch phase/N1` in the main checkout before N1.3's 2-dealer `cars_cr_test` smoke, N1.11 profiling scans into scratch DB `cars_cr_rooftops`. No prod access by agents; push/tag of v1.5.5 waits for the owner's OK. |
| 2026-10-18 | Decision brief (`docs/remediation/DECISION_BRIEF_2026_10.md`, 105 open decisions) | Owner: "okacceptall": every recommendation accepted as written in the brief, including the changed D-N6 | Asked in plain terms on 10-18 with the visible ones named (D-N12, D-N19, D-N14, D-N6, D-IF1). Rows below record each answer. The same reply approved the v1.5.5 push (N1.10, no deploy). |
| 2026-10-18 | D-N12 a New car with no odometer value (IL-01) | As recommended: Complete. Show no mileage line on New cars. Keep "Mileage not listed" on used cars | Needed by N2.1, today |
| 2026-10-18 | D-N19 show "Inventory last checked <date>" while prod is stale | As recommended: Yes. One plain text line per dealer page and car page, with no card or box. It hides itself once the data is under 48 h old | Needed by N3.11 |
| 2026-10-18 | D-DEP1 remove orphan dependencies and lift lxml to 6.1 or later | As recommended: (a) | Needed by P15B.1 (inside N3) |
| 2026-10-18 | D-SEC6 X-Forwarded-For spoofing sent straight to the Railway origin | As recommended: (c) | Needed by N3.13 (the owner env step after P3.2 ships) |
| 2026-10-18 | D-SEC5 count the admin pass-through fix as applied | As recommended: Applied in effect. `DEV_IP_ALLOWLIST` stays unset until P3.2 ships and the Cloudflare CIDRs are live | Needed by N3.13 |
| 2026-10-18 | D-DB6 how migrations reach prod | As recommended: (a) | Needed by P4.8 and every later migration |
| 2026-10-18 | D-DB7 objects the code relies on that the migration chain does not create | As recommended: (a). `NULLS NOT DISTINCT` is fine: prod runs 18.6, local runs 17.10 | Needed by P4.3 |
| 2026-10-18 | D-PR8 the InventoryEnricher's Haiku fallback and `haiku_spec_cache` | As recommended: (a) | Needed by P4.3 |
| 2026-10-18 | D-N30 the 4 untracked log tables | As recommended: Adopt in V026, then export and drop them in P13B.4 with the other backup tables (D-DB1) | Needed by P4.3 |
| 2026-10-18 | D-N13 the new-arrival window | As recommended: 3 days, for price and photo gaps only. Show a plain "details coming" line, and never hide a car inside the window. The window is measured against the car's latest scrape, not the wall clock | Needed by N6.4 |
| 2026-10-18 | D-N14 price floor | As recommended: $1,000. A value repeated across 5 or more models at one dealer is withheld (stored as NULL with a provenance note) | Needed by N6.7 |
| 2026-10-18 | D-N15 do placeholder-only images count as photos | As recommended: No. A placeholder-only car counts as "no photo", and "no photo" counts as incomplete only outside the D-N13 window | Needed by N6.8 |
| 2026-10-18 | D-N16 MY2026-27 cars filed as used at 0 miles (F4) | As recommended: Report it as a data question and do not count these cars as incomplete | Needed by N6.9 |
| 2026-10-18 | D-N17 identifier gate at write time | As recommended: Refuse anything that is not 17 characters now (order numbers, Koons' 8-character codes). The I/O/Q rows wait for D-N17b in N21 | Needed by N6.9 |
| 2026-10-18 | D-PR1 window sticker vs vPIC | As recommended: (a) | Needed by P8A.2 (inside N6) |
| 2026-10-18 | D-PR4 scope of the drivetrain catalog tiebreak | As recommended: (a). On the sub-question: yes, when vPIC says 4x2 or nothing and the trim decoder agrees. A less specific feed value never replaces a stored drive end | Needed by P8A.2 |
| 2026-10-18 | D-PR6 several listing signals contradict vPIC on drive | As recommended: VPIC wins, and the row is marked cannot_assess | Needed by P8A.2 |
| 2026-10-18 | D-PR7 vPIC cylinders at write time | As recommended: Only when they do not contradict the engine text | Needed by P8A.2 |
| 2026-10-18 | D-N24 replacement for Railway config-as-code | As recommended: Whichever one Railway documents as supported after 12-01. N4.1 verifies that before building anything | Needed by N4.1 |
| 2026-10-18 | D-PLAN1 run the Postgres harness right after N5 | As recommended: Yes. The plan already schedules N7 this way; a "no" moves N7 to later | Needed by N7 |
| 2026-10-18 | D-TC3 Postgres image for CI | As recommended: Match prod: pg18 with pgvector 0.8.x | Needed by P14A.3 |
| 2026-10-18 | D-TC6 the `integration` marker and CI job | As recommended: Replace the job and keep the marker (it means "needs live data, dev machine only") | Needed by P14A.3 |
| 2026-10-18 | D-TC9 may tests create `pytest_*` databases on the local server that holds `cars` | As recommended: Yes, guarded. The guard refuses `cars`, `cars_cleanroom`, non-local hosts and port 15432 | Needed by P14A.1 |
| 2026-10-18 | D-N5 Hendrick used cars at Terry Labonte's street address (RET-9) | As recommended: Yes | Needed by N8.8 |
| 2026-10-18 | D-N6 scope of mbbeverlyhills-com and fletcherjones-com | As recommended (brief's changed form): keep mbbeverlyhills-com, recipe pointed at CarsCommerce `listings/6074385` shipped with the parser fix that reads the store label; drop fletcherjones-com as a store: N8.9 first moves its rows to the stores they name (Henderson now, the Las Vegas stores once N1.11 has profiled them), then it leaves the scan roster | Needed by N8.9 |
| 2026-10-18 | D-N7 may `vdp_host_other_store` also un-list cars | As recommended: Refuse the write only, until N11 proves the rule | Needed by N8.5 |
| 2026-10-18 | D-N34 clean up the 7 overlapping-recipe dealers in N8.9 instead of N13 | As recommended: Yes. Keep one recipe set per dealer and archive the other in the repair file. If you decline, the clean-up stays in N13 as answered on 10-08 | Needed by N8.9 |
| 2026-10-18 | D-N8 role of `dealer_feed_scope` (CF-15) | As recommended: Display only, refreshed from the scanner's `group_feed_unstamped` flag with your OK | Needed by N9.12 |
| 2026-10-18 | D-RC3 how far a replay walk goes | As recommended: Walk to the total, capped at 150 (`SCANNER_REPLAY_MAX_PAGES`) | Needed by P5B.2 |
| 2026-10-18 | D-RC7 add the `listing_retirements` table | As recommended: Yes | Needed by P5B.3 |
| 2026-10-18 | D-SR4 verdict for a truncated replay | As recommended: (a), and reconcile also reads the flag | Needed by P5A.7 |
| 2026-10-18 | D-N10 does a first pass with verdict `no_rows` write its rows | As recommended: Write. The prior-window rule and the two-strike rule protect against wrong retirements | Needed by P5A.1 |
| 2026-10-18 | D-N11 accept that failing closed keeps sold cars listed for one or two more runs | As recommended: Yes | Needed by P5A.1 |
| 2026-10-18 | D-N21 alarm threshold for the first live prod retirement | As recommended: Gap-aware. Alarm when a dealer retires more than min(60%, 10% + 1.5% × days since its rows were last scraped) of its prior active rows. Nightly runs keep the fixed 10% | Needed by P5A.7, N11.7 and N12.8 |
| 2026-10-18 | D-IF5 how the VIN guard survives gaps between scans | As recommended: Keep the 48 h default. Gap cover applies only to owners with a persisted strong tier. No blanket 72 h. Known pairs go through D-N3's `group_pools.json` | Needed by P6A.3' |
| 2026-10-18 | D-SR2 stopping two hosts from scanning the same dealer at once | As recommended: (c) | Needed by P11A.3 |
| 2026-10-18 | D-RC5 wrong-retirement candidates that cannot be verified live | As recommended: Rescan first. Restore only what is verified live; send cannot_assess dealers to dealer-discovery | Needed by P5B.8 |
| 2026-10-18 | D-RC6 repair order | As recommended: Local, then prod | Needed by P5B.8 |
| 2026-10-18 | D-IF4 nightly run settings | As recommended: 8 × 6 at `0 9 * * *`, alarm at 14 GB (under the 16 GB cap) | Needed by N12.7 and N12.11 |
| 2026-10-18 | D-N22 prod ownership before scanning resumes (CF-14) | As recommended: Let the rules re-file during fleet #1, with snapshots and `car_move_log`. Reset by hand only pairs that DB checks verify, and only if any remain after fleet #2 | Needed by N12.7 |
| 2026-10-18 | D-N23 guard hours for fleet #1 | As recommended: The default 48 h | Needed by N12.7 |
| 2026-10-18 | D-IF1 the dealers that answer only from a home IP | As recommended: Interim, you run a bridge from the MBP every 3 days or less; final (N14), (a) on the MBP | Needed by N12.10 (interim) and N14 (final) |
| 2026-10-18 | D-RS1 Which recipe store is canonical | As recommended: (b) now; (a) once Railway does all scanning | Needed by P7.9 |
| 2026-10-18 | D-RS2 What re-synthesis does to old recipes | As recommended: Replace-with-archive | Needed by P7.5 |
| 2026-10-18 | D-RS3 How far versioning goes | As recommended: DB-authoritative with CAS | Needed by P7.1 |
| 2026-10-18 | D-RS4 Blocked status per host | As recommended: Per host | Needed by P7.6 |
| 2026-10-18 | D-IF3 Who runs admin "onboard dealer" jobs | As recommended: (a); P11A.5 is then dropped and D-SR1 needs no answer | Needed by P15A.3 |
| 2026-10-18 | D-IF6 Fate of the retired laptop nightly and k8s crons | As recommended: Per step, from the N14.1 desk review | Needed by P15A.4 |
| 2026-10-18 | D-N28 Replace the multi-GB scratch restore with a desk review of code and git history | As recommended: Yes; restore only for a step the review cannot settle | Needed by N14.1 |
| 2026-10-18 | D-PR2 May a rescan overwrite a higher-ranked value | As recommended: (a), only while the stored provenance still matches the column | Needed by P8B.1 |
| 2026-10-18 | D-PR3 Dictionary engine correction | As recommended: (b) + (d) | Needed by P8B.5 |
| 2026-10-18 | D-PR5 How every scan path gets a vPIC decode | As recommended: (a) | Needed by P8B.4 |
| 2026-10-18 | D-N32 Does a noncanonical body style count as missing | As recommended: Normalize only | Needed by P8B.6 |
| 2026-10-18 | N17 heal window When the prod fact heal runs | As recommended: A weekday with no fleet running, after the N16 deploy is verified | Needed by P8C.7 |
| 2026-10-18 | D-OD3 Validation fetches through the scan's own replay | As recommended: (A) | Needed by P12A.2 |
| 2026-10-18 | D-OD7 One scanner fetch policy | As recommended: (A), confirmed by N18.1; 429 gets a backoff, never rotation | Needed by P12A.2 |
| 2026-10-18 | D-OD8 Retry transient errors in replay | As recommended: One retry after 2 s, only if N18.1 or P6B.4 sees transient 429/5xx | Needed by P12A.2 |
| 2026-10-18 | D-OD13 Does the DuckDuckGo spec search go through the proxy | As recommended: (a) | Needed by P12A.3 |
| 2026-10-18 | D-OD1 Minimum-VIN rule | As recommended: Revised: one constant `RECIPE_MIN_VINS=5`, with the condition-pinned exception | Needed by P12B.5 |
| 2026-10-18 | D-OD2 Which count decides pass | As recommended: (A) | Needed by P12B.2 |
| 2026-10-18 | D-OD4 `validate_recipe` | As recommended: Thin wrapper | Needed by P12B.3 |
| 2026-10-18 | D-OD5 Cascade recipes go through the set gate | As recommended: Yes | Needed by P12B.6 |
| 2026-10-18 | D-OD6 GET recipe with a non-JSON `post_template` | As recommended: Mirror the scan | Needed by P12B.3 |
| 2026-10-18 | D-HY1 Where the vehicle dictionary lives | As recommended: Keep tracked | Needed by P9.7 |
| 2026-10-18 | D-HY9 Serve trim-ladder content in prod | As recommended: (a) | Needed by P9.8 |
| 2026-10-18 | D-N18 Which price is "the price" | As recommended: The advertised price before taxes and government fees; MSRP only in `msrp`; add-on fees only if the site includes them | Needed by N21.1 |
| 2026-10-18 | D-N17b 17-character IDs with I, O or Q | As recommended: Keep the rows, skip the vPIC decode, mark VIN-derived facts cannot_assess, leave them out of the VIN guard | Needed by N21.2 |
| 2026-10-18 | D-N31 May a single vPIC trim fill feeds with no trim (Gardena) | As recommended: Yes, provenance `vpic`, only when vPIC returns exactly one trim | Needed by N21.4 |
| 2026-10-18 | D-N33 Dealer-relative interior color rule | As recommended: Not now; revisit after N21 | Needed by N21.7 |
| 2026-10-18 | D-EPA1 How the 12,126 dictionary EPA CSVs are corrected | As recommended: (a); you approve the pinned file (2026-09-29, 50,407 rows) | Needed by P10A.5 |
| 2026-10-18 | D-ET2 How "naturally aspirated" is shown | As recommended: (a); revisit after the P10C.9 dry run | Needed by P10A.1 |
| 2026-10-18 | D-ET3 Rows flagged both turbo and supercharged | As recommended: Turbocharged; raw flags kept | Needed by P10A.1 |
| 2026-10-18 | D-ET4 Role of the make/model heuristic | As recommended: Last resort | Needed by P10C.2 |
| 2026-10-18 | D-ET5 Twin-turbo labels | As recommended: Keep, one shared helper | Needed by P10A.1 |
| 2026-10-18 | D-ER1 The one way EPA data enters `epa_master` | As recommended: (a) | Needed by P10B.3 |
| 2026-10-18 | D-ER2 Permanent catalog key | As recommended: `epaId` when present, else a sticky content key | Needed by P10A.2 |
| 2026-10-18 | D-ER3 The 20,006 legacy catalog rows | As recommended: Re-resolve, then archive unreferenced rows | Needed by P10B.6 |
| 2026-10-18 | D-ER4 Foreign-key policy | As recommended: RESTRICT, after a 0-dangling check on prod | Needed by P10D.7 |
| 2026-10-18 | D-ER5 Root `DICTIONARY/` folder | As recommended: Git rm after the prod provenance backfill | Needed by P10D.8 |
| 2026-10-18 | D-ER6 How EPA work reaches prod | As recommended: The agent prepares each step; you approve and run it | Needed by P10D.6 |
| 2026-10-18 | D-N29 Accept the conditional tail units with revival triggers | As recommended: Accept (plan Section 8 lists each trigger) | Needed by N26 |
| 2026-10-18 | D-SR6 Where the upsert measurement runs | As recommended: MBP | Needed by P11B.1 |
| 2026-10-18 | D-DB3 Once every DB is strict | As recommended: (a); `INVENTORY_AUTO_MIGRATE=1` kept for dev | Needed by P13B.1 |
| 2026-10-18 | D-DB4 Where boot-time DB init runs | As recommended: (a) | Needed by P13A.5 |
| 2026-10-18 | D-DB8 `cars.zip_code` | As recommended: (b) keep; no migration | Needed by P13A.8 |
| 2026-10-18 | D-DB1 Backup tables | As recommended: (a), after N25; list adds prod `cars_owner_backup_20260929` (6,961 rows), the 4 log tables and the N12 backups | Needed by P13B.4 |
| 2026-10-18 | D-SEC7 Lock files per image | As recommended: (a), run only if confirmed | Needed by P15B.8 |
| 2026-10-18 | D-SEC2 Dead TOTP/phone storage | As recommended: (a) | Needed by P16.2 |
| 2026-10-18 | D-SEC3 Users data isolation on Postgres | As recommended: (a), sized by P16.10 | Needed by P16.10 |
| 2026-10-18 | D-TC4 Breadth of the Postgres test tier | As recommended: Targeted | Needed by P14B.6 |
| 2026-10-18 | D-TC8 pytest-xdist | As recommended: Decide from P14C.5 | Needed by P14C.5 |
| 2026-10-18 | D-DC1 Confirm the HTTP-only plan decisions | As recommended: Confirm; D2 now reads "MBP primary" | Needed by P17B.11 |
| 2026-10-18 | D-DC5 Retire `--allow-browser` and the Playwright fallbacks | As recommended: (a) | Needed by P17B.3 |
| 2026-10-18 | D-DC6 Dealer-group LLM adjudication (cannot run today) | As recommended: (b) | Needed by P17B.9 |
| 2026-10-18 | D-DC7 `backend/oem/vehicle_reference` | As recommended: (b) | Needed by P17B.10 |
| 2026-10-18 | D-DC9 The `requires_browser` scan hint | As recommended: (c) | Needed by P17B.7 |
| 2026-10-18 | D-OD9 Brochure CLI | As recommended: Recommended set | Needed by P17C.2 |
| 2026-10-18 | D-OD10 Move the vPIC getter to module level | As recommended: Yes | Needed by P17C.1 |
| 2026-10-18 | D-OD11 Scanner env knobs | As recommended: Registry + lint first | Needed by P17C.6 |
| 2026-10-18 | D-HY3 `dealer_logs` in git | As recommended: (a) | Needed by P18A.4 |
| 2026-10-18 | D-HY4 The 47 force-added `workspace/` files | As recommended: (a) | Needed by P18A.3 |
| 2026-10-18 | D-IF9 Deploy assets to delete | As recommended: Delete the k8s, car-scanner and postgres manifests and the k8s block of `build.sh`; keep launchd plists until replaced | Needed by P18A.7 |
| 2026-10-18 | D-TC7 How far ruff goes | As recommended: First option; later waves conditional (D-N29) | Needed by P18A.8 |
| 2026-10-18 | D-HY6 Single entry document | As recommended: (a) | Needed by P18B.1 |
| 2026-10-18 | D-HY7 Location-logging contract | As recommended: (a); you approve the CLAUDE.md wording | Needed by P18B.7 |
| 2026-10-18 | Settled by D-N2 (b), confirmed by "accept all" | D-DB5 = (b) the mini's Postgres is retired; D-IF10 = the MBP holds the canonical `dealer_logs`; D-IF1 host = the MBP | No separate answer needed (brief "Read this first") |
| 2026-10-18 | No answer needed (brief) | D-RC4, D-DC10, D-OD12, D-HY8 keep their defaults; D-SR1 is moot because D-IF3 = (a); D-HY2 stays an owner action | |
| 2026-10-18 | D-N4 scope: does D-N4 apply to missing rooftops outside the 22 roster states (UT, NM, ID, LA, KY, WV)? | Owner: "yes they should be added" (against the recommendation to wait). Out-of-state rooftops get profiles, scans and roster entries like any other; the 3 KY batch-1 profiles stand; batch 3 of N1.11 profiles the 13 out-of-state batch-2 records plus Ford of Harvey; N8.13 queues out-of-state candidates too | Needed by N1.11 batch 3, N8.9, N8.13 |
