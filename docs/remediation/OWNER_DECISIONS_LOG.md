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
