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
