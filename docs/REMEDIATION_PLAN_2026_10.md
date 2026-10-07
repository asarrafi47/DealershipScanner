# Remediation Plan, October 2026

- Written: 2026-10-07, against VERSION 1.5.2, branch `feature/http-only-scans`, HEAD `1344cbd09`.
- Sources: 14 cluster specs (release, reconcile, recipes, epa-turbo, epa-rebuild, precedence, db-layer, tests-ci, infra, hygiene, dead-code, owner-decisions, security, scanner-runtime), each with an adversarial reviewer verdict. Every verdict was applied: disputed claim statuses are corrected (Appendix A), unit issues are folded into the units below, missing units are added, and work resting on refuted claims is dropped (Appendix B).
- Scope: about 305 work units in 39 phases. Each phase is one Claude Code Workflow run.

## 1. Purpose

This plan turns the October 2026 audit findings into an ordered sequence of small, reviewable workflow runs. The order follows risk:

1. Stop the data damage that is happening now and disarm the destructive scripts (retirement correctness, the `epa_master` reload, recipe-store sync, the stale scanner worker still attached to prod).
2. Build the release and CI foundation, so every later phase is tested on GitHub and ships as a tagged release.
3. Fix data correctness and repair the data (retirement, recipes, vehicle-fact precedence, dictionary lookups, EPA catalog, forced induction, recipe validation).
4. Finish the DB layer and put the test suite on Postgres.
5. Infra: Railway nightly, home lane, image slimming.
6. Consolidation and dead code.
7. Hygiene and docs.

Security items are slotted by severity: a hotfix in Phase 1A, a high-severity wave in Phase 3, the rest in Phase 16.

Section 6 explains where the order departs from that outline and why.

## 2. How to run a phase as a workflow

Conventions every phase relies on:

- **Integration branch.** Until Phase 2B this is `feature/http-only-scans`. After Phase 2B it is chosen by D-REL2. Under the recommended trunk model, each phase works on a short-lived `phase/<id>` branch cut from `main` and fast-forwarded into `main` at its release step.
- **Worktrees.** One git worktree per unit, created from the integration branch at the start of its wave. Some units are marked **MAIN**. These touch `workspace/` (gitignored, so absent from worktrees; `RECIPES_DIR` and `DEALER_LOGS_ROOT` resolve there), the local Postgres, Railway, or git refs. MAIN units run serially in the main checkout, after the code units they depend on have merged.
- **Waves.** The `Grp` column holds a wave letter. Units with the same letter run in parallel. Wave B starts only after every wave-A unit has merged, and so on. Units in one wave never share a file (Appendix C.2 lists the serialized files). The one exception is the append-only log below.
- **Append-only ops log.** When units of the same wave each add a block to `docs/SCANNING_OPS_LOG.md`, each writes `docs/ops_log/<unit id>.md` instead. The phase's integration step appends those blocks to SCANNING_OPS_LOG.md in unit-id order and deletes the block files.
- **Test paths.** Test file names in exit gates live under `backend/tests/` unless a path is given.
- **Tests.** Run only the unit's targeted tests, in chunks under 120 s per tool call (workflow agents are killed after 180 s per call). Use `run_in_background` for anything longer. Never run the full suite in one call. From Phase 2A on, CI runs the full offline suite on every push.
- **Machine load.** No heavy local work without the owner's OK: no Docker builds, no full-suite runs, no full-fleet relinks while the MBP is on battery. Mini commands need the `INVENTORY_DATABASE_URL=postgresql://localhost:15432/cars` prefix.
- **Prod.** Agents never get prod access on their own. Units marked `needs prod` are prepared by an agent (command pack, dry-run script, backup command) and executed by the owner, or by an agent the owner explicitly authorizes for that unit only.
- **Dealer logs.** Every dealer touched by a scan, rescan, repair or measurement gets dated blocks under `workspace/dealer_logs/<dealer_id>/` (discovery.md, scan_runs.md, summary.md, scan_instructions.md, location evidence per D-HY7), and general lessons roll up into `_learning/`, as `docs/NETWORK_SCAN_PROCESS.md` requires. A scan without its logs is not finished.
- **Data rules.** vPIC outranks the dealer feed for drivetrain and electrification. The EPA catalog never outranks the dealer's own engine text or the VIN decode. Never write `ON CONFLICT DO NOTHING` where a dropped row would go unnoticed. Grep every consumer before deleting anything ("audit deletions before editing").
- **Releases.** Never bump `VERSION` inside a unit. Each phase ends with one release step: CHANGELOG `[Unreleased]` lines, `scripts/bump_version.sh <level>`, a secrets scan of the range, then push. The pre-push hook refuses a push whose VERSION is unchanged. Never commit `.env`.
- **Migrations.** Every migration follows the D-DB6 protocol in Appendix C.1. Apply it to the local Postgres (and the mini's, if live) as part of the merge. Apply it to prod by hand, after a verified backup, before deploying code that contains it. Migration numbers come from the registry in Appendix C.1.

### Template prompt (paste into a new Claude Code session, edit the brackets)

```
Run Phase <ID> of docs/REMEDIATION_PLAN_2026_10.md as a Workflow.

Answered owner decisions for this phase:
  <D-xxx: chosen option>   (one line each; the phase lists the ones it needs)
Owner approvals for this run:
  <e.g. "Docker build on AC power OK", "P6B.2 prod apply approved after I review the dry-run">

Rules:
1. Read the phase section, every decision it cites (Section 7), CLAUDE.md,
   docs/NETWORK_SCAN_PROCESS.md, and the cross-cluster notes in Appendix C.
2. Integration branch: <feature/http-only-scans | phase/<ID> from main>.
3. Fan out by wave. One agent per unit, each in its own git worktree off the
   integration branch. Units marked MAIN run serially in the main checkout
   after their dependencies merge. Do not start a unit whose blocking
   decision is unanswered: mark it BLOCKED and continue.
4. Each implementing agent does exactly its unit (Change / Tests / Accept),
   runs only that unit's tests in chunks under 120 s, never touches prod,
   never bumps VERSION, never commits .env, and writes dealer logs for any
   dealer it touches.
5. Run an adversarial reviewer on each unit. It checks every Accept item one
   by one, the phase's review lens, the audit-deletions rule, SQLite-vs-
   Postgres behaviour, and that the diff stays inside the unit's file list.
   Send failures back to the implementer, at most 2 rounds.
6. Integrate in the phase's stated order onto the integration branch. Re-run
   the Exit gate checks and add CHANGELOG [Unreleased] lines.
7. Stop before the Release step and report: units DONE / BLOCKED / DROPPED,
   exit-gate results, anything that needs my decision, and the exact
   release commands to run.
```

## 3. Status legend

| Status | Meaning |
|---|---|
| TODO | Not started; entry gate not yet checked |
| BLOCKED | Waiting on an owner decision or a prior phase |
| READY | Entry gate met; can run |
| IN PROGRESS | Workflow running |
| IN REVIEW | Integrated; waiting for owner review between phases |
| DONE | Exit gate passed and released |
| DROPPED | Removed with a reason (Appendix B) |
| DEFERRED | Kept for later; reason in Appendix B |

Sizes: S is up to about half a day of agent work and at most 4 files of substance. M is about one day. L is larger and is split wherever a verdict asked for it. Kinds: code, test, docs, deletion, data_repair, investigation, ops.

## 4. ID conventions

- Plan units are `P<phase>.<n>`, for example `P5A.4`. Each unit row names its source cluster units in parentheses, for example `(reconcile-3a)` or `(recipes-1 + scanner-runtime-7)` when units were merged. Appendix C.3 maps every cluster unit to its plan unit.
- Owner decisions are namespaced by area: D-REL, D-TC, D-RC, D-RS, D-PR, D-DB, D-EPA/D-ET/D-ER, D-IF, D-HY, D-DC, D-DEP, D-OD, D-SEC, D-SR, D-PLAN. The original per-cluster numbers are kept where possible (D-RC1 is the reconcile spec's D1). Merged decisions are noted in Section 7.

## 5. Phase overview

| Phase | Goal | Units | Sizes | Blocking decisions | Needs prod? | Status |
|---|---|---|---|---|---|---|
| 0A | Gate facts and operational stops: Railway audit, prod census, retire the stale scanner worker, unload stale launchd jobs, test baseline | 6 | 5S 1M | D-REL1, D-IF7 | Yes (owner-run reads and Railway ops) | TODO |
| 0B | Decision-input measurements (read-only) | 6 | 2S 4M | none (owner approves network/CPU) | Optional read-only counts | TODO |
| 1A | Stop retirement and catalog damage; disarm destructive scripts; declare missing SDK | 6 | 5S 1M | none | Optional web hotfix deploy | TODO |
| 1B | Recipe store Phase 0, test-isolation guard, footgun fixes | 8 | 8S | D-RS5 | No | TODO |
| 1C | Local repairs after the damage stops (test leak, saved_at backfill, store reconcile) | 6 | 5S 1M | D-SR7 | No | TODO |
| 2A | CI that actually runs: lint green, all-branch triggers, release guard, shakedown | 7 | 4S 2M 1L | D-REL3, D-TC5 | No (GitHub only) | TODO |
| 2B | Release flow: guarded deploys, first release, `main` fast-forward, tags, ruleset, prune | 8 | 3S 5M | D-REL2..D-REL10 | Yes (deploy) | TODO |
| 3 | Security wave 1 (high severity): OAuth takeover, XFF spoofing, SSRF CGNAT, CSRF, CSS injection, Chromium out of web | 9 | 7S 2M | D-SEC1, D-SEC4 | Yes (deploy web) | TODO |
| 4 | Migration foundation: drift report, `migrate --target`, conditional V019, V026 chain completeness | 8 | 3S 5M | D-DB5, D-DB6, D-DB7, D-PR8 | Yes (V026 apply) | TODO |
| 5A | Retirement single writer and pure policy, walk report, shadow run | 8 | 3S 4M 1L | D-RC1, D-SR4 | No | TODO |
| 5B | Walk completeness, retirement provenance, audit tool, local repair | 9 | 3S 6M | D-RC2, D-RC3, D-RC5, D-RC6, D-RC7 | No (local) | TODO |
| 6A | Scanning-resumption code: advisory locks, run window, VIN-guard gap cover, log pull, recipe hygiene | 7 | 4S 3M | D-IF5, D-IF10 | No | TODO |
| 6B | Resume Railway scanning: deploy, prod recipe repair, smoke, supervised fleet, prod retirement repair, cron | 8 | 4S 4M | D-IF1 (docs only), D-IF4, D-RC5, D-RC6 | Yes | TODO |
| 7 | Recipe store authority: rev CAS, DB-authoritative load, mutation API, archive, per-host status | 9 | 2S 7M | D-RS1..D-RS4 | Yes (V029 apply; DB-to-DB reconcile) | TODO |
| 8A | Precedence foundation: invariants, `precedence.py`, apply helper, vPIC writer, decode refresh, PHEV heal | 7 | 3S 4M | D-PR1, D-PR4, D-PR6, D-PR7 | No | TODO |
| 8B | Every fact writer goes through precedence | 9 | 2S 7M | D-PR2, D-PR3, D-PR5, D-PR8 | No | TODO |
| 8C | Precedence close-out: retire duplicate writers, cycle test, docs, local and prod heal | 7 | 4S 3M | none new | Yes (P8C.7) | TODO |
| 9 | Dictionary Complete_Options lookups work in prod; prune duplicates | 8 | 7S 1M | D-HY1, D-HY9 | Yes (deploy and verify) | TODO |
| 10A | EPA catalog code foundation: induction mapper, permanent key, flags and provenance migration, importer | 8 | 3S 4M 1L | D-EPA1, D-ET2, D-ET3, D-ET5, D-ER2 | No | TODO |
| 10B | EPA catalog builder, provenance backfill, legacy-row re-resolve (local) | 8 | 4S 4M | D-ER1, D-ER3 | No (local) | TODO |
| 10C | Forced-induction resolver, linker reconcile, consumers, invariants, heal tool | 9 | 3S 6M | D-ET2, D-ET4 | No | TODO |
| 10D | EPA data repairs, prod rollout, constraints, DICTIONARY/ deletion | 9 | 5S 4M | D-EPA1, D-ER4, D-ER5, D-ER6 | Yes | TODO |
| 11A | Scanner runtime: lock exit codes, cross-host per-dealer lock, upsert telemetry and retries | 8 | 5S 3M | D-SR1, D-SR2 | No | TODO |
| 11B | Upsert cost: measure, batch post-write, batched writes | 4 | 3S 1M | D-SR6 | No | TODO |
| 12A | One scanner fetch policy | 6 | 3S 3M | D-OD3, D-OD7, D-OD8, D-OD13 | No | TODO |
| 12B | One recipe validation rule; under-collection rescans | 10 | 3S 7M | D-OD1, D-OD2, D-OD4, D-OD5, D-OD6 | Yes (canary, rescans) | TODO |
| 13A | DB layer: loud adapter, DDL guard, strict mode everywhere, side-effect-free import | 8 | 5S 3M | D-DB3, D-DB4, D-DB8 | Yes (strict env) | TODO |
| 13B | Delete legacy DDL; drop backup tables | 4 | 4M | D-DB1, D-DB3 | Yes | TODO |
| 14A | Postgres test harness and first PG tests | 9 | 4S 4M 1L | D-TC3, D-TC6, D-TC9 | No | TODO |
| 14B | PG tests for queue, grid, search parity, stores, admin | 8 | 1S 7M | D-TC4 | No | TODO |
| 14C | SQL corpus check, boot test, skip budget, coverage map, runtime, TESTING.md | 6 | 5S 1L | D-TC8 | No | TODO |
| 15A | Home-IP lane and onboarding | 6 | 2S 4M | D-IF1, D-IF3, D-IF6 | Yes | TODO |
| 15B | Dependencies and images: purge, split, CPU torch, embedding pin, pip-audit | 8 | 3S 5M | D-DEP1, D-SEC7 | Yes (deploys) | TODO |
| 16 | Security wave 2: session revocation, dead MFA columns, scanner SSRF, non-root, CSP styles | 11 | 4S 7M | D-SEC2, D-SEC3 | Yes | TODO |
| 17A | Consolidation: shims, facades, dead functions | 11 | 8S 3M | none | No | TODO |
| 17B | Consolidation: browser path retirement, platform registry, LLM/OEM leftovers | 11 | 4S 6M 1L | D-DC1, D-DC5, D-DC6, D-DC7, D-DC9 | Optional (P17B.8) | TODO |
| 17C | Consolidation: vPIC getter, brochure CLI and split, comments_db split, env knobs, ScanConfig | 8 | 2S 5M 1L | D-OD9, D-OD10, D-OD11 | No | TODO |
| 18A | Hygiene: junk, runtime state, workspace scratch, nightly scripts, k8s, ruff | 11 | 7S 4M | D-HY3, D-HY4, D-IF6, D-IF7, D-IF9, D-TC7 | No | TODO |
| 18B | Docs: entry page, README, banners, security doc re-scope, location contract | 7 | 6S 1M | D-HY6, D-HY7 | No | TODO |

Phase 0B may overlap Phases 1A and 1B, because those use worktrees only and 0B writes only measurement docs and dealer logs. 0B must finish before 1C starts, because both write `workspace/dealer_logs/_learning/`.

## 6. Why this order (and where it departs from the default)

1. **Phase 0 comes before any push.** The stale June `scanner-worker` image is running against prod Postgres now, and it claims any job the admin UI enqueues (`backend/dealer/admin/dealers_hub.py:40`). It was created with `railway add --branch main`, and a stray `git push origin HEAD:main` passes today's hook (exit 0, 1.5.2 differs from 0.2.0). So it has to be stopped and detached before Phase 1 pushes anything (release verdict).
2. **Phase 1 is local only.** It needs no CI and is pushed once, at the end of 1C. Phase 2 then gives every later phase CI and a tagged-release path.
3. **Security wave 1 (Phase 3) moves ahead of data correctness.** These issues are exploitable today: pre-registration OAuth takeover, including the username-equals-email variant; X-Forwarded-For spoofing that defeats `DEV_IP_ALLOWLIST`; a root Chromium running without a sandbox in the web container that holds every secret. They touch web-auth files that no data-correctness phase touches. The probable prod chat outage (the `anthropic` SDK is undeclared) is handled earlier still, as a Phase 1A hotfix.
4. **A small DB-layer slice (Phase 4) moves ahead of data correctness (dependency).** Phases 5B, 7, 10A and 10D add migrations V027-V032. Today the chain cannot be replayed from empty (V019), `migrate.py` has no `--target` (staged EPA migrations are impossible), and the prod web applies every pending file at boot (`scripts/docker-entrypoint-web.sh:21-24`). The bulk of the DB-layer work (loud adapter, legacy-DDL deletion, import-time init) stays in Phase 13, as the default order asks.
5. **Scanning resumption (Phase 6) moves ahead of the DB/tests phases.** Prod has had no scan since 2026-09-29, so sold cars stay listed, and that is ongoing damage. The gates that block the nightly cron (retirement correctness, recipe un-stale, VIN-guard gap) are all closed by Phases 1, 5 and 6A. Nothing in the DB-layer or tests phases blocks the cron. Recipe-store authority (Phase 7) comes after resumption: the recipe verdict found that Phase 0 of the recipe work is enough for the Railway re-enable, and cross-host compare-and-swap matters only once hosts share a store.
6. **Precedence (8) comes before EPA (10).** Both rewrite `enrich_from_dictionary.enrich_car` and `backend/scanner/upsert/sql.py`. Precedence sets the arbiter and the stale-provenance rule that the forced-induction resolver plugs into.
7. **The EPA catalog provenance and re-resolve (10B) come before the forced-induction repairs (10D).** The family tier reads flags only from id'd rows, and 136,001 active cars point at id-less legacy rows until they are re-resolved.
8. **Postgres tests (Phase 14) follow the default late slot.** Until then, every unit that changes SQL must show one local-Postgres check in its PR (Appendix D, R-03). D-PLAN1 lets the owner pull P14A.1-P14A.3 forward to right after Phase 4.

## 7. Owner decisions (consolidated)

The Options column is short; each phase repeats the context it needs. "Rec" is the recommendation after the verdicts were applied.

### Release, CI, deploy

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-REL1 (= infra D2) | What happens to the legacy `scanner-worker` and `scanner-scheduler` services, which run a June image against prod and claim admin-enqueued `dealer_jobs`? | (a) back up variable names, stop, delete; (b) disconnect the GitHub source, turn off autodeploy, and stop the running deployments; (c) leave | (a). If unsure, (b) including `railway down` (disconnecting alone leaves the container running) | P0A.4, P2B.3, P2B.7 (docs state the services' fate) |
| D-REL2 | Branching model | (a) trunk: short `phase/*` branches fast-forwarded into `main`, retire `feature/http-only-scans` after the first release; (b) a long-lived dev branch | (a) | P2B.7, P2B.8, every later release step |
| D-REL3 (= tests-ci D2) | Which pushes must carry a new VERSION, and which pushes does the CI `release-guard` job judge? | (a) every push bumps (status quo); (b) only pushes to `main`; (c) pre-release suffix. Sub-question: may throwaway `ci/*` branches push without a bump during the CI shakedown? (A new branch is compared with `origin/main`, which is at 0.2.0, so it passes today.) | (a) for now. `release-guard` runs only on pushes to `main` and on PRs to `main`, as a "releasable?" check, so work-in-progress pushes do not go red. Yes to `ci/*` during P2A.5 only. Approve the rule that a release needs a CHANGELOG section | P2A.4, P2A.5, P2B.1, P2B.7 (RELEASING.md bump rule) |
| D-REL4 | Unmerged remotes `feat/inventory-consolidation-0.4.0` (15 commits, incl. collaborator work) and `feature/crawler-llm-standardization` | (a) annotated `archive/*` tag, then delete the branch; (b) keep; (c) port first | (a), after telling collaborator ksarrafi | P2B.8 |
| D-REL5 | Repository visibility. GitHub says PUBLIC; memory says it was made private on 08-02 | (a) public; (b) private on Free (rulesets stop applying, 2,000 Actions minutes); (c) private on Pro/Team | Confirm intent. If private, choose (c). CI on every branch push costs minutes when private | P2B.5 |
| D-REL6 (+ tests-ci D1) | How strict is the protection on `main`, and what does a red CI block? | (a) ruleset: no deletion, no force-push, the 4 required checks pinned to GitHub Actions (integration_id 15368), no PR requirement, no linear-history rule; (b) (a) plus required PRs; (c) none. Sub-question: an admin bypass, so an Actions outage cannot block a hotfix | (a) with an owner-only bypass. Add "green CI for the release SHA" to the release checklist | P2B.5 |
| D-REL7 | Tag past releases 1.3.2..1.5.2 retroactively? | (a) yes; (b) no | (a). v1.3.2 uses its commit subject | P2B.4 |
| D-REL8 | Deploy the first release now? | (a) web and scanner-nightly; (b) web now, scanner-nightly at fleet start; (c) hold | (b). Scanner-nightly ships in P6B.1 | P2B.6 |
| D-REL9 | Patch or minor for the first release | (a) patch; (b) minor | (a) | P2B.3 |
| D-REL10 (new) | Must scanner-nightly deploys come from a tag on `main`? | (a) yes, with a loud `ALLOW_UNRELEASED_DEPLOY=1` override for a mid-fleet hotfix; (b) branch HEAD allowed | (a) | P2B.2 |

### Tests and CI

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-TC3 | Postgres image for the CI pg job | match prod's major (pgvector/pgvector, pinned 0.8.x) / pg17 / postgres:16 (has no pgvector, so V001 cannot apply) | Match prod. P0A.2 reads `SHOW server_version` and the vector extversion | P14A.3 |
| D-TC4 | Breadth of the Postgres tier | targeted tier / whole suite on PG / stop at the write path | Targeted, with the SQL corpus check as the breadth net | P14B.6-P14B.8 |
| D-TC5 | How CI installs dependencies | full requirements plus CPU torch first / as-is / trimmed subset | Full plus CPU torch. Prod installs the default (CUDA) torch, so this is not image parity, and no test imports torch | P2A.2 |
| D-TC6 | `integration` marker and job | replace the job with `pytest-pg`; the marker means "needs live data, dev machine only" / keep both / fold | Replace the job, keep the marker | P14A.3, P14A.4 |
| D-TC7 | How far ruff tightening goes | F811 fix, then F541, facade `__all__`, F401 (consumer-audited), F841, then B023/PLE/E7 without E741/B904 / gate only / plus BLE001, S608, PLW0603 | The first option | P18A.9, P18A.11 |
| D-TC8 | pytest-xdist in the offline job | yes if measured over 12 min and safe / no | Decide from P14C.5 | P14C.5 |
| D-TC9 | May the pg tier create and drop `pytest_*` databases on the local Postgres server that holds `cars`? | yes, guarded (admin DSN only from `TEST_PG_ADMIN_DSN`; refuses Railway hosts and port 15432) / separate instance / CI only | Yes, guarded | P14A.1 |

### Listing retirement (reconcile)

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-RC1 | Which layer writes retirements | A: the scanner (full and delta) via `plan_retirement`; the pipeline only reports / B: the pipeline writes / C: both | A | P5A.4, P5A.5, P5A.7 |
| D-RC2 | Retire after one eligible miss, or after two runs at least 20 h apart? | single / two-strike / three | Two-strike, kept in a side table | P5B.4 |
| D-RC3 (= scanner-runtime D3) | How far a replay walk goes | keep 40 pages and only flag / walk to the payload total, hard cap 150 / no cap | Walk to the total with cap 150 (`SCANNER_REPLAY_MAX_PAGES`). Measure the 403 rate on one shard first; the truncation flag is the immediate guard | P5B.2 |
| D-RC4 | One recipe of a dealer fails: what may still be retired? | nothing / covered buckets only | Nothing (the policy default in P5A.1). Revisit after 2 fleet runs | none |
| D-RC5 | Wrong-retirement candidates that cannot be verified live | leave retired, log, rescan / reactivate all / reactivate if recent and bucket empty | Rescan first. Restore only verified-live residue; send cannot_assess dealers to dealer-discovery | P5B.8, P6B.6 |
| D-RC6 | Which databases are repaired, in what order | local rehearsal then prod / prod only / local only | Local, then prod | P5B.8, P6B.6 |
| D-RC7 | Add the append-only `listing_retirements` table? | yes / summary_json only | Yes | P5B.3, P5B.4 |

### Recipe store

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-RS1 | Canonical recipe store | (a) prod for every scanner (`RECIPES_DATABASE_URL`) / (b) two stores plus the reconcile tool after each fleet and release / (c) independent | (b) now; (a) once Railway owns all scanning | P7.9 |
| D-RS2 | What re-synthesis does to existing recipes | replace / replace-with-archive / full merge | Replace-with-archive. Archived entries live in a separate `superseded_json` column that old code never reads | P7.5 |
| D-RS3 | How far the versioning fix goes | stop after Phase 0 / DB-authoritative with rev CAS / DB only | DB-authoritative with CAS | P7.1-P7.4 |
| D-RS4 | Track "blocked" status and the lifecycle guard per host? | shared / per host | Per host | P7.6 |
| D-RS5 | Should audit and attribution scripts write recipe state? | yes / no (`persist=False`) | No | P1B.5 |

### Vehicle-fact precedence

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-PR1 | Sticker vs vPIC | (a) vPIC wins wheels, electrification, cylinders; the sticker wins engine text, transmission, colors, and the drive end when vPIC is silent or says 4x2 / (b) sticker forces / (c) vPIC everywhere | (a) | P8A.2, P8B.2 |
| D-PR2 | May a rescan's feed value overwrite a higher-ranked stored value? | (a) no; record `feed_said` / (b) yes / (c) after N days | (a), honoured only while the stored provenance entry's `value` still matches the column (stale-provenance rule) | P8B.1 |
| D-PR3 | Dictionary engine correction and `--all` | (a) fill blanks only / (b) correct when vPIC corroborates / (c) today / (d) new: correct when the dealer's own title or trim corroborates the EPA row | (b) plus (d). The A6 fixture is a (d) case | P8B.5 |
| D-PR4 | Drivetrain catalog tiebreak scope | (a) today's scope plus a 4MATIC/xDrive veto / (b) only replace 2WD/4x2/blank / (c) only when vPIC says 4x2. Sub-question: may an explicit dealer FWD/RWD permanently lose to the tiebreak? | (a). On the sub-question, yes when vPIC says 4x2 or nothing and the trim decoder agrees; less specific feed values never replace a stored end | P8A.2, P8A.4 |
| D-PR5 | How every scan path gets a decode | (a) inline, budgeted, via `asyncio.to_thread` / (b) post-scan, which requires moving the vPIC step above the scan-only return (an env-only change does nothing) / (c) status quo | (a) | P8B.4 |
| D-PR6 | Two or more listing signals contradict vPIC on drive | vPIC wins and the row is marked cannot_assess / silently / dealer wins | First option | P8A.2 |
| D-PR7 | Write-time vPIC cylinders | only when they do not contradict the engine text / unconditional / keep both rules | Only when not contradicting | P8A.2, P8A.4 |
| D-PR8 (= db-layer D2) | InventoryEnricher's Haiku fallback and `haiku_spec_cache` | (a) never write fact columns; opt-in env, default off; keep the cache table / (b) fill at lowest rank / (c) today / (d) delete the fallback and the table after a consumer audit | (a) | P4.3, P8B.8 |

### DB layer

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-DB1 | The 12 backup tables from V001 plus `cars_epa_link_backup_20260921` (about 145 MB locally) | (a) export to dumps, drop via migration / (b) move to a `backups` schema / (c) keep | (a). Not before Phase 10D finishes: `epa_master_dump_id_map` and `epa_extended_specs_bak_*` are EPA recovery sources | P13B.4 |
| D-DB3 | Once every DB is current and strict | (a) strict only, delete legacy DDL / (b) warn without DDL / (c) keep the fallback | (a), with `INVENTORY_AUTO_MIGRATE=1` kept for dev | P13B.1-P13B.3 |
| D-DB4 | Where boot-time DB init runs | (a) explicit `backend.web.db_bootstrap` step in every launcher / (b) cheap at import / (c) gunicorn hook | (a). Every launcher must call it (`start.sh`, `scripts/run-web-local.sh`, the launchd/systemd units, the entrypoint) | P13A.5, P13A.6 |
| D-DB5 | Is the mini's own Postgres a live inventory DB? | (a) live: migrate and enforce strict / (b) retired, tunnel DSN only | (b), if nothing writes to it | P4.8, P13A.4 |
| D-DB6 | How migrations reach prod (applies to every migration in this plan) | (a) manually: backup, dry-run, apply before the deploy, so the boot step is a no-op / (b) at web boot | (a) | P4.8 and every phase with a migration |
| D-DB7 | Out-of-chain objects the code relies on (`uq_option_rejections_standing`; pgvector `*_embeddings` if found in prod) | (a) adopt into the chain, with any dedupe as a separate data repair / (b) drop. Sub-question: `NULLS NOT DISTINCT` (PG 15+) for the option_rejections index | (a). Choose NULLS NOT DISTINCT if prod is 15+ | P4.3 |
| D-DB8 (new) | `cars.zip_code`: the chain creates and indexes it (V001, V025) while `drop_dead_zip_code_column.py` drops it outside the chain | (a) drop via migration and delete the script, the V025 index and the SQLite copy / (b) keep the column, delete the script | (b) unless the owner wants the column gone. It is cheap and avoids an ACCESS EXCLUSIVE lock on `cars` | P13A.8 |

### EPA catalog and forced induction

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-EPA1 (merges epa-turbo D1 and the epa-rebuild "dictionary columns" unit) | How the 12,126 dictionary EPA CSVs are corrected and extended | (a) in-place patch from a pinned `vehicles.csv`: forcedInduction and engineDisplay, plus new columns `epaId`, `atvType`, `cityE`, `highwayE`; every other cell byte-identical; `normalize_csv_columns` preserves an EPA-specific column set / (b) full regeneration from the new vintage (MPG revisions, manifest rebuild) / (c) FI patch only, no new columns | (a). Without `atvType` and the EPA id, D-ER1(a) loses the resolver's decisive electrification signal for every future model year | P10A.5, P10D.1 |
| D-ET2 | How "naturally aspirated" is represented | (a) explicit 'Naturally Aspirated', shown as a facet value / (b) explicit but hidden / (c) blank plus EPA-aware fallbacks / (d) a provenance column | (a). Local scale: about 77k NULL cars on NA families become 'Naturally Aspirated', about 19.8k missing boost labels are filled, about 1.8k boost labels flip to NA, 1,640 text-vs-catalog conflicts. These are the reviewer's read-only family join (2026-10-07), which is the "measure before deciding" input. P10C.9's dry run re-measures them with the real resolver before any write; the owner may revisit D-ET2 then, and P10D.1/P10D.5 wait for that confirmation | P10A.1, P10A.4, P10A.5, P10C.*, P10D.1, P10D.4, P10D.5 |
| D-ET3 | EPA rows flagged both turbo and supercharged (229) | 'Turbocharged' / a new combined value | 'Turbocharged'; the raw flags stay in `t_charger`/`s_charger` | P10A.1 |
| D-ET4 | Role of the make/model heuristic | last resort, tightened, still persisted / display only / remove | Last resort | P10C.2, P10C.5 |
| D-ET5 | Twin-turbo labels on catalog rows | keep where EPA says T and the classifier says twin, via one shared helper (pre-2016 rows use the existing label as the hint) / collapse to 'Turbocharged' | Keep, one helper shared by the CSV patch and the `epa_master` refresh | P10A.5, P10A.8, P10D.1 (the patch run writes twin labels) |
| D-ER1 | The one way EPA data enters `epa_master` | (a) vehicles.csv → dictionary CSVs → `build_epa_master_pg --apply` / (b) both paths as separate sources / (c) vehicles.csv directly. The datascripts audit recommended (c) | (a), given D-EPA1(a) | P10B.3, P10D.9 |
| D-ER2 | The key that makes catalog ids permanent | (a) normalized content key plus an ordinal / (b) EPA vehicle id / (c) (year, make, model, trim) | Revised: `epaId` when present, else a sticky content key whose ordinal is assigned once and never recomputed. Ordinals never decide matching | P10A.2, P10B.1, P10B.2 |
| D-ER3 | The 20,006 legacy rows from root DICTIONARY/ (229,060 links, 136,001 active) | (a) twin repoint (refuted) / (b) label only / (c) re-resolve | Revised: re-resolve every car linked to a legacy row with legacy rows excluded (what future scans do anyway), then archive unreferenced legacy rows | P10B.6, P10B.8 |
| D-ER4 | FK policy | RESTRICT on the cars FK and the extended-specs FK / SET NULL plus CASCADE / none | RESTRICT, merged only after the local repairs and a prod 0-dangling check | P10D.7 |
| D-ER5 | Root `DICTIONARY/` folder (10,200 files) | git rm after audit / move / keep frozen | git rm, after the prod provenance backfill (it is the positive signal for legacy rows) | P10D.8 |
| D-ER6 | How EPA work reaches prod | agent with approval at each step / owner runs it / guards only | Agent with per-step approval, migrations staged (V030 → backfill/relink → V031 → V032 in a later release) | P10D.6 |

### Infra and scanning operations

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-IF1 | How the about 31 dealers that answer only from a residential IP are scanned | (a) home lane on the mini (prod DSN, dedicated clone) / (b) a paid residential proxy shard / (c) accept the loss | (a), with (b) as fallback. Record it in EXECUTION_PLAN as a narrow exception | P6B.8 (docs), P15A.1, P15A.2, P15A.6 |
| D-IF3 | Who runs admin "onboard dealer" jobs | (a) retire queue onboarding; hand off to dealer-discovery / (b) onboard-only worker on the mini / (c) a Railway discovery service | (b) if D-IF1=(a), else (a) | P15A.3 |
| D-IF4 | Nightly run parameters | 8 shards x batch 6 at `0 9 * * *` UTC with a 14 GB alarm under the 16 GB cap / batch 4 / investigate first | 8x6 with a 14 GB alarm | P6B.5, P6B.7 |
| D-IF5 | How the VIN-owner guard survives scan gaps | (a) auto-extend / (b) manual env / (c) default 72 h / (d) refuse known (owner, claimant) pairs | (a), computed from the OLDEST active `scraped_at` among roster dealers that own contested VINs (p05 fallback), plus (c) 72 h now. (d) deferred | P6A.3, P6B.5 |
| D-IF6 | Capabilities of the retired laptop nightly and the k8s crons | port per step / home lane / drop | Per step, from the P0B.5 numbers | P15A.4, P18A.5 |
| D-IF7 | The data-quality and rooftop-refusal report jobs | (a) unload on the MBP now, re-home later / (b) against prod read-only / (c) retire | (a) | P0A.5, P18A.6 |
| D-IF9 (merges hygiene OD-5) | Deploy assets to delete | delete `deploy/k8s`, `deploy/car-scanner`, `deploy/postgres`, the k8s block of `deploy/build.sh`; `deploy/always-on`: delete or keep for disaster recovery; launchd plists: keep until replaced | Delete k8s, car-scanner, postgres and always-on; keep the plists until the Railway cron is proven | P18A.7 |
| D-IF10 | Canonical `dealer_logs` tree | the mini / the MBP / both | The mini, pulling and merging the Railway volume logs after each night | P6A.5, P15A.6 |

### Hygiene and dictionary

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-HY1 (OD-1) | Where the vehicle dictionary lives | keep tracked, prune, fence script-only trees / LFS / object storage / generate / Postgres | Keep tracked | P9.7 |
| D-HY2 (OD-2) | The public history holds users.db, dev_users.db and dealer_portal.db (2026-04, commits c40bed8fe..9e8324620; 7 bcrypt hashes with emails) | (a) rotate or disable those accounts, no rewrite / (b) filter-repo / (c) both | (a) | owner action only |
| D-HY3 (OD-3) | Should `dealer_logs` be in git? | (a) only the curated `_learning` files (`platform_playbook.md`, `location_patterns.md`); `errors_index.md` and `platform_candidates.md` stay ignored because they are machine-written / (b) all / (c) none, plus a sync | (a), plus (c) through P6A.5 | P18A.4, P18B.7 |
| D-HY4 (OD-4) | The 47 force-added `workspace/` files | (a) untrack one-offs, move the 92694 manifest to fixtures, keep `dealers_backup.json` until its 43,105 extra dealers exist elsewhere / (b) untrack all / (c) leave | (a) | P18A.3 |
| D-HY6 (OD-6) | The single entry document | (a) RESUME_HERE as a short pointer page; retire master-todo into EXECUTION_PLAN / (b) refresh both / (c) delete both | (a) | P18B.1 |
| D-HY7 (OD-7) | Location-logging contract | (a) align NETWORK_SCAN_PROCESS.md and CLAUDE.md to what the pipeline writes; `location.md` only for quirks / (b) add a `location.md` writer | (a). The owner edits or approves the CLAUDE.md wording | P18B.7 |
| D-HY8 (OD-8) | Live EIA gas prices in prod | (a) status quo fallback anchors / (b) store the sync result in Postgres | (a) | none (deferred) |
| D-HY9 (new, from the hygiene verdict) | Prod has resolved Complete_Options CSVs for only 54 of the top 300 groups, because the catalog DB never shipped. Should prod start serving that trim-ladder content for about 91k more active cars? | (a) yes, after the 20-car before/after diff review / (b) retire options CSVs from the request path | (a) | P9.2 (deploy), P9.8 |

### Dead code, dependencies, recipe validation, scanner runtime

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-DC1 | Confirm the recorded HTTP-only plan decisions D2 (mini primary, MBP fallback), D3 (capture-only deleted, verified), D4 (superseded by D-DC5). D1 (web Chromium) is now D-SEC1 | confirm / reopen | Confirm | P17B.11 |
| D-DC5 | Retire `scanner.py --allow-browser`, the recovery page strategies and the chain/gap_fill Playwright fallbacks? | (a) now / (b) per platform / (c) keep | (a), after P0A.1 shows `SCANNER_ALLOW_BROWSER` unset on Railway web. Note: the on-demand listing/sticker fetch reachable from the web (`routes/car_detail/packages_ensure.py:287`) loses its browser fallback | P17B.3-P17B.6, P17B.8 |
| D-DC6 | Dealer-group LLM adjudication, which cannot run (no `LLMClient` implementation) | (a) add an opt-in adapter / (b) delete the LLM branch and the ABC, keep the rules-only batch / (c) leave | Revised: (b). (a) is new paid-feature scope; choose it only if dealer-group copyright inference is still wanted | P17B.9 |
| D-DC7 | `backend/oem/vehicle_reference` (1,669 lines, no importers) | (a) keep / (b) move the EPA web-service client, delete the rest / (c) delete all | (b) | P17B.10 |
| D-DC9 | The `requires_browser` scan hint (19 local rows) | (a) relabel / (b) retire the key with a data repair / (c) drop the special skip branch, keep the key, fix the hint-note text | Revised: (c), which is what `HTTP_ONLY_SCANS_PLAN.md:91` asked | P17B.7 |
| D-DC10 | Facades `recipe_synth.py` and `dealer_pipeline.py` | keep / slim / delete | Keep | none |
| D-DEP1 (= dead-code D8; security-1; infra-8b) | Remove orphan dependencies (crawl4ai, pyotp, segno, ollama, langchain-core/openai/text-splitters, langsmith, plus starlette/tornado/pygments when nothing requires them) and lift lxml to 6.1 or later, unblocking PYSEC-2026-87 | (a) now, with CI plus local Docker smokes / (b) with the next web deploy / (c) security cluster | (a), in P15B.1 | P15B.1 |
| D-OD1 (= scanner-runtime D5) | Minimum-VIN rule across capture (3), synth (5), CLI (>0 / 5 total), cascade (5), scan (10 per recipe even in union) | Revised. After recipe hygiene (P6A.6), the scan union admits a recipe under the floor only if it is a condition-pinned side of a paginated inventory shape the set gate judged ok or uncertain. It refuses pagination=none sub-floor recipes outright. First-hit replay accepts at least min(global floor, max(3, 0.5 x recipe.vehicle_rows)). One constant `RECIPE_MIN_VINS=5` | As stated. The original "admit anything above 0" would admit 38 live widget/VIN-list/fragment recipes on 33 dealers | P12B.5, P12B.10 |
| D-OD2 | Which count decides pass and `vehicle_rows`? | (A) one full walk at save; 2 pages for re-checks / (B) 2 pages / (C) both | (A). It arms the coverage reject more often; the verdict flips are measured in P0B.1 | P12B.2-P12B.4 |
| D-OD3 | Validation fetches through the scan's own replay? | (A) yes, after porting synth's HTML-edge handling into replay / (B) the scan uses synth.http / (C) two stacks | (A), gated on P0B.1 | P12A.2, P12B.3 |
| D-OD4 | `validate_recipe` | thin count wrapper / delete / separate | Thin wrapper | P12B.3 |
| D-OD5 | Cascade recipes through the set gate and dealer logs? | yes, rejected shapes fall through to the next / no / retire cascade | Yes | P12B.6 |
| D-OD6 | GET recipe with a non-JSON `post_template` | mirror the scan / error / repair at load | Mirror the scan | P12B.3 |
| D-OD7 | One scanner fetch policy? | (A) one block set {403,405,429,503}, rotation starting from the per-host winner, the profile sets the UA unless measured worse / (B) block set and rotation only / (C) status quo | (A). Constraints: 429 gets backoff and a retry, never rotation; never rotate inside VDP slow-host serialization; a challenge page is its own outcome, never marked stale | P12A.2-P12A.5 |
| D-OD8 | Retry transient statuses in feed replay? | one retry with 2 s backoff / none / synth-style | One retry, only if P0B.1 sees transient 429/5xx on replay pages | P12A.2 |
| D-OD9 | Brochure CLI | recommended set (probe every --brand; `--user-agent both` outside the probe is an error; document the quarantine sweep; keep strings; no subcommands) / status quo / per item | Recommended set | P17C.2 |
| D-OD10 | Move the vPIC getter out of its closure to module level? | yes / no | Yes | P17C.1 |
| D-OD11 | Scanner env knobs (131 names, 101 never set in tracked deploy files) | registry plus lint, retire dead browser-era knobs, ScanConfig later / registry only / all at once / leave | The first option | P17C.6-P17C.8 |
| D-OD12 | Brochure grid 'P' marker: standard or package? | per OEM from each brochure's printed legend / package / standard | Decide from P0B.4. The parser fix belongs to an enrichment follow-up (deferred) | none |
| D-OD13 (new) | Does the DuckDuckGo spec search in gap_fill go through `SCANNER_HTTP_PROXY`? | (a) no, excluded / (b) yes | (a) | P12A.3 |
| D-SR1 | Worker scan exits 75 (lock held) | (a) fail the job with 'lock_held' / (b) requeue / (c) today (marked ok) | (a) | P11A.5 |
| D-SR2 (reframed) | Cross-process and cross-host exclusion of a dealer | (a) delta takes the file lock / (b) none / (c) Postgres advisory lock per dealer around replay, write and reconcile in `dealer_run` and `delta_scan`, plus (a) as hygiene | (c). File locks are per shard and per host, so they never exclude a dealer | P11A.3 |
| D-SR4 | Verdict for a truncated replay | (a) ok becomes thin, with a stable `replay_truncated:` reason token / (b) a new verdict / (c) a note only | (a). Reconcile must also read the flag | P5A.7 |
| D-SR6 | Where the upsert measurement and parity runs happen | MBP on AC / mini over the tunnel / skip | Either, with the owner's go-ahead | P11B.1, P11B.4 |
| D-SR7 | Delete the 2026-10-05 test-leak artifacts (3 dirs, `errors_index.md:119-135`) after a backup? | delete / move / leave | Delete after a tarball backup | P1C.1 |

### Security

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-SEC1 (merges infra D8, dead-code D1) | Headless Chromium in the web image. Car chat launches it as root with `--no-sandbox` (Playwright default) for any logged-in user, for search (Brave) and page fallback | (a) remove from web; WebResearcher HTTP-only by default, the operator script keeps browser mode / (b) keep, hardened (non-root, sandbox, request routing) / (c) move research to a worker | (a), unless P0B.3 shows the browser (fallback plus Brave) produces a large share of answers. Infra preferred keep plus CPU torch; security preferred remove. A stopgap env change happens in Phase 0 either way | P3.8 |
| D-SEC2 | Dead TOTP/phone storage (2FA was removed in SEC-014) | (a) drop the columns (`totp_secret`, `mfa_phone`, `mfa_method`; `totp_enabled` kept as always 0 unless its consumers are rewritten) and optionally scrub the data / (b) scrub only / (c) encrypt | (a). The scrub (P16.3) is optional, because SQLCipher protects the data at rest and the copier copies only columns PG has | P16.2, P16.3 |
| D-SEC3 | How users data is isolated once it moves to Postgres (SEC-088 answer) | (a) a web-only schema/role; scanners get a restricted role / (b) disk encryption plus DSN hygiene, after the TOTP drop / (c) a separate database | (a), sized by P16.10. The implementation belongs to the users cutover (deferred) | none in this plan |
| D-SEC4 | OAuth sign-in matches an unverified password account | (a) no auto-link; link after a password login / (b) link and kill the password / (c) today | (a). The lookup must also become email-only (username collision) | P3.1 |
| D-SEC5 | Prod `ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1` departs from SEC-083 | (a) revert, use `DEV_IP_ALLOWLIST` / (b) keep and record | (a), but only after P3.2 is deployed and `TRUSTED_PROXY_CIDRS=cloudflare` is set (until then the allowlist is spoofable) | owner env only |
| D-SEC6 | Direct-to-origin X-Forwarded-For spoofing | (a) `TRUSTED_PROXY_CIDRS=cloudflare` / (b) remove the *.up.railway.app domain / (c) both | (c). (b) alone does not close it, because the Railway edge routes by Host | owner env after P3.2 |
| D-SEC7 | Lock files per image | (a) `uv pip compile` with hashes plus a CI drift check / (b) floors plus pip-audit | (a) | P15B.8 |

### Plan-level

| ID | Question | Options | Rec | Blocks |
|---|---|---|---|---|
| D-PLAN1 | Pull the Postgres test harness (P14A.1-P14A.3) forward to run right after Phase 4? | yes / keep the stated order | Keep the order, and revisit after Phase 4. Pulling it forward gives Phases 5-12 a pg tier, at the cost of delaying data repair by one phase | Phase 14A placement |

## 8. Owner-only actions

Grouped by the phase that needs them. Items marked (any time) do not gate a phase.

**Before or during Phase 0**
1. Authorize the read-only Railway and prod queries (P0A.1, P0A.2, P0A.3), or run the prepared command packs yourself. Check github.com/settings/installations for the Railway GitHub App's access to the repo.
2. Answer D-REL1. Stop and retire `scanner-worker`/`scanner-scheduler` (P0A.4). Turn on Railway failure notifications for every service.
3. Unload `com.sarraficars.nightly-data-quality` on the MBP (`launchctl unload ~/Library/LaunchAgents/com.sarraficars.nightly-data-quality.plist`) and check the mini's launchd/crontab (P0A.5).
4. Stopgap for D-SEC1: on Railway web, set `WEB_RESEARCH_ALLOWED_HOSTS` to a short list of auto reference sites, or `CAR_CHAT_WEB_RESEARCH=0`. Reversible, env only.
5. Check prod logs for car-chat Claude failures. That tells you whether the Phase 1A `anthropic` hotfix is urgent.
6. (any time) Add DNS for `www.sarraficars.com` (a proxied CNAME to the apex) plus a Cloudflare www→apex redirect. On 2026-10-07 `dig` returns nothing for www.
7. (any time) Change the admin password in the UI, rotate Railway `ADMIN_PASSWORD`, and confirm `BOOTSTRAP_FORCE_ADMIN_PASSWORD` is absent from web.
8. (any time) D-HY2: rotate or disable the 7 accounts whose emails and bcrypt hashes are in public git history, or confirm they are dead test accounts. Decide repo visibility (D-REL5).
9. (any time) Update stale memory entries: "TRUST_PROXY_HEADERS on Railway — OPEN" (resolved 2026-09-30); "repo made private 08-02" (GitHub says PUBLIC); "prod schema at V018" (local is at V025; prod per P0A.2).
10. Tell collaborator ksarrafi before `main` moves and before the unmerged branches are archived (D-REL4).

**Phase 1**: approve the local repairs in 1C; let the mini fetch the 1B and 1C commits straight from the MBP over SSH (Phase 1 is pushed only at the end of 1C; see the 1C entry gate), and `git pull` the pushed Phase 1C commit before any mini scan; verify prod car chat after any hotfix deploy.

**Phase 2**: run `git config core.hooksPath scripts/git-hooks` on the mini and every other clone; approve CLAUDE.md wording changes (P2B.1, P2B.7); confirm Actions email notifications for failed runs; approve the Docker build of the release stage on AC power (P2B.6).

**Phase 3**: after deploy, set `TRUSTED_PROXY_CIDRS=cloudflare`, then decide D-SEC5 (revert pass-through, set `DEV_IP_ALLOWLIST`); confirm the live `TRUSTED_PROXY_HOPS` before anyone runs `sync-vault-to-railway.sh`.

**Phase 4**: take and verify the prod `pg_dump -Fc` backup and run the prod `migrate --apply` yourself (D-DB6); say whether the mini's own Postgres is written by anything (D-DB5).

**Phases 5-6**: keep the Railway cron OFF until P6B.7. Approve the shadow runs and local repairs. Review `candidates.csv` before any `--apply`. Pause every fleet, and get every scanner host on the same commit before P6B. Run the home-IP recipe repair (P6B.2) from a home IP with no `SCANNER_EGRESS_TAG`. Approve the supervised fleet window and then the cron. Confirm that the lexusofknoxville-com 2026-09-27 hand retirement (1,417 foreign rows, user-authorized) stays excluded from every restore; P5B.5 seeds its exclude file with it. Approve the dealer list and backup for the home verification scan (P6B.3).

**Phase 7**: pause fleets; deploy the same VERSION to the MBP, the mini and Railway together; apply V029 to prod first.

**Phases 8-10**: approve prod repair windows (precedence, EPA); approve the pinned EPA source (`vehicles.csv.zip`, 2026-09-29, 50,407 rows) and keep a copy with its sha256; restart web workers after catalog repairs; keep `workspace/backups/prod_20261004_pre150.dump` and the untracked `dump-dealership_scanner-202607012302.sql` until P10D.6 is verified.

**Phase 9 (in addition to Phases 8-10)**: review and sign the 20-car before/after trim-ladder diff (D-HY9) before the deploy; approve the local stage build (P9.8).

**Phase 11**: choose the upsert-measurement host (D-SR6) and approve the measurement and parity runs on AC power or the mini (P11B.1, P11B.4); run the read-only 0-dangling prod check before V032 ships with 11A.

**Phase 12**: approve the local stage build and the canary shard (P12B.9); take the prod `\copy` backup of the under-collected dealers before the first nightly after the deploy (P12B.10); run the read-only prod cascade query (P12B.10); approve the re-judge stale list (P12B.7).

**Phase 13**: set `INVENTORY_SCHEMA_CHECK=strict` on Railway web and scanner-nightly and in the local and mini env, then `railway redeploy`. Take verified per-DB dumps before the backup-table drop migration merges.

**Phase 14**: update the ruleset contexts when `pytest-pg` replaces `pytest-integration` (P14A.3); set `REQUIRE_LOCAL_ASSETS=1` for test runs on the asset-bearing mini (P14C.6); approve the one-off 20x concurrency repeat (P14A.7).

**Phase 15**: create the 0600 `~/.config/sarraficars/home_lane.env` on the mini with the prod public DSN; decide on Tailscale for the mini; approve the image builds.

**Phase 16**: create the Postgres roles if D-SEC3=(a) (cutover cluster); approve or skip the prod MFA scrub; set `RESEND_API_KEY` and a verified `RESEND_FROM`, then `PASSWORD_RESET_ENABLED=1` after P16.1 deploys and `EMAIL_VERIFICATION_ENABLED=1` after P3.1 deploys; configure the Stripe Customer Portal before billing goes live.

**Phase 17**: remove the now-unread env vars wherever set (`.env`, Railway, the mini): `SCANNER_WARMUP_PHASE_TIMEOUT_SEC`, `SCANNER_VDP_PHASE_TIMEOUT_SEC`, `SCANNER_FAILURE_HAR`, `ANTHROPIC_VISION_MAX_RETRIES`; run `docker volume rm` on the stale `scanner_node_modules` volume if it exists.

**Phase 18**: decide whether the 5 dealer ids found only in `backend/dealers.json` and the 1 found only in `dealers_28173_50mi.json` are dropped or routed through discovery (P18A.1); confirm that `dealers_backup.json`'s 43,105 extra dealers exist elsewhere before it is untracked (D-HY4); edit or approve the CLAUDE.md location-logging wording (P18B.7).

---

# Part 2: Phases

Each phase has the same parts: Goal, Entry gate, Work units table, Unit detail (Change / Tests / Accept), Workflow shape, Data repair, Exit gate, Release step. "Grp" is the wave letter (Section 2). "MAIN" marks a unit that runs in the main checkout.

## Phase 0A: Gate facts and operational stops

**Goal.** Establish the prod and Railway facts that later phases cite. Stop the live hazards that need no code: the June scanner-worker still attached to prod Postgres, launchd jobs grading a frozen local DB, and (through the owner stopgap) the web Chromium exposure. Record a test-suite baseline.

**Entry gate.** D-REL1 answered (P0A.4) and D-IF7 answered (P0A.5). The owner authorizes read-only Railway and prod access for P0A.1-P0A.3, or runs the prepared command packs.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P0A.1 | release-1, infra-1 (Railway part), dead-code missing (env precondition) | Railway service, trigger and variable audit (read-only) | investigation, needs prod | docs/ops_log/P0A.1.md (integration creates docs/SCANNING_OPS_LOG.md from it) | S | A | none |
| P0A.2 | infra-1 (SQL), db-layer-1, tests-ci D3, epa-rebuild-16 step 1 | Prod and local read-only census | investigation, needs prod | docs/db_layer/census_2026_10.sql (new), docs/db_layer/SCHEMA_LEDGER_2026_10.md (new) | M | A | none |
| P0A.3 | release-7 | Non-git files in the prod web image (ground truth) | investigation, needs prod | docs/ops_log/P0A.3.md (appended to SCANNING_OPS_LOG.md at integration) | S | B | P0A.1 |
| P0A.4 | release-2, infra-13, release missing "stop the running worker now" | Stop and retire scanner-worker/scheduler; remove every GitHub trigger | ops, needs prod | docs/ops_log/P0A.4.md (appended to SCANNING_OPS_LOG.md at integration) | S | B | P0A.1 |
| P0A.5 | infra missing (host job inventory), D-IF7 | Inventory and unload stale launchd/cron jobs on MBP and mini | ops | docs/ops_log/P0A.5.md (appended to SCANNING_OPS_LOG.md at integration) | S | A | none |
| P0A.6 | dead-code missing (baseline) | Baseline: chunked offline suite counts, ruff, import sweep | investigation | docs/remediation/BASELINE_2026_10.md (new) | S | A | none |

**P0A.1: Railway audit.**
- Change: read-only. For every service in the production environment (web, Postgres, scanner-nightly, scanner-worker, scanner-scheduler, kmac-vault, any other), record:
  - Source type, and repo plus branch from the GraphQL `deploymentTriggers` of each service instance. Use the memory recipe: token from `~/.railway/config.json`, railway-cli User-Agent. Missing `RAILWAY_GIT_*` variables prove nothing.
  - Autodeploy on push, Wait for CI, config-as-code path, `RAILWAY_DOCKERFILE_PATH`.
  - Latest deployment id, status, date, trigger and commit; the active deployment and replica count; attached volumes; restart policy.
  - Whether a `*.up.railway.app` domain is enabled on web, and scanner-nightly's `cronSchedule`.
  - Variable NAMES for every service. Values only for this whitelist: `SCAN_FLEET`, `SCAN_DEALERS`, `SCANNER_EGRESS_TAG`, `SCAN_SHARDS`, `SCAN_BATCH`, `SCANNER_RECONCILE*`, `SCANNER_ALLOW_BROWSER`, `SCANNER_LISTING_FETCH_CHAIN`, `SCANNER_RECIPE_MIN_VEHICLES`, `SCANNER_SYNTH_FETCH_DELAY`, `DICTIONARY_CATALOG_DB_PATH`, `TRUST_PROXY_HEADERS`, `TRUSTED_PROXY_HOPS`, `PUBLIC_BASE_URL`, `PLAYWRIGHT_NO_SANDBOX`, `CAR_CHAT_WEB_RESEARCH`, `CAR_CHAT_WEB_RESEARCH_PUBLIC`, `WEB_RESEARCH_ALLOWED_HOSTS`, `ALLOW_APP_ADMIN_DEV_PASS_THROUGH`, `LISTING_EMBEDDING_MODEL`, `GUNICORN_WORKERS`, `MIGRATE_ON_BOOT`, `INVENTORY_SCHEMA_CHECK`, `PASSWORD_RESET_ENABLED`, `EMAIL_VERIFICATION_ENABLED`.
  - Presence only (set or unset) for `SCANNER_HTTP_PROXY`, `DEV_IP_ALLOWLIST`, `BOOTSTRAP_FORCE_ADMIN_PASSWORD`, `SCANNER_WARMUP_PHASE_TIMEOUT_SEC`, `SCANNER_VDP_PHASE_TIMEOUT_SEC`, `SCANNER_FAILURE_HAR`, `ANTHROPIC_VISION_MAX_RETRIES`.
  - Never write a secret value anywhere. The owner checks github.com/settings/installations for the Railway GitHub App.
- Accept:
  - A per-service table.
  - An explicit yes/no to "would a push to ANY branch start a build on any service", with the linked branch per service.
  - Every service's deployment list is unchanged before and after.
  - Scanner-nightly's deployed commit, and whether it contains `1707e1349` (egress tag).
  - The open question from the release verdict is settled: would a main-triggered scanner-worker build use root config-as-code (`deploy/railway/railway.scanner-worker.toml:7`) or `RAILWAY_DOCKERFILE_PATH`?
  - The owner's check of prod web logs for car-chat Claude errors is recorded (it decides the P1A.6 hotfix).

**P0A.2: Read-only census.**
- Change: write `docs/db_layer/census_2026_10.sql`, opening with `SET default_transaction_read_only = on; SET statement_timeout = '20s';`. The agent runs it on the local MBP; the owner (or an authorized agent) runs it on prod; it runs on the mini's own Postgres only if D-DB5=(a). Also run `INVENTORY_DATABASE_URL=<dsn> python -m backend.scripts.migrate --dry-run` and record the redacted host/db it connected to. Queries:
  1. `server_version`; extversion of vector and dblink; `max_connections` and current usage; `idle_in_transaction_session_timeout` (expected 5 min, commit 68ce8433c); `idle_session_timeout`.
  2. `schema_migrations` (version, name, checksum, applied_at); pending migrations and checksum drift.
  3. `to_regclass` for review_reports, haiku_spec_cache, car_move_log, car_move_log_id_seq, cars_trim_quarantine, package_values_msrp_quarantine, and every `*_embeddings` table; the `pg_extension` list.
  4. `pg_indexes` for uq_option_rejections_standing, idx_car_move_log_car and idx_cars_active_zip; whether `cars.zip_code` exists.
  5. option_rejections duplicate groups, including NULL-key rows, under the script's `IS NOT DISTINCT FROM` key (feed_package_registry_from_vision.py:497-503).
  6. Every public table matching `bak|backup|dump`, with `pg_total_relation_size` and reltuples.
  7. `incomplete_listings_meta` row `index_bootstrap_v1`.
  8. A histogram of active-row `scraped_at` over the last 14 days. Never `max()` alone.
  9. vin_owner_conflicts: count, MAX(seen_at), top 30 (owner, claimant) pairs; row count of `cars_owner_backup_20260929`.
  10. dealer_recipes: recipe_status prefix counts; lifecycle keys; rows with `max_saved_at=0 AND recipe_count>0`; `'+cascade'` recipes; dealers whose recipes are all auth-staled (31 in the 10-04 dump); hints-only rows (`recipes_json = '[]'`).
  11. dealer_jobs and dealer_catalog counts by status.
  12. epa_master: count, min and max id; provenance split (ids in `epa_master_dump_id_map`, id-only rows that carry a vid, the rest); duplicate vids; epa_extended_specs count; dangling `cars.epa_master_id` across ALL listings; links to ids 1..20006.
  13. Active `cars.forced_induction` distribution.
  14. users: row count and count of non-null `totp_secret`.
- Accept:
  - The ledger doc has one row per DB.
  - It says whether prod is at V025, which settles or overturns the "prod at V018" docstrings (fixed in P4.5).
  - It lists the DBs that must receive V026, every out-of-chain object found, and a diff against the 10-04 dump numbers.
  - Every session ran READ ONLY.

**P0A.3: Non-git files in the web image.**
- Change: run `railway ssh -s web -- find /app -type f` (or `ls` the key dirs) and diff it against `git ls-files` at the deployed commit. Any emulation runs with `git -c core.ignorecase=false`, because on this Mac `DICTIONARY/` would match `backend/dictionary/`. Classify each non-git path as must ship (track it or generate it at build), must not ship (add it to BOTH `.railwayignore` and `.dockerignore`; note that `.dockerignore` patterns are root-anchored), or irrelevant. Already known from the verdict:
  - `backend/dictionary/index/dictionary_catalog.db` is excluded by `.railwayignore:20 *.db`.
  - `manifest.json` reaches the image but has no runtime reader.
  - `backend/data/spec_pages/` is read by html_spec_sources.py:99.
  - Also in the upload: `derived/*_quarantine/`, `*_fetch_log.jsonl`, `tools/ocr/bin/macocr`, `comment_uploads/`, `.idea/`, `.ruff_cache/`, and 122 `.br/.gz` files that Dockerfile.web:36 regenerates anyway.
  - Still unknown: whether `backend/dictionary/` exists in prod at all.
- Accept: a list of every non-git path with its size, its consumer (file:line, or "none found") and its classification; plus the explicit must-ship allowlist that P2B.2 stages.

**P0A.4: Retire the legacy scanner services.**
- Change, in order:
  1. Record each service's Dockerfile path, variable NAMES and non-secret values (never a resolved `railway variables --json` dump), deployment id and commit, replicas, volumes (setup-full-stack.sh@1746932b7:39-40 set `/data` paths) and restart policy. Screenshot Settings → Source/Deploy.
  2. Confirm prod dealer_jobs has no queued or running rows (from P0A.2).
  3. Dry run: list the exact changes. Get the owner's OK.
  4. Stop now, whatever D-REL1 says: `railway down -s scanner-worker` and `-s scanner-scheduler` (or scale to 0). Verify that no worker log line claims a job afterwards.
  5. Apply D-REL1. (a): list the attached volumes, get the owner's OK to drop them, then delete both services. (b): disconnect the source and turn off autodeploy.
  6. On every remaining service, disconnect any GitHub trigger P0A.1 found.
  7. Re-run the P0A.1 trigger query.
- Accept:
  - No service builds from GitHub on any branch.
  - The web and scanner-nightly deployment lists are unchanged.
  - kmac-vault and every other pre-existing service are unchanged (before/after list in the ops log).
  - The cost line is noted (about $0.60/month saved).

**P0A.5: Host job inventory.**
- Change: on the MBP and the mini:
  - Record `launchctl list | grep -i sarraficars`, `crontab -l` and `ls ~/Library/LaunchAgents/com.sarraficars.*`.
  - Unload `com.sarraficars.nightly-data-quality` on the MBP (D-IF7a), and on the mini any job that runs `scanner.py --delta` or grades a frozen DB.
  - Record each host's checkout path, branch, commit and `core.hooksPath`, and the host (never the credentials) of the mini's `.env` `INVENTORY_DATABASE_URL`.
  - Report whether the mini's `workspace/dealer_logs` has the test-leak dirs (`lifecycle-dealer-com`, `bravo-norows-com`, `charlie-norecipe-com`); cleanup is P1C.1.
- Accept: a per-host table in the ops log; no `com.sarraficars` job left grading a frozen DB; nothing deleted.

**P0A.6: Baseline.**
- Change: on AC power, with the owner's OK, at the current HEAD:
  - Run the offline suite chunked by file prefix (`-m "not integration and not slow" -q -p no:cacheprovider`, each chunk under 120 s, the loop under `run_in_background`).
  - Record pass/fail/skip/xfail per chunk and the ASSET-GATED SKIPS count.
  - Record `ruff check .` (expected: 3x F811 at test_csrf_delete_routes.py:7,15,24).
  - Run `python scripts/scanner_import_sweep.py` with a timeout.
- Accept: `docs/remediation/BASELINE_2026_10.md` holds the SHA and the counts. Pre-existing failures are listed, not fixed.

**Workflow shape.** Wave A: agents prepare and run the local parts of P0A.1, P0A.2, P0A.5 and P0A.6. The prod parts of P0A.1 and P0A.2 run from the prepared packs (owner, or an agent authorized for that unit). Wave B: P0A.3 and P0A.4, owner-run. P0A.1, P0A.3, P0A.4 and P0A.5 all log to `docs/SCANNING_OPS_LOG.md`. Per the Section 2 convention, each writes `docs/ops_log/<unit id>.md`, and integration creates SCANNING_OPS_LOG.md from P0A.1's block, then appends the others in unit order. Review lens: no secret value in any written file; read-only is proven (READ ONLY sessions, deployment lists unchanged). Integration: commit the docs to the branch, no push.

**Data repair.** None.

**Exit gate.**
- The ops log has blocks for P0A.1 and P0A.3-P0A.5; `SCHEMA_LEDGER_2026_10.md` and `BASELINE_2026_10.md` exist.
- The legacy services are stopped and then deleted or detached, and the trigger query shows no GitHub-built service.
- The MBP nightly-data-quality job is unloaded.
- The D-SEC1 stopgap env is set on Railway web.
- `git status` shows only the new docs.

**Release step.** None. The docs are pushed with Phase 1C.

## Phase 0B: Decision-input measurements (read-only)

**Goal.** Produce the numbers that D-OD1/2/3/7/8, D-OD12, D-SEC1, D-IF6 and D-DC5 wait on, plus the ground truth that the retirement repairs (P5B, P6B.6) need.

**Entry gate.** The owner approves the measurement hosts and network use (P0B.1, P0B.2, P0B.5: the mini with the tunnel prefix, or the MBP on AC). P0A.2 is done if P0B.2 adds prod counts. 0B can overlap 1A and 1B but must finish before 1C.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P0B.1 | owner-decisions-1 | Validation paths, VIN floors, fetch policy on a real sample (MAIN) | investigation | docs/monolith_audit_2026_10_01/measurements/validation_paths_2026_10.md (new); dealer discovery.md | M | B | none |
| P0B.2 | scanner-runtime-11 + reconcile retirement diff | Network audit of capped and group-feed dealers (MAIN) | investigation | workspace/dealer_logs/<8 dealers>/; _learning/platform_playbook.md | M | B | none |
| P0B.3 | security-10 | What headless Chromium contributes to car-chat research | investigation | docs/monolith_audit_2026_10_01/measurements/web_research_paths_2026_10.md (new) | S | A | none |
| P0B.4 | owner-decisions-17 | Brochure grid 'P' marker data check | investigation | docs/monolith_audit_2026_10_01/measurements/p_marker_2026_10.md (new) | S | A | none |
| P0B.5 | infra-18 | Value of the retired nightly and k8s post steps (MAIN, mini) | investigation | docs/SCANNING_OPS_LOG.md | M | B | none |
| P0B.6 | dead-code-22 | Which inventory_scrape branches discovery needs | investigation | docs/monolith_audit_2026_10_01/measurements/inventory_scrape_branches_2026_10.md (new) | M | A | none |

**P0B.1: Validation paths and floors.**
- Change, offline part. Read `workspace/recipes/*.json` with plain `json.load`, never `load_recipes` (which writes), and run SELECTs on dealer_recipes.
  - (a) Every live recipe under 10 VINs and under 5 VINs (33 dealers / 38 recipes on 10-07), classified by shape: pagination none; VIN in the URL or body; vehicles-recommendations or ws-rec widget; Gatsby page-data; `?vin=`; facet-filtered fragment (getInventoryAndFacets, 1-9 rows); a real condition-pinned side. Plus the dealers whose scan union changes under each D-OD1 option.
  - (b) The 53 HTML-walk recipes (dep_srp_page / html_page_query / jazel_srp_page / html_cards) that carry a stored total_count (42 equal to vehicle_rows, 10 with total > rows).
- Change, online part. About 40 dealers stratified by shape (cosmos_pt, page_query, dep_srp_page, html_page_query/jazel, carscommerce, dealer_com). Exclude P0B.2's eight dealers, so no two units append to the same dealer log at once. Include hondaofelcajon-com, toyotaofhb-com, 10 HTML-walk dealers, and one real small side. mclarennb-com has 0 live cars (last scraped 2026-08-05), so use darcarshondatenafly or covertbuickgmc. For every live recipe record:
  - the `validate_recipe` count;
  - `_check_recipe`/`validate_recipe_set` at pages=2 AND pages=40, with verdicts and reasons side by side (D-OD2 flips);
  - per-page statuses and challenge pages; VINs found beyond the stored total for HTML walks; 405/429/503 frequency.
- VDP part: 10 dealers x 5 VDPs. Compare chrome-only against rotation, and an explicit Chrome UA against the profile's own UA. Add 2-3 Dealer Inspire hosts at fleet-like concurrency (rotation vs slow-host serialization).
- Method:
  - Pass a paced wrapper as `fetch`: synth is paced by `SCANNER_SYNTH_FETCH_DELAY`, read once at import (synth/http.py:69); replay is not paced at all.
  - Randomize walk order per dealer, so one walk does not trip the WAF for the next.
  - Never call `gate_recipes`.
  - Run with `run_in_background` and a resumable checkpoint file, in the main checkout.
  - State in the doc that home-IP results may not transfer to Railway egress.
- Accept:
  - The doc has a per-shape agreement table, the recipe-by-recipe D-OD1 impact list, the HTML-walk dealers with VINs past the stored total, the VDP rotation clear rate and the UA effect.
  - A sha256 manifest of `workspace/recipes` and `SELECT max(updated_at) FROM dealer_recipes` are identical before and after.
  - Every dealer touched has a dated "validation-path measurement" block in discovery.md.
  - The numbers are quoted into D-OD1/2/3/7/8.

**P0B.2: Capped and group-feed dealers.**
- Change: HTTP only, per NETWORK_SCAN_PROCESS. Dealers: autosavvy-com, terrylabontechevy-com, toyotacarlsbad-com, avondaletoyota-com, mountainstatestoyota-com, cavendertoyota-com, duvalford-com, mbofstevenscreek-com. For each, replay page 1 of every stored recipe and read the site's own total (carscommerce `total_vehicle_count`, dealer.com `pageInfo.totalCount`, the ws-inv count), then compare it with the local active count (read-only). Questions to answer:
  - autosavvy-com (3,958 active used rows across 29 makes under one store name, unscoped carscommerce listing 6048971): does the feed carry several AutoSavvy rooftops, and how does it name them? The rooftop gate kept about 99% of a 4,000-row feed, which points to gate under-refusal, not early walk termination.
  - terrylabontechevy-com: reproduce offline from the captured payload why the replay kept 324 VINs across 40 pages but only 11 were persisted (scan_runs 2026-09-28T17:16, `capture_coverage.n=11`). Suspect: the second gate at `phases/dealer_run_steps/attribution.py:41` (roster_place) disagrees with the replay's `DealerCtx.for_store` (recipes.py:1005). Same class as Tutton CDJR 346→5.
  - Capped walks: the true lot size against the 40-page cap.
  - Retirements that followed capped walks (mountainstatestoyota 921 on 09-24, cavendertoyota 586, mbofstevenscreek 396): diff the retired VIN sets against an uncapped page-total walk where possible.
  - Prod counts for these dealers only with the owner's approval.
- Accept: each dealer has a dated discovery.md block (site total, recipe page sizes, rooftop fields seen) and a written verdict: correctly attributed / misfiled (VIN count) / truncated (true lot size) / retired live cars (count, or cannot_assess). No DB writes. The verdicts feed P5B.5-P5B.8 and P6B.6.

**P0B.3: Web research paths.**
- Change: run about 30 representative car-chat research questions across about 10 makes. For each, record which candidates came from direct guides, DuckDuckGo, or the Brave browser search (`_brave_search_links`, web_researcher.py:526-572, used whenever DuckDuckGo returns fewer than 2 links), and which fetch path produced the answer (`fetch_page_text_http` or `_fetch_with_playwright`), with latency and failures. agent.py:890 calls `search_and_summarize(query)` without year/make/model. Use one process, a scratch script outside the repo, on mains power.
- Accept: a table with HTTP-only success %, browser-dependent % (fallback or Brave), median latencies, and a D-SEC1 recommendation.

**P0B.4: 'P' marker.**
- Change: read-only, over `backend/dictionary/derived/brochure_text` (dictionary_paths.py:18-21; 2,966 files; there is no `backend/data/brochure_text`). Count 'P' cells by OEM and model year. Capture the legend lines ('P = Package', 'S = Standard', Wingdings check glyphs that extract as 'P'). Report which reading matches each legend: `brochure_extract._STANDARD_MARKERS` (L135, L763) or `brochure_trim_candidates._GRID_LETTER_MARKS` (L250, L782). List the overlays and adds whose standard-feature sets would change.
- Accept: a per-OEM table with legend text, counts and cited sample files; D-OD12 updated; nothing else written.

**P0B.5: Retired nightly steps.**
- Change: on the mini, against a scratch restore of the local inventory DB (owner approval; never the MBP on battery), run each candidate once and record rows and fields filled, runtime, packages/keys needed, and whether it writes price:
  - harvest_carscommerce.py, heal_from_recipes.py, harvest_html_jsonld.py;
  - rebuild_listings_index.py (and whether the web reads incomplete_listings);
  - `backend.scanner.post_scan.job --hours 24`;
  - repair_inventory_fields + enrich_from_dictionary (the k8s enrichment cron).
  Also copy every k8s cronjob's schedule and command into the table; P18A.7 deletes those manifests later.
- Accept: one row per step with measured numbers and a port / home-lane / drop recommendation; no writes to the real local DB; no prod access.

**P0B.6: inventory_scrape branches.**
- Change: read-only code study. discovery_capture only counts the records `scrape_inventory_path` returns. For each branch, say whether its output reaches the NetworkObserver ledger (keep) or produces rows discovery throws away (slimming candidate):
  - dealer.com bulk fetch / nudge (:8);
  - autowall HTTP plus Playwright fallback (:330-355);
  - shopperexpress API plus page fallback (:357-390);
  - pixel_motion pagination and cookie banner (:154, :272-276);
  - card-location scrape (:102);
  - location filter.
- Accept: a table with file:line, ledger reach and keep/slim. Any slimming is written as a follow-up spec for P17B, not done here.

**Workflow shape.** Wave A (P0B.3, P0B.4, P0B.6) runs in worktrees. Wave B (P0B.1, P0B.2, P0B.5) is MAIN, in the background with checkpoints, on separate hosts where possible (P0B.5 on the mini). These three are the one exception to "MAIN units run serially". They may overlap because all are read-only on the DB and write disjoint files: P0B.1 excludes P0B.2's dealers, only P0B.2 writes `_learning/platform_playbook.md`, and only P0B.5 writes the ops log. Review lens: zero writes to recipes or the DB (manifests prove it), pacing used, dealer logs present.

**Data repair.** None.

**Exit gate.** All six measurement outputs exist. The owner can answer D-OD1, D-OD2, D-OD3, D-OD7, D-OD8, D-OD12, D-SEC1 and D-IF6, and D-DC5 is informed. Every dealer touched by P0B.1/P0B.2 has dated log blocks.

**Release step.** None (docs ride with Phase 1C).

## Phase 1A: Stop retirement and catalog damage; disarm destructive scripts

**Goal.**
- Close the retirement holes that remove live listings today: one-condition and low-share buckets in all three writers, and `--no-reconcile` that leaves the scanner retiring.
- Make the EPA catalog reloads and the linker unable to wipe or dangle links.
- Stop InventoryEnricher's ACCESS EXCLUSIVE lock attempt on `cars`.
- Turn the "do not run" owner rules into refusals.
- Declare the SDK that prod chat imports.

**Entry gate.** Phase 0A exit is needed only for the optional hotfix deploy. No decisions block. Everything is local.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P1A.1 | reconcile-0 + reconcile missing 0b | Interim per-bucket retirement guards in all three writers | code | backend/scanner/pipeline/{reconcile,run,lifecycle}.py, backend/scanner/inventory_reconcile.py, backend/scanner/delta_scan.py, backend/tests/test_pipeline_reconcile_20260926.py, test_reconcile_condition_guard.py, test_pipeline_no_reconcile_env.py (new), test_reconcile_interim_buckets.py (new) | M | A | none |
| P1A.2 | epa-rebuild-1 (+ reviewer fix), hygiene-5 repoint | Disarm destructive catalog reloads; builder reads backend/dictionary/epa | code | backend/scripts/build_epa_master_pg.py, backend/scripts/import_epa_master.py, backend/tests/test_epa_catalog_destructive_guards.py (new) | S | A | none |
| P1A.3 | epa-rebuild-2 | Linker never clears links on a catalog error; deterministic ties | code | backend/catalog/resolver.py, backend/catalog/linker.py, backend/scripts/link_cars_to_catalog.py, backend/tests/test_catalog_resolver.py, test_catalog_linker.py | S | A | none |
| P1A.4 | db-layer-13a | InventoryEnricher issues no DDL on Postgres | code | backend/enrichment/service.py, backend/tests/test_enrichment_pg_schema.py (new) | S | A | none |
| P1A.5 | epa-turbo missing (retire heuristic backfill), precedence owner action, epa-turbo finding (Phase B) | Disarm destructive enrichment scripts | deletion + code | backend/scripts/backfill_forced_induction_pg.py (delete), backend/dictionary/enrich_from_dictionary.py (CLI), backend/scripts/heal_cylinders_from_vpic.py (CLI), docs/monolith_audit_2026_10_01/datascripts.md, backend/tests/test_destructive_script_guards.py (new) | S | A | none |
| P1A.6 | security-0 (security missing unit) | Declare anthropic, pdfplumber, brotli | code | requirements.txt, backend/tests/test_requirements_hygiene.py (new) | S | A | none |

**P1A.1: Interim retirement guards.**
- Change:
  1. One shared helper, `condition_bucket(cond) -> 'new' | 'used' | 'unknown'`, with blank meaning unknown. Reuse `inventory_reconcile.condition_bucket` (:113-120) in both paths. Do not reuse assess's `rows_used`, which counts blanks as used (assess.py:87-88).
  2. Pipeline: run.py:41-47 also captures the pre-run per-bucket baseline (COUNT FILTER per bucket over the same 30-day window). `reconcile_dealer(..., baseline_buckets=None, rows_buckets=None)` retires a bucket only when `rows_bucket > 0` and `rows_bucket >= RECONCILE_MIN_SHARE x baseline_bucket`. Unknown-bucket rows retire only when both new and used qualify. Output gains `kept_missing_condition` and `kept_low_bucket`. Wire it from run.py:160 and from lifecycle.py:330, which uses the retry run's own counts.
  3. Scanner: add a per-bucket matched-coverage check in `inventory_reconcile` (`matched_in_bucket / active_in_bucket >= MIN_COVERAGE`) next to the zero-only guard at :223-227. A bucket that fails it is kept.
  4. Delta: delta_scan.py:342-343 passes `condition_buckets_from_vehicles(vehicles)`.
  5. `--no-reconcile` sets `os.environ['SCANNER_RECONCILE']='0'` inside `run()` before the batches (runner.py:106 copies os.environ). No signature change, which keeps the stubs at test_recipe_lifecycle.py:389/:442/:459 and the pins at test_dealer_pipeline_surface.py:149 intact.
- Tests:
  - A 300-new + 200-used lot where the run re-sees the 300 new: the pipeline retires 0 used.
  - The run returns 300 new + 5 used: both paths keep the used rows.
  - Blank-condition rows retire only when both buckets qualify.
  - The `--no-reconcile` env reaches the scanner subprocess (monkeypatched subprocess.run).
  - The four existing cases in test_pipeline_reconcile_20260926.py pass unmodified.
- Accept: the tests above pass; grep shows every `reconcile_dealer` call site passes the bucket arguments; test_dealer_pipeline_surface.py and test_recipe_lifecycle.py pass unchanged.

**P1A.2: Disarm catalog reloads.**
- Change:
  - `build_epa_master_pg.py` refuses EVERY non-dry-run write, `--rebuild` included, during argument handling and before any DB connection (exit 2, "writes disabled until the id-preserving builder lands (P10B.2)"). Without this, interim inserts would later be mislabelled as legacy rows.
  - Its source becomes `dictionary_paths.EPA_DIR` via `rglob("*_EPA.csv")`, with a `--source-dir` override, and it refuses when fewer than `--min-files` (10,000) files are found. `--dry-run` still works.
  - `import_epa_master.import_csv(append=False)` and the `--yes` full replace raise RuntimeError (exit 2, counts printed) when any `cars.epa_master_id` is set or `epa_extended_specs` has rows. Run the extended-specs probe inside a savepoint (or roll back on error) so a missing table cannot abort the Postgres transaction. A full replace still works on an empty, unlinked catalog. `--append-years` is unchanged.
- Tests: refuses before connecting; default mode refuses writes; recursive source dir; short source dir refused; a full replace refuses when a car is linked (SQLite) and is allowed on an empty, unlinked catalog.
- Accept:
  - The reviewer confirms by reading both scripts that no reachable path runs `DELETE FROM epa_master` while links or extended specs exist.
  - `--rebuild` exits 2 without opening a connection.
  - `--dry-run` on local reports about 12,1xx files.
  - `git grep 'ROOT / "DICTIONARY"'` is empty outside `.claude/worktrees`.

**P1A.3: Linker safety.**
- Change:
  - `resolver._query_year_make` (:81-92) adds `ORDER BY id`. It returns `[]` only for SQLite's `OperationalError 'no such column'`, and raises a new `CatalogUnavailableError` for any other failure.
  - The sort key at :335 becomes `(-score, int(id))`, so the lowest id wins a tie.
  - `linker.link_cars_by_vins` aborts the batch with no writes on CatalogUnavailableError.
  - `link_fleet` exits non-zero before any UPDATE.
  - MIN_CONFIDENCE (0.45) and `apply_vin_facts` are untouched.
- Tests: `test_tie_between_identical_twins_picks_lowest_id`, `test_query_error_raises_catalog_unavailable`, `test_catalog_error_never_clears_links`, `test_unresolvable_car_link_cleared`.
- Accept: the tests above pass. On AC power, run `link_cars_to_catalog.py --dry-run` (full mode) on local PG before and after the change, with a per-car diff, and record in the PR how many links would flip (expected: small).

**P1A.4: Enricher without DDL.**
- Change:
  - `ensure_enrichment_columns` (service.py:97-105) returns without DDL on Postgres; V001 owns engine_l, mpg_city, mpg_highway and packages. Today the PRAGMA no-op (inventory_compat.py:23-27) leads to an `ALTER TABLE cars ADD COLUMN engine_l` that takes ACCESS EXCLUSIVE before it fails with DuplicateColumn.
  - `_ensure_haiku_cache_table` (:108-127) checks `to_regclass('haiku_spec_cache')` on Postgres and creates the table only when it is absent, under `SET LOCAL lock_timeout='3s'` (V026 takes ownership in P4.3).
  - SQLite is unchanged. The Haiku gate waits for D-PR8 (P8B.8).
- Tests: a fake Postgres compat connection sees no ALTER or CREATE on `cars`, and nothing raises; the SQLite enrichment tests are unchanged.
- Accept: `InventoryEnricher()` reached from `post_scan --post-enrich` (post_scan/pipeline.py:570) and from dev/routes.py:1236 issues no `ALTER TABLE cars`.

**P1A.5: Disarm enrichment scripts.**
- Change:
  1. Run a consumer grep (scripts, deploy, docs, tests, string and importlib paths), then delete `backend/scripts/backfill_forced_induction_pg.py`. It is marked DEAD at datascripts.md:598, and one run re-guesses about 125k NULL active cars. Mark the datascripts row deleted.
  2. `enrich_from_dictionary.py --all` refuses (and explains why) unless `ALLOW_DICTIONARY_OVERWRITE=1`. P8B.5 replaces this guard with precedence.
  3. `heal_cylinders_from_vpic.py` Phase B (forced_induction, :120-148) runs only with an explicit `--phase-b` flag. It clears EPA- and VIN-backed labels; P10C.6 fixes it properly.
  The non-destructive defaults are unchanged.
- Tests: `--all` refuses without the env var; Phase B does not run by default; no references remain to the deleted script.
- Accept: the tests above pass and the grep is clean.

**P1A.6: Declare imported packages.**
- Change: add to requirements.txt:
  - `anthropic>=0.116`: backend/llm/client.py:216,406 is the only Claude transport since 75671a38f, and nothing installs it today.
  - `pdfplumber>=0.11`: post_scan/window_sticker.py:256 and enrichment/brochure_extract.py.
  - `brotli>=1.1`: scripts/build_static_compressed.py:34 runs in the Dockerfile.web build and gets brotli only through crawl4ai today.
  No removals here (D-DEP1, P15B.1). Add `backend/tests/test_requirements_hygiene.py`: an AST scan of `backend/` and `scripts/` in which every third-party top-level import must be declared, or listed in a short commented allowlist with reasons (httpx, numpy, werkzeug are transitive today).
- Tests: test_requirements_hygiene.py.
- Accept: the test passes. In a scratch venv (owner OK, mains power), `pip install -r requirements.txt` succeeds, then `python -c 'import anthropic, pdfplumber, brotli'` succeeds.

**Workflow shape.** All six units run in wave A, in parallel worktrees (disjoint files). Review lens: no path retires more rows than before; no write to epa_master is reachable; no DDL on Postgres from enrichment; every guard fails closed. Integration order: P1A.6, P1A.4, P1A.5, P1A.2, P1A.3, P1A.1.

**Data repair.** None.

**Exit gate.**
- pytest, chunked: test_pipeline_reconcile_20260926.py, test_reconcile_condition_guard.py, test_pipeline_no_reconcile_env.py, test_reconcile_interim_buckets.py, test_scanner_inventory_reconcile.py, test_delta_scan.py, test_delta_scan_full_scan_parity.py, test_dealer_pipeline_surface.py, test_recipe_lifecycle.py, test_epa_catalog_destructive_guards.py, test_catalog_resolver.py, test_catalog_linker.py, test_enrichment_pg_schema.py, test_destructive_script_guards.py, test_requirements_hygiene.py.
- `python backend/scripts/build_epa_master_pg.py --rebuild; echo $?` prints 2 without connecting.
- Read-only local SQL: epa_master and epa_extended_specs counts unchanged (70,496 / 49,912 at planning time).
- `ruff check` on the changed files is clean.

**Release step.** No push yet; Phase 1 pushes once, at the end of 1C. Optional hotfix, if P0A.1 found prod car chat failing: the owner deploys web from the Phase 1A integration commit. Use a clean checkout, then `docker build -f Dockerfile.web` and a run against local Postgres on AC power, then `railway up --service web --detach`. Verify that car chat answers through Claude. P2B.6 later redeploys from a tag.

## Phase 1B: Recipe store Phase 0, test isolation, footgun fixes

**Goal.**
- Recipe writes are atomic, the cache is anchored and tagged with the store it mirrors, `saved_at` is a real last-write stamp (so stale flags and synthesized sets propagate), and store I/O is off the event loop.
- Audit scripts stop writing shared recipe state.
- Tests can no longer write the real `workspace/`.
- `synthesize_recipes --dry-run` is really dry, and the platform-candidates report stops mislabelling dealers.

**Entry gate.** Phase 1A merged (both edit delta_scan.py). D-RS5 answered (P1B.5).

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P1B.1 | scanner-runtime-1 (+ reviewer) | Test-isolation guard for workspace/ | test | conftest.py, backend/tests/test_workspace_isolation_guard.py (new) | S | A | none |
| P1B.2 | recipes-1 + scanner-runtime-7 + recipes/infra missing (cache override) | Atomic recipe writes; anchored cache dir; RECIPES_CACHE_DIR override | code | backend/scanner/recipes.py, backend/scanner/vdp/vdp_recipes.py, backend/scanner/recipe_synth.py, backend/scanner/recipe_store.py (log level), backend/tests/test_recipe_atomic_write.py, test_recipes_dir_anchor.py (new) | S | A | none |
| P1B.3 | recipes-2 (+ reviewer), recipes-14a (docstrings) | saved_at becomes the last-write stamp; push-up logged; store harness | code | backend/scanner/recipes.py, backend/scanner/recipe_store.py, backend/tests/recipe_store_harness.py (new), test_recipe_sync_freshness.py (new) | S | B | P1B.2 |
| P1B.4 | recipes-3 (+ reviewer) | Recipe-store I/O off the event loop, including delta hint writes | code | backend/scanner/recipes.py, phases/dealer_run_steps/feed.py, discovery_capture.py, delta_scan.py, backend/tests/test_recipe_async_io.py (new) | S | C | P1B.3 |
| P1B.5 | recipes-10 | `persist=False` for geocode, attribution and audit replays | code | backend/scanner/recipes.py, backend/scripts/{geocode_dealers,attribute_feed_rooftops,audit_recipe_coverage}.py, backend/tests/test_recipes.py | S | D | P1B.4 |
| P1B.6 | recipes missing (store identity) | Cache tagged with its store; push-ups refused on mismatch | code | backend/scanner/recipes.py, backend/scanner/vdp/vdp_recipes.py, backend/tests/test_recipe_cache_identity.py (new), docs/RAILWAY_SCANNING.md | S | E | P1B.3 |
| P1B.7 | scanner-runtime-12 + hygiene-19 | platform_candidates: fail closed, census, dated verdicts, no contradictions | code | backend/scripts/platform_candidates.py, backend/tests/test_platform_fingerprint.py | S | B | P1B.2 (reads its anchored RECIPES_DIR) |
| P1B.8 | owner-decisions-11 | `gate_recipes(record=)`; `synthesize_recipes --dry-run` writes nothing | code | backend/scanner/recipe_validation.py, backend/scripts/synthesize_recipes.py, backend/tests/test_synthesize_recipes_cli.py (new) | S | A | none |

**P1B.1: Test-isolation guard.**
- Change:
  - The root conftest.py sets `DEALER_LOGS_ROOT` to a session temp dir at import time, before any backend import, the same way HERMETIC_DB_ENV does (:12-44). That covers the import-time LOG_ROOT constants in pipeline/constants.py:16, scripts/platform_candidates.py:35, scripts/discovery_probe.py:42 and recipe_validation.py:79.
  - An autouse fixture monkeypatches `backend.scanner.recipes.RECIPES_DIR` to a per-test tmp dir unless the test set it. First grep for tests that read real recipe files and exempt them explicitly.
  - `pytest_sessionstart` snapshots names and mtimes under `workspace/dealer_logs`, `workspace/recipes` and `workspace/recipes/vdp`. `pytest_sessionfinish` fails the session and lists the touched paths. It downgrades to a warning when `scanner_liveness.scanner_pids()` is non-empty or a scanner lock is live, and honours `WORKSPACE_GUARD=0`.
- Tests: pytester or a subprocess against a scratch ROOT copy (never the real tree) proves that a deliberate write is reported, and that the live-scanner downgrade works.
- Accept: running test_recipe_lifecycle.py, test_platform_fingerprint.py, test_scan_timing.py and test_scan_lock_path_env.py (chunked) leaves `ls -laT workspace/dealer_logs/_learning workspace/recipes | md5` identical; test_dealer_pipeline_surface.py passes.

**P1B.2: Atomic writes and anchored cache.**
- Change:
  - `_atomic_write_json(path, obj)`: write to `<name>.json.tmp.<pid>.<thread>` in the same dir, flush and fsync, `os.replace`, and remove the temp file on any exception. Use it in save_recipes (:276), in the DB-adoption write in load_recipes (:258), and in save_vdp_recipes (vdp_recipes.py:240).
  - `RECIPES_DIR = Path(os.environ.get('RECIPES_CACHE_DIR') or ROOT/'workspace'/'recipes')`. `VDP_RECIPES_DIR` follows the override (`<override>/vdp`). Keep the attribute names; 4 test files monkeypatch RECIPES_DIR.
  - `recipe_synth.RECIPES_DIR` (:288, a dead duplicate pinned only by test_recipe_synth_surface.py:29) refers to `recipes.RECIPES_DIR`.
  - recipe_store write-through failures (:161) log at WARNING, rate-limited to once per process and error class.
- Tests:
  - A failure mid-write leaves the previous file byte-identical, with no temp file left.
  - The VDP save is atomic.
  - The dir resolves to `<repo>/workspace/recipes` with cwd=/tmp, and to `/app/workspace/recipes` when the fake `__file__` is `/app/backend/scanner/recipes.py` (the Railway symlink layout, scripts/railway_scan_fleet.sh:10-17).
  - The override is honoured; `_recipe_path('x')` is unchanged after `chdir(tmp)`.
  - The `*.json` globs in import_recipes_to_db, heal_from_recipes, audit_recipe_coverage and migrate_recipe_aliases never match temp files.
- Accept: no `path.write_text` remains in the recipe or VDP save paths; test_recipes.py, test_vdp_recipes.py, test_recipe_synth_surface.py and test_html_cards_20260926.py pass.

**P1B.3: saved_at as last-write stamp.**
- Change:
  - save_recipes stamps every row's `saved_at` with one `time.time()` taken per call, before the file and DB writes. That covers mark_stale, replay success, un-stale, ensure_recipe, cascade, synthesize and promote.
  - The push-up branch of load_recipes (:262-268) does NOT stamp. It logs a WARNING naming the dealer id and how many DB rows it overwrites.
  - Docstrings (load_recipes; recipe_store :11-16): saved_at means "last write of this dealer's set", and it stays a per-write stamp in later phases.
  - New `backend/tests/recipe_store_harness.py`: a per-test SQLite file wired through `recipe_store._conn`, `_table_ready` reset, and two RECIPES_DIRs standing in for two hosts. It asserts the DB path is under tmp_path, which replaces the leaky shared session DB at test_recipes.py:150-155.
- Tests: a stale flag, an un-stale and a coverage update each reach the other host; a synthesized set saved with saved_at=0 rows is not reverted by an older cache; the push-up warning is logged.
- Accept: test_recipes.py, test_recipe_lifecycle.py, test_recipe_validation.py and test_egress_tag_blocked.py pass. Deployment note: the P1C.3 backfill may run only on hosts that already have this code. The mini pulls it before 1C; Railway gets it in P6B.1.

**P1B.4: Store I/O off the loop.**
- Change:
  - Move the success-persist block of `try_fetch_via_recipes` (recipes.py:1135-1144) into `_persist_replay_success` and call it with `await asyncio.to_thread`.
  - feed.py:42: `await asyncio.to_thread(_apply_recipe_provider_hint, run)`.
  - In discovery_capture, call load_recipes (:86, :179) and promote_from_ledger (:169) via `to_thread`.
  - In delta_scan, call `_write_hint_note` at :165, :213, :266 and :273 via `to_thread`.
- Tests: record `threading.current_thread()` in the store calls and prove none runs on the loop thread, for replay, the feed provider hint, the discovery promote (stub browser) and the delta hint notes.
- Accept: the replay tests pass; test_dealer_run_golden_20261001.py passes with the 31 recorded `load_recipes` events in the same order.

**P1B.5: persist=False.**
- Change: `try_fetch_via_recipes(..., persist: bool = True)`. With False it makes no mark_stale, save_recipes or status writes. geocode_dealers.py:311, attribute_feed_rooftops.py:217 and audit_recipe_coverage.py:59 pass False. heal_from_recipes and unstale_host_blocked_recipes keep persisting.
- Tests: `persist=False` makes zero store writes (spies on save_recipes, mark_stale, set_scan_hints) and returns the same records.
- Accept: the test passes; grep shows all 3 scripts pass `persist=False`.

**P1B.6: Store identity.**
- Change: write `<cache dir>/_store.json` holding the fingerprint of the store this cache mirrors (a hash of DSN host plus dbname, never credentials). If a process loads with a different store fingerprint, it warns once and refuses file-to-DB push-ups, unless `RECIPES_CACHE_DIR` points elsewhere or the cache is re-seeded with a small `--reseed` helper. This makes the home-IP prod repair (P6B.2) safe: the MBP cache mirrors the local DB, and today its newer files would be pushed into prod (recipes.py:262-268). Document it in RAILWAY_SCANNING.md.
- Tests: matching fingerprint behaves normally; mismatch means no push-up plus a warning; a missing `_store.json` is created on the first successful load.
- Accept: the tests pass and the doc paragraph is present.

**P1B.7: platform_candidates.**
- Change:
  - `recipe_state` (:117-141) returns `unknown` when the lookup raised, or when a recipe file exists for the slug but nothing came back. It reads the anchored real path from P1B.2, or does a read-only DB read; never load_recipes, which writes.
  - `unknown` dealers are counted, not clustered.
  - `render_markdown` (:199-241) prints a census line (live / none / stale / rejected / unknown), plus the root and cwd used.
  - `last_verdict` (:93-111) returns the verdict's date, and member lines show it.
  - A member whose newest scan_runs.md assess verdict (ok / thin / inaccurate) is newer than its probe stamp is dropped with a note (e.g. autosavvy-com: 403 probe on 09-26, 3,958 rows on 09-28).
  - Never print "nearest known template: none ... all false" when a template already detects the members (L225-237).
  - Diagnose why `load_recipes` returned [] for autosavvy-com while `workspace/recipes/autosavvy-com.json` exists (cwd, aliases, parse failure, or the 10-05 test stubs), and record the cause in the PR.
- Tests: extend backend/tests/test_platform_fingerprint.py, which already covers this module.
- Accept: a tmp root with an unreadable recipe file gives `unknown`; a dealer with a newer ok scan is excluded; the census header is present; no contradictory template lines; the existing cases pass.

**P1B.8: Dry run that is dry.**
- Change: `gate_recipes(..., record=True)` (recipe_validation.py:914-939). With `record=False` it skips write_discovery_log and record_recipe_status but still returns the verdict. synthesize_recipes passes `record=False` under `--dry-run`, and checks `_healthy_recipe_exists` before gating (or gates with `record=False` when it will not save). Today the gate at :247 runs before the healthy check at :255 and the dry-run check at :258.
- Tests: `--dry-run` writes no discovery.md under a tmp log root and never calls set_scan_hints; a healthy recipe without `--force` leaves recipe_status unchanged; a real save still writes both.
- Accept: the tests pass.

**Workflow shape.** Wave A: P1B.1, P1B.2, P1B.8. Wave B: P1B.3 and P1B.7 (disjoint files; P1B.7 needs P1B.2's anchor). Then serial on recipes.py: C (P1B.4), D (P1B.5), E (P1B.6). Review lens: no behaviour change beyond the stated ones; tests never touch the real workspace; the harness never touches the hermetic session DB. Integration order follows the waves.

**Data repair.** None (1C).

**Exit gate.** pytest, chunked: test_workspace_isolation_guard.py, test_recipe_atomic_write.py, test_recipes_dir_anchor.py, test_recipe_sync_freshness.py, test_recipe_async_io.py, test_recipe_cache_identity.py, test_recipes.py, test_vdp_recipes.py, test_recipe_lifecycle.py, test_recipe_validation.py, test_egress_tag_blocked.py, test_dealer_run_golden_20261001.py, test_platform_fingerprint.py, test_synthesize_recipes_cli.py, test_recipe_synth_surface.py, test_delta_scan.py. After the run, the `workspace/` mtime snapshot is unchanged.

**Release step.** No push (end of 1C).

## Phase 1C: Local repairs after the damage stops

**Goal.** With the causes fixed, repair the local state:
- remove the 2026-10-05 test-leak artifacts and regenerate a truthful platform-candidates report;
- backfill the 201 `saved_at=0` recipe rows so older caches stop reverting synthesized sets;
- reconcile each host's recipe cache against the local DB.
Then push Phase 1.

**Entry gate.**
- 1A and 1B merged; Phase 0B finished (both write `_learning`); D-SR7 answered.
- The mini is on the 1B commit (`git rev-parse` on the mini). Phase 1 is not pushed until the end of 1C, so the mini fetches the integration branch straight from the MBP checkout over SSH (`git fetch <mbp-host>:<repo path> feature/http-only-scans`, then a fast-forward). It does the same again for the wave-A 1C code before its P1C.4 run.
- No scanner is running on any host: lock files on the MBP and the mini, `pgrep -f scanner.py` on both, and `pg_stat_activity` shows no scanner and no idle-in-transaction sessions.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P1C.1 | scanner-runtime-13a | Remove the 2026-10-05 test leak (MAIN) | data_repair | workspace/dealer_logs/_learning/errors_index.md, workspace/dealer_logs/{lifecycle-dealer-com,bravo-norows-com,charlie-norecipe-com}/ | S | B | none |
| P1C.2 | recipes-11 (+ reviewer) | `reconcile_recipe_store` tool on Phase 0 code; safe import_recipes_to_db | code | backend/scripts/reconcile_recipe_store.py (new), backend/scripts/import_recipes_to_db.py, backend/tests/test_reconcile_recipe_store.py (new) | M | A | none |
| P1C.3 | recipes-4 (+ reviewer) | Backfill `saved_at=0` recipe rows (code, then MAIN run) | code + data_repair | backend/scripts/backfill_recipe_saved_at.py (new), backend/tests/test_backfill_recipe_saved_at.py (new) | S | A | none |
| P1C.4 | recipes-12 (+ reviewer) | Local store repair: MBP and mini caches vs local Postgres (MAIN) | data_repair | workspace/recipes/, workspace/backups/, dealer discovery.md | S | C | P1C.2, P1C.3 |
| P1C.5 | scanner-runtime-13b | Regenerate platform_candidates.md (MAIN) | data_repair | workspace/dealer_logs/_learning/platform_candidates.md | S | D | P1B.7, P1C.4 |
| P1C.6 | recipes-14a | Docs for Phase 0 recipe semantics, CHANGELOG | docs | docs/data_architecture_plan.md, docs/CLOUD_RESTRUCTURE_PLAN.md, docs/RAILWAY_SCANNING.md, CHANGELOG.md | S | A | none |

**P1C.1: Test leak.**
- Change:
  1. Back up `workspace/dealer_logs/_learning` and the 3 dirs to `workspace/backups/dealer_logs_testleak_<stamp>.tgz`.
  2. Read-only checks: no cars rows and no recipe files for lifecycle-dealer-com, bravo-norows-com, charlie-norecipe-com or broken-com.
  3. Dry run: print errors_index.md:119-135. That is 17 lines: 15 lines for lifecycle/bravo/charlie; the avondaletoyota-com fake cap_hit at :134, which matches test_scan_timing.py's 1140 s / 1667-page fixture; the broken-com line at :135. Several carry the fixed test stamp `2026-09-28 12:00 UTC`, so also scan the rest of the file for that stamp and the fixture ids, and list any older leak.
  4. Owner OK; apply.
  5. Append one lesson line describing the leak and the guard (P1B.1).
  6. Repeat on the mini if P0A.5 found the artifacts there.
- Accept: the tarball lists every removed path; the diff removes only the dry-run lines; the lesson line is present.

**P1C.2: Store reconcile tool.**
- Change: new `backend/scripts/reconcile_recipe_store.py`.
  - Reads cache JSON with `json.load`, never through load_recipes.
  - Classifies each dealer as identical, cache-only, db-only or differs, and merges per key:
    - last_ok_at, field_coverage and total_count come from the side with the newer last_ok_at;
    - stale follows the DB unless the cache shows a strictly newer success for that recipe;
    - ties resolve to live (stale_retry re-marks a dead recipe at the cost of one request);
    - a cache-only key is added only if its saved_at is newer than the DB `max_saved_at` (after P1C.3). Never use `DB.updated_at`, which every hint write bumps (683 of 687 local rows carry hints);
    - DB-only keys are kept.
  - Modes: `--cache-dir` (file → DB, then the caches are rewritten from the DB atomically) and `--source-dsn/--target-dsn` (DB → DB, used by P7.9).
  - Dry run is the default and writes `report.tsv` under `workspace/backups/recipes_reconcile_<stamp>/`.
  - `--apply` requires `--backup-dir`, writes a tar of the cache dir and a JSON export of every row it will touch first, applies through a guarded `UPDATE ... WHERE dealer_id=? AND updated_at=?`, and skips and reports any row changed since it was read. It switches to `db_mutate` after P7.1.
  - auth_headers are never printed.
  - `import_recipes_to_db.py` keeps its CLI, becomes a `--cache-dir` wrapper, and never overwrites a newer DB row wholesale (it does today at :50-60). Grep consumers first (datascripts.md:425, the comment at recipes.py:150).
- Tests:
  - Every class.
  - A cache key older than the DB write is dropped; a newer one is added.
  - The stale-vs-newer-success rule.
  - A `saved_at=0` DB row against an older cache resolves to the DB.
  - The import wrapper leaves a normreeves-like September DB set unchanged against a July file.
  - Apply requires a backup and is idempotent; a concurrently modified row is skipped.
- Accept: the tests pass. A dry run on the MBP reproduces the 2026-10-07 baseline classes (549 identical / 12 differs with equal saved_at / 31 DB newer / 1 file newer over saved_at=0 / 94 DB-only / 0 file-only), allowing for drift.

**P1C.3: saved_at backfill.**
- Change: new script. For each dealer_recipes row whose entries have `saved_at` 0 or null:
  - Set those entries to the epoch of the row's `max(last_ok_at)`; ensure_recipe sets last_ok_at at validation (pipeline/recipes.py:70). Fall back to `updated_at`. Recompute `max_saved_at` via `_derive_meta`.
  - Write with a guarded UPDATE (`... AND updated_at=?`), skipping and reporting rows that changed.
  - If a local cache file has a key whose cache last_ok_at is newer than the DB's (today only scottclarkhonda-com), skip the dealer and leave it to P1C.4.
  - Never touch scan_hints; never print auth_headers.
  - `--dry-run` is the default (counts plus up to 20 ids). `--apply` needs `--backup-dir` and writes a JSON export of the touched rows first. `--restore` is available.
  - MAIN run: `pg_dump -t dealer_recipes` into `workspace/backups/`; dry run (expect about 201 rows: 195 from 09-28, 4 from 09-29, 2 from 08-06); owner OK; apply; verify. Write a dated discovery.md line for every dealer whose effective set changes on this host, plus one errors_index.md line.
  - The prod run happens in P6B.2.
- Tests: dry run writes nothing (md5 of recipes_json unchanged); apply sets saved_at from last_ok_at; a changed row is skipped; the scottclarkhonda shape is skipped; restore round-trips.
- Accept: `SELECT count(*) FROM dealer_recipes WHERE max_saved_at=0 AND recipe_count>0` is 0 locally, apart from the listed skips. normreeves-com adopts the September DB set (1,141-char post_template), and the July file (827 chars) is not pushed up.

**P1C.4: Local store repair.**
- Change, on each host (the MBP; the mini with the tunnel prefix):
  1. Tar `workspace/recipes` into `workspace/backups/` FIRST.
  2. MBP only: `pg_dump -t dealer_recipes`.
  3. `reconcile_recipe_store --cache-dir` dry run. Review report.tsv, especially scottclarkhonda-com, normreeves-com and the 9 dealers whose DB last_ok is newer than the cache (alfaromeoofanaheimhills, aaronfordofescondido-com, hendrickporsche, lambonb, audihuntsville, 5starford, mclarennb, autoboutiqueohio, audiofcostamesa).
  4. Apply.
  5. Write a `_reconciled` marker for the store fingerprint (P1B.6) in each cache dir.
  6. Write a dated discovery.md line for each dealer whose live/stale state changed, on the host where it changed, plus one errors_index.md line for the "recipe store divergence" class.
- Accept: backups are non-empty before apply; a second dry run on each host reports 0 differs; 5 sampled dealers load identically on the MBP and the mini.

**P1C.5: Regenerate the report.**
- Change: record `SELECT count(*), max(updated_at) FROM dealer_recipes` and a sha256 manifest of `workspace/recipes` first. After P1B.7, `recipe_state` reads files and the DB read-only and never calls load_recipes (which writes), so both must be unchanged after the run. The previous report is already in P1C.1's tarball. Run `python -m backend.scripts.platform_candidates` from the repo root.
- Accept: the census header is present; none of the 15 sampled dealers with live recipe files (autosavvy-com through mallofgamazda-com) is listed as "recipe none"; the dealer_recipes count/max and the recipes manifest are unchanged.

**P1C.6: Docs.**
- Change:
  - docs/data_architecture_plan.md: saved_at is the last-write stamp; the push-up warning; `RECIPES_CACHE_DIR`; the store fingerprint.
  - Correct CLOUD_RESTRUCTURE_PLAN.md:295 ("prefers the newer copy" is wrong).
  - Correct RAILWAY_SCANNING.md:104-112: the volume cache keeps the 09-29 stale flags until Phase 0 code is deployed (P6B.1).
  - CHANGELOG `[Unreleased]` lines for Phase 1, plus the 6 commits from 10-01..10-07 that were never recorded (787546e21, 5986b498b, d096c2221, 77cddcb56, 1344cbd09, 39d7241db).
  - Leave the recipe_store.py docstring alone; P1B.3 owns it.
- Accept: the docs no longer claim the old freshness rule.

**Workflow shape.** Wave A (P1C.2 code, P1C.3 code, P1C.6) in worktrees. Then MAIN, serially: P1C.1, then the P1C.3 run, P1C.4 and P1C.5. Review lens: a backup exists before every write; dry-run outputs are attached to the PR; no output contains an auth header.

**Data repair.** Local only, as in P1C.1 and P1C.3-P1C.5: backup, dry run, owner OK, apply, verify. Rollback: `--restore` from the JSON export, or restore the tarballs.

**Exit gate.**
- pytest: test_reconcile_recipe_store.py and test_backfill_recipe_saved_at.py, plus a one-time chunked re-run of every 1A and 1B targeted test on the integrated branch.
- The SQL checks above pass, and a second reconcile dry run reports 0 differs.
- Dealer logs are written for every dealer whose state changed.

**Release step (Phase 1).**
1. Confirm CHANGELOG `[Unreleased]` is complete (P1C.6).
2. Run `scripts/bump_version.sh patch`.
3. Secrets-scan `git log -p origin/feature/http-only-scans..HEAD` for AIza, sk-ant-, sk_live_, AKIA, gh[pousr]_, PRIVATE KEY, `postgres://user:pw@` and `eyJ...eyJ`.
4. `git push origin feature/http-only-scans`, but only after P0A.1 confirmed that no service builds from this branch and P0A.4 is done.
5. No deploy. Scanner-nightly is deployed in P6B.1 and web in P2B.6.

## Phase 2A: CI that actually runs

**Goal.** Make the lint gate green and trigger CI on every branch push. Add the server-side release guard. Shake CI down on throwaway branches, using CI itself as the source of truth for Linux and Python 3.12 failures, until it is green.

**Entry gate.**
- Phase 1C pushed.
- D-REL3 answered (including the `ci/*` sub-question) and D-TC5 answered.
- P0A.1 confirmed that no Railway service builds from a non-main branch.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P2A.1 | tests-ci-1 (= release-3) | Make lint green: F811 fixture re-export becomes a helper | code | backend/tests/test_hidden_dealers.py, backend/tests/test_csrf_delete_routes.py | S | A | none |
| P2A.2 | release-4 + tests-ci-2 (+ reviewers) | CI on every branch push, PRs to main and dispatch; pinned test deps; CPU torch; node; junit | ops | .github/workflows/ci.yml, requirements-test.txt (new), backend/tests/test_ci_workflow.py (new) | M | A | none |
| P2A.3 | tests-ci missing (offline network guard) | `TESTS_BLOCK_NETWORK=1` refuses non-loopback connects | test | backend/tests/conftest.py, backend/tests/test_network_guard.py (new) | S | A | none |
| P2A.4 | release-5 (+ reviewer) | Server-side release guard: VERSION must rise, CHANGELOG section required | code | scripts/release_guard.py (new), .github/workflows/ci.yml, scripts/git-hooks/pre-push, backend/tests/test_release_tooling.py (new) | M | B | P2A.2 |
| P2A.5 | release-0 | CI shakedown on throwaway `ci/shakedown-<n>` branches (MAIN, git ops) | ops | none (git refs) | S | C | P2A.1-P2A.4 |
| P2A.6 | tests-ci-3a | Failure ledger from the CI junit artifacts | investigation | docs/monolith_audit_2026_10_01/tests.md | S | D | P2A.5 |
| P2A.7 | tests-ci-3b..n | Fix test-only failure classes (one worktree per class) | test | backend/tests/* (per the ledger) | L | E | P2A.6 |

**P2A.1: Lint green.**
- Change: move the body of the `env` fixture (test_hidden_dealers.py:78-86) into a plain helper, `make_account_env(monkeypatch, tmp_path, app_factory)`. Each module defines its own thin `env` fixture that returns the helper's result. test_csrf_delete_routes.py imports `CSRF`, `DEALER_ID`, `_login` and `make_account_env`; it no longer re-exports the fixture and uses no `noqa`. No per-file-ignore: pyproject forbids new ignores for fresh violations. This replaces release-3's per-file-ignore (Appendix B). `backend/tests/conftest.py` is untouched.
- Tests: both modules collect and pass the same number of tests as before.
- Accept: `.venv/bin/ruff check . --no-cache` with ruff 0.15.22 exits 0; pyproject.toml is unchanged.

**P2A.2: CI triggers and install.**
- Change, in `.github/workflows/ci.yml`:
  - `on: push: branches: ['**']; pull_request: branches: [main]; workflow_dispatch:`. Dispatch only works once ci.yml is on `main`, i.e. after P2B.3. No `tags-ignore` (redundant with `branches`).
  - Top-level `permissions: contents: read`.
  - `concurrency: {group: ci-${{ github.event.pull_request.number || github.ref }}, cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}}`.
  - Do NOT add the `if:` that skips PR runs for same-repo branches: skipped jobs report success on the same SHA and would mask a red push run. The push run and the PR run therefore both run for a branch with an open PR to main, because their concurrency groups differ (`refs/heads/x` vs the PR number). That duplication is accepted knowingly; note it in RELEASING.md (P2B.7) for the D-REL5 minutes estimate.
  - `timeout-minutes`: lint 5, pytest 30, pytest-integration 15.
  - Keep the job ids `lint`, `pytest` and `pytest-integration`; they become required checks in P2B.5.
  - pytest job:
    1. setup-python 3.12 with a pip cache keyed on `requirements*.txt`.
    2. `pip install torch --index-url https://download.pytorch.org/whl/cpu` (D-TC5; prod installs CUDA torch, so this is not image parity, and no test imports torch).
    3. `pip install -r requirements.txt -r requirements-test.txt`.
    4. `actions/setup-node@v4` with node 20, so test_js_unit.py runs instead of skipping.
    5. `python -m pytest backend/tests -m "not integration and not slow and not pg" -q -p no:cacheprovider --timeout=300 --durations=40 --junitxml=reports/offline.xml`, with `TESTS_BLOCK_NETWORK=1`.
    6. Upload the junit file as an artifact.
  - Rewrite the stale comment at ci.yml:52-70: there are 3 integration tests, not 1.
  - New `requirements-test.txt` pins `pytest==9.1.1`, `pytest-timeout` and `pyyaml`.
  - New `backend/tests/test_ci_workflow.py` loads ci.yml and reads the triggers with `d.get('on', d.get(True))`, because PyYAML parses `on:` as `True`. It pins the triggers, the permissions, the 3 job ids, `needs: lint` and the ruff version. pyyaml is declared, so the test cannot silently skip.
- Tests: test_ci_workflow.py.
- Accept: the test passes locally; ci.yml installs the same requirements and runs the same selection plus `not pg`.

**P2A.3: Network guard.**
- Change: in `backend/tests/conftest.py`, when `TESTS_BLOCK_NETWORK=1`, an autouse fixture monkeypatches `socket.socket.connect` and `socket.create_connection` to refuse any non-loopback destination (127.0.0.0/8, ::1 and unix sockets are allowed) and counts the attempts. CI sets it. Today only vPIC and `fake_dns` are stubbed (conftest.py:92, :148-179).
- Tests: test_network_guard.py: connecting to 8.8.8.8 raises under the env; localhost works; with the env unset, behaviour is unchanged.
- Accept: a local chunked run with the env set lists the tests that touch the network. They become class (f) in the P2A.6 ledger.

**P2A.4: Release guard.**
- Change:
  - New `scripts/release_guard.py`: stdlib only and Python 3.9 compatible, because hooks run whatever `python3` is on PATH and `/usr/bin/python3` is 3.9.6. It takes `--base <ref|sha>`. A HEAD already contained in the base passes. Otherwise it fails unless VERSION is numerically semver-greater than the base's (1.5.10 > 1.5.9) AND CHANGELOG.md has a `## [<VERSION>]` heading followed by at least one non-blank line before the next `## [`.
  - CI job `release-guard` (`needs: lint`). Per D-REL3 it runs only when `github.ref == 'refs/heads/main' || github.event_name == 'pull_request'`, as a "releasable?" check, so work-in-progress pushes stay green. Steps: `git fetch --no-tags --depth=50 origin main`, then run the script.
  - The pre-push hook calls the script with `--base "$remote_sha"` (pre-push:17), not the possibly stale local `origin/main`, for pushes to `refs/heads/main` only. Other refs keep today's behaviour until D-REL3 changes it (P2B.1).
- Tests: test_release_tooling.py, in a `tmp_path` git repo with `user.name`/`user.email` set locally (the runner has neither):
  - bump_version.sh patch/minor/major/explicit; the Unreleased conversion; staging of VERSION and CHANGELOG;
  - the hook refuses a same-VERSION push, accepts a bump-only push, skips tags and deletions, compares a new branch against origin/main, and refuses a main push with a lower VERSION;
  - the guard's semver and CHANGELOG parsing, written as tmp-repo cases (1.5.3 with a filled section passes; 1.5.2 vs 1.5.2 fails; an empty section fails).
- Accept: the tests pass in under 30 s with no network; the job is named exactly `release-guard`; feature-branch hook behaviour is unchanged.

**P2A.5: Shakedown.**
- Change (MAIN, git):
  1. Preconditions: P0A.1 shows no service builds from non-main branches; secrets-scan the range (the repo is public); D-REL3 allows `ci/*` pushes without a bump.
  2. Push HEAD to `ci/shakedown-1`. Each iteration uses a NEW branch name: a new branch is compared with `origin/main` and passes the hook, while re-pushing an existing branch would need a bump.
  3. `gh run list --branch ci/shakedown-N`; download the junit artifacts.
  4. Delete the shakedown branches once green.
- Accept: a run that is green on lint and pytest; pytest-integration green (its tests skip); release-guard skipped on non-main, as designed.

**P2A.6: Failure ledger.**
- Change: classify every CI failure and error from the junit:
  - (a) needs a gitignored local asset or the dev DB copy but is not an asset-gated skip;
  - (b) Python 3.12 vs local 3.14;
  - (c) macOS-only assumption or case-sensitive path;
  - (d) missing tool (node, pdftotext);
  - (e) real product bug: file it, do not fix it;
  - (f) network access (from the P2A.3 counter).
  Local runs only reproduce; CI is the source of truth. Append the ledger (test, class, planned fix, owner) to tests.md.
- Accept: every failure is classified.

**P2A.7: Class fixes.**
- Change: one worktree per class (a)-(d) and (f), each with an explicit file list from the ledger. Fixes are test-only and small: convert to `pytest.skip` with a reason phrase from `_ASSET_GATE_SKIP_REASONS`, seed via `sqlite_inventory`, add 3.12-compatible constructs, add tool guards, stub the network. Class (e) bugs are filed as follow-ups in tests.md and never fixed here. Then repeat P2A.5 with a new branch.
- Accept: the CI pytest job is green on a fresh `ci/shakedown-N`; the ASSET-GATED SKIPS count and wall time are recorded (the budget for P14C.3).

**Workflow shape.** Wave A: P2A.1, P2A.2, P2A.3. Wave B: P2A.4 (also edits ci.yml). Wave C: P2A.5 (MAIN). Wave D: P2A.6. Wave E: P2A.7, as one worktree per class, then loop back to C until green. Review lens: no masked red (skipped jobs never stand in for passes), required job names stay stable, no product-code changes in P2A.7.

**Data repair.** None.

**Exit gate.**
- `.venv/bin/ruff check . --no-cache` exits 0.
- test_ci_workflow.py, test_release_tooling.py and test_network_guard.py pass.
- The last `ci/shakedown-N` run is green on every job.
- The ledger is in tests.md, and the shakedown branches are deleted.

**Release step.** Patch: `scripts/bump_version.sh patch`, push the integration branch, and confirm CI is green on that SHA. This is the first CI run on the real branch, and it is required before Phase 2B cuts the release.

## Phase 2B: Release flow, first release to `main`, protection, cleanup

**Goal.**
- Deploys come only from tagged releases on `main`, and /health reports the commit.
- The release flow is documented.
- `main` is fast-forwarded from 0.2.0 to the current release, past releases are tagged, `main` is protected, and merged branches and worktrees are pruned.

**Entry gate.**
- Phase 2A exit: CI green on the integration branch.
- P0A.3's must-ship allowlist is available.
- Decisions answered: D-REL2 (P2B.7, P2B.8), D-REL3 (P2B.1), D-REL4 (P2B.8), D-REL5 and D-REL6 (P2B.5), D-REL7 (P2B.4), D-REL8 (P2B.6), D-REL9 (P2B.3), D-REL10 (P2B.2).

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P2B.1 | release-6 | Apply D-REL3 to the hook and the CLAUDE.md wording (may close with no change) | code | scripts/git-hooks/pre-push, backend/tests/test_release_tooling.py, CLAUDE.md (owner approval) | S | A | P2A.4 |
| P2B.2 | release-8 (+ reviewer), D-REL10 | Guarded deploy scripts; BUILD_COMMIT; `/health` reports the commit | code | deploy/railway/deploy_web.sh (new), deploy/railway/deploy_scanner_nightly.sh, backend/routes/site_misc.py, backend/routes/health.py, backend/tests/test_deploy_guard.py (new), backend/tests/test_health_ready.py | M | A | P0A.3 |
| P2B.3 | release-10 + CHANGELOG backfill (missing) (+ reviewer) | Cut the release and fast-forward `main` (MAIN) | ops, needs prod (verification) | CHANGELOG.md, VERSION | M | C | P0A.4, P2A, P2B.1, P2B.2, P2B.7 |
| P2B.4 | release-11 | Tag past releases | ops | git tags | S | D | P2B.3 |
| P2B.5 | release-13 (+ reviewer) | GitHub ruleset on `main` (and `v*` tags) | ops | GitHub settings | S | D | P2B.3 |
| P2B.6 | release-14 (+ reviewer) | Deploy the release to web from the tag stage | ops, needs prod | none | M | D | P2B.2, P2B.3 |
| P2B.7 | release-9 (+ reviewer) | RELEASING.md; drop "deploy from main"; legacy-services docs; RESUME_HERE quick fix | docs | docs/RELEASING.md (new), deploy/railway/README.md, docs/RAILWAY_SCANNING.md, RESUME_HERE.md, CLAUDE.md (one-line pointer, owner approval) | M | B | P2B.1, P2B.2 |
| P2B.8 | release-12 (+ reviewer) | Prune merged branches and worktrees; archive the unmerged remotes (MAIN) | deletion | git refs, .claude/worktrees | M | D | P2B.3 |

**P2B.1: Bump rule.** Only if D-REL3 chooses (b): enforce the bump only for `refs/heads/main` (plus the P2A.4 semver check). CLAUDE.md's "Every push carries a new VERSION" bullet is reworded to the new rule, with owner approval. Otherwise close the unit with no change. Tests: extend test_release_tooling.py for both the main and feature-branch paths. Accept: the CLAUDE.md wording matches the hook exactly.

**P2B.2: Guarded deploys.**
- Change:
  - New `deploy/railway/deploy_web.sh`. It refuses unless the tree is clean, HEAD == `origin/main`, and HEAD carries the annotated tag `v$(cat VERSION)`. `ALLOW_UNRELEASED_DEPLOY=1` overrides with a loud warning.
  - It stages `git archive <tag>` plus the P0A.3 must-ship allowlist, writes `BUILD_COMMIT` (full SHA) and `BUILD_TAG`, then runs `railway up <stage> --path-as-root --service web --detach`.
  - `--dry-run` prints the plan and the file count without calling railway. `--keep-stage <dir>` leaves the stage behind for a local Docker build.
  - deploy_scanner_nightly.sh gets the same guard (D-REL10, with the override reserved for mid-fleet hotfixes) and writes BUILD_COMMIT too.
  - Add `_build_commit()` next to `_app_version()` (site_misc.py:15-27, health.py:85-88): it reads BUILD_COMMIT next to VERSION, falling back to "unknown". `/health` and `/api/health` return `{status, version, commit}`.
  - Grep the consumers of the /health payload first: the app_surface golden, iOS. The Railway healthcheck only needs a 200.
- Tests:
  - test_deploy_guard.py drives both scripts' `--dry-run` in a tmp git repo with a fake `railway` on PATH. It asserts that railway is never invoked on a refusal, and that the stage equals `git ls-files` of the tag plus the allowlist.
  - test_health_ready.py is extended.
- Accept: `--dry-run` exits non-zero on a dirty tree, on HEAD != origin/main and on a missing tag, and exits 0 on a tagged main; both health endpoints include `commit`; the healthcheck path is unchanged.

**P2B.3: First release to `main`.**
- Change (MAIN), in order:
  1. Preconditions: P0A.4 verified; Phase 2A exit; clean tree; `git fetch origin`; `git merge-base --is-ancestor origin/main HEAD`.
  2. CHANGELOG: `[Unreleased]` holds everything not yet released. Backfill a `[1.3.2]` section (the subject of commit ba79193da) and correct the 0.2.0 date to 2026-06-14 (the tag date).
  3. `scripts/bump_version.sh patch` (D-REL9) and commit `Release X.Y.Z`.
  4. Secrets-scan `git log -p origin/main..HEAD`. The whole 318+ commit range becomes public on `main`. Push protection is on, but scan anyway.
  5. Run ruff and the targeted test chunks. Push the branch.
  6. Wait for green CI on the SHA (`gh run list --commit <sha>`). If it is red, fix forward with another bump.
  7. Optional record: `gh pr create --base main`.
  8. `git push origin <branch>:main`. Never `--force`: GitHub would accept a force push until P2B.5 lands.
  9. `git tag -a vX.Y.Z` and push it.
  10. `git fetch origin main:main`.
  11. Within 15 minutes, check `railway deployment list` for every service: no new deployment (this check moved here from release-2).
  12. Rollback, if a build fires anyway: the per-service deploymentRollback GraphQL recipe. Never force-push `main` back.
- Accept:
  - `git ls-remote origin refs/heads/main` == the release SHA == `origin/<branch>`.
  - `git show origin/main:VERSION` prints the new version, and the CHANGELOG section is non-empty.
  - CI is green on lint, pytest, pytest-integration and release-guard.
  - An annotated tag points at the release SHA.
  - No Railway deployment appeared in the 15 minutes after the push.
  - Local `main` == `origin/main`.

**P2B.4: Retro tags.** Annotated tags: v1.3.2 → ba79193da (message: its commit subject, since CHANGELOG had no section), v1.4.0 → f5194d353, v1.4.1 → bddc390c6, v1.4.2 → d5283fada, v1.4.3 → ead243177, v1.4.4 → 5536ae58c, v1.5.0 → db4b6df8d (deployment 62f93905), v1.5.1 → 90c9d6160 (deployment c9872e66), v1.5.2 → 5336903cd. Each message carries the caveat that `railway up` shipped working trees, which may contain uncommitted files. Check `git show <sha>:VERSION` for each before tagging. Push by name. Accept: `git ls-remote --tags origin 'v1.*'` lists all 9, each `^{}` SHA matches this table, and every tag's VERSION matches its name.

**P2B.5: Ruleset.**
- Change: only after P2B.3 (CI has been green once). `gh api -X POST repos/asarrafi47/DealershipScanner/rulesets --input ruleset.json`:
  - `name: main`, `target: branch`, `enforcement: active`, `conditions.ref_name.include: ['refs/heads/main']`;
  - rules: `deletion`, `non_fast_forward`, and `required_status_checks` with `parameters: {strict_required_status_checks_policy: false, required_status_checks: [{context: lint, integration_id: 15368}, {context: pytest, integration_id: 15368}, {context: pytest-integration, integration_id: 15368}, {context: release-guard, integration_id: 15368}]}`;
  - `bypass_actors` per D-REL6 (owner admin bypass recommended).
  - No `required_linear_history`: the range holds 4 merge commits (662b195e7, fa3b8c814, bacd5f03f, 8b3449164). No PR requirement.
  - Optionally, a second ruleset on `refs/tags/v*` blocking deletion and update. Set `delete_branch_on_merge=true`.
  - If D-REL5 makes the repo private on Free, rulesets stop applying.
  - When P14A.3 replaces `pytest-integration` with `pytest-pg`, update the contexts in the same release.
- Accept: `gh api repos/asarrafi47/DealershipScanner/rules/branches/main` lists the 3 rule types and the 4 pinned contexts.

**P2B.6: Deploy the release.**
- Change:
  - Web: run `deploy_web.sh --dry-run --keep-stage <dir>`. Build the exact stage with Docker and run it against local PG (owner OK, AC power). In the container, assert BUILD_COMMIT and the allowlisted files are present; `curl /api/health` shows the version and commit; run the auth/CSRF smoke checks. Then deploy, verify https://sarraficars.com/api/health, and record the deployment id in RELEASING.md or the tag message.
  - Scanner-nightly is deferred to P6B.1 (D-REL8 b). No dealer is scanned, so no dealer logs.
  - Rollback: the deploymentRollback recipe.
- Accept: the local container answers with the new version before any Railway call, and prod `/api/health` shows the tag SHA.

**P2B.7: Release docs.**
- Change:
  - New docs/RELEASING.md:
    - the branching model (D-REL2) and the bump rule (D-REL3);
    - the release checklist (CHANGELOG → bump → secrets scan → push branch → CI green on all 4 jobs → `git push origin <branch>:main`, fast-forward only → annotated tag → deploy from the tag with deploy_web.sh / deploy_scanner_nightly.sh → verify `/api/health` version and commit → record the deployment id);
    - the Railway source policy (no service builds from GitHub; how to verify; the P0A.1/P0A.4 findings);
    - the rollback recipe; `git config core.hooksPath scripts/git-hooks` on every clone; a pointer to the migration protocol (Appendix C.1).
  - deploy/railway/README.md:30-36: drop "connect the GitHub repo ... deploy from main", point to deploy_web.sh, and warn never to attach a GitHub source.
  - RAILWAY_SCANNING.md:289-290: the legacy services' new state.
  - RESUME_HERE.md:3 and :5: the version and the stale branch name (the full rewrite is P18B.1).
  - A one-line CLAUDE.md pointer to RELEASING.md, if the owner approves.
- Accept: `grep -rn "deploy from .main" deploy docs` is empty; every RELEASING.md step has a verification command, and its flags and paths match the scripts exactly.

**P2B.8: Prune.**
- Change (MAIN):
  1. Run `git bundle create workspace/backups/repo_all_refs_<date>.bundle --all` with `run_in_background` (the pack is 524 MiB), then `git bundle verify`. Save `git for-each-ref --format='%(refname) %(objectname)'` to workspace/backups.
  2. Before removing worktrees, check for live sessions: ListAgents, and `lsof -a -d cwd +D .claude/worktrees`.
  3. `git worktree remove` completeness-fixes, discovery-lifecycle, profile-features and timing-fingerprint. They are clean; if git asks for `--force`, stop and inspect. Then `git worktree prune`.
  4. `git branch -d`: backup/pre-key-rewrite, feature/admin-scanner-ops-hub, feature/completeness-fixes, feature/discovery-lifecycle, feature/profile-preferences, feature/scanner-website-destructuring-layers, feature/spec-data-and-zumai-rework, feature/timing-fingerprint. feature/data-architecture-scan-reliability needs `merge-base --is-ancestor ... origin/main` first, then `-D` (its upstream is 39 commits behind).
  5. Remote branches Dev, feature/admin-scanner-ops-hub, feature/data-architecture-scan-reliability and feature/scanner-website-destructuring-layers: dry-run the delete, run an ancestor check against `origin/main`, then delete.
  6. Per D-REL4: create annotated tags `archive/feat-inventory-consolidation-0.4.0` (87bb1b689) and `archive/feature-crawler-llm-standardization` (7006ed7ff). Messages list the unmerged files (hd_truck_mpg, ev_motor_count, fueleconomy_catalog, json_column_storage, merge_laptopdb, catalog_schema, DEVELOPMENT_WORKFLOW.md) and the authors. Tell ksarrafi first. Push the tags, verify them, then delete the branches.
  7. Retire feature/http-only-scans (D-REL2) only after the next release starts from `main`, and only after the mini's clone shows no unpushed work (`git status`, `git log @{u}..`).
  8. `git fetch --prune origin`.
- Accept:
  - The bundle's `list-heads` includes every deleted ref.
  - `git worktree list` shows only the main checkout (about 1.75 GB freed).
  - `git ls-remote origin` shows `main`, the `v*` tags and the two `archive/*` tags, whose SHAs equal the deleted tips.
  - Every deleted remote branch was an ancestor of `origin/main` or is preserved by a tag.

**Workflow shape.** Wave A: P2B.1, P2B.2. Wave B: P2B.7. Wave C: P2B.3 (MAIN). Wave D: P2B.4, P2B.5, P2B.6, P2B.8 (owner-run or authorized agents). Review lens: no force pushes; no secrets; the deploy guards refuse correctly; the ruleset contexts exactly match the CI job ids; nothing unmerged is deleted without an archive tag.

**Data repair.** None. The P2B.8 git bundle is the backup.

**Exit gate.**
- P2B.3 acceptance holds; the ruleset is active; all 9 retro tags exist.
- Prod `/api/health` shows the release version and commit.
- The worktrees are pruned and RELEASING.md is merged.

**Release step.** P2B.3 is the release (patch). From here on, under D-REL2(a), every phase starts on `phase/<id>` from `main` and releases by fast-forward plus a tag.

## Phase 3: Security wave 1 (high severity)

**Goal.** Close the exploitable web-security issues:
- OAuth pre-registration account takeover, including the username-equals-email variant;
- X-Forwarded-For spoofing through a direct-to-origin request (which also defeats `DEV_IP_ALLOWLIST`);
- SSRF guards that let CGNAT/Tailscale addresses through;
- unprotected operator DELETE routes and an unused CSP origin;
- CSS injection from dealer image URLs;
- headless Chromium running as root without a sandbox in the web container.
Also guard the vault-sync script against resetting `TRUSTED_PROXY_HOPS`.

**Entry gate.** Phase 2B exit. D-SEC4 (P3.1) and D-SEC1 (P3.8, informed by P0B.3) answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P3.1 | security-4 (+ reviewer) | OAuth: email-only lookup, verified provider email, link only after a password login | code | backend/auth/google_oauth.py, backend/auth/apple_oauth.py, backend/auth/pages.py, frontend/templates/login.html, backend/db/users_db/auth.py, backend/db/users_db/accounts.py, backend/auth/registration_validation.py, backend/tests/test_google_oauth.py, test_apple_oauth.py | M | A | none |
| P3.2 | security-12 | `client_ip` trusts only known proxy CIDRs | code | backend/utils/client_ip.py, backend/tests/test_client_ip.py | S | A | none |
| P3.3 | security-3 (+ reviewer) | SSRF guards block every non-global address (CGNAT, IPv4-mapped, 6to4) | code | backend/utils/outbound_url.py, backend/utils/safe_listing_url.py, backend/tests/test_outbound_url.py, test_outbound_url_dns.py, test_safe_listing_url.py | S | A | none |
| P3.4 | security-2 (+ reviewer) | Drop esm.sh from CSP; CSRF on operator DELETE routes; url_map coverage test | code | backend/web/security.py, backend/tests/test_csp_policy.py (new), test_csrf_coverage.py (new) | S | A | none |
| P3.5 | security-7 | `css_url` filter for `style="background-image:url(...)"` | code | backend/web/templating.py, frontend/templates/{home,landing,compare}.html, frontend/templates/inventory/{dashboard,listings}.html, backend/tests/test_css_url_filter.py (new) | S | A | none |
| P3.6 | security-6 | Production config warnings; per-account reset cooldown | code | backend/utils/production_security.py, backend/auth/password_reset.py, backend/tests/test_production_security.py, test_password_reset.py | S | A | none |
| P3.7 | hygiene-14 | Railway README service list; vault-sync refuses an unset TRUSTED_PROXY_HOPS | docs + code | deploy/railway/README.md, deploy/railway/sync-vault-to-railway.sh | S | A | none |
| P3.8 | security-11 (+ reviewer), D-SEC1 | Web research is HTTP-only by default; Chromium leaves the web image | code | backend/utils/web_researcher.py, backend/scripts/analyze_trim_adds_with_web.py, Dockerfile.web, backend/tests/test_web_researcher.py, test_car_chat_policy.py | M | A | none |
| P3.9 | security-18a | SECURITY_MASTER_TODO doc step for wave 1 | docs | docs/SECURITY_MASTER_TODO.md | S | B | P3.1-P3.8 |

**P3.1: OAuth takeover.**
- Change:
  - OAuth linking resolves with a new strict lookup, `lower(email)=lower(?)`, never `get_user_by_login`. That function (users_db/auth.py:98) matches username OR email; usernames may contain '@', and uniqueness is per column (V013:115-119).
  - Google requires `info.get('email_verified') is True`; today only `is False` is rejected (:267). Apple requires the email_verified claim to be true before the email-match path.
  - In `_resolve_or_create_user`: when the email-matched account has no provider sub, has a usable password and has an empty `email_verified_at`, do not link. Store `session['pending_oauth_link'] = {provider, sub, email, exp: 10 min}` and redirect to /login with a notice. After a successful password login as that same user (`pages.login_page` and `api_auth_login`), link the sub, mark the email verified and clear the pending key.
  - Accounts created through OAuth, and already-verified accounts, keep auto-linking.
  - Registration and profile update (registration_validation.py:25-41, users_db/accounts.py:240-250 `user_exists_by_username`, users_db/auth.py:138 profile-update uniqueness check) reject a username that contains '@' or equals an existing email.
- Tests, for both providers: the pre-registration takeover is blocked; a username equal to the victim's email does not match; a missing email_verified is rejected; the link completes after a password login; the existing cases pass.
- Accept: the tests pass.

**P3.2: Proxy CIDRs.**
- Change: optional `TRUSTED_PROXY_CIDRS`, either comma-separated CIDRs or the token `cloudflare`, which expands to a dated constant of Cloudflare's IPv4 and IPv6 ranges with a refresh note. When it is set, walk X-Forwarded-For from the right, skip trusted entries, and return the first untrusted one. When it is unset, the hop-count rule (:83-88) is unchanged. Log once when `TRUSTED_PROXY_HOPS>=2` and no CIDRs are configured. dev/routes.py:356-361 (`DEV_IP_ALLOWLIST`) uses `client_ip()`, so it is covered too.
- Tests: through Cloudflare, 'client, 172.67.1.1' resolves to the client; the direct-origin spoof 'spoof, 203.0.113.9' with hops=2 and CIDRs=cloudflare resolves to 203.0.113.9 (today it returns 'spoof'); a DEV allowlist spoof is refused; behaviour with CIDRs unset is unchanged.
- Accept: the tests pass.

**P3.3: SSRF non-global.**
- Change: `destination_host_blocked` (:23-33) and `destination_host_blocked_after_dns` (:55-62) unwrap ipv4_mapped and 6to4 addresses, then block when `not ip.is_global or ip.is_multicast`. `safe_listing_url._host_is_blocked` (:21-27) applies the same rule and keeps its non-production localhost allowance. Its docstring states the threat model: these are image URLs the visitor's browser fetches, so this is client-side hardening, not SSRF.
- Tests: blocked: 100.64.0.1, 100.127.255.254, 100.100.100.200, ::ffff:100.64.0.1, 198.18.0.1, 192.0.0.8. Allowed: 8.8.8.8 and a public IPv6 address. A name that a monkeypatched getaddrinfo resolves to 100.x is blocked. The cases pass on Python 3.12 (CI) as well as locally.
- Accept: the tests pass.

**P3.4: CSP and CSRF.**
- Change:
  - Remove https://esm.sh from script-src and connect-src in both the enforced policy and the report-only policy (security.py:161-162, :178-179). Nothing loads from it.
  - The DELETE branch (:87-92) also protects the `api_admin_operator_` prefix, which covers `api_admin_operator_delete_incomplete_car` (operator_api.py:54) and `api_admin_operator_api_delete_dealer`. dev.js `devFetch` already sends `X-CSRF-Token`.
  - Add a url_map coverage test. Every rule allowing POST/PUT/PATCH/DELETE must be classified as one of:
    - in the form or header lists;
    - under a protected prefix (`dealer_portal.`, `store_admin.`, `api_admin_operator_`);
    - the dev blueprint (own hook, dev/routes.py:390-411) or dev_console (own hook, console.py:27-43);
    - IN_VIEW (community_api endpoints that call `validate_csrf_header` at :231/:303/:336, users_hub, dealers_hub);
    - EXEMPT (`billing.stripe_webhook`, `billing.premium_webhook`, both signature-verified);
    - an MFA stub (`mfa_legacy_*`, `mfa_qr_complete_gone`, `mfa_qr_confirm_gone`, pages.py:316-326).
  - List any further unclassified endpoints in the PR.
- Tests: test_csp_policy.py, test_csrf_coverage.py.
- Accept:
  - The CSP header has no esm.sh.
  - A DELETE to `/api/admin/operator/incomplete-cars/1` without the token returns 403.
  - The coverage test fails when a dummy unclassified POST is registered.
  - test_app_security_basics, test_admin_operator_api, test_no_inline_style_elements and test_csp_self_hosted_fonts pass.

**P3.5: CSS url filter.**
- Change: a `css_url` Jinja filter. It accepts only http(s) URLs (via `normalize_listing_image_url`) or same-origin `/static/` and `/car-images/` paths, percent-encodes `' " ( ) \ < >`, whitespace and control characters, and falls back to the placeholder. Apply it at home.html:94,123,151,181; landing.html:92; compare.html:41; inventory/dashboard.html:55; inventory/listings.html:47,49,90. Background: autoescape turns `'` into `&#39;`, but the HTML parser decodes it back before CSS parsing, so a dealer URL containing `');...` injects CSS today.
- Tests: the filter neutralizes the injection payload and passes normal CDN URLs through byte-identical; a template scan finds no `url(` inside a style attribute without `css_url`.
- Accept: the tests pass; manual check with the run skill shows photos on home, landing, compare and inventory.

**P3.6: Config warnings.**
- Change:
  - `assert_production_security_config` warns when:
    - (1) any of PASSWORD_RESET_ENABLED, EMAIL_VERIFICATION_ENABLED or BILLING_STRIPE_ENABLED is on while PUBLIC_BASE_URL is unset (relative email links, password_reset.py:58-70; host_url fallback, stripe_billing.py:47-52);
    - (2) an email flow is on with neither RESEND_API_KEY nor SMTP_HOST set (send failures are swallowed at :105-111);
    - (3) `ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1`.
  - `request_password_reset` adds `allow_request(f'forgot_pw_user:{uid}', 3/hour)` per account; the response is unchanged.
- Tests: test_production_security.py, test_password_reset.py.
- Accept: each warning fires exactly when its condition holds; a 4th request within the hour sends no email and returns the same page.

**P3.7: Vault sync guard.** sync-vault-to-railway.sh:92-98 requires `TRUSTED_PROXY_HOPS` to be set explicitly and exits with a message before the first `railway variables set`; today it silently uses `${TRUSTED_PROXY_HOPS:-1}`. deploy/railway/README.md: the current service list (web; scanner-nightly; legacy services retired per P0A.4) and the proxy-hops rule for the Cloudflare-fronted domain (2). Keep the users-DB-on-volume section. Accept: `bash -n` passes; the reviewer confirms the guard runs before any railway call; the owner confirms the live value before merge.

**P3.8: Chromium out of web.**
- Change:
  - Grep first for every in-web Playwright launch site. Scanner sites are behind browser_gate; the gap_fill path from `packages_ensure.py:287` needs `SCANNER_ALLOW_BROWSER`, which P0A.1 confirms is unset.
  - `WebResearcher(..., allow_browser=False)` gates BOTH `_brave_search_links` (web_researcher.py:526-529, :534-572, called unguarded at :648) and `_fetch_with_playwright` (:583-591).
  - The car-chat callers (agent.py:890, :1226) keep the default and become HTTP-only. analyze_trim_adds_with_web.py:336 passes `allow_browser=True`.
  - Dockerfile.web: remove `playwright install chromium && playwright install-deps chromium` (:30) and the Chromium-only apt libraries (:6-9), and rewrite the comment at :20-29. The playwright pip package stays until P15B.2 splits requirements.
  - If D-SEC1=(b): this unit instead adds `chromium_sandbox=True` plus `context.route()` aborting non-document and blocked-host requests, and moves after P16.6 (non-root), since Chromium refuses to sandbox as root.
- Tests: with the playwright imports patched to raise, `WebResearcher()` touches neither Brave nor the fallback; `allow_browser=True` reaches the mocked fallback.
- Accept: the tests pass. A local Docker build of the stage (owner OK) has no `ms-playwright` directory; record the size drop; a car-chat research answer is served from the HTTP path.

**P3.9: Security doc.** In SECURITY_MASTER_TODO:
- Fix the summary table (:21-30): SEC-096, SEC-097 and SEC-099 are In progress.
- Correct SEC-095 (:833): Playwright adds `--no-sandbox` unless `chromium_sandbox=True`.
- Under SEC-083, note the prod exception (`ALLOW_APP_ADMIN_DEV_PASS_THROUGH=1`) until D-SEC5 is applied.
- Mark SEC-099's security property Done and track its go-live separately.
- Add rows SEC-104 onward for P3.1-P3.8, each with Validation, Last verified and a Changelog line.
The re-scoping of rows that point at deleted files is P18B.5. Accept: the summary counts match the per-row statuses.

**Workflow shape.** Wave A: P3.1-P3.8 in parallel (disjoint files). Wave B: P3.9. Review lens: auth bypass paths; header trust; templates still render; Dockerfile.web builds. Integration order: P3.3, P3.2, P3.6, P3.7, P3.4, P3.5, P3.1, P3.8, P3.9.

**Data repair.** None.

**Exit gate.**
- pytest, chunked: test_google_oauth.py, test_apple_oauth.py, test_client_ip.py, test_outbound_url.py, test_outbound_url_dns.py, test_safe_listing_url.py, test_csp_policy.py, test_csrf_coverage.py, test_app_security_basics.py, test_admin_operator_api.py, test_no_inline_style_elements.py, test_csp_self_hosted_fonts.py, test_css_url_filter.py, test_production_security.py, test_password_reset.py, test_web_researcher.py, test_car_chat_policy.py.
- CI is green.
- A local Docker smoke of the release stage passes (owner OK).

**Release step.** Patch. Bump, push, CI green, fast-forward `main`, tag. Then `deploy_web.sh` (after the local stage build and run). Afterwards the owner sets `TRUSTED_PROXY_CIDRS=cloudflare` and then applies D-SEC5. Verify `/api/health`, a password login, a test-account OAuth login, and car chat.

## Phase 4: Migration foundation

**Goal.** Make the migration chain safe to extend before the data phases add V027-V032:
- a drift report;
- `migrate.py` that records V019 only when it is safe, applies up to a target, serializes concurrent boots and logs its target;
- V026, which puts every runtime-only and V019-only object into the chain;
- strict mode that actually propagates;
- the three hottest runtime-DDL paths gated;
- the comment-blind placeholder adapter fixed;
- V026 applied to every DB.

**Entry gate.** Phase 2B exit. The P0A.2 ledger exists (which DBs, server version, out-of-chain objects, option_rejections duplicates). D-DB5, D-DB6, D-DB7 and D-PR8 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P4.1 | db-layer-2 | Schema drift report: chain-built scratch DB vs a live DB | code | backend/scripts/schema_drift_report.py (new), backend/tests/test_schema_drift_report.py (new) | M | A | none |
| P4.2 | db-layer-6 + tests-ci-4 + epa-rebuild missing (`--target`) (+ reviewers) | migrate.py: conditional record-only V019, `--record-only`, `--target`, target log, lock_timeout, advisory lock | code | backend/scripts/migrate.py, backend/db/schema_version.py, migrations/README.md, backend/tests/test_migrate.py, backend/tests/test_schema_from_migrations.py (describe_gap test) | M | A | none |
| P4.3 | db-layer-5 + tests-ci missing V026 (+ reviewers) | V026 chain completeness | code | migrations/V026__chain_completeness.sql (new), backend/scripts/capture_module_ddl_golden.py (new), backend/tests/fixtures/module_ddl_schema_golden.json (new), backend/tests/test_schema_from_migrations.py | M | B | P4.2 |
| P4.4 | db-layer missing (dedupe) | option_rejections dedupe script and per-DB run (MAIN run) | code + data_repair | backend/scripts/dedupe_option_rejections.py (new), backend/tests/test_dedupe_option_rejections.py (new) | S | A | none |
| P4.5 | db-layer-7 (+ reviewer) | Strict schema mode propagates in web init and scanner upserts; docstrings corrected | code | backend/db/repositories/schema_repo.py, backend/scanner/database.py, backend/db/schema_version.py, backend/db/inventory_pg.py (docstring), backend/tests/test_schema_strict_mode.py (new; test_schema_from_migrations.py belongs to P4.3 in this wave) | S | B | P4.2 |
| P4.6 | db-layer-8, scoped to hot paths (+ reviewer) | Gate the per-request and per-process DDL; scanner session lock_timeout leak | code | backend/db/runtime_ddl.py (new), backend/reviews/store.py, backend/scanner/recipe_store.py, backend/scanner/job_queue.py, backend/db/inventory_pg.py (:338), backend/tests/test_runtime_ddl_gating.py (new) | M | C | P4.3, P4.5 |
| P4.7 | tests-ci-16a (+ reviewer) | Comment-aware SQLite→Postgres placeholder rewrite (pure) | code | backend/db/inventory_pg.py (:117-161, :252-260), backend/tests/test_inventory_pg_adapter.py (new) | S | A | none |
| P4.8 | db-layer-9 part 1 (+ reviewer) | Apply V026 everywhere; strict stays off (MAIN, owner for prod) | ops, needs prod | none | M | D | P4.3, P4.4 |

**P4.1: Drift report.**
- Change: a CLI that builds the chain into `schema_drift_<uuid>` on the local server and snapshots both the scratch DB and the target with the same catalog queries `test_schema_from_migrations._snapshot` uses (copied, not edited). It prints live-only objects, chain-only objects and definition mismatches. Until P4.2 merges it records V019 by itself; afterwards it uses `migrate.main(['--apply'])`. The target opens through `connect(read_only=True)`. The scratch DB is dropped in `finally`. The tool refuses when the scratch name equals the target. The diff is a pure function over two snapshot dicts. Building V001 (2,252 lines) costs CPU: run on AC power.
- Tests: pure diff over dict fixtures; a read-only assertion through a fake connect; opt-in integration under `SCHEMA_PARITY_PG_DSN`.
- Accept: run against local, it lists uq_option_rejections_standing and cars_epa_link_backup_20260921 as live-only; zero writes to the target; the owner can run it against prod read-only (its output feeds D-DB7).

**P4.2: Migration runner.**
- Change, in migrate.py:
  1. `RECORD_ONLY = {19: ...}`. V019 is recorded without running only when V001 is pending in the same run (a fresh build), or when all 48 V001-shared objects already exist (`to_regclass`). Otherwise the runner refuses with a clear error. That protects a DB on prod's old lineage (baselined at V001, later actually ran V019) from being marked current while missing tables.
  2. An explicit `--record-only N[,M]` flag (tests-ci-4).
  3. `--target N`: apply pending migrations up to N only.
  4. Log the redacted target host/db before `--apply`.
  5. `SET lock_timeout` per file (5 s), at least on the `MIGRATE_ON_BOOT` path.
  6. `pg_advisory_xact_lock`, so concurrent boots serialize.
  7. A pure classify helper (run / record / refuse).
- Change, in schema_version: `describe_gap` picks its hint by `to_regclass('public.cars')`. An empty DB gets `migrate --apply` (the fresh path records V019). A populated legacy DB without `schema_migrations` gets `--baseline 1`. Keep the `BASELINE_V019_COMMAND` alias, and update test_describe_gap_messages (test_schema_from_migrations.py:332-339). `_run_auto_migrate` uses the fresh path.
- Change, in migrations/README.md: a V019 section; the migration protocol (Appendix C.1); "a new table is a new migration, never runtime DDL"; the `MIGRATE_ON_BOOT` consequence; `--target` usage.
- Tests: with a tmp migrations dir and a fake connection, the three record-only cases (fresh build records; all objects present records; otherwise refuses); `--target` stops at N; `--record-only` is explicit; the advisory lock is taken; dry run prints "would record".
- Accept: `migrate --dry-run` on local still reports 0 pending and no drift.

**P4.3: V026.**
- Change: `migrations/V026__chain_completeness.sql`, one transaction with `SET LOCAL lock_timeout='5s'`, fully guarded and additive, touching nothing on `cars`:
  - `car_move_log_id_seq` (CREATE SEQUENCE IF NOT EXISTS); `car_move_log` (V019 column types, nextval default); `ALTER SEQUENCE ... OWNED BY`; `car_move_log_pkey` inside a `DO $$ ... IF NOT EXISTS (pg_constraint) ... $$` block, because ADD CONSTRAINT has no IF NOT EXISTS; `idx_car_move_log_car`. car_move_log is live: cars_repo.py:279 and attribution_repo.py:63 read it.
  - `cars_trim_quarantine` (read by scan_lab_report.py:226); `package_values_msrp_quarantine` in the V019 shape. Note for the script owner: quarantine_package_value_msrps.py:159 uses `CREATE ... LIKE package_values`, which drifts.
  - `review_reports`, exactly `reviews/store.py _DDL_REPORTS_PG`.
  - `haiku_spec_cache` in Postgres types (D-PR8 keeps it).
  - `uq_option_rejections_standing`: a DO block that RAISEs if duplicate groups exist (the dedupe is P4.4, never inside a migration), then `CREATE UNIQUE INDEX IF NOT EXISTS`, with `NULLS NOT DISTINCT` if D-DB7 and the server is 15+.
  - The pgvector `*_embeddings` tables, only if P0A.2 found them in prod and D-DB7 adopts them.
  - Capture `module_ddl_schema_golden.json` by running each module's `ensure_*` on an empty scratch Postgres, and commit the capture script. A hand-written golden proves nothing.
  - Static test: the chain creates every table, column and index in that golden.
  - Static test `test_fresh_chain_has_every_migration_relation`: parse every `CREATE TABLE|SEQUENCE|INDEX public.X` across migrations and assert each exists after a fresh build (opt-in integration).
  - The parity integration test builds with `migrate.main(['--apply'])` instead of the version==19 special case (:426-431). Correct the file's "V019 duplicates V001" docstring (48 of its 53 objects are shared; 5 are V019-only).
- Tests: the static tests above; the opt-in integration.
- Accept:
  - `test_expected_schema_version_matches_repo_chain` passes at 26 or above.
  - On a scratch DB built V001..V025 (V019 recorded), V026 creates every listed object.
  - On local, V026 creates only review_reports and haiku_spec_cache and takes no lock on cars.
  - The P4.1 drift report on local shows no live-only or chain-only difference for these objects.

**P4.4: option_rejections dedupe.**
- Change: a script, dry run by default. Per DB, count duplicate groups under the `IS NOT DISTINCT FROM` key (NULL-key rows included); `COPY` the rows to be deleted into `workspace/backups/option_rejections_dupes_<db>_<date>.csv`; `DELETE` them in one transaction; verify 0 groups. Local has 0 groups (no-op). On prod it runs before V026 (P4.8) if P0A.2 found groups.
- Tests: the dry run writes nothing; apply keeps one row per group and the backup holds the rest.
- Accept: the tests pass; local reports 0 groups.

**P4.5: Strict propagation.**
- Change: add `except SchemaNotMigratedError: raise` before the broad `except Exception` in `schema_repo.init_inventory_db` (:269-280) and `scanner/database._ensure_schema` (:214-231). The scanner today swallows it on every upsert (:384), and the web fails late via `init_dealer_portal_db`. Reword those warnings to "re-assert failed". Correct the "prod at V018" docstrings (schema_version.py:18-19, inventory_pg.py:301-302) using the P0A.2 facts. The test-file docstring "V019 duplicates V001" is corrected by P4.3, which owns test_schema_from_migrations.py in this wave.
- Tests, in the new test_schema_strict_mode.py: with `INVENTORY_SCHEMA_CHECK=strict` and a fake behind-the-chain Postgres, `init_inventory_db()` raises and `_ensure_schema` raises; warn mode is unchanged.
- Accept: the tests pass; no docstring claims V018 unless P0A.2 found it.

**P4.6: Hot-path DDL gating.**
- Change:
  - New `backend/db/runtime_ddl.py`: `pg_legacy_ddl_needed()` (False when on Postgres and `schema_is_current()`), and `run_pg_legacy_ddl(cur, stmts)`, which issues `SET LOCAL lock_timeout='3s'` first.
  - Apply it to the three hot paths:
    - `reviews ensure_reviews_table` and `ensure_review_reports_table`: a per-process flag plus `schema_is_current`. Today they run 3 DDL statements on every dealership page request (dealership_page.py:636).
    - `recipe_store._ensure_table` (:83-109): on Postgres, check `information_schema` and never run the ALTER, which takes ACCESS EXCLUSIVE even with IF NOT EXISTS.
    - `job_queue.ensure_job_tables` (:25-69).
  - inventory_pg.py:338: the session-level `SET lock_timeout` becomes `SET LOCAL` (or is reset after use), so the scanner's raw connection does not keep a 3 s lock_timeout on every later upsert.
  - The remaining modules wait for P13B.3.
- Tests: test_runtime_ddl_gating.py: zero DDL when the schema is current; lock_timeout comes first when behind; SQLite unchanged; a dealership page request issues no CREATE.
- Accept: the tests pass; the reviews/recipe_store/job_queue tests pass (chunked).

**P4.7: Placeholder rewrite.**
- Change: `qmarks_to_percent_s` (inventory_pg.py:117-161) skips `--` line comments, `/* */` blocks and double-quoted identifiers. Today UPSERT_CARS_SQL adapts correctly only because the 4 apostrophes in its comments happen to balance. The generic INSERT OR IGNORE fallback (:258-260) strips a trailing `;`.
- Tests, in test_inventory_pg_adapter.py:
  - `?` inside and outside literals, `''` escapes, apostrophes / `?` / `%` inside comments;
  - `LIKE 'http%'` becomes `%%`; IFNULL, `excluded.`, `datetime('now')`, INSTR, GROUP_CONCAT;
  - the OR REPLACE / OR IGNORE mappings and the unmapped warning; PRAGMA returns None;
  - a test documenting that Postgres DO NOTHING does not skip NOT NULL or CHECK violations the way SQLite OR IGNORE does;
  - a differential test: for every module-level SQL constant whose first keyword is SELECT/INSERT/UPDATE/DELETE/WITH and whose comments contain no quote, the old and new adapters produce byte-identical output.
- Accept: adding an apostrophe to a comment in UPSERT_CARS_SQL no longer changes the adapted placeholder count; the test_dealer_portal.py adapter assertions are unchanged.

**P4.8: Roll out V026.**
- Change, in order:
  1. Local (MAIN): apply V026 as part of the P4.3 merge (`migrate --dry-run`, then `--apply`), before any local process runs the new code. Otherwise warn-mode processes fall back to legacy DDL. The mini's own DB too, if D-DB5=(a).
  2. Prod (owner):
     1. `pg_dump -Fc` into `workspace/backups/prod_<date>_preV026.dump`.
     2. Verify the dump with `pg_restore -l | grep 'TABLE DATA public cars'` and a size sanity check.
     3. Run P4.4 on prod if P0A.2 found groups (dry run first, owner approval).
     4. `INVENTORY_DATABASE_URL=<prod> python -m backend.scripts.migrate --dry-run`, then `--apply --target 26`.
     5. Re-run the relevant P0A.2 queries.
  3. Deploy the code only after that, so the boot-time `MIGRATE_ON_BOOT` is a no-op.
  Strict mode stays OFF here (P13A.4).
- Accept: every DB in the ledger records V026 with no checksum drift; the backup is verified; web boots and /login returns 200 after the deploy.

**Workflow shape.** Wave A: P4.1, P4.2, P4.4 (code), P4.7. Wave B: P4.3, P4.5. Wave C: P4.6. Wave D: P4.8 (MAIN, then owner). Review lens:
- migrations are guarded, additive and touch nothing on `cars`;
- nothing deletes data inside a migration;
- each `inventory_pg.py` region is edited by exactly one unit (P4.5 docstring, P4.6 :338, P4.7 :117-161 and :252-260);
- SQLite and Postgres both covered.

**Data repair.** P4.4 per DB: backup CSV, dry run, apply, verify 0 groups.

**Exit gate.**
- pytest, chunked: test_migrate.py, test_schema_from_migrations.py (static), test_schema_strict_mode.py, test_schema_drift_report.py, test_inventory_pg_adapter.py, test_runtime_ddl_gating.py, test_dedupe_option_rejections.py, test_dealer_portal.py, and the reviews / specials / recipe_store / job_queue tests.
- One opt-in local run of the `SCHEMA_PARITY_PG_DSN` integration (owner OK, AC power): a fresh build with plain `migrate --apply` contains car_move_log, cars_trim_quarantine and package_values_msrp_quarantine.
- The drift report has been run on local.
- The ledger shows V026 on every DB.

**Release step.** Minor (schema). Bump, push, CI green, fast-forward, tag. Then the P4.8 prod apply, and only after it `deploy_web.sh`. Scanner-nightly is deployed at P6B.1.

## Phase 5A: Retirement single writer, pure policy, walk report

**Goal.** Replace the conflicting retirement writers with one pure policy, `plan_retirement`, executed only by the scanner (full scans and delta scans). The policy:
- measures coverage per condition bucket, matched by VIN against the prior state;
- refuses to retire on incomplete walks, on recovery-replaced runs, and on evidence-free refusals.

Every replay records why its walk stopped. The pipeline becomes report-only, and assess flags truncated runs. A dry-run shadow comparison on named dealers comes before anything ships.

**Entry gate.** Phase 4 exit. D-RC1 (P5A.4, P5A.5, P5A.7) and D-SR4 (P5A.7) answered. Railway cron OFF.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P5A.1 | reconcile-1 (+ reviewer) | Pure retirement policy `plan_retirement()` | code | backend/scanner/retirement_policy.py (new), backend/tests/test_retirement_policy.py (new) | M | A | none |
| P5A.2 | reconcile-2 + scanner-runtime-8 + scanner-runtime missing (feed catch-all) (+ reviewers) | Replay walk report: stop reason, completeness, in-union; result and summary_json | code | backend/scanner/recipes.py, phases/dealer_run_steps/feed.py, delta_scan.py, backend/db/repositories/dealers_repo.py, backend/tests/test_recipe_replay_stop_reason.py (new), test_dealer_run_golden_20261001.py (stub signature only), test_attribution_golden_20261001.py (stub only) | L | A | none |
| P5A.3 | reconcile-12 (+ reviewer) | One-condition allow-list as a tracked file with one loader | code | backend/config/one_condition_ok.txt (new), backend/scanner/recipe_validation.py, backend/scanner/pipeline/assess.py, backend/tests/test_one_condition_allowlist.py (new) | S | A | none |
| P5A.4 | reconcile-3a (+ reviewer) | inventory_reconcile adopts the policy; guarded UPDATE; dry mode | code | backend/scanner/inventory_reconcile.py, backend/tests/test_scanner_inventory_reconcile.py, test_reconcile_condition_guard.py, .env.example | M | B | P5A.1, P5A.3 |
| P5A.5 | reconcile-3b (+ reviewer) | Evidence wiring: run start, replay report, protected VINs, recovery source | code | phases/dealer_run_steps/{after_write,state,enrich}.py, backend/attribution/disown.py (new helper beside `split_refusals`), backend/scanner/database.py (expose dropped VINs), dealers_repo.py, backend/tests/test_reconcile_scanner_wiring.py (new), fixtures/dealer_run_golden_20261001.json | M | C | P5A.2, P5A.4 |
| P5A.6 | reconcile-4 (+ reviewer) | Delta scan applies the same policy | code | backend/scanner/delta_scan.py, backend/tests/test_delta_scan.py, test_delta_scan_full_scan_parity.py | S | D | P5A.5 |
| P5A.7 | reconcile-5 + scanner-runtime-9 + reconcile missing (stale-bucket alarm) (+ reviewers) | Pipeline reconcile is report-only; truncation verdict; retirement audit and alarms in triage | code | pipeline/{reconcile,run,lifecycle,dealer_logs,triage,constants,__init__,assess}.py, backend/scripts/dealer_pipeline.py, .claude/workflows/dealer-discovery.js (one line on the `replay_truncated:` token), backend/tests/test_pipeline_reconcile_20260926.py, test_assess_stamped_rows.py, test_dealer_pipeline_surface.py, test_pipeline_retirement_report.py (new), test_pipeline_assess_truncation.py (new) | M | D | P5A.5 |
| P5A.8 | reconcile missing (shadow comparison) | Dry-run shadow scans on named dealers against local PG (MAIN) | investigation | workspace/dealer_logs/<5 dealers>/ | S | E | P5A.6, P5A.7 |

**P5A.1: Pure policy.**
- Change: new module with no DB and no env access.
  - `ActiveRow(id, vin_norm|None, bucket, make, scraped_at)`. The bucket comes from the shared `condition_bucket` (P1A.1); make is the stored row's make.
  - `RunEvidence(seen_vins, matched_makes, walks, protected_vins, run_started_iso, source, error, rows)`. `matched_makes` holds the makes of stored rows matched by VIN, plus normalized feed makes for VINs that are new; vPIC heal rewrites makes, so feed makes alone would misfire. `source` is `recipe` or `recovery:<strategy>`.
  - `Policy(min_rows=20, bucket_share=0.60, baseline_days=30, fallback_days=90, make_guard_min=10, one_condition_ok=False, block_on_incomplete_walk=True, retire_on_recovery_replaced=False, strikes=1, min_gap_hours=20)`.
  - `plan_retirement(rows, evidence, policy, now) -> RetirementPlan(eligible, reason, retire_ids, kept{condition_missing, bucket_low_coverage, make_missing, protected_refusal, walk_incomplete, recovery_replaced}, buckets{b: prior, matched, coverage, retirable}, walks_summary)`.
- Rules, in order:
  1. Run error, or rows < min_rows: retire nothing.
  2. `source` is recovery and `retire_on_recovery_replaced` is False: retire nothing (`partial:recovery_replaced`). This is the hyundaiofcookeville 09-28 shape.
  3. Any live recipe walk (not a stale retry) is incomplete: `partial:walk_<end>`, retire nothing (D-RC4).
  4. Each bucket's prior baseline is the active rows with `scraped_at` in `[run_started − 30d, run_started)`. That excludes rows this run refreshed: reconcile loads rows after persist, so without the upper bound a 20-row run would make itself "fresh". When the window is empty, fall back to 90 days, then to all active rows with `scraped_at < run_started`. Coverage = prior rows re-seen by VIN / prior rows. A bucket is retirable only when coverage ≥ bucket_share and matched > 0. A row-count floor is secondary only.
  5. `one_condition_ok` makes only an ABSENT bucket retirable (stray rows). A present bucket still needs the coverage share.
  6. Make guard: never retire a make with ≥ make_guard_min prior rows of which none were matched.
  7. Never retire seen or protected VINs.
  8. Invalid-VIN rows retire only when their bucket is retirable and `scraped_at < run_started`. This keeps the pipeline's ability to clear VIN-less rows and the July backlog.
- Tests (at least 16, under 2 s, no `backend.db` import):
  - parksidekia 306 new / 0 used keeps the used rows;
  - lexusofchattanooga 231 new / 0 used against baseline 237 retires 0 used;
  - a page_cap walk is partial; a mid-walk 403 is partial; a stale retry failing again does not block;
  - the Right Toyota shape (3,034 active, 1,367 fresh, 1,273 seen across both buckets) retires the July ghosts;
  - Gunn Honda's 8-row run retires nothing; MB Beverly Hills (2 stamped) retires nothing;
  - an allow-listed used-only dealer retires its stray new rows, but a partial used capture there retires nothing;
  - protected VINs are kept; a make returned as 0 with ≥10 prior rows is kept;
  - 5 used seen of 300 prior used makes used non-retirable;
  - 300 VINs that match nothing stored retire nothing;
  - 400 stale rows plus a 25-row complete run retire nothing;
  - a recovery-replaced run retires nothing.
- Accept: the tests pass. The Gunn Honda, July-ghost and MB Beverly Hills cases are expressed here before P5A.7 deletes the pipeline UPDATE (audit deletions).

**P5A.2: Walk report.**
- Change:
  - `try_fetch_via_recipes(..., replay_out: dict | None = None)`. For each tried recipe, record `{key, url, stale_retry, pages, raw_vins, kept_vins, total_count, end, complete, in_union}`.
    - `raw_vins` counts unique VINs, kept plus refused.
    - `end` is one of single_page, total_reached, exhausted, short_page, page_cap, http_<code>, fetch_failed, auth_dead, unparsed_mid, unreplayable.
    - `complete`: when total_count is known, it is `raw_vins >= 0.98 x total_count`, whatever the end reason. Without a total, it is end ∈ {single_page, total_reached, exhausted, short_page}.
    - `in_union` is separate from `end`. A complete walk that is dropped for low yield gets `in_union=False`, and its kept VINs go to `replay_out['low_yield_vins']`, so P5A.5 protects them instead of blocking the dealer.
  - Summary: `{truncated, capped_recipes, source: 'recipe'}`. Log one WARNING per page_cap or mid-walk stop. Fix the union log line at recipes.py:1160-1163, which counts pages as "recipe replay(s)".
  - Termination logic and return values are unchanged.
  - feed.py:36-37 passes `replay_out` and sets `run.result['recipe_replay']` only when non-empty. The catch-all at feed.py:65-66 logs at WARNING (not DEBUG) and sets `result['recipe_replay'] = {'error': ...}`.
  - delta_scan.py:157 does the same into `out['recipe_replay']`.
  - The dealers_repo summary_json whitelist (~:20-60) gains `recipe_replay`.
  - The golden stubs (test_dealer_run_golden_20261001.py:159, test_attribution_golden_20261001.py:165) accept `**kw`. That is a harness change only, with no fixture data change.
- Tests, with a fake `_replay_request`:
  - more than 40 pages → page_cap, incomplete;
  - 403 at page 3 → http_403, incomplete, VINs kept;
  - total reached → complete; an all-duplicate page → exhausted;
  - 401 on page 1 → auth_dead;
  - an 8-VIN recipe → `in_union=False` and excluded from the union;
  - "exhausted at 48 of total 500" and "single_page 100 of total 500" → incomplete;
  - a SQLite scan_runs row written through dealers_repo carries `summary_json.recipe_replay`.
- Accept: these pass with no fixture change: test_recipes.py, test_delta_scan.py, test_delta_scan_full_scan_parity.py, test_dealer_run_golden_20261001.py, test_attribution_golden_20261001.py, test_recipe_walk_fixes_20260925.py, test_store_scoped_recipe_gate_20260925.py, test_recipe_lifecycle.py, test_recipe_validation.py, test_geocode_dealers.py.

**P5A.3: Allow-list.**
- Change: move the 3 ids from `workspace/pipeline/one_condition_ok.txt` (gitignored and dockerignored, so Railway sees an empty list) to the tracked `backend/config/one_condition_ok.txt`. One loader, `recipe_validation.one_condition_ok_ids`, merges the tracked file and the workspace file. `assess._one_condition_ok` and the policy both use it. Keep both names as thin wrappers: test_dealer_pipeline_surface.py:37 pins one, and test_recipe_validation.py:296 monkeypatches the other. Grep consumers first: recipe_validation.py:80/405, assess.py:20-27, dealer_pipeline.py:61.
- Tests: test_one_condition_allowlist.py.
- Accept: the loader returns the union of both files; `.dockerignore` lets the tracked file into the build context; the assess and recipe_validation tests pass.

**P5A.4: Scanner adopts the policy.**
- Change:
  - `reconcile_dealer_inventory_after_scan(dealer_id, url, scraped_vins, stats, *, scraped_conditions=None, replay=None, protected_vins=None, run_started_iso=None, source='recipe', _conn=None)` loads the active rows (id, vin, condition, make, scraped_at), builds RunEvidence and calls `plan_retirement`.
  - Policy from env:
    - `SCANNER_RECONCILE_BUCKET_SHARE` (new, default 0.6).
    - `SCANNER_RECONCILE_MIN_COVERAGE` keeps its documented "0 disables" meaning (.env.example:117-120), or is deprecated with a warning. It never silently becomes the per-bucket share.
    - `SCANNER_RECONCILE_MIN_ROWS`, default 20.
    - `one_condition_ok` from P5A.3.
  - The UPDATE runs in batches: `WHERE id IN (...) AND TRIM(dealer_id)=? AND COALESCE(listing_active,1)=1 AND (scraped_at IS NULL OR scraped_at < ?)`. That guards against rows re-upserted mid-flight.
  - `SCANNER_RECONCILE` accepts `1`, `0` or `dry`; `dry` computes and records the plan but writes nothing.
  - `stats['reconcile']` = eligible, reason, retired, kept by reason, per-bucket coverage.
  - Rewritten test assertions, each named in the PR: test_scanner_inventory_reconcile.py:336-340 (MIN_COVERAGE=0 with 10 rows retired 90; min_rows becomes 20, so the fixture grows); fixtures with NULL scraped_at/condition (:40-55) get values.
  - test_reconcile_condition_guard.py semantics are preserved or strengthened. Document the knobs in .env.example.
- Tests: a row whose scraped_at moved past run_started between plan and UPDATE is not retired; `dry` writes 0 rows; with both buckets complete, missing VINs are retired.
- Accept: the tests pass; the changed assertions are listed with reasons.

**P5A.5: Evidence wiring.**
- Change:
  - DealerRun gains a wall-clock `run_started_iso`; state.py:28 has only `perf_counter`.
  - after_write.py:124-136 passes: `result['recipe_replay']`, the conditions, `run_started_iso`, `source` (`recovery:<winner>` when recovery replaced the feed, recovery.py:67-75), and the protected VINs:
    - (a) unidentified rooftop refusals, via a NEW helper `unprotected_refusal_vins(refused)` next to `split_refusals`. `split_refusals`' return shape is pinned by gate.py:101-104, delta_scan.py:202, after_write.py:74, test_dealer_attribution.py:803-821 and test_attribution_golden_20261001.py:96,239, so it stays untouched.
    - (b) VINs dropped by `filter_sister_stores` (enrich.py:107-123), `drop_detail_page_sister_rows` (enrich.py:155+) and `drop_unattributable_vehicles` (database.py:335-358, :376), unless the reason is in `EVIDENCE_BACKED_REJECTS`.
    - (c) `low_yield_vins` from P5A.2.
  - summary_json gains the `reconcile` plan.
  - The golden fixture diff is limited to the reconcile event's argument shape, and the reviewer checks it.
- Tests: test_reconcile_scanner_wiring.py:
  - new recipe complete plus used recipe http_403 → 0 retired, reason `partial:walk_http_403`, and summary_json carries `recipe_replay`;
  - both walks complete → the missing VINs are retired;
  - a sister-store-filtered VIN is protected;
  - a recovery-replaced run retires nothing.
- Accept: the tests pass, and split_refusals is unchanged (diff).

**P5A.6: Delta parity.**
- Change: delta_scan passes the replay report, conditions, protected VINs and run start, and uses `Policy(bucket_share=0.8)`. This is a deliberate semantic change, stated in the PR: today's gate is valid scraped VINs over ALL active rows, ghosts included (delta_scan.py:324-326), while the per-bucket prior coverage is looser for backlog dealers. The plan is recorded in `out['reconcile']`.
- Tests: a delta replay returning only new keeps the used rows; a truncated walk retires nothing; the full and delta paths produce the same plan for the same feed, except for the share; a backlog-dealer parity case.
- Accept: the tests pass and the existing delta tests are green.

**P5A.7: Pipeline report-only; truncation verdict.**
- Change:
  - `pipeline/reconcile.reconcile_dealer` keeps its name and signature (the surface test pins them) but no longer writes. It reads `summary_json['reconcile']` from the scan_runs row assess selected; the lifecycle retry reads the retry run's row (lifecycle.py:326-331). It returns `{source: 'scanner', eligible, retired, reason, kept_*, buckets}`.
  - `--no-reconcile` now means `SCANNER_RECONCILE=dry`, and the help text says so.
  - `RECONCILE_MIN_SHARE` stays exported as the policy default, so there is one source.
  - Assess reads `recipe_replay`. When the replay was truncated, an `ok` verdict becomes `thin` with the reason prefix `replay_truncated:` (D-SR4). Other verdicts get the note appended, and a verdict is never upgraded. The dealer-discovery workflow consumes thin dealers from triage.json, so the token is stable and documented: it means a page-cap or walk-length issue (D-RC3, P5B.2), not a parser or field-coverage problem. Add one line saying so to the dealer-discovery workflow text (`.claude/workflows/dealer-discovery.js`, the triage step) and to `_learning/platform_playbook.md` (MAIN, post-merge).
  - Triage gains `retirement_audit`: retired count and retired/prior ratio per dealer. It alarms when a whole bucket, or more than 40% of the prior lot, is retired. A stale-bucket alarm fires when a bucket has not been seen in 3 consecutive runs, and that dealer goes to needs_discovery.
  - scan_runs.md prints the plan and a "replay: truncated (...)" line. Every alarm appends a line to `_learning/errors_index.md`.
  - Rewritten assertions, each named in the PR: test_pipeline_reconcile_20260926.py:30-57 and test_assess_stamped_rows.py:44,52.
- Tests: test_pipeline_retirement_report.py (report-only returns the scanner's numbers); test_pipeline_assess_truncation.py (a truncated full-coverage row becomes thin with the token; the same row without truncation stays ok; scan_runs.md under a tmp `DEALER_LOGS_ROOT` has the truncation line).
- Accept: `grep -n 'UPDATE cars' backend/scanner/pipeline/` returns nothing; the surface pins pass, and any pin change is explained; triage.json has `retirement_audit`.

**P5A.8: Shadow comparison.**
- Change (MAIN, AC power, local PG): run `scanner.py --dealer-id <id> --scan-only` with `SCANNER_RECONCILE=dry` for:
  - lexusofchattanooga-com (one-condition);
  - the Right Toyota dealer (backlog);
  - mbbeverlyhills-com (group feed);
  - duvalford-com or toyotacarlsbad-com (40-page cap);
  - Gunn Honda (small run).
  These are real local scans: they upsert rows, and only retirement is dry. Compare each plan with what the legacy guard would have retired; P1A.1's interim code can compute that in dry mode. Write scan_runs.md and summary.md for each dealer per NETWORK_SCAN_PROCESS.
- Accept: a per-dealer plan-vs-legacy table in the PR, signed off by the owner before the Phase 5A release.

**Workflow shape.** Wave A: P5A.1, P5A.2, P5A.3. Wave B: P5A.4. Wave C: P5A.5. Wave D: P5A.6, P5A.7. Wave E: P5A.8 (MAIN). Review lens:
- no path retires on incomplete evidence;
- golden fixture diffs are limited to the declared keys;
- `split_refusals` is untouched;
- the guarded UPDATE's SQL is checked once on local PG (the shadow run).

**Data repair.** None. The repair is P5B.8.

**Exit gate.**
- pytest, chunked: test_retirement_policy.py, test_recipe_replay_stop_reason.py, test_one_condition_allowlist.py, test_scanner_inventory_reconcile.py, test_reconcile_condition_guard.py, test_reconcile_scanner_wiring.py, test_delta_scan.py, test_delta_scan_full_scan_parity.py, test_pipeline_reconcile_20260926.py, test_assess_stamped_rows.py, test_dealer_pipeline_surface.py, test_pipeline_retirement_report.py, test_pipeline_assess_truncation.py, test_dealer_run_golden_20261001.py, test_attribution_golden_20261001.py, plus every must-pass list above.
- The shadow table is signed off.
- `grep -rn "UPDATE cars SET listing_active" backend/scanner/pipeline` returns nothing.

**Release step.** Minor (scan behaviour). Bump, push, CI green, fast-forward, tag. No deploy (scanner-nightly ships in P6B.1). Release note: about 20 dealers will move from ok to thin in triage counts, because truncated replays are now visible.

## Phase 5B: Walk completeness, provenance, audit tool, local repair

**Goal.**
- Group-feed walks end on raw-row novelty, and stop at the site's own total rather than a stored synth-time number.
- Every retirement is recorded with its path, behind a two-strike rule.
- An audit tool classifies, verifies and restores wrongly retired rows, with a backup.
- Replay-to-persist gaps are visible.
- Repair the local DB, rescan-first.

**Entry gate.** Phase 5A released. D-RC2 (P5B.4), D-RC3 (P5B.2), D-RC5 and D-RC6 (P5B.8), D-RC7 (P5B.3) answered. P0B.2 verdicts available.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P5B.1 | scanner-runtime-10a + reconcile-6 (part 1) | Replay walk ends on no new RAW VIN; total check on raw rows | code | backend/scanner/recipes.py, backend/tests/test_recipe_walk_group_feed.py (new) | S | A | none |
| P5B.2 | owner-decisions-2 + reconcile-6 (part 2) (+ reviewers) | One site-total reader; HTML walks ignore the stored total; total-derived page cap | code | backend/scanner/recipe_totals.py (new), backend/scanner/recipe_validation.py (re-exports), backend/scanner/recipes.py, backend/tests/test_recipe_site_total.py (new) | M | B | P5B.1 |
| P5B.3 | reconcile-7 (+ reviewer) | `listing_retirements` provenance table (V027), written by every retirement writer | code | migrations/V027__listing_retirements.sql (new), backend/db/repositories/schema_repo.py, backend/scanner/inventory_reconcile.py, backend/attribution/disown.py, backend/dealer/portal_sync.py, backend/tests/test_listing_retirements_audit.py (new) | M | A | none |
| P5B.4 | reconcile-8 (+ reviewer) | Two-strike retirement in a side table (V028) | code | migrations/V028__listing_miss_strikes.sql (new), backend/scanner/retirement_policy.py, backend/scanner/inventory_reconcile.py, backend/db/repositories/schema_repo.py, backend/scanner/pipeline/triage.py, backend/tests/test_retirement_two_strike.py (new) | M | B | P5B.3 |
| P5B.5 | reconcile-9a (+ reviewer) | `audit_retirements.py`: classify and report (read-only) | code | backend/scripts/audit_retirements.py (new), backend/tests/test_audit_retirements.py (new) | M | A | none |
| P5B.6 | reconcile-9b/c (+ reviewer) | Audit verify (replay/VDP, host match) plus apply and rollback | code | backend/scripts/audit_retirements.py, backend/tests/test_audit_retirements.py | M | B | P5B.5, P5B.3 |
| P5B.7 | scanner-runtime missing (parity telemetry) + scanner-runtime-6 (reframed pin) | Replay→persist gap telemetry and parity pin | code + test | phases/dealer_run_steps/persist.py, backend/scanner/pipeline/assess.py, dealers_repo.py, backend/tests/test_replay_persist_gap.py (new) | S | A | none |
| P5B.8 | reconcile-10 (+ reviewer) | Local repair of wrongly retired rows, rescan first (MAIN) | data_repair | workspace/data_quality/, workspace/backups/, workspace/dealer_logs/ | M | C | all above |
| P5B.9 | reconcile-13 (+ reviewer) | Retirement docs, knobs, playbook | docs | docs/NETWORK_SCAN_PROCESS.md, docs/RAILWAY_SCANNING.md, .env.example (`_learning/platform_playbook.md` as a MAIN post-merge step) | S | C | P5B.1-P5B.7 |

**P5B.1: Raw-row termination.**
- Change: the page loop keeps `seen_raw` over the VINs of `page_vehicles` plus `_refused`, and breaks when a page adds no new raw VIN. `vins` and `n_vins` stay kept-only. The total_count stop (recipes.py:1125) compares raw rows seen against the total; kept VINs are compared with a group total today. Record `end='no_new_rows'`.
- Tests: page 2 holds only sibling rows and page 3 holds store rows: the page-3 VINs are collected and `n_vins` excludes every sibling VIN.
- Accept: the golden tests pass, or the fixture diff only adds page events and is reviewed.

**P5B.2: Site totals and the cap.**
- Change:
  - New `recipe_totals.py` holds `SITE_TOTAL_EXTRACTORS`, `extract_site_total`, `platform_of` and the `_total_*` helpers, moved verbatim and re-exported from recipe_validation. It imports `PAGINATION_*` lazily.
  - Replay page 1 takes the total from count fields only: `max(extract_site_total count readers, get_total_count)`. That adds the DEP `data-vehicle_count` and the autoWALL `<title>` count. List-length readers (`_total_list_length` for wp_vehicles_index and generic_json, recipe_validation.py:285-291) never stop a paginated walk.
  - HTML paginations (DEP_SRP, HTML_PAGE, JAZEL_SRP, html_cards) without an on-page total ignore the stored `recipe.total_count`, which synthesize_recipes.py:227-228 froze at synth time, and stop on the first page that adds no new raw VIN.
  - Page cap: when total and page size are known, `pages = min(HARD_CAP, ceil(total/page_size)+1)`; otherwise 40. `HARD_CAP` is `SCANNER_REPLAY_MAX_PAGES` (default per D-RC3). `page_cap` is reported only when the hard cap stops the walk before the total.
  - Pacing is unchanged. The one-shard 403 measurement happens in P6B.4.
- Tests:
  - A DEP fixture with stored total 56, `data-vehicle_count` 100 and 48 per page walks 3 pages and returns 100 VINs (today: 96 after 2 pages).
  - An Overfuel html_page fixture with stored total 30 and no site total continues until no new VIN.
  - A paginated generic_json fixture is not capped at page 1.
  - Total 3,000 at 48 per page walks 63 pages when the cap is 63 or more, and reports page_cap otherwise.
  - The request count is bounded.
  - For every platform fixture in test_recipe_validation.py, the scan and the set gate read the same total.
- Accept: the tests pass, and test_scanner_http_characterization.py is unchanged.

**P5B.3: Retirement provenance.**
- Change:
  - V027 creates `listing_retirements(id bigserial, car_id, vin, dealer_id, retired_at, path, scan_ref, reason, plan_json)`, where path is one of scanner_reconcile, delta_reconcile, disown, portal, manual_repair, restore. The DDL is idempotent in schema_repo (SQLite) and in the migration.
  - Every writer (inventory_reconcile for scanner and delta, disown.py:72-81, portal_sync.py:202) uses `UPDATE ... RETURNING id, vin` (Postgres; SQLite 3.35+) and a plain INSERT of exactly those rows in the same transaction. Never `ON CONFLICT DO NOTHING`.
  - The pipeline's UPDATE is already gone (P5A.7).
  - Hand-run SQL is the remaining writer. The repair runbook (P5B.9) says manual retirements go through `audit_retirements --retire`, or an INSERT documented beside the UPDATE.
- Tests: for each of the four writers, the rows recorded equal the UPDATE rowcount (SQLite); a grep test checks that every `SET listing_active = 0` in `backend/` outside tests has an adjacent `listing_retirements` insert.
- Accept: the tests pass; the migration applies cleanly on a scratch PG and leaves `cars` untouched.

**P5B.4: Two-strike.**
- Change:
  - V028 creates a side table, `listing_miss_strikes(car_id PK, dealer_id, missed_runs, first_missed_at, last_run_ref)`. The upsert is NOT touched; that protects the 48-column upsert and the Chapman Ford deadlock work.
  - On an eligible plan, each candidate gets `missed_runs+1`, and `first_missed_at` if it was NULL. It retires only when `missed_runs+1 >= strikes` and `first_missed_at <= now − 20h`.
  - The reconcile resets strikes for the VINs it saw.
  - `listing_removed_at` is stamped from `first_missed_at`, so turn-time analytics (inventory_signals.py:400-405, :485-492) keep the true sold time.
  - Triage shows pending strikes per dealer.
- Tests: the first miss retires nothing and records a strike; a second miss 20 h or more later retires; a VIN re-seen in between resets.
- Accept: the tests pass, plus one local Postgres run of the SQL (output in the PR).

**P5B.5: Audit, classify.**
- Change: `backend/scripts/audit_retirements.py`, read-only; the test asserts the connection never commits.
  - Group retired rows since `--since` by `(dealer_id, listing_removed_at)`. Exclude disown batches: use `listing_retirements.path` where it exists; for older rows, match the scanner-log line "belonged to sibling rooftops" and split batches stamped in the same second.
  - Signatures:
    - one_condition: the triggering run re-saw 0 rows of the bucket and at least 20 of the other;
    - bucket_now_empty;
    - large_share: more than 40% of the prior lot;
    - truncation_suspect: `summary_json.recipe_replay` incomplete, or, with `--scanner-log`, "from 40 page(s)" or "HTTP 4xx after N page(s)".
  - `--exclude-file` is seeded with the lexusofknoxville-com 2026-09-27 hand retirement (1,417 foreign rows, user-authorized).
  - Outputs: `workspace/data_quality/retirements_<stamp>/{batches.csv, candidates.csv, report.md}`. The P0B.2 verdicts feed the review.
- Tests: a one-condition batch is flagged; an excluded batch is not; a disown batch is not.
- Accept: the tests pass, and a default run performs zero writes.

**P5B.6: Audit, verify, apply, rollback.**
- Change:
  - `--verify replay`: union recipe replay over HTTP with `persist=False`.
  - `--verify vdp`: a paced GET of `source_url`. The VIN in the body with no sold marker means live. 404/410, or a redirect to the SRP, means gone. 403/429/timeout/challenge means cannot_assess. The `source_url` host must equal the dealer's host; otherwise the row is cannot_assess, because group sites answer for sibling stores.
  - `--apply` requires a verify file. It writes `workspace/backups/retire_restore_<stamp>.json` first, then runs `UPDATE cars SET listing_active=1, listing_removed_at=NULL WHERE id=? AND listing_active=0 AND listing_removed_at=<batch ts> AND dealer_id=<batch dealer>`. It records `path='restore'` rows, appends dated blocks to the dealer's scan_runs.md and summary.md, and adds one `_learning` line.
  - `--rollback` re-applies the backup.
- Tests: `--apply` touches only verified-live ids; a row re-retired or moved since the audit is skipped; rollback round-trips; a host mismatch gives cannot_assess.
- Accept: the tests pass.

**P5B.7: Replay→persist gap.**
- Change: persist.py computes `replay_persist_gap`. When persisted rows are under 50% of replay-kept VINs, it logs a WARNING, sets `result['replay_persist_gap'] = {kept, persisted, dropped_by}`, adds an assess note, and adds the key to the whitelist.
- Tests: a parity pin feeds the same fake two-store unscoped carscommerce feed through `try_fetch_via_recipes` and then `run_dealer` (golden harness), and asserts the persisted VIN set equals the replay-kept set. If it reproduces the terrylabonte 324→11 drop, mark it `xfail(strict=True)` with that reason and file it to the attribution cluster.
- Accept: the gap telemetry test passes; the pin either passes or is a strict xfail with a filed finding.

**P5B.8: Local repair, rescan first.**
- Change (MAIN):
  - Preconditions: AC power; no scanner lock live on any host; the mini on the same commit; `pg_stat_activity` shows no scanner or idle-in-transaction sessions.
  1. Backup: the tool's JSON, plus `CREATE TABLE cars_retire_backup_<stamp> AS SELECT id, vin, dealer_id, condition, listing_active, listing_removed_at, scraped_at FROM cars WHERE listing_removed_at >= '2026-09-23'` (about 102k rows). Add the table to the D-DB1 cleanup list.
  2. Dry run: `audit_retirements --since 2026-09-23`. Expect at least 12-13 one-condition batches (1,399-1,480 rows) and the 6 bucket-empty dealers: arlingtontoyota-com new 394, mountainstatestoyota-com used 229, lexusofknoxville-com used 81 (excluding the 09-27 authorized batch), nissanofcookeville-com used 70, hyundaiofcookeville-com used 58, cleveland-nissan-com used 16. Plus the P0B.2 truncation cases.
  3. Rescan first: `dealer_pipeline --dealers <affected>` with the fixed policy. It re-lists live cars through the upsert (sql.py:118-119) and writes dealer logs.
  4. Re-run the audit; run `--verify replay`, then `--verify vdp`, on the residue.
  5. The owner reviews candidates.csv.
  6. `--apply` the residue.
  7. Verify:
     - the restored ids are a subset of the verified-live ids;
     - every dealer with verified rows has more than 0 active rows in that bucket;
     - no row with a changed dealer_id was touched;
     - the active-row delta equals the restored count;
     - a re-run of the audit shows the candidates resolved;
     - the local grid cards are rebuilt with `FLASK_ENV=production`.
  8. Write dealer logs and a `_learning` line. cannot_assess bucket-empty dealers go to needs_discovery.
- Accept: the backup exists before any write; report.md lists every cannot_assess row by dealer and is linked from each dealer's summary.md.

**P5B.9: Docs.**
- Change:
  - NETWORK_SCAN_PROCESS.md gets a "Listing retirement" section: single writer, gates, walk completeness, two-strike, the audit command, and a checklist line for the reconcile plan.
  - RAILWAY_SCANNING.md: `--no-reconcile` semantics; where the retirement audit appears in triage; a placeholder for the `SCANNER_REPLAY_MAX_PAGES` 403 measurement (filled in P6B.4).
  - .env.example: `SCANNER_RECONCILE=1|0|dry`, `SCANNER_RECONCILE_BUCKET_SHARE`, `SCANNER_RECONCILE_MIN_ROWS`, `SCANNER_REPLAY_MAX_PAGES`.
  - `_learning/platform_playbook.md` (MAIN, post-merge, because workspace is untracked): mid-walk 403 truncation on Dealer eProcess and dealer.com SRP pagination.
- Accept: the docs name the module and function that implement each rule.

**Workflow shape.** Wave A: P5B.1, P5B.3, P5B.5, P5B.7. Wave B: P5B.2, P5B.4, P5B.6. Wave C: P5B.9, then P5B.8 (MAIN). Review lens:
- the migrations follow the protocol;
- inserts are plain (no DO NOTHING);
- restores never re-list a sibling's car;
- walk changes stay bounded and paced.

**Data repair.** P5B.8 (local only). Prod is P6B.6.

**Exit gate.**
- pytest, chunked: test_recipe_walk_group_feed.py, test_recipe_site_total.py, test_recipe_validation.py, test_scanner_http_characterization.py, test_listing_retirements_audit.py, test_retirement_two_strike.py, test_audit_retirements.py, test_replay_persist_gap.py, test_dealer_run_golden_20261001.py, test_attribution_golden_20261001.py.
- V027 and V028 applied locally.
- The P5B.8 verification queries pass, and the dealer logs are written.

**Release step.** Minor (schema plus scan behaviour). Bump, push, CI, fast-forward, tag. Do NOT deploy web or scanner-nightly from this release until P6B.1 has applied V027 and V028 to prod. Any process running this code against a V026 database would treat it as behind the chain and fall back to legacy DDL.

## Phase 6A: Scanning-resumption code

**Goal.** Make the Railway nightly safe to schedule:
- cross-host advisory locks;
- a run window, so a redeploy or variable change never starts an unscheduled fleet scan;
- a fleet lock;
- a VIN-owner guard that survives the gap since 09-29;
- kill switches;
- log retrieval and merge;
- capture-time refusal of VIN-list and widget recipes;
- a pre-fleet host parity check.

**Entry gate.** Phase 5B released. D-IF5 (P6A.3) and D-IF10 (P6A.5) answered. P0A.2 has recorded `max_connections` and usage.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P6A.1 | infra-2 (+ reviewer) | Cross-host scan locks via Postgres advisory locks | code | backend/scanner/scan_leases.py (new), backend/tests/test_scan_leases.py (new) | S | A | none |
| P6A.2 | infra-3a (+ reviewer) | fleet_scan: run window, fleet lock, capped idle hold, post-run hold | code | backend/scripts/fleet_scan.py, scripts/railway_scan_fleet.sh, backend/tests/test_fleet_scan_safety.py (new) | M | B | P6A.1 |
| P6A.3 | infra-3b + infra missing (72 h default) (+ reviewer) | VIN-guard gap cover from the oldest owner's scrape; default 72 h | code | backend/scripts/fleet_scan.py, backend/scanner/database.py (:53), backend/tests/test_fleet_scan_safety.py, test_upsert_vin_owner_guard.py, docs/RAILWAY_SCANNING.md (:98) | S | C | P6A.2 |
| P6A.4 | infra missing (kill switches) | Kill-switch matrix: tests and doc | test + docs | backend/tests/test_fleet_scan_safety.py, docs/RAILWAY_SCANNING.md | S | D | P6A.3 |
| P6A.5 | infra-7 (+ reviewer) | Pull Railway volume logs; merge them into the canonical dealer_logs tree | code | scripts/pull_railway_scan_logs.sh (new), backend/scripts/merge_dealer_logs.py (new), backend/tests/test_merge_dealer_logs.py (new) | M | A | none |
| P6A.6 | owner-decisions missing (recipe hygiene) (+ reviewer) | Refuse VIN-list, widget and single-VIN recipes at capture; list and stale existing ones | code + data_repair | backend/scanner/recipes.py (promote_from_ledger), backend/scripts/rejudge_live_recipes.py (new, `--hygiene` mode), backend/tests/test_recipe_hygiene.py (new) | M | A | none |
| P6A.7 | reconcile missing (host parity and env audit) | Pre-fleet check script and checklist (MAIN run) | ops | scripts/prefleet_check.sh (new), docs/RAILWAY_SCANNING.md | S | E | P6A.2, P6A.4 (both edit RAILWAY_SCANNING.md) |

**P6A.1: Advisory locks.**
- Change: `fleet_lock(lane)` (a context manager) and `try_lock_dealers(ids) -> (acquired, held_elsewhere)`, built on `pg_try_advisory_lock(<const classid>, hashtext(key))` over a dedicated autocommit connection with `connect_timeout`, keepalives and `application_name='scanner:<lane>:<hostname>'`. The holder is reported from `pg_locks` joined with `pg_stat_activity`. On SQLite both succeed as no-ops. Locks release on close or process exit.
- Tests: SQLite always acquires; the Postgres test (skipped without `TEST_PG_DSN`) is run ONCE against local PG, lock-only with no writes, and its output goes in the PR. The lock connection is never idle in transaction. The test asserts `idle_session_timeout` is unset, since it would silently drop the lock.
- Accept: the tests pass, and the local PG output is attached.

**P6A.2: Run window and fleet lock.**
- Change:
  - `SCAN_FLEET_WINDOW_UTC='HH:MM-HH:MM'` (wraps past midnight). With `SCAN_FLEET=1`, the fleet scans only inside the window; outside it prints `fleet   outside run window` and takes the idle path.
  - The idle hold is capped to end at least 5 minutes before the window opens. Today `SCAN_IDLE_HOLD_SECONDS=1500` (RAILWAY_SCANNING.md:90), so a redeploy at 08:35 would hold across the 09:00 cron fire, and Railway skips a fire while the previous run is up.
  - An invalid window means idle plus a WARN. An unset window keeps today's behaviour.
  - `SCAN_DEALERS` ignores the window, but fleet_summary records a WARN when it is set during a window run: a leftover value silently turns the cron run into a partial run (fleet_scan.py:99-101).
  - A whole-fleet run takes `fleet_lock(lane)` and exits 3 before spawning any shard when the lock is held, printing the holder.
  - `SCAN_POST_RUN_HOLD_SECONDS` keeps the container up after a run, so logs can be pulled even if a cron-scheduled service does not start on a redeploy.
  - fleet_summary.json records lane, window, lock holder and reason.
- Tests: window parsing (normal, wrap-around, invalid); idle outside the window; `SCAN_DEALERS` runs regardless and warns; lock contention exits 3 without spawning dealer_pipeline; the idle-hold cap.
- Accept: the tests pass, and test_fleet_roster_rule.py passes.

**P6A.3: VIN-guard gap cover.**
- Change: when `SCANNER_VIN_OWNER_GUARD_HOURS` is not set explicitly, compute the gap from the OLDEST active `scraped_at` among roster dealers that appear as `owner_dealer_id` in vin_owner_conflicts, falling back to the roster's p05. A median would leave owners the last run did not scan exposed: the 6 auth-staled owners downeyhyundai, duvalford, easyhonda, hondaofelcajon, mymetrohonda and overdrive-usa. Export `ceil(gap + 24)`, capped at 720, into every shard env, and record `vin_guard_hours` in fleet_summary. Raise `_VIN_OWNER_GUARD_DEFAULT_HOURS` (database.py:53) from 48 to 72 (D-IF5c), updating test_upsert_vin_owner_guard.py and RAILWAY_SCANNING.md:98. Refusing known (owner, claimant) pairs (D-IF5d) is deferred.
- Tests: a fixture where one owner was not scanned in the last run, so its age drives the hours; an explicit env value wins; the 720 cap; the 72 h default.
- Accept: the tests pass.

**P6A.4: Kill switches.**
- Change: tests proving that an unset `SCAN_FLEET_WINDOW_UTC` and the new `SCAN_FLEET_LOCK=0` restore today's behaviour, plus a "Kill switches" table in RAILWAY_SCANNING.md: variable → effect → revert by variable change, not by deploy. P15A.5 extends it for lanes, and P11A.3 for dealer locks.
- Accept: the tests pass and the table is present.

**P6A.5: Log pull and merge.**
- Change:
  - `pull_railway_scan_logs.sh` refuses while the current time is inside `SCAN_FLEET_WINDOW_UTC`, or unless `SCAN_FLEET` is unset. It prefers to pull during the post-run hold, with `railway ssh -- tar czf - workspace/dealer_logs workspace/pipeline/fleet_<latest>`, into `workspace/railway_pulls/<stamp>/`. It never triggers a redeploy of a cron-scheduled service (redeploy-to-idle is unverified there).
  - `merge_dealer_logs.py` (dry run by default, `--apply` to write) appends `## ` blocks that are not already present (hash of header plus body) to the canonical `workspace/dealer_logs/<id>/*.md` and `_learning/*.md`, with an "appended from <source> <stamp>" marker. It never edits existing blocks and creates new dealer dirs. It skips `scan_instructions.md` and the placeholder `summary.md` ("none recorded yet") that `write_instructions_if_first_success` writes on an empty volume (pipeline/dealer_logs.py:84-104) when a canonical file exists.
- Tests: merging the same pull twice adds nothing; existing text stays byte-identical; the placeholder is skipped; the dry run prints counts.
- Accept: the tests pass.

**P6A.6: Recipe hygiene.**
- Change:
  - `promote_from_ledger` (recipes.py ~:369-470) refuses capture candidates whose URL or body carries a 17-character VIN, `vin=`, `vehicles-recommendations`/`ws-rec`, Gatsby `page-data`, or a single-VIN endpoint.
  - New `rejudge_live_recipes.py --hygiene`, dry run by default, lists every live recipe of those shapes. Today there are 13 VIN-in-URL recipes on 10 dealers; audifletcherjones-com (last_ok 2026-09-25) and mckennasubaru-com (2026-09-28) succeeded with 10 or more VINs, so they re-see sold cars on every scan.
  - `--apply`, on an owner-reviewed list, backs up `workspace/recipes` (sha256 manifest plus tar) and the dealer_recipes rows (`\copy`), marks each recipe stale through `mark_stale` with reason `hygiene:<shape>`, and writes a discovery.md entry per dealer. It prints the dealers whose VIN-list recipes succeeded, for the retirement audit.
  - The local apply runs MAIN in this phase; the prod apply is P6B.2.
  - Verify after apply: re-run the `--hygiene` dry run. Every applied recipe now shows stale `hygiene:<shape>`, the sha256 manifest diff covers only the listed dealers' recipe files, and the dealer_recipes `\copy` diff covers only the listed rows. Rollback: restore the tar and the rows from the backup.
- Tests: refusal per shape; the dry run writes nothing; apply touches only listed ids.
- Accept: the tests pass, and the local apply is done, verified and recorded in dealer logs.

**P6A.7: Pre-fleet check.**
- Change: `scripts/prefleet_check.sh` checks and reports:
  - every scanner host (the MBP, the mini, the scanner-nightly image) is on the same release commit;
  - Railway scanner-nightly has `SCANNER_RECONCILE*` as intended, `SCANNER_ALLOW_BROWSER` unset, `SCANNER_EGRESS_TAG=railway` and `SCAN_FLEET` empty;
  - the mini's `.env` DSN host;
  - no scanner lock is held anywhere;
  - `pg_stat_activity` shows no idle-in-transaction sessions.
  The checklist goes in RAILWAY_SCANNING.md. The MAIN run happens at the start of P6B.
- Accept: the script exits non-zero on any mismatch, and its report goes to the ops log.

**Workflow shape.** Wave A: P6A.1, P6A.5, P6A.6. Wave B: P6A.2. Wave C: P6A.3. Wave D: P6A.4. Wave E: P6A.7 (P6A.3, P6A.4 and P6A.7 all edit RAILWAY_SCANNING.md, so they are serial). Review lens: unset variables keep today's behaviour; no prod writes; nothing triggers a redeploy.

**Data repair.** The P6A.6 local hygiene apply (backup, dry run, owner OK, apply).

**Exit gate.**
- pytest, chunked: test_scan_leases.py (plus the local PG output), test_fleet_scan_safety.py, test_fleet_roster_rule.py, test_upsert_vin_owner_guard.py, test_merge_dealer_logs.py, test_recipe_hygiene.py, test_recipes.py.
- The local hygiene apply is logged.

**Release step.** Minor. Bump, push, CI, fast-forward, tag. Deployment happens in P6B.1.

## Phase 6B: Resume Railway scanning

**Goal.** Get prod scanning again, safely:
1. Migrate prod and deploy the release.
2. Repair prod recipes from a home IP.
3. Refresh the exposed VIN owners.
4. Smoke-test 20 dealers.
5. Run one supervised full fleet with ownership snapshots.
6. Repair the residue of wrong retirements.
7. Enable the nightly cron and watch it for three nights.

**Entry gate.**
- Phase 6A released, and the P6A.7 check is green.
- D-IF4, D-RC5 and D-RC6 answered.
- The Railway cron is OFF, and the owner is available for the windows.
- All MBP and mini fleets are paused.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P6B.1 | infra-14 (deploy part), D-DB6 | Apply V027/V028 to prod; deploy the release to scanner-nightly (idle) and web | ops, needs prod | docs/SCANNING_OPS_LOG.md | S | A | Phase 6A |
| P6B.2 | infra-12 + recipes-13a + recipes-4 (prod run) + P6A.6 prod apply (+ reviewers) | Prod recipe repairs from a home IP | data_repair, needs prod | workspace/backups/, dealer discovery.md, _learning/errors_index.md | M | B | P6B.1 |
| P6B.3 | infra missing (home verification scan) | Supervised home-IP scan of the un-staled dealers (the 6 VIN owners first) | data_repair, needs prod | dealer scan_runs.md / summary.md | S | C | P6B.2 |
| P6B.4 | infra-14 (smoke) (+ reviewer) | 20-dealer Railway smoke plus the one-shard 403 measurement for the new walk length | ops, needs prod | docs/SCANNING_OPS_LOG.md, docs/RAILWAY_SCANNING.md | M | D | P6B.3 |
| P6B.5 | infra-15 (+ reviewer) | Supervised full fleet with ownership snapshots and gate review | data_repair, needs prod | workspace/backups/, docs/SCANNING_OPS_LOG.md | M | E | P6B.4 |
| P6B.6 | reconcile-11 (+ reviewer) | Prod retirement audit and residue repair | data_repair, needs prod | workspace/data_quality/, workspace/backups/, dealer logs | M | F | P6B.5 |
| P6B.7 | infra-16 (+ reviewer) | Enable the nightly cron; watch three nights | ops, needs prod | docs/SCANNING_OPS_LOG.md | S | G | P6B.6 |
| P6B.8 | infra-22 (+ reviewer) | Bring RAILWAY_SCANNING.md, EXECUTION_PLAN and the Railway configs in line with the facts | docs | docs/RAILWAY_SCANNING.md, docs/EXECUTION_PLAN_2026_09.md, deploy/railway/README.md, deploy/railway/railway.scanner-nightly.json, deploy/railway/deploy_scanner_nightly.sh | S | G | P6B.1 |

**P6B.1: Migrate and deploy.**
- Change:
  1. Prod backup with `pg_dump -Fc`, verified.
  2. `migrate --dry-run`, then `--apply --target 28` (owner).
  3. Deploy scanner-nightly with `deploy_scanner_nightly.sh`, from the tag. Confirm the first start prints idle (window closed, `SCAN_DEALERS` empty). Record the deployed commit, and whether the build log shows `railway.scanner-nightly.json` was honoured (for P6B.8).
  4. Deploy web with `deploy_web.sh`.
  5. The mini pulls the release commit.
- Accept: prod records V028; the scanner-nightly commit equals the tag; the idle line appears in its logs; web `/api/health` shows the release.

**P6B.2: Prod recipe repairs.**
- Change:
  1. Backup: `pg_dump -Fc -t dealer_recipes`, with a `pg_dump` at least as new as the server's major version. Verify with `pg_restore -l`.
  2. Run P1C.3's `saved_at` backfill on prod: dry run, owner OK, apply. P1B.3's code is now on every writer host: the MBP, the mini and scanner-nightly.
  3. Un-stale, from a home IP, with:
     - `RECIPES_CACHE_DIR` pointing at an empty scratch dir; the P1B.6 store fingerprint also refuses mismatched push-ups;
     - `SCANNER_EGRESS_TAG=` exported EMPTY, not unset, because `.env` refills unset variables (`load_project_dotenv(override=False)`);
     - `INVENTORY_DATABASE_URL` set to the prod public URL.
     Run `python -m backend.scripts.unstale_host_blocked_recipes` in list mode and diff it against P0A.2 (31 in the 10-04 dump). Then `--apply`. Count three outcomes: un-staled, still blocked, and skipped (no active cars or URL; pass the URL from dealer_recipes or the registry, or list the dealer for manual handling).
  4. Apply the P6A.6 hygiene list on prod (owner-reviewed).
  5. Verify:
     - list mode shows only the still-blocked recipes;
     - active cars counts and per-dealer `max(scraped_at)` are unchanged (no cars writes);
     - in P6B.4's logs, the Railway volume cache adopts the DB copy for 5 sampled dealers.
  6. Logs: a dated discovery.md block per candidate ("un-staled from home IP" or "still 401/403 from home"); summary.md for errors. Still-blocked dealers go to needs_discovery. One errors_index line per new class.
  Rollback: restore the dealer_recipes rows from the dump.
- Accept: un-staled + still-blocked + skipped = candidates; the backup TOC lists dealer_recipes.

**P6B.3: Home verification scan.**
- Change: first, the owner approves the dealer list (the un-staled set from P6B.2). dealer_pipeline has no dry-run flag, so the pre-run review is that list plus a read-only per-dealer count of active rows, newest `scraped_at` and open vin_owner_conflicts pairs. Back up those dealers' prod rows, all VINs active and inactive, with `\copy (SELECT * FROM cars WHERE dealer_id IN (...))` into `workspace/backups/prod_home_verify_<stamp>.csv`, and record the per-dealer active counts. Then run `dealer_pipeline --dealers <un-staled dealers>` against prod from a home IP, on one shard lock. Run it under `PROJECT_DOTENV_DISABLE=1` with explicit `INVENTORY_DATABASE_URL=<prod>`, `RECIPES_CACHE_DIR=<prod-lane cache>` and an empty `SCANNER_EGRESS_TAG`, so `.env` cannot redirect it. The full process applies: vPIC heal, verify, dealer logs. This refreshes `scraped_at` for the 6 VIN owners (downeyhyundai, duvalford, easyhonda, hondaofelcajon, mymetrohonda, overdrive-usa), which the 48 h guard does not protect today, and provides the "ok from home" evidence for D-IF1.
- Accept: the backup exists before the run; each dealer has scan_runs.md and summary.md blocks; the 6 owners' `scraped_at` is within 24 h; per-dealer active counts before and after are recorded, and any retirement is explained by the P5A policy plan in summary_json. Rollback: restore the dealers' rows from the CSV.

**P6B.4: 20-dealer smoke.**
- Change:
  - Set `SCAN_DEALERS` to the 20-dealer list in RAILWAY_SCANNING.md:175-196, with `SCANNER_VIN_OWNER_GUARD_HOURS` set explicitly from P6A.3's formula and recorded. Compare the results with that table.
  - lambonb-com should get `blocked:railway` with live recipes, or be skipped as no_recipe without new stale writes.
  - Record the 403 rate per host for the longer walks (P5B.2) in RAILWAY_SCANNING.md.
  - Then set `SCAN_DEALERS` empty (idle), and pull and merge the logs (P6A.5).
- Accept: at least 19 of 20 dealers scanned, with rows within 5% of the 09-28 Railway column; no dealer newly staled with http_401/403 while the egress tag is set; the dealer logs are merged; the ops log block is written.

**P6B.5: Supervised full fleet.**
- Change:
  1. Pre-snapshot, read-only and in the background: `\copy (SELECT vin, dealer_id, dealer_name, dealer_url, dealership_registry_id, zip_code, listing_active, scraped_at FROM cars)` for ALL VINs into `workspace/backups/prod_owner_pre_<stamp>.csv`. Inactive rows are included, so a VIN a claimant reactivates can be restored.
  2. Run `SCAN_FLEET=1` with `SCAN_FLEET_WINDOW_UTC` opened around now. Confirm the summary shows the extended `vin_guard_hours`. Monitor in the background and report anomalies only.
  3. Take the same snapshot afterwards. Diff the VINs whose dealer_id changed, grouped by (old, new) pair, and cross-check them against the vin_owner_conflicts pairs.
  4. Gate review against D-IF4: wall time under 3 h; peak memory (14 GB alarm); 403/429 by host; verdict counts vs 09-29; conflicts delta; the Railway-blocked list (ad hoc SQL on `scan_hints`; lanes arrive in P15A).
  5. If any reassignment comes from a known claimant pair: dry-run count; then in ONE transaction, `CREATE TABLE cars_owner_backup_<stamp>`, `UPDATE` from the pre-snapshot, and check "still mismatched = 0" before `COMMIT` (the 09-29 pattern).
  6. Pull and merge the dealer logs.
- Accept: both snapshots exist; the reassignment diff is reported per pair; any repair leaves 0 mismatches and a backup table; the gate table is in the ops log; no recipes were newly staled by Railway 403s; exit 0 with all shards rc=0, or the failures are explained.

**P6B.6: Prod retirement repair.**
- Change:
  - Precondition: every scanner host is on the release commit (P6A.7).
  1. From a home IP, run `audit_retirements` read-only against prod `--since 2026-09-23`. Prod inherited local's 09-28 state plus the Railway 09-29 run. Share the report.
  2. The P6B.5 fleet already rescanned under the fixed policy, which re-lists live cars, so re-run the audit for the residue.
  3. Backup: a narrow `CREATE TABLE AS` on prod plus the tool's JSON. A full `pg_dump` needs a matching client version (Docker postgres:18 if the local client is older); ask the owner first.
  4. `--verify` from the home IP (Railway egress would turn everything into cannot_assess). The owner reviews the candidates. `--apply` the residue.
  5. Verify as in P5B.8.
  6. Rebuild the listings grid cards with `FLASK_ENV=production`, and spot-check 3 restored VINs on sarraficars.com.
  7. Write dealer logs. cannot_assess dealers go to needs_discovery.
  Exclude the lexusofknoxville-com 09-27 batch. If P0B.2 judged autosavvy-com a group misfile, fix its recipe scope through the dealer-discovery workflow, not in this repair.
- Accept: the owner signed off on the candidate list before `--apply`; the backup row count matches the candidates; restored = verified-live; the grid cards are rebuilt and the spot-checked VINs show.

**P6B.7: Enable the cron.**
- Change:
  - Set `SCAN_FLEET_WINDOW_UTC` (e.g. 08:50-09:40), then `SCAN_FLEET=1`. The redeploy those trigger must print "outside run window".
  - Set `cronSchedule '0 9 * * *'`: in railway.scanner-nightly.json if P6B.1 showed the file is honoured, otherwise via `serviceInstanceUpdate` (RAILWAY_SCANNING.md:79). Turn on Railway failure notifications.
  - Runbook: no variable changes or redeploys between 08:30 and 12:30 UTC.
  - For the first three nights, read the FLEET SUMMARY, fleet_summary.json (`vin_guard_hours`, alarms), the conflicts delta, the 403 table and the retirement audit. Night 1 repeats the ownership snapshot diff. Pull and merge logs every night.
  - Rollback: set `cronSchedule` to null and `SCAN_FLEET` empty.
- Accept: three consecutive nights exit 0 with wall time under 3 h and peak memory under 14 GB; no unexpected reassignment pairs on night 1; the logs are merged.

**P6B.8: Docs reconcile.**
- Change, in RAILWAY_SCANNING.md and EXECUTION_PLAN_2026_09.md:
  - The 6,961-VIN repair is history (2026-09-29 14:53Z, `cars_owner_backup_20260929`).
  - The idle-in-transaction timeout is already 5 min; drop it from the prerequisites (:437).
  - Add the conflict breakdown (top pairs; 146 claimants).
  - The 09-29 peak was 12.7 GB, over the doc's 8 GB gate; state the D-IF4 threshold.
  - Replace "Why not flip the cron" with the gated phase list. Document the window, fleet lock, guard extension and kill switches, and that redeploys with `SCAN_FLEET=1` are idle only outside the window.
  - EXECUTION_PLAN step 2 follows D-IF1 (a home-lane exception if chosen; implemented in P15A).
  - Record the fate of railway.scanner-nightly.json, and adjust deploy_scanner_nightly.sh:20-21 if the file is dropped.
- Accept: no instruction contradicts the code or the P0A facts.

**Workflow shape.** Serial and owner-gated: P6B.1 → P6B.2 → P6B.3 → P6B.4 → P6B.5 → P6B.6 → P6B.7 (three nights). P6B.8 can be drafted in a worktree in parallel and merged last. Review lens: a backup before every write; dry runs reviewed; dealer logs merged into the canonical tree; nothing but the fleet runs inside the window.

**Data repair.** P6B.2, P6B.3, P6B.5 and P6B.6, each with backup, dry run, owner OK, apply and verify, as described.

**Exit gate.**
- Prod is at V028 and its recipes are repaired.
- The cron has been live for 3 nights within D-IF4.
- The retirement-audit residue is resolved.
- RAILWAY_SCANNING.md and EXECUTION_PLAN are updated, and the dealer logs are merged.

**Release step.** Patch for the docs (P6B.8). The code shipped in 6A.

## Phase 7: Recipe store authority

**Goal.** Make the DB row the authority for each dealer's recipes:
- a row version with compare-and-swap;
- a recipe-only write stamp (`recipes_written_at`), because every hint write bumps `updated_at`;
- key-scoped mutations, with an offline op-log tied to the store's identity;
- re-synthesis that archives replaced recipes outside `recipes_json`;
- "blocked" status and the daily lifecycle guard tracked per host.

The file stays an atomically refreshed cache. Rollout is coordinated across the MBP, the mini and Railway.

**Entry gate.** Phase 6B exit (scanning live, so the rollout needs a paused window). The P1C.4 `_reconciled` markers exist on every host. D-RS1 (P7.9), D-RS2 (P7.5), D-RS3 (P7.1-P7.4) and D-RS4 (P7.6) answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P7.1 | recipes-5 + recipes missing (schema; Postgres CAS test) (+ reviewer) | V029 (rev, recipes_written_at, superseded_json); CAS primitives; `mutate_scan_hints` | code | migrations/V029__dealer_recipes_rev.sql (new), backend/scanner/recipe_store.py, backend/tests/test_recipe_store_cas.py (new), test_recipe_store_cas_pg.py (new, opt-in) | M | A | none |
| P7.2 | recipes-6a (+ reviewer) | Store-authoritative load; refresh only on change; guard for unreconciled caches | code | backend/scanner/recipes.py, backend/scanner/recipe_cache.py (new), backend/tests/test_recipe_store_sync.py (new) | M | B | P7.1 |
| P7.3 | recipes-6b (+ reviewer) | Key-scoped mutation API with an offline op-log | code | backend/scanner/recipes.py, backend/scanner/recipe_cache.py, backend/tests/test_recipe_store_sync.py | M | C | P7.2 |
| P7.4 | recipes-7 (+ reviewer) | Replay and promotion use the mutation API | code | backend/scanner/recipes.py, backend/tests/test_recipes.py, test_recipe_lifecycle.py | M | D | P7.3 |
| P7.5 | recipes-8 (+ reviewer) | Replace-with-archive into `superseded_json` | code | backend/scanner/pipeline/recipes.py, backend/scanner/recipes.py, backend/scripts/{cascade_recipes,synthesize_recipes,unstale_host_blocked_recipes}.py, backend/tests/test_recipe_validation.py, test_recipe_lifecycle.py | M | E | P7.4 |
| P7.6 | recipes-9 (+ reviewer) | Per-host blocked status and per-host lifecycle guard | code | backend/scanner/recipes.py, backend/scanner/pipeline/lifecycle.py, backend/scanner/recipe_validation.py, backend/scripts/unstale_host_blocked_recipes.py, backend/tests/test_egress_tag_blocked.py, test_recipe_lifecycle.py | M | F | P7.5 |
| P7.7 | recipes missing (backcompat, rollback) | Backward-compatibility test and rollback runbook | test + docs | backend/tests/test_recipe_store_backcompat.py (new), docs/data_architecture_plan.md | S | G | P7.6 |
| P7.8 | recipes-14b | Docs for the authority model; CHANGELOG | docs | backend/scanner/recipe_store.py (docstring), docs/data_architecture_plan.md, docs/RAILWAY_SCANNING.md, docs/NETWORK_SCAN_PROCESS.md, CHANGELOG.md | S | H | P7.6, P7.7 (both edit data_architecture_plan.md) |
| P7.9 | recipes-13b | DB-to-DB reconcile, local vs prod, plus the Railway volume cache (MAIN, owner) | data_repair, needs prod | workspace/backups/, dealer discovery.md | M | I | release |

**P7.1: Schema and CAS.**
- Change:
  - V029: `ALTER TABLE dealer_recipes ADD COLUMN IF NOT EXISTS rev BIGINT NOT NULL DEFAULT 0`, plus `recipes_written_at` (epoch seconds, bumped only by recipe writes) and `superseded_json TEXT NOT NULL DEFAULT '[]'`. Use `SET LOCAL lock_timeout='5s'`, outside scan windows (manifest.py reads the table).
  - `recipe_store._ensure_table`: on Postgres, check `information_schema` and never ALTER (P4.6 already stopped the scan_hints ALTER). On SQLite, keep the ALTER ladder. The ensure becomes per database (keyed by DSN or path) instead of the process-global `_table_ready`.
  - `db_read(dealer_id) -> StoreRow(rows, rev, hints, updated_at, max_saved_at, recipes_written_at)` or None, with the alias fallback. A row is authoritative only when `recipes_written_at` is set. Hints-only rows (`'[]'`, 93 locally) are not.
  - `db_mutate(dealer_id, fn, *, create=True, retries=8)`:
    - no row: `INSERT ... ON CONFLICT (dealer_id) DO NOTHING`, requiring rowcount == 1, otherwise retry as an update;
    - row exists: `UPDATE ... SET recipes_json=?, rev=rev+1, recipes_written_at=?, recipe_count=?, provider_hint=?, max_saved_at=?, last_ok_at=?, stale_count=?, updated_at=? WHERE dealer_id=? AND rev=?`, requiring rowcount == 1, otherwise re-read and retry;
    - returns None after the retries are used up.
  - `mutate_scan_hints(dealer_id, fn)` runs on the same CAS and supports nested merges (P7.6 needs them). `set_scan_hints` is rebuilt on it with unchanged semantics; hint writes bump `rev` but not `recipes_written_at`.
  - `db_save_recipes` and `db_load_recipes` stay as thin wrappers.
  - Failures log at WARNING, rate-limited, and are counted by `store_stats()`.
- Tests:
  - `adapt_sql_for_postgres_execute` returns non-None for both CAS statements, with `%s` placeholders and the ON CONFLICT intact.
  - 8 threads × 50 increments leave exactly 400 and rev=400 (SQLite).
  - First-insert race; concurrent hint keys both survive; a rowcount of 0 is never success; a second SQLite DB gets the columns; the static migration test.
  - Opt-in Postgres test (`TEST_PG_DSN`, scratch table on local PG): the 8×50 increment, which exercises READ COMMITTED re-evaluation and rowcount through `InventoryCursor.__getattr__` (inventory_compat.py:59-61). Run it once and put the output in the PR.
- Accept: the tests pass; test_schema_from_migrations.py passes.

**P7.2: Authoritative load.**
- Change: new `recipe_cache.py` takes over the atomic write from P1B.2 and adds `fcntl.flock` on `<slug>.json.lock`.
  - When the store is enabled, `db_read` succeeds and the row is authoritative, `load_recipes` returns the DB rows and rewrites the cache atomically only when the content differs.
  - With no authoritative row, the alias fallback still works. Today a hints-only row under the current slug hides the old slug's recipes, because `_db_load_recipes_exact` returns `([], 0.0)` for `'[]'` (recipe_store.py:210-216); fix that.
  - With no recipes row but a cache file, seed the DB through `db_mutate(create)`.
  - Until `<cache>/_reconciled` exists for the current store fingerprint, a differing cache is backed up to `workspace/backups` before it is overwritten.
  - When the DB is unreachable, return the file rows (today's behaviour).
  - Keep maintaining `max_saved_at` for old readers.
- Tests: a corrupt or truncated cache yields the DB rows and the cache is rewritten; a hints-only row does not hide alias recipes; a differing cache without the marker is backed up; with `RECIPES_DB_DISABLED=1` the alias tests pass unchanged.
- Accept: the tests pass.

**P7.3: Mutation API.**
- Change:
  - `mark_stale(dealer_id, recipe, reason, *, observed_at=None)` is key-scoped, does nothing when the key is gone, and is skipped when that recipe's `last_ok_at` is newer than `observed_at`.
  - `record_replay_ok(dealer_id, key, *, last_ok_at, field_coverage, unstale, total_count=None)`.
  - `merge_recipes(dealer_id, candidates, *, max_recipes=8)` takes over promote's merge, rank and cap rules (:440-460).
  - `replace_recipes` is a stub until P7.5.
  - Every op stamps the recipe's `mutated_at` AND `saved_at`. saved_at stays a per-write stamp because mixed-version hosts still compare it. Recipes-6's "saved_at untouched" is dropped (Appendix B).
  - When `db_mutate` returns None (store unreachable), append an op-log entry `(op, key, observed_at, payload, base_rev)` under `workspace/recipes/_pending/<store_fingerprint>/<slug>`, and apply the op to the cache under flock.
  - On the next load with the store reachable, replay the op-log through `db_mutate`; never merge a snapshot. Never re-add a key that is absent from a DB whose rev is above `base_rev`, and never un-supersede.
  - `save_recipes` stays an explicit replace for scripts and tests, routed through `db_mutate`.
- Tests:
  - A marks a recipe stale and B sees it.
  - Concurrent mutations on keys X and Y both persist.
  - Replaced keys are not brought back by an older cache.
  - Offline mutation goes to the op-log; once the DB is back it is replayed and the log is removed.
  - A stale mark older than another host's success is ignored.
  - An op-log recorded against another store is never replayed here.
- Accept: the tests pass. flock is verified on the Railway volume during the rollout.

**P7.4: Replay and promotion on the API.**
- Change: in `try_fetch_via_recipes`, a 401/403 before any VIN calls `mark_stale(..., observed_at=<timestamp before page 1>)` via `to_thread`, and so does the unreplayable-template path. A success calls `record_replay_ok` via `to_thread`, persisting the learned `total_count` for the same recipe key only. `promote_from_ledger` validates on a snapshot, then applies through `merge_recipes` inside `db_mutate`.
- Tests: a recipe another host adds during promote's validation survives; a stale mark from a replay that started before another host's success is ignored; the learned total is persisted; a smaller section total never shortens another key's walk.
- Accept: the tests pass; grep finds no `load_recipes(...)` followed by `save_recipes(...)` pair in recipes.py.

**P7.5: Replace with archive.**
- Change:
  - `replace_recipes(dealer_id, recipes, *, reason)` makes the new set live and moves every replaced recipe into `superseded_json` as `{key, recipe, superseded_at, reason}`, keyed by `(key, superseded_at)`. It never goes into `recipes_json`, for three reasons:
    - a re-synthesized recipe usually has the same `key()` (recipes.py:110-117), so the live and archived copies would collide;
    - old code would replay and un-stale archived entries (recipes.py:1008, :1139-1143);
    - the old promote cap would drop them.
  - Archived entries are pruned after `SUPERSEDED_KEEP_DAYS=30` and restorable with an operator command.
  - `ensure_recipe` (:89) uses reason `synth:force`, `synth:cc_section_scoped` or `synth:cosmos_single_section`. A non-force re-synthesis supersedes only the trigger class.
  - cascade_recipes.py:102 and `synthesize_recipes._upsert_recipe` (:100-109) use `merge_recipes`.
  - Consumers that read only `recipes_json` never see archived entries: synth/common.py:31-34, platform_candidates.py:134-139, vdp/prefetch.py:727, `_derive_meta`, and the unstale script.
  - URLs logged to discovery.md drop their query strings, which can carry API keys (recipes.py:16-17).
- Tests:
  - A forced re-synthesis over a captured recipe with the same key: the new set is live and the captured recipe sits in `superseded_json`.
  - A superseded recipe is never replayed (the stub counter stays 0 over 2 scans).
  - Restore brings it back.
  - A non-force section-scoped re-synthesis supersedes only the scoped recipes.
  - cascade keeps the existing recipes.
  - A simulated old-code loader never sees archived entries.
  - discovery.md has no query strings.
  - `test_ensure_recipe_refuses_a_rejected_synth` passes.
- Accept: the tests pass.

**P7.6: Per-host status.**
- Change:
  - A tagged host writes `scan_hints.recipe_blocked = {<tag>: 'blocked:<tag>:<status>:<iso>'}` through `mutate_scan_hints` and leaves `recipe_status` alone. That covers `record_stale_status` (:334-348) and the tagged-host auth reject in recipe_validation.
  - `clear_stale_status` removes only this host's tag entry, and keeps the stale:/rejected: → ok rule for `recipe_status`.
  - `route_verdict` (lifecycle.py:73-115) reads `recipe_blocked[egress_tag()]` and still honours a legacy `blocked:<tag>:` value.
  - `lifecycle_last_attempt` is keyed by host class: `lifecycle_last_attempt@<tag>` on tagged hosts.
  - The unstale script reads both the legacy and the new fields.
- Tests: a home success keeps Railway's block; a Railway block does not overwrite a home `rejected:` status; a Railway lifecycle attempt does not block the home host the same UTC day; the legacy value is honoured.
- Accept: the tests pass.

**P7.7: Backcompat and rollback.**
- Change: a test that loads rows written by this phase's code through a simulated Phase-0 reader (`recipes_json` plus `max_saved_at` only), proving `max_saved_at` is maintained, the `recipes_json` schema is unchanged and archived entries are invisible. A "Rollback" section in data_architecture_plan.md:
  1. Stop fleets.
  2. Revert to the previous tag.
  3. Keep V029 (it is additive).
  4. Replay or archive the `_pending` dirs before deleting them.
- Accept: the test passes and the runbook is present.

**P7.8: Docs.**
- Change: the recipe_store module docstring and data_architecture_plan.md describe the authority model: rev CAS, `recipes_written_at`, cache semantics, the op-log, archived entries, per-host status and the reconcile tool. NETWORK_SCAN_PROCESS.md:61 keeps the per-dealer recipe file as a deliverable, now an atomically refreshed cache. RAILWAY_SCANNING.md describes the volume cache. Add a CHANGELOG entry.
- Accept: no doc claims the old file-vs-DB freshness rule.

**P7.9: DB-to-DB reconcile.**
- Change (after the rollout):
  1. Tar the Railway volume's `workspace/recipes` (railway ssh).
  2. Run `reconcile_recipe_store --source-dsn <local> --target-dsn <prod>` and the reverse direction as D-RS1(b) needs: dry run, owner review, then apply through `db_mutate`.
  3. Check that the volume cache matches prod for 10 sampled dealers.
  4. Write dealer logs for every dealer whose state changed.
- Accept: a second dry run reports 0 differences.

**Workflow shape.** Strictly serial on recipe_store.py and recipes.py: waves A to F, then G (P7.7), then H (P7.8; it shares data_architecture_plan.md with P7.7), then I (P7.9, MAIN, after the release). Review lens:
- lost-update proofs: the SQLite threads test plus one Postgres run;
- old code reads new rows correctly;
- no auth headers or query strings written to logs;
- saved_at never regresses.

**Data repair.** P7.9.

**Exit gate.**
- pytest, chunked: test_recipe_store_cas.py, test_recipe_store_sync.py, test_recipes.py, test_recipe_lifecycle.py, test_recipe_validation.py, test_egress_tag_blocked.py, test_html_cards_20260926.py, test_recipe_store_backcompat.py, test_schema_from_migrations.py.
- The Postgres CAS output is attached; prod records V029.
- 5 dealers load identically on the MBP, the mini and Railway.
- P7.9 reports 0 differences.

**Release step.** Minor (schema).
1. Close the window, so no fleet runs that night.
2. Apply V029 to prod (backup, dry run, `--target 29`).
3. Deploy the same version to scanner-nightly and web, and pull it on the MBP and the mini.
4. Verify flock on the volume with a one-dealer `SCAN_DEALERS` run.
5. Reopen the window.

## Phase 8A: Vehicle-fact precedence foundation

**Goal.** Build one write-time arbiter for vehicle-fact columns:
- precedence ranked per field and source;
- the stale-provenance rule, so old labels cannot freeze junk values;
- legacy provenance labels mapped to "unknown";
- a single place where disagreements are recorded.

Also: a shared apply helper; route the vPIC writer through the arbiter; give decode refresh a dry run that is really dry; align read-time drivetrain. Then heal the 1,409 PHEV labels left after the F09 fix, fuel only and locally.

**Entry gate.** Phase 7 released. D-PR1, D-PR4, D-PR6 and D-PR7 answered. For P8A.7, fleets paused and no scanner running on any host.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P8A.1 | precedence-1 (+ reviewer) | Read-only precedence invariants | code | backend/scripts/data_quality_invariants.py, backend/scripts/data_quality_invariants_baseline.json (+ history), backend/tests/test_data_quality_invariants.py | M | A | none |
| P8A.2 | precedence-2 (+ reviewers) | `backend/vehicle_facts/precedence.py` (pure) | code | backend/vehicle_facts/precedence.py (new), backend/vehicle_facts/__init__.py, backend/tests/test_vehicle_facts_precedence.py (new) | M | A | none |
| P8A.3 | precedence missing (shared apply helper) | `apply_fact_proposals`: the one DB-aware apply path | code | backend/vehicle_facts/apply.py (new), backend/tests/test_vehicle_facts_apply.py (new) | M | B | P8A.2 |
| P8A.4 | precedence-3 (+ reviewer) | vpic_facts through precedence; provenance with value; chunked heal; `fields=` filter | code | backend/enrichment/vpic_facts.py, backend/tests/test_vpic_facts.py, test_vpic_override_cylinders.py, test_vpic_heal_derived_fields_f06.py, test_vpic_facts_provenance.py (new) | M | C | P8A.3 |
| P8A.5 | precedence missing (decode refresh) + precedence-16 fix | Decode refresh; `heal_from_vpic --dry-run` writes nothing | code | backend/enrichment/vpic_facts.py, backend/scripts/heal_from_vpic.py, backend/tests/test_vpic_decode_refresh.py (new) | S | D | P8A.4 |
| P8A.6 | precedence-11 (+ reviewer) | Read-time drivetrain agrees with the write-time rule | code | backend/enrichment/verified_specs/drivetrain.py, backend/tests/test_verified_specs_vpic_precedence.py (new), test_merge_verified_specs_golden.py | S | A | none |
| P8A.7 | precedence-15a | Local fuel-only PHEV heal (MAIN) | data_repair | workspace/backups/, dealer summary.md | S | E | P8A.4, P8A.5 |

**P8A.1: Invariants.**
- Change: stored-tier `SqlInvariant`s (Postgres, READ ONLY, pre-filtered so each finishes in 20 s; an unfiltered JSON join timed out at 60 s), each with count_sql, example_sql (LIMIT 20) and denominator_sql:
  - (a) vpic_electrification_fuel_conflict;
  - (b) vpic_drive_wheels_conflict;
  - (c) vpic_cylinders_conflict, split by engine-text agreement;
  - (d) provenance_stale_drivetrain;
  - (e) sticker_vs_vpic_conflict;
  - (f) engine_text_vs_vpic_displacement (|Δ| ≥ 0.15).
  (g) catalog_tiebreak_over_explicit_end is informational: either add `informational: bool` to SqlInvariant with `compare()` support and a test (it never fails the gate), or move it into a separate report, since the nightly gate fails closed on any increase. Regexes use Postgres word boundaries (`\y`, `\m`, `\M`); `\b` is backspace in Postgres.
- Baselines: add entries for the new ids by a reviewed hand edit of the baseline JSON plus a history line, or a new `--add-baseline <ids>` flag. Never run the CLI write_baseline, which rewrites every invariant. Lowering baselines after a repair belongs to P8A.7 and P8C.6.
- Tests: test_data_quality_invariants.py asserts every `SQL_INVARIANTS` id has a baseline entry.
- Accept: `--sql-only` on local PG lists the 7 ids with counts and examples and writes nothing; each finishes under `PGOPTIONS='-c statement_timeout=20000'`; the PR records the expected about 1,409 for (a), about 73 for (d) and about 139 for (g).

**P8A.2: precedence.py.**
- Change: a pure module with no DB and no network.
- Ranking: per (field, source), not a global rank.
  - vPIC wins only on driven wheels, electrification and diesel, and cylinders (cylinders only when they do not contradict the engine text, D-PR7).
  - vPIC-synthesized engine text, transmission and body values are fill-only and lose to dealer text (34,689 active engine_description rows are tagged nhtsa_vpic locally).
- Sources: nhtsa_vpic, oem_window_sticker, oem_window_sticker_unverified (fill-only), dealer_feed, dealer_vdp, dealer_normalized, dealer_text_heuristic, html_jsonld, recipe_heal, epa_catalog (linked epa_master, epa_dictionary, epa_catalog_tiebreak), web_search (below the catalog), model_specs, trim_heuristic, llm (never writes fact columns, D-PR8), unknown.
- Legacy labels: `inventory_repair` and `listing_gap_fill` map to `unknown`. Both are mixed sources: locally, inventory_repair tags 60,818 cylinders, 38,695 drivetrains, 26,396 engine texts and 26,180 transmissions, mixing dealer-cleaned values, dealer-text heuristics and EPA; gap_fill includes DuckDuckGo snippets. `nhtsa_vpic_heal` and `ev_cylinders_heal` map to `nhtsa_vpic`.
- Stale-provenance rule: a stored entry's rank counts only when its `value` normalizes to the current column value; otherwise the source is treated as `unknown`. This covers the 73 feed-reverted tiebreak rows and F10's 141 'Other' values under nhtsa_vpic. Every new entry carries `value` and `fetched_at`.
- Field rules:
  - Drivetrain: AWD ≡ 4WD (keep the dealer's wording). vPIC 4x2 is a constraint {FWD, RWD, 2WD}, never mapped to RWD. A less specific feed value ('2WD', '4x2', 'Other', blank) never replaces a stored specific end. Catalog tiebreak per D-PR4, with a 4MATIC/xDrive veto from the trim decoder.
  - Fuel: vPIC BEV, PHEV, strong HEV and Diesel override; mild HEV or silence changes nothing; a sticker 'Hybrid' never downgrades PHEV or BEV.
  - Cylinders: as above. EVs carry none. The catalog and model_specs never contradict the engine text.
  - Engine text and engine_l: dealer or sticker text outranks the catalog. The catalog fills blanks, or corrects only with corroboration (D-PR3, used in P8B.5).
  - Transmission: sticker > dealer > vPIC fill > catalog fill.
  - Body: fill-only.
  - Colors: sticker > dealer; the catalog never writes them.
  - `forced_induction` delegates to `forced_induction.resolve_car_forced_induction` once P10C.2 lands; until then it is fill-only.
- Discrepancy sink: `spec_source_json['<field>'].discrepancy = {source, value, seen_at}`, plus a `cannot_assess` flag (D-PR6).
- `decide()` runs together with the existing write-time guards: `cars_repo._guard_mild_hybrid_fuel_type` (:633, applied in update_car_row_partial :710-711), `sanitize_cylinder_count` (:712-717) and `normalize_fuel_type_for_storage`.
- API: `field_source`, `decide`, `plan_updates`, `provenance_patch`. It reuses `vehicle_facts.drivetrain`, `vehicle_facts.electrification` and `utils.engine_consistency`; no new normalizers.
- Tests, table-driven per rule, including:
  - a vPIC-synthesized engine_description is replaced by later dealer text;
  - a vPIC transmission fill loses to a dealer '8-Speed';
  - a tiebreak RWD survives a rescan that says '2WD';
  - a stale entry lets the feed win;
  - mild-hybrid normalization is identical through both paths;
  - idempotence;
  - the test_vpic_facts.py and test_phev_fuel_bucket_f09.py expectations restated as precedence cases;
  - no `backend.db` or `requests` import.
- Accept: the tests pass.

**P8A.3: Apply helper.**
- Change: `apply_fact_proposals(conn, row, proposals, source, *, vpic=None, dry_run=False)` reads the current row and its provenance (or takes them), calls `plan_updates`, writes through `update_car_row_partial` (which keeps the existing guards), and merges the provenance via `merge_spec_source_json` in the same transaction. It returns applied, kept and discrepancies. Every writer in 8B gathers proposals and calls this helper, so the rules are not re-implemented seven times.
- Tests, on SQLite: only accepted fields are written; the provenance carries `value`; discrepancies are recorded; dry run writes nothing.
- Accept: the tests pass.

**P8A.4: vPIC writer.**
- Change:
  - `vin_overrides`, `derived_fills` and the `heal_rows` catalog tiebreak delegate to precedence. Signatures are unchanged.
  - `override_vehicles` merges a per-field provenance entry (`nhtsa_vpic`, detail "scan-time cached decode", `previous`, `value`, `fetched_at`) into `v['spec_source_json']`, so the upsert stores it. Today the `_field_source` keys (vpic_facts.py:110) are read only by tests.
  - `heal_rows` commits every `_HEAL_FLAT_CHUNK` rows instead of once at the end (:318-319), and gains a `fields=` filter.
  - The tiebreak's boolean grouping (:338-339) is made explicit. This is cosmetic; the behaviour is intended per :334-337. It implements D-PR4.
  - `decode_missing_vins` counts real inserts, not DO NOTHING no-ops (:186-193).
  - The module docstring points at precedence.py.
- Tests: an override on a cached-PHEV vehicle stores `fuel_type.source == 'nhtsa_vpic'` through a SQLite upsert; a heal over 1,200 synthetic rows commits at least 3 times; `--dry-run` makes no writes; the tiebreak never fires on dealer AWD/4WD; the `fields=` filter; `stored` equals the real inserts.
- Accept: the tests pass, and the existing vPIC tests pass unchanged.

**P8A.5: Decode refresh.**
- Change: `decode_vins(conn, vins, refresh=False)` uses `ON CONFLICT (vin) DO UPDATE` when refreshing rows whose `fetched_at` is older than `--max-age-days`. In heal_from_vpic.py, `--decode` becomes a separate, declared write step, and `--dry-run` never writes the cache. Today `--decode --dry-run` inserts through `decode_missing_vins` (heal_from_vpic.py:38-47). Add `--refresh-decode --max-age-days N`.
- Tests: a dry run writes neither the cache nor `cars`; refresh updates only stale rows; the offline stub guard holds.
- Accept: the tests pass.

**P8A.6: Read-time drivetrain.**
- Change: `resolve_drivetrain` (verified_specs/drivetrain.py:8-35) lets a stated vPIC drivetrain override `drive_ver` whenever the driven wheels disagree, not only when the dealer column disagrees. 4x2 is never mapped to an end. cylinders.py changes only if D-PR7 departs from today's read-time rule. Use targeted `merge_verified_specs` fixtures instead of the full rendered-tier invariant run, which took 617 s at 73k rows and exceeds the 180 s per-call limit at 214k.
- Tests: stored 4WD + vPIC 4WD + EPA RWD → 4WD; blank + vPIC 4WD + EPA RWD → 4WD; dealer RWD + vPIC 4WD → 4WD; vPIC None → unchanged.
- Accept: the tests pass; the golden diff is limited to drivetrain on the vPIC-disagreement fixtures.

**P8A.7: Fuel-only PHEV heal (local).**
- Change (MAIN):
  - Preconditions: AC power. No scanner client in `pg_stat_activity`, `ssh mini pgrep -f scanner.py` finds nothing, and no lock files.
  1. Backup: `CREATE TABLE cars_precedence_backup_<date> AS SELECT id, vin, fuel_type, spec_source_json FROM cars WHERE COALESCE(listing_active,1)=1`; its count must equal the active count.
  2. Record the invariant counts.
  3. `heal_from_vpic.py --all --fields fuel_type --dry-run`; expect about 1,409 PHEV upgrades and 1 BEV.
  4. Owner OK, then apply (chunked commits).
  5. Invariant (a) is about 0. Lower its baseline with a dated history line.
  6. Rollback is guarded: `UPDATE ... FROM backup WHERE current value = the value the heal wrote` (taken from the provenance `value`), so later rescans are never clobbered.
  7. Group the changed rows by dealer and append a line to each dealer's summary.md, plus one `_learning` line. Spot-check 10 VINs against vPIC.
- Accept: the backup exists before any write; the dry-run summary is in the PR; invariant (a) is about 0.

**Workflow shape.** Wave A: P8A.1, P8A.2, P8A.6. Wave B: P8A.3. Wave C: P8A.4. Wave D: P8A.5. Wave E: P8A.7 (MAIN). Review lens:
- no rule lets a lower source overwrite a higher one;
- stale provenance is never trusted;
- invariant SQL is pre-filtered and uses Postgres regex syntax;
- golden diffs are explained.

**Data repair.** P8A.7: backup, dry run, OK, apply, verify, guarded rollback.

**Exit gate.**
- pytest, chunked: test_data_quality_invariants.py, test_vehicle_facts_precedence.py, test_vehicle_facts_apply.py, test_vpic_facts.py, test_vpic_facts_provenance.py, test_vpic_heal_derived_fields_f06.py, test_vpic_override_cylinders.py, test_vpic_decode_refresh.py, test_verified_specs_vpic_precedence.py, test_merge_verified_specs_golden.py, test_phev_fuel_bucket_f09.py.
- A local `--sql-only` invariants run.
- P8A.7 verified.

**Release step.** Minor (the vPIC write semantics change). Bump, push, CI, fast-forward, tag. Deploy scanner-nightly outside the window, and web.

## Phase 8B: Every fact writer goes through precedence

**Goal.** Route every remaining writer of drivetrain, fuel, cylinders, engine text, transmission and colors through the arbiter and the apply helper:
- the scan upsert;
- the window sticker;
- the EPA dictionary;
- the structured backfill;
- storage repair;
- model_specs;
- InventoryEnricher;
- gap_fill.

Fix the post-scan stage order (decode first, heal last) and decode inline on every scan path.

**Entry gate.** Phase 8A released. D-PR2 (P8B.1), D-PR3 (P8B.5), D-PR5 (P8B.4) and D-PR8 (P8B.8) answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P8B.1 | precedence-4 (+ reviewer) | The scan upsert stops reverting higher-ranked stored facts | code | backend/scanner/upsert/{guard,serialize,write}.py, backend/scanner/database.py (prefetch threading), backend/tests/test_upsert_fact_precedence.py (new), test_upsert_vehicles_golden.py | M | A | P8A.3 |
| P8B.2 | precedence-5 (+ reviewer) | Window-sticker column writes go through precedence (fetch, eligibility and parser untouched) | code | backend/enrichment/window_sticker_service.py, backend/tests/test_window_sticker_precedence.py (new) | S | A | P8A.3 |
| P8B.3 | precedence-9 (+ reviewer) | Post-scan: decode first, heal last; no fleet-wide model_specs call; per-batch pipeline heal | code | backend/scanner/orchestrator.py, backend/scanner/post_scan/job.py, backend/scanner/pipeline/run.py, backend/tests/test_post_scan_stage_order.py (new) | M | A | P8A.4 |
| P8B.4 | precedence-10 (+ reviewer) | Inline vPIC decode on every scan path, off the event loop | code | backend/enrichment/vpic_facts.py, backend/scanner/scan_efficiency.py, phases/dealer_run_steps/enrich.py, backend/tests/test_vpic_inline_decode.py (new), conftest.py (default off) | M | B | P8A.4 |
| P8B.5 | precedence-6 (+ reviewer) | EPA dictionary writer: corroboration-only engine corrections; no colors, no FI | code | backend/dictionary/enrich_from_dictionary.py, backend/scanner/post_scan/pipeline.py (dictionary stage), backend/tests/test_dictionary_enrich_precedence.py (new), test_epa_engine_resolve.py | M | B | P8B.3 |
| P8B.6 | precedence-7a (+ reviewer) | Structured backfill: VIN before catalog; honest tier-1 labels | code | backend/enrichment/spec_structured_backfill.py, backend/utils/inventory_repair.py (new `*_proposals` functions), backend/tests/test_spec_structured_backfill.py, test_inventory_repair.py | M | A | P8A.6 |
| P8B.7 | precedence-7b | Storage repair, repair_inventory_fields and backfill_specs go through precedence | code | backend/scanner/post_scan/pipeline.py (storage repair), backend/scripts/repair_inventory_fields.py, backend/scripts/backfill_specs.py, backend/tests/test_storage_repair_precedence.py (new) | M | C | P8B.5, P8B.6 |
| P8B.8 | precedence-8 + db-layer-13b (+ reviewer) | model_specs and InventoryEnricher at the lowest rank; Haiku opt-in, never into fact columns | code | backend/scanner/database.py (apply_model_specs_corrections, incl. IN-list chunking), backend/scripts/apply_model_specs.py (new, fleet backfill flag; or a flag on an existing script), backend/enrichment/service.py, backend/tests/test_model_specs_corrections_precedence.py (new), test_enrichment_service_precedence.py (new) | M | C | P8B.1, P8B.3, P8B.4 |
| P8B.9 | precedence missing (gap_fill) | gap_fill writes through precedence; DuckDuckGo values tagged web_search | code | backend/scanner/post_scan/gap_fill.py, backend/tests/test_gap_fill_precedence.py (new) | S | A | P8A.3 |

**P8B.1: Upsert.**
- Change:
  - `prefetch_existing` (guard.py:54-57) also selects the fact columns: drivetrain, fuel_type, cylinders, engine_description, engine_l, transmission, exterior_color, interior_color, forced_induction. Thread the map through database.py:388-401 and write.py:36.
  - In `prepare_row`, for each fact column, call `decide(field, stored row + provenance, feed value, 'dealer_feed')`. When the stored, non-stale source outranks the feed, bind NULL so `COALESCE(excluded.x, cars.x)` keeps the stored value, and add `feed_said` and `feed_seen_at` to that field's provenance. Rows without prior provenance behave as today.
  - Derived columns (forced_induction via `classify` at sql.py:207, transmission_type, interior_color_buckets) are computed from the BOUND, post-arbitration values.
  - `UPSERT_CARS_SQL` is unchanged, which keeps SQLite/Postgres parity and the `cars.col` right-hand-side rule for ON CONFLICT.
  - `data_quality_score` uses the un-nulled values, via a separate bound dict (PreparedRow.v is not mutated).
- Tests:
  - A heal stores 4WD from nhtsa_vpic; a rescan with feed 'RWD' keeps 4WD and records `feed_said`.
  - The same holds for PHEV vs a feed 'Hybrid', for cylinders, and for a sticker-sourced transmission.
  - A stored tiebreak 'RWD' plus a feed '2WD' keeps RWD.
  - Stale provenance lets the feed win.
  - A nulled engine_description does not change forced_induction.
  - A cursor spy shows no added per-row SELECT.
- Accept: the tests pass; test_upsert_keeps_vdp_only_fields, test_upsert_data_quality_score and test_upsert_vehicles_golden pass, with any golden change limited to provenance and explained.

**P8B.2: Sticker writes.**
- Change: only `_car_fields_from_parsed_sticker` (:395-450) and the two provenance merges (~:613-624, ~:1188-1199) change.
  - Proposals go through the apply helper: source `oem_window_sticker` for authoritative OEMs, `oem_window_sticker_unverified` (fill-only) for the rest.
  - Fields that come from the `known_oem_engine_from_car` title heuristics (window_sticker.py:1673-1700, merged at window_sticker_service.py:525-530) are tagged `trim_heuristic` (fill-only).
  - Read the decode UNCACHED (`vpic_facts._vpic_flat_rows` plus `vehicle_facts.vpic_electrification` on Results[0]). `lookup_vpic_from_cache` memoizes "no decode" for the life of the process (knowledge_engine.py:1331-1332), and this writer also runs inside web workers (routes/cars_pages.py:103,248; gap_fill.py:390; listing_packages_service.py:184).
  - The Ram eTorque case relies on the existing mild-hybrid guard in `update_car_row_partial`.
  - Fetch, eligibility and parser are untouched ("leave OEM sticker fetch alone").
- Tests:
  - A Grand Cherokee 4xe stored as PHEV with sticker 'Hybrid': fuel unchanged, discrepancy recorded.
  - Sticker '8-Speed Automatic' over feed 'Automatic' is written.
  - Sticker 'RWD' with vPIC 4x2 is written; with vPIC 4WD it is not.
  - Non-authoritative OEMs stay fill-only.
  - A test through the `cars_pages` entry point.
- Accept: the tests pass, and the diff touches no other function in the file.

**P8B.3: Stage order.**
- Change:
  - Split `post_scan_vpic` into `decode_missing_vins`, which runs first, and `heal_rows`, which runs last, in both orchestrator.py:125-200 and post_scan/job.py:150-205. The dictionary and model_specs stages then have a decode, and vPIC still writes last.
  - Delete the post-scan fleet-wide `apply_model_specs_corrections()` call (orchestrator.py:172, job.py:190) after a consumer grep; post_write already runs it per VIN (post_write.py:72-75). database.py:528-534 builds one unchunked IN list, which exceeds Postgres's 65,535 binds on a full fleet, and the exception is swallowed at orchestrator.py:175-176. P8B.8 owns that function, so it does the chunking by 900 and adds a script flag that keeps the fleet backfill reachable. This unit does not edit database.py, which P8B.1 edits in the same wave.
  - The pipeline calls `vpic_for_dealers(batch)` after each batch (run.py:99-138), and keeps the final call as a backstop.
  - If D-PR5=(b), move the vPIC step above the `if scan_only: return` at orchestrator.py:462.
- Tests: decode runs before the dictionary enrich and heal runs last, in both entry points; one call per batch plus the final one; the fleet-wide model_specs call is gone (the chunking test is P8B.8's).
- Accept: the tests pass; test_dealer_run_golden_20261001.py and the pipeline tests pass.

**P8B.4: Inline decode.**
- Change:
  - Decode uncached VINs inside `override_vehicles`, so the golden test's monkeypatch still covers it.
  - It runs via `asyncio.to_thread`, or as a separate awaited step before `apply_vin_facts`. `apply_vin_facts` is synchronous inside the async `run_dealer` (dealer_run.py:118), and the pipeline runs `--dealer-concurrency=len(batch)`, so a blocking decode would stall every dealer.
  - Knobs: `SCANNER_VIN_DECODE_INLINE` (default per D-PR5; conftest sets 0) and `SCANNER_VIN_DECODE_INLINE_BUDGET_S` (20).
  - Per-request timeout is `min(remaining budget, 15 s)` (today 90 s, vpic_facts.py:149). Skip the 0.6 s sleep once over budget (:184, :197).
  - Pop only the decoded VINs from `_VPIC_MEMO`, never `clear_vpic_lookup_cache()`.
  - Add counts to `run.result['vin_facts']` and the enrich.py:269-282 log line.
- Tests: with a stubbed fetch, uncached VINs are decoded once and overridden; budget exhaustion or an HTTP error fails open; no network; the golden is unchanged; a second dealer coroutine makes progress while a decode is in flight.
- Accept: the tests pass.

**P8B.5: Dictionary writer.**
- Change:
  - `enrich_car` proposes `(value, 'epa_dictionary')` through the apply helper, with the decode available (P8B.3).
  - The engine-correction branch (:447-480) applies only when vPIC corroborates (displacement within 0.15 L and the same cylinders) or when the dealer's own title or trim corroborates (D-PR3 d).
  - `--all` refills only fields whose current source ranks at or below epa_catalog. This replaces P1A.5's env guard.
  - Provenance `epa_dictionary` is written in the CLI UPDATE (:637-640) and in `run_dictionary_enrich_for_vins` (post_scan/pipeline.py:239-267, via update_car_row_partial). Fix that function's docstring: it reads the CSVs, and it does overwrite.
  - A drivetrain fill from the most-common-row fallback (`_best_row` :256-268) respects the vPIC two-wheel constraint.
  - EPA `exteriorColors` are no longer written (:106, :524-528).
  - `forced_induction` leaves FILLABLE_FIELDS: the CSV labels are about 51% wrong, and P10C.4 wires the resolver.
  - Update `test_enrich_car_corrects_conflicting_audi_engine_fields` with a corroborating fixture.
- Tests:
  - The A6 '3.0L' + 4 cylinders is corrected with a vPIC 2.0/4 stub or with title corroboration, and is not corrected otherwise (a discrepancy is recorded).
  - `fill_all` with cylinders from nhtsa_vpic proposes no change.
  - A blank drivetrain with vPIC 4x2 is never filled AWD/4WD.
  - No exterior_color from EPA.
  - `--dry-run` prints the count skipped by precedence.
- Accept: the tests pass.

**P8B.6: Structured backfill.**
- Change:
  - For vPIC-covered fields (drivetrain, fuel_type, cylinders, body_style, engine_description fill), evaluate the vPIC decode before the tier-1 values (spec_structured_backfill.py:239-290). Both tiers go through the apply helper.
  - Tier-1 sources: `epa_catalog` when catalog-derived; `dealer_normalized` or `dealer_text_heuristic` when they come from `clean_car_row_dict` or the spec_field_normalize heuristics (inventory_repair.py:48-56, :119; spec_field_normalize.py:349-383).
  - Add `collect_row_storage_proposals()` and `collect_merge_spec_storage_proposals()` beside the existing functions, whose signatures stay.
  - Replace the private `_vpic_cache_get/_put` (:181-208) only after grepping its consumers.
- Tests: blank drivetrain + vPIC 4WD + EPA RWD stores 4WD from nhtsa_vpic; vPIC 4x2 + EPA AWD gives no AWD fill; a tier-1 cylinder count never contradicts the engine text; the trim-corroboration guard is unchanged.
- Accept: the tests pass; `SPEC_STRUCTURED_VPIC_OVERWRITE_DEALER` is documented as "only where precedence allows".

**P8B.7: Storage repair callers.**
- Change: `run_storage_repair_for_vins` (post_scan/pipeline.py:514-556; default-on via SCANNER_POST_REPAIR; writes without provenance at :541-546), `repair_inventory_fields.py:181` and `backfill_specs.py:52` use the `*_proposals` functions and the apply helper, and write provenance.
- Tests: one per caller.
- Accept: every write records its source; the existing tests pass.

**P8B.8: model_specs and InventoryEnricher.**
- Change:
  - `apply_model_specs_corrections` (database.py:501-620) proposes source `model_specs` through the apply helper and stays fill-only.
    - A drivetrain fill respects the vPIC two-wheel constraint when a decode exists. When none exists, the fill proceeds, tagged `model_specs` (rank 10), so a later decode overrides it.
    - A cylinder fill never contradicts the engine text or vPIC.
    - Provenance is written per field.
    - The VIN IN list (database.py:528-534) is chunked by 900 (moved here from P8B.3). A script flag (e.g. `backend/scripts/apply_model_specs.py --all`, or a flag on an existing script after a consumer grep) keeps the fleet-wide backfill reachable now that post-scan no longer calls it.
  - InventoryEnricher:
    - `apply_catalog_best` proposals use `epa_catalog`.
    - The Haiku fallback (`_ask_haiku_specs` :1051) sits behind an opt-in env var, default off. Per D-PR8 its answers never reach the fact columns; they stay in packages and provenance.
    - `_do_batch_write` (:997-1004) merges provenance.
- Tests: a blank drivetrain with vPIC 4x2 and model_specs 'AWD' is not filled; a cylinder fill is skipped when the text says V6 and model_specs says 4; with the decode absent the fill is tagged model_specs; Haiku-only proposals produce no fact-column update; a 2,000-VIN call issues chunked IN lists of at most 900 binds.
- Accept: the tests pass.

**P8B.9: gap_fill.**
- Change: `run_listing_gap_fill_for_vins`, which runs after the heal (orchestrator.py:159-166, job.py:178-184), goes through the apply helper. DuckDuckGo snippet values (gap_fill.py:560-572) are tagged `web_search`, which ranks below the catalog. VDP-parsed values are tagged `dealer_vdp`. New writes no longer use the `listing_gap_fill` label (:587-592).
- Tests: a DuckDuckGo 'AWD' on a 4x2 VIN is rejected; a VDP value is accepted over the catalog.
- Accept: the tests pass.

**Workflow shape.** Wave A: P8B.1, P8B.2, P8B.3, P8B.6, P8B.9. Wave B: P8B.4, P8B.5. Wave C: P8B.7, P8B.8 (P8B.7 shares post_scan/pipeline.py with P8B.5; P8B.8 shares database.py with P8B.1). Review lens:
- every write records its source;
- no write bypasses the apply helper;
- the event loop is never blocked;
- the OEM sticker fetch is untouched;
- golden diffs are limited to provenance.

**Data repair.** None (8C).

**Exit gate.**
- pytest, chunked: test_upsert_fact_precedence.py, test_upsert_vehicles_golden.py, test_upsert_keeps_vdp_only_fields.py, test_upsert_data_quality_score.py, test_window_sticker_precedence.py, test_window_sticker_service.py, test_post_scan_stage_order.py, test_vpic_inline_decode.py, test_dealer_run_golden_20261001.py, test_dictionary_enrich_precedence.py, test_epa_engine_resolve.py, test_enrich_dictionary_heuristics.py, test_spec_structured_backfill.py, test_inventory_repair.py, test_storage_repair_precedence.py, test_model_specs_corrections_precedence.py, test_enrichment_service_precedence.py, test_gap_fill_precedence.py, test_llm_client_transport.py, test_llm_call_site_parity.py.
- One local `dealer_pipeline` run on 2 dealers, rescanned twice, with dealer logs written, shows no fact column changing between rescans.

**Release step.** Minor. Deploy scanner-nightly outside the window, and web.

## Phase 8C: Precedence close-out

**Goal.** Retire the duplicate and orphan spec writers without losing the only live re-decode capability. Prove no ping-pong over full cycles. Document the table. Relabel legacy provenance. Run the full local heal, then the prod heal and relink.

**Entry gate.** Phase 8B released. For P8C.7, the owner approves a prod window with the Railway window closed that night.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P8C.1 | precedence missing (spec_backfill, persist_enrichment) | Retire or route the remaining spec writers | deletion + code | backend/enrichment/spec_backfill.py, backend/enrichment/persist_enrichment.py, backend/dev/routes.py, related tests | S | A | none |
| P8C.2 | precedence-12 (+ reviewer) | heal_cylinders_from_vpic Phase A = decode refresh + heal_rows | code | backend/scripts/heal_cylinders_from_vpic.py, backend/scripts/heal_ev_cylinders.py (docstring) | S | A | P8A.5 |
| P8C.3 | precedence-13 (+ reviewer) | Cycle test with a snapshot after every step | test | backend/tests/test_fact_precedence_cycle.py (new) | M | A | none |
| P8C.4 | precedence-14 (+ reviewer) | VEHICLE_FACT_PRECEDENCE.md; scan checklist links | docs | docs/VEHICLE_FACT_PRECEDENCE.md (new), docs/NETWORK_SCAN_PROCESS.md, backend/tests/test_vehicle_facts_precedence.py | S | A | none |
| P8C.5 | precedence missing (legacy relabel) | Relabel legacy provenance (tool, plus the local run as MAIN) | code + data_repair | backend/scripts/relabel_spec_provenance.py (new), backend/tests/test_relabel_spec_provenance.py (new) | M | A | none |
| P8C.6 | precedence-15 | Full local heal, then catalog relink dry run (MAIN) | data_repair | workspace/backups/, dealer summary.md | S | B | P8C.1-P8C.5 |
| P8C.7 | precedence-16 (+ reviewer) | Prod heal, relabel and relink | data_repair, needs prod | workspace/backups/ | M | C | P8C.6, release |

**P8C.1: Orphan writers.** Grep consumers first. `persist_enrichment.py` has no non-test callers: delete it and its tests. `spec_backfill.py` (dev route dev/routes.py:1254-1265, with a web-search tier) goes through the apply helper with source `web_search`, or the dev route is retired with the owner's OK. Accept: the consumer grep is in the PR and no write path bypasses precedence.

**P8C.2: Duplicate healer.** Phase A becomes `decode_vins(refresh=True)` followed by `heal_rows(fields=['cylinders'])`. That keeps the only live re-decode capability (today Phase A posts to vPIC directly), which is the mitigation for a frozen wrong decode. Phase B stays behind P1A.5's guard until P10C.6. Update the docstring reference in heal_ev_cylinders.py. Accept: the consumer grep is recorded; `--dry-run` is read-only; cylinder healing remains reachable from this CLI and from heal_from_vpic.py.

**P8C.3: Cycle test.**
- Change: on SQLite, with a stubbed vPIC cache and stubbed EPA rows, run the cycle twice: upsert, heal, sticker write, rescan upsert, dictionary enrich, model_specs, gap_fill, heal. Snapshot after EVERY step. Fixtures: a tiebreak row; stale nhtsa_vpic provenance holding 'Other'; a 4xe sticker row; a DuckDuckGo gap-fill row; an inventory_repair-labelled row. Assert post_write ran, or stub it explicitly.
- Accept: the rescan upsert step changes no column whose stored source outranks the feed; cycles 1 and 2 end identical; every non-null fact column has a provenance entry; `feed_said` is recorded where the feed disagreed.

**P8C.4: Docs.** docs/VEHICLE_FACT_PRECEDENCE.md holds the per-field, per-source rank table, quotes each decision, and documents the stale-provenance rule and the discrepancy sink. The NETWORK_SCAN_PROCESS.md checklist (items 4-5) links it and names the invariant ids. A test asserts the doc lists every source id. The `_learning/errors_index.md` line ("feed reverted vPIC/sticker values on rescan → P8B.1") is a post-merge MAIN step, since workspace is untracked. Accept: the doc table matches the code (the test enforces it).

**P8C.5: Legacy relabel.**
- Change: a tool that relabels legacy `inventory_repair` and `listing_gap_fill` entries to `unknown`, deterministically, or re-derives the label where the detail allows. Dry run by default, with counts per label and field. `--apply` requires a backup: `CREATE TABLE AS` of `id, spec_source_json` for the affected rows. Run it locally (MAIN) after merge; the prod run is in P8C.7.
- Accept: the dry-run and applied counts match, and the backup exists before any write.

**P8C.6: Full local heal.**
- Change (MAIN), with the P8A.7 preconditions:
  0. Backup first: `CREATE TABLE cars_precedence_backup_<date>_full AS SELECT id, vin, drivetrain, fuel_type, cylinders, engine_description, engine_l, transmission, body_style, spec_source_json FROM cars WHERE COALESCE(listing_active,1)=1`; its count must equal the active count. The guarded rollback in step 4 reads this table. Add it to the P13B.4 cleanup list.
  1. `heal_from_vpic.py --all --dry-run` under the full rules: cylinders only where non-contradicting; the tiebreak with the veto; derived fills.
  2. Review; owner OK; apply with chunked commits.
  3. Lower the baselines with history lines.
  4. Guarded rollback, as in P8A.7.
  5. Per-dealer summary lines and one `_learning` line.
  6. `link_cars_to_catalog.py --dry-run` on AC power (the resolver scores on drivetrain and fuel). Record the delta and apply it if the owner agrees.
- Accept: the backup table exists with the active count before any write; invariants (a)-(f) are at or below the dry-run predictions; 10 VINs are spot-checked against vPIC.

**P8C.7: Prod repair.**
- Change, after the 8C code is deployed and the window is closed that night:
  1. Back up the affected columns on prod (`CREATE TABLE AS`).
  2. Run the invariants read-only.
  3. Step 4a: `heal_from_vpic.py --decode`, a declared write to the cache.
  4. Step 4b: `heal_from_vpic.py --all --dry-run`, which writes nothing.
  5. Review, apply, then re-run the invariants and lower the prod baseline.
  6. Run the P8C.5 relabel on prod.
  7. `link_cars_to_catalog --dry-run`; record the delta; apply with approval.
  8. Site health stays green throughout (chunked commits). Use the guarded rollback.
  9. Merge the per-dealer summary lines into the canonical tree. List the rows whose dealer engine text the EPA dictionary overwrote: provenance was never kept for them, so they cannot be restored from data, and the next rescan restores them.
- Accept: the backup is verified before writing; the before/after invariant counts are recorded; the relink delta is recorded and applied.

**Workflow shape.** Wave A: P8C.1-P8C.5. Wave B: P8C.6 (MAIN). Wave C: P8C.7 (owner, after the release). Review lens: no capability is lost (fresh decode is kept); rollbacks are guarded so they never clobber later rescans.

**Data repair.** P8C.5, P8C.6 and P8C.7, as described.

**Exit gate.**
- pytest: test_fact_precedence_cycle.py, test_vehicle_facts_precedence.py, test_relabel_spec_provenance.py, test_vpic_facts.py.
- The invariants are at their post-repair baselines on local and prod.
- The relink delta is recorded.

**Release step.** Patch for P8C.1-P8C.5, released before the P8C.7 prod repair.

## Phase 9: Dictionary Complete_Options lookups work in prod

**Goal.** Prod resolves a Complete_Options CSV for only 54 of the top 300 active (year, make, model) groups, covering 18,281 cars. The SQLite catalog that makes the sharded tree visible never ships, and the fallback globs non-recursively. Locally, with the catalog, it resolves 223 groups (109,240 cars). Replace the lookup with one normalized in-memory index used everywhere. Keep the 97 brochure-summary rows that exist only in root copies. Prune the 614 root duplicates and the 2,004 header-only stubs. Fence script-only trees out of the images.

**Entry gate.** Phase 8C exit. D-HY1 (P9.7) and D-HY9 (P9.2 deploy, P9.8) answered. P0A.1 has recorded whether prod sets `DICTIONARY_CATALOG_DB_PATH`.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P9.1 | hygiene missing (hygiene-23) | Merge the 97 root-only '[Brochure Summary]' rows into the options/raw copies | data_repair (tracked files) | backend/dictionary/options/raw/<Make>/*_Complete_Options.csv (97), backend/tests/test_dictionary_layout.py | S | A | none |
| P9.2 | hygiene-1 + hygiene missing (catalog guard) (+ reviewer) | Normalized in-memory options index as the single options resolver | code | backend/enrichment/dictionary_catalog.py, backend/tests/test_dictionary_catalog.py | M | A | none |
| P9.3 | hygiene-3 (+ reviewer) | Delete the 614 root-level Complete_Options duplicates (content pre-flight) | deletion | backend/dictionary/*_Complete_Options.csv (614), backend/scripts/import_brochures_to_dictionary.py, backend/dictionary/README.md, backend/tests/test_dictionary_layout.py | S | B | P9.1, P9.2 |
| P9.4 | hygiene-4 | Delete the 2,004 header-only options stubs | deletion | backend/dictionary/options/stubs/, backend/dictionary/README.md, backend/tests/test_dictionary_layout.py | S | C | P9.3 |
| P9.5 | hygiene-6 | Rebuild the untracked local index on every machine, or record "not needed" (MAIN) | data_repair | backend/dictionary/index/ (untracked) | S | D | P9.3, P9.4 |
| P9.6 | hygiene-2 (+ reviewer) | Decide on the in-image catalog (expected: close as not needed) | investigation | Dockerfile.web, Dockerfile.scanner, backend/scripts/build_dictionary_manifest.py, backend/dictionary/README.md | S | D | P9.2 |
| P9.7 | hygiene-7 (+ reviewer) | Fence script-only dictionary trees out of the images | code | .dockerignore, .railwayignore, backend/tests/test_brochure_text_runtime_gate.py (new) | S | D | P9.3 |
| P9.8 | hygiene-22 (+ reviewer) | Release and verify in prod | ops, needs prod | VERSION, CHANGELOG.md | S | E | all |

**P9.1: Summary rows.** Dry run: list the 97 of 132 rich root files whose `[Brochure Summary]` row (written by import_brochures_to_dictionary.py:815-822) is missing from the options/raw copy. Example: 2021_Kia_Telluride's "Harman Kardon audio | Nappa". Append each row to the raw copy, keeping the column order (`normalize_csv_columns` semantics). Then verify that every root row, as a full tuple, exists in its sharded copy. Attach the manifest. Rollback is `git revert`. Accept: the content pre-flight shows 0 root-only rows across all 614 root files.

**P9.2: Single options resolver.**
- Change:
  - Build an in-memory index keyed on `(_norm_token(canonical_make(make)), _norm_token(model), year)` from `parse_csv_filename` over `iter_dictionary_csv_paths('options')`: options/raw first, then root, skipping stubs.
  - Match with the same LIKE-prefix semantics as the `_catalog_lookup_candidates` SQL (dictionary_catalog.py:382-384). Rank by year distance, then options/raw before root, then name.
  - Cache it with `lru_cache`, cleared by `invalidate_catalog_cache` (L702).
  - Preferred: make it THE options resolver and drop the SQLite path for options, so prod, CI and local share one code path. Minimum: before accepting a nearest-year catalog row, consult the index for an exact-year hit; today a stale catalog row whose file is missing falls back to a different year (2020 Buick Enclave resolved to the 2019 file).
  - The EPA fuzzy fallback is unchanged; it already reaches parity, 217/217.
  - Filename-glob matching (fnmatch on `_filename_token`) reached only 203/300 in the hygiene verdict's prototype, because of hyphenated models (CR-V, HR-V, C-Class, F-250, Mach-E, C-HR) and make aliases (Mercedes-Benz vs a "Mercedes" filename).
- Tests: a hyphenated model; a make alias; a stale catalog row pointing at a deleted file must not fall back to another year; without the catalog, 2025 Toyota Camry resolves to options/raw; 2020 Buick Enclave resolves to the raw copy (6 rows), not the root copy (1 row).
- Accept:
  - Without the catalog, top-300 coverage is at least 223/300, with the numbers in the PR.
  - Index build is under 2 s and a lookup under 5 ms.
  - The owner has reviewed a 20-car before/after `resolve_trim_ladder` diff (10 raw-only YMMs, 10 with root duplicates) before any deploy (D-HY9).

**P9.3: Delete root duplicates.** Re-run the content pre-flight after P9.1. `git rm` the 614 root CSVs, keeping the 6 non-CSV root files. Remove the legacy-root branch in `import_brochures_to_dictionary._csv_path` (L730-732) and the "legacy flat CSVs supported" README line. If the SQLite catalog still serves options lookups after P9.2, P9.5 must run in the same session on every machine that has a catalog. Accept: `git ls-files 'backend/dictionary/*_Complete_Options.csv' | awk -F/ 'NF==3'` prints nothing; no-catalog coverage is unchanged; no remaining writer targets the root.

**P9.4: Delete stubs.** `git rm -r backend/dictionary/options/stubs`: 2,004 files of 241 bytes, including the 4 Mazda mojibake names. Every reader already skips them. Keep the `OPTIONS_STUBS_DIR` constant. Accept: `python -m backend.scripts.build_dictionary_manifest --stats` (read-only, L22-31) shows `options_stub=0` and an unchanged `options_rich`; trim_coverage_report.py still runs.

**P9.5: Local index (MAIN).** If P9.2 retired the catalog for options, record "not needed for lookups" and stop. Otherwise: back up the index files with sha256; check read-only (257 entries point at root files); rebuild outside pytest; verify 0 entries without '/' and `options_stub=0`. Repeat on the mini and in any worktree that has a catalog.

**P9.6: In-image catalog.** Measure the trim-ladder latency for 50 cars with and without the catalog. With P9.2 as the sole resolver, close the unit and record the numbers. Otherwise, add `RUN python -m backend.scripts.build_dictionary_manifest --db-only` (a new flag) to Dockerfile.web and Dockerfile.scanner, and verify it from a `git archive` stage: a local `docker build` bakes in the local catalog, because `.dockerignore`'s `*.db` is root-anchored, so a local smoke proves nothing. Assert the catalog is absent before the RUN and present after. Either way, the README says manifest.json has no runtime reader.

**P9.7: Image fencing.**
- Change: add to `.dockerignore` (root-anchored) and `.railwayignore`:
  - `derived/trim_candidates/`, `derived/html_spec_text/`, `derived/html_spec_overlays/`;
  - `derived/trim_overlay_validation.jsonl`, `derived/brochure_content_index.json`, `derived/trim_citation_verification.json`;
  - `index/manifest.json`.
  `derived/brochure_text/` (149.6 MB) has a dormant request-path reader: `_rung_order_from_brochure_text` (brochure_extract.py:2213-2245), reached from `attach_rung_order` :2334 for `brochure_text_quoted` overlays without `order_basis`. Today there are 27 such overlays, 0 of them missing it. Exclude brochure_text only together with a CI test proving every `brochure_text_quoted` overlay carries a non-empty `order_basis`, with the fallback branch logged and flagged. Otherwise keep it.
  Never exclude: epa/, options/raw/, curated/, trim_adds_by_year/, brochure_text_slim/, trim_spec_sheets/, epa_ev_range_miles.json, make_aliases.json, platform_registry.json.
- Accept: git grep evidence for each excluded path; `resolve_trim_ladder` output is identical for 20 cars with `DICTIONARY_ROOT` pointing at a scratch symlink tree without the excluded paths; the image size delta is measured only with owner approval.

**P9.8: Release and verify.**
- Change:
  1. Before deploying, capture the HTML/JSON of 3 prod car pages, including a 2025 or 2026 Toyota Camry (raw-only YMM), read-only.
  2. Patch release.
  3. Local Docker run from a `git archive` stage, asserting the catalog is absent (prod-equivalent).
  4. `deploy_web.sh`. Deploy scanner-nightly only if a scanner-side unit shipped, and then with `deploy_scanner_nightly.sh`: `railway redeploy` reuses the old build.
  5. Diff the 3 pages; check `/health`; watch 30 minutes of web logs.
  Rollback: the deploymentRollback recipe.
- Accept: the Camry page shows the Complete_Options-derived ladder content it lacked; no dictionary errors in the logs.

**Workflow shape.** Wave A: P9.1, P9.2. Wave B: P9.3. Wave C: P9.4. Wave D: P9.5 (MAIN), P9.6, P9.7. Wave E: P9.8. Review lens: no options row lost; local, CI and prod resolve identically; the image keeps every file read on the request path.

**Data repair.** P9.1 is a tracked CSV change. P9.5 is local.

**Exit gate.** pytest, chunked: test_dictionary_catalog.py, test_dictionary_layout.py, test_brochure_text_runtime_gate.py, the trim_ladder tests, test_epa_file_identity.py. The coverage numbers are recorded, the owner has signed the 20-car diff, and the prod pages are verified.

**Release step.** Patch (P9.8).

## Phase 10A: EPA catalog code foundation

**Goal.** Build the code foundation for the EPA catalog:
- label forced induction from EPA's per-row flags (tCharger/sCharger plus eng_dscr), never the make/model guess;
- give catalog rows a permanent key (EPA id when present, otherwise a sticky content key);
- one migration for flags, provenance, archive tables and the two missing indexes;
- an importer that writes flags, the EPA id, atvType and cityE/highwayE, scopes by `--years`, and can patch the CSVs in place;
- relink modes that can repair dangling links;
- a set-based `epa_master` refresh that can be restored.

**Entry gate.** Phase 9 exit. D-EPA1, D-ET2, D-ET3, D-ET5 and D-ER2 answered. The owner has approved the pinned EPA source: `vehicles.csv.zip`, internal date 2026-09-29, 50,407 rows, sha256 recorded, copy kept in `workspace/backups/`.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P10A.1 | epa-turbo-1 (+ reviewer) | Induction vocabulary, EPA-flag mapper (explicit rule order), shared twin helper | code | backend/utils/forced_induction.py, backend/tests/test_forced_induction_epa_flags.py (new) | S | A | none |
| P10A.2 | epa-rebuild-3 (+ reviewer) | Permanent catalog key: EPA id, else a sticky content key | code | backend/catalog/epa_key.py (new), backend/tests/test_epa_natural_key.py (new) | S | A | none |
| P10A.3 | epa-turbo-4 + epa-rebuild-4 (+ reviewers) | V030: epa_master flags, provenance, archive tables, `epa_vehicle_id` and `cars.epa_master_id` indexes | code | migrations/V030__epa_master_flags_provenance.sql (new), backend/db/repositories/schema_repo.py, backend/db/inventory_pg.py (legacy list), backend/scanner/database.py (SQLite DDL), backend/db/dictionary_schema.py, backend/tests/test_epa_catalog_schema.py (new) | M | A | none |
| P10A.4 | epa-turbo-2a | epa_engine honours the catalog row's label | code | backend/dictionary/epa_engine.py, backend/tests/test_forced_induction_display.py, test_epa_engine_resolve.py | S | B | P10A.1 |
| P10A.5 | epa-turbo-2b + epa-rebuild missing (dictionary columns) (+ reviewers) | Importer: EPA flags, new columns, `--years`, `--patch-induction-only`, `--verify` | code | backend/scripts/import_epa_to_dictionary.py, backend/enrichment/dictionary_catalog.py (EPA column set in `normalize_csv_columns`), backend/tests/test_import_epa_to_dictionary.py (new), backend/tests/fixtures/epa_vehicles_sample.csv (new) | L | C | P10A.1, P10A.4 |
| P10A.6 | epa-rebuild-6 + epa-rebuild missing (quiescence) (+ reviewer) | Provenance backfill script with positive legacy classification | code | backend/scripts/backfill_epa_master_provenance.py (new), backend/tests/test_backfill_epa_master_provenance.py (new) | M | B | P10A.2, P10A.3 |
| P10A.7 | epa-rebuild-10 | Relink modes: `--dangling`, `--include-inactive`, `--clear-unresolved`, `--vins-file`, `--exclude-source` | code | backend/scripts/link_cars_to_catalog.py, backend/tests/test_link_cars_to_catalog.py (new) | M | A | none |
| P10A.8 | epa-turbo-5 + epa-turbo missing (rollback) (+ reviewer) | `import_epa_master --refresh-induction` (set-based) and `--restore` | code | backend/scripts/import_epa_master.py, backend/tests/test_import_epa_master_induction.py (new) | M | C | P10A.1, P10A.3 |

**P10A.1: Mapper.**
- Change:
  - Constants: `TURBO`, `TWIN_TURBO`, `SUPERCHARGED`, and `NA_LABEL='Naturally Aspirated'` (D-ET2).
  - `forced_induction_from_epa(t_charger, s_charger, eng_dscr, model, atv_type, *, twin_hint=None)` applies its rules in this order:
    1. atvType EV, or no combustion engine → `''`.
    2. Flags: T → `TURBO`, or `TWIN_TURBO` via twin_hint; both T and S → `TURBO` (D-ET3); S → `SUPERCHARGED`.
    3. eng_dscr markers: TRBO, TURBO or T/C (including `'TRBO)'`) → TURBO; S-CHARGE, SC, S/C or SUPERCHARG → SUPERCHARGED.
    4. A model badge (Turbo, Biturbo, Kompressor, Supercharged) that is not inside a slash list like '240 DL/GL/Turbo' (about 35 ICE rows; the 32 Taycan Turbo EVs are caught by rule 1).
    5. Otherwise `NA_LABEL`.
  - `has_forced_induction(label)`: False for `NA_LABEL`, '' and None.
  - A shared twin helper (D-ET5), used by both P10A.5 and P10A.8: EPA says T, and the current classifier says twin. For pre-2016 rows, where the classifier returns None, the existing label decides. Without this, 514 pre-2016 twin rows (2011-15 EcoBoost, N54 335i, GT-R) would split between the CSV and `epa_master`.
  - `forced_induction_short_suffix` and `apply_forced_induction_to_engine_display` already add no suffix for 'Naturally Aspirated', because a non-empty value skips the classifier (:67). Pin that with tests; no code change there.
- Tests: T, S and TS; blank ICE → NA; blank EV → ''; 'DSL,TRBO' and 'TRBO)' → Turbocharged; '(FFS) (S-CHARGE)' → Supercharged; 'C230 Kompressor' with blank flags → Supercharged; '240 DL/GL/Turbo' → NA; 'Taycan Turbo' EV → ''; 2013 F-150 3.5 EcoBoost → Twin.
- Accept: the tests pass, and test_forced_induction_display.py passes unchanged.

**P10A.2: Permanent key.**
- Change: `natural_key(row)`:
  - `'vid:<epaId>'` when the EPA vehicle id is present (vehicles.csv `id` is unique per row);
  - otherwise a normalized base key over (year, make, model, trim, engine_description, trany, drive, displacement, cylinders, fuel_type), with strip/lower, numbers formatted `%g` and None as '', plus an ordinal `#n` that is assigned ONCE (at insert or backfill), stored, and never recomputed.
  - Matching CSV rows to DB rows inside one base-key group uses the exact `(body_style, city08, highway08, engine_display, forced_induction)` first, then the nearest values (an assignment), never ordinal position. 236 duplicate groups are ambiguous on (body_style, city08, highway08) alone.
  - engine_description is part of the key, so it is never an updatable column.
  - Pure; no IO.
- Tests: CSV-typed and DB-typed rows give the same key; the vid key; deterministic ordinal assignment; matching by value, not position (two rows that differ only in engineDisplay pair correctly).
- Accept: the tests pass.

**P10A.3: V030.**
- Change, one migration with `SET LOCAL lock_timeout='5s'`:
  - `epa_master` gains `t_charger`, `s_charger`, `source`, `natural_key` (text) and `loaded_at` (timestamptz).
  - New tables `epa_master_archive` (the epa_master columns plus `archived_at`, `archive_reason`, `superseded_by`) and `epa_extended_specs_archive`.
  - `idx_epa_master_vehicle_id`: without it, a per-id refresh would run about 50k sequential scans.
  - `idx_cars_epa_master_id ON cars(epa_master_id) WHERE epa_master_id IS NOT NULL`. This takes a SHARE lock on `cars`, so apply it only in a quiet window, after `pg_stat_activity` shows no idle-in-transaction session.
  - Refreshed rows store '' rather than NULL for blank flags, so invariants can tell "refreshed, NA" from "never refreshed".
  - Mirror the new columns in the SQLite DDL (schema_repo, scanner/database.py) and in dictionary_schema's ALTER list. Add them to the inventory_pg legacy list too, for parity until P13B.1 deletes it.
  - The SQLite `epa_master` lacks `trim`, `engine_display` and `forced_induction`, and SQLite has no `epa_extended_specs` or `epa_master_dump_id_map` at all, so tests create those fixtures themselves.
  - Not parallel-safe: schema_repo.py is a hot shared file.
- Accept: test_schema_from_migrations.py passes; a fresh SQLite DB has the new columns and tables; the migration is idempotent; it applies cleanly on a scratch Postgres.

**P10A.4: Row label honoured.**
- Change:
  - `_epa_row_car_context` (:234) takes `row['forcedInduction']` when the car has no label.
  - `catalog_engine_fields` (:87) never calls the classifier when the row holds an explicit label, NA included.
  - `engine_updates_from_dictionary_row` (:130) writes the row label only when the car's value is empty and the car's strong text (engine_description, trim, title; not the free-text description) has no FI marker.
- Tests: the 2017 330i (EPA T) still yields 'Turbocharged' (`test_enrich_car_engine_from_dictionary_overwrites`).
- Accept: the tests pass.

**P10A.5: Importer.**
- Change, in import_epa_to_dictionary.py:
  1. `process_epa_csv` computes the label with `forced_induction_from_epa` (twin helper included) BEFORE `catalog_engine_fields`.
  2. The bare `except Exception: pass` (:168-174) becomes a counted, logged failure, and the script exits non-zero if any row failed.
  3. Per D-EPA1(a), `DICT_COLUMNS` (:33-40) gains `epaId`, `atvType`, `cityE` and `highwayE`. `normalize_csv_columns` gets an EPA-specific canonical set that preserves them; today it strips non-canonical columns (dictionary_catalog.py:661), and the Options CSVs keep their set unchanged.
  4. `--years Y1,Y2` regenerates only those years. Today only `--min-year` exists, and it rewrites every file at or above it (:191).
  5. `--patch-induction-only` (dry run by default; `--apply` to write):
     - read each existing `backend/dictionary/epa/**/*_EPA.csv`;
     - join each row to the pinned vehicles.csv on the importer key (Year, Make, baseModel, Trim, engineOptions, transmission, drive, fuel, VClass, cylinders, displ, city08, highway08, comb08);
     - treat a key as ambiguous when it matches several rows OR its rows carry conflicting T/S flags (92 such);
     - rewrite forcedInduction and engineDisplay, and fill the four new columns; every other cell stays byte-identical;
     - unmatched or ambiguous rows get a blank label and the base display, unless an explicit text marker exists;
     - report matched/unmatched/ambiguous counts and every label transition. Report base-display changes separately (31 Ram HD Cummins rows go from '6.7L V6' to '6.7L I6' under today's formatter);
     - print the source sha256 and row count.
  6. `--verify`: 0 label disagreements against the EPA flags on matched rows.
- Tests:
  - The fixture vehicles.csv (about 14 rows: 2000 Integra, 2019 RDX T, 2016 A6 3.0 S, 2019 XC90 T8 TS, 2017 Bolt EV, 1984 Jetta DSL/TRBO, 2003 C230 Kompressor, 1984 Volvo 240 DL/GL/Turbo, 2013 F-150 EcoBoost, Taycan Turbo EV) yields exactly the expected label and display; the Integra gets NA and '1.8L I4' with no 'Turbo'.
  - Patching two tmp CSVs changes only the target and new columns.
  - A broken row makes the importer exit non-zero.
  - Flag-conflict ambiguity is detected.
  - `--years` scope.
- Accept: the tests pass.

**P10A.6: Provenance backfill.**
- Change: dry run by default.
  - Classification:
    - a row whose id is in `epa_master_dump_id_map.live_id` → `dictionary_epa`, keyed via epa_key, with the vid key when the row has `epa_vehicle_id`;
    - `epa_vehicle_id` present but not in the map → `epa_vehicles_csv`, key `vid:<id>`;
    - LEGACY only when classified POSITIVELY: id ≤ 20006 AND its base key appears in a re-read of root `DICTIONARY/`. That re-read must happen before P10D.8 deletes the folder.
  - Refuse if any row matches no class (so interim inserts can never be mislabelled), and refuse on any (source, natural_key) collision, listing the first 20.
  - Write in one transaction by id, only where `source IS NULL` unless `--recompute`. Never touch `cars` or `epa_extended_specs`.
  - Assert the counts afterwards. Record that the dump rows carry vid (49,933), atv_type (6,939) and city_e (1,947), which no builder may ever null.
  - Quiescence guard: refuse while any non-self session is active or idle in transaction (`pg_stat_activity`). Mini shards write the MBP's Postgres over the tunnel, and lock files are host-local, so a lock-file check would miss them.
- Tests: the three provenances; a collision is refused with no writes; an unmatched row is refused; a re-run is a no-op; the quiescence refusal.
- Accept: the tests pass.

**P10A.7: Relink modes.**
- Change:
  - `--dangling`: active or inactive cars whose `epa_master_id` has no row are re-resolved, and cleared if nothing resolves. Full mode already repairs dangling active cars that still resolve (:116-131), but it leaves unresolvable active cars, all inactive cars, and `--only-missing` runs.
  - `--include-inactive` drops the filter at :74.
  - `--clear-unresolved` makes full mode clear links that no longer resolve, matching linker.py:61-69.
  - `--vins-file`.
  - `--exclude-source <name>` filters that source's rows out of the candidates.
  - Every run prints changed / cleared / unchanged / skipped_catalog_error counts, and commits every 1,000 rows.
  - A library function, `relink_cars(ids, exclude_sources=...)`, for P10B.6.
- Tests: `--dangling` repairs both an inactive and an active dangling link; `--clear-unresolved`; `--exclude-source dictionary_legacy_root` never returns a legacy id; the default mode is backward compatible.
- Accept: the tests pass.

**P10A.8: epa_master refresh.**
- Change:
  - New rows store `t_charger`/`s_charger` and get their label from `forced_induction_from_epa`.
  - `--refresh-induction --csv <pinned>`, on Postgres:
    1. COPY the computed `(epa_vehicle_id, t, s, label, display)` into a temp table.
    2. Run one `UPDATE epa_master m SET ... FROM tmp WHERE m.epa_vehicle_id = tmp.id AND (m.forced_induction, m.engine_display, m.t_charger, m.s_charger) IS DISTINCT FROM (...)`.
    That updates every duplicate of a vid (5,208 vids are duplicated). The script's `?`-row loop runs row by row on Postgres (:70-76), which over the tunnel takes tens of minutes, so the row loop is for SQLite tests only.
  - Only the SUFFIX of an existing `engine_display` is rewritten; a NULL display is never created (the 461 id-only rows would otherwise get layouts guessed from the cylinder count).
  - Never DELETE. Report id-less rows (32 of them are labelled locally).
  - Print the transition matrix and the sha256.
  - `--restore <backup.csv>`: temp table plus UPDATE FROM.
  - Run in the background with a progress log.
- Tests (SQLite): seeded rows (Integra wrongly turbo, RDX right, A6 3.0 wrongly turbo, a duplicate-id pair) end at the EPA truth after `--apply`; both duplicates are updated; the dry run changes nothing; the transition counts equal the applied counts; restore round-trips.
- Accept: the tests pass.

**Workflow shape.** Wave A: P10A.1, P10A.2, P10A.3, P10A.7. Wave B: P10A.4, P10A.6. Wave C: P10A.5, P10A.8. Review lens: rule order; one twin rule shared by both writers; nothing nulls the vid, atv_type or city_e columns; set-based SQL on Postgres; migration lock windows.

**Data repair.** None. Apply V030 locally at merge, in a quiet window.

**Exit gate.**
- pytest: test_forced_induction_epa_flags.py, test_forced_induction_display.py, test_epa_natural_key.py, test_epa_catalog_schema.py, test_schema_from_migrations.py, test_epa_engine_resolve.py, test_import_epa_to_dictionary.py, test_backfill_epa_master_provenance.py, test_link_cars_to_catalog.py, test_import_epa_master_induction.py.
- V030 is applied locally.

**Release step.** Minor (schema). The owner applies V030 to prod in a quiet window (D-DB6) before any deploy of this release.

## Phase 10B: EPA catalog builder, provenance backfill, legacy re-resolve (local)

**Goal.** Label every local catalog row with its provenance. Replace the delete-and-reload builder with an id-preserving diff and upsert. Align the vehicles.csv importer with the single ingestion path. Re-resolve every car linked to a legacy root-DICTIONARY row, with legacy rows excluded, then archive the unreferenced legacy rows. Everything here is local and can be rolled back.

**Entry gate.** Phase 10A released and V030 applied locally. D-ER1 (P10B.3) and D-ER3 (P10B.6, P10B.8) answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P10B.1 | epa-rebuild-8a | Builder loader and diff report (dry run only) | code | backend/scripts/build_epa_master_pg.py, backend/tests/test_build_epa_master_pg.py (new) | M | A | P10A |
| P10B.2 | epa-rebuild-8b (+ reviewer) | Builder `--apply`, `--archive-missing` and guards; lifts P1A.2's write refusal | code | backend/scripts/build_epa_master_pg.py, backend/tests/test_build_epa_master_pg.py | M | C | P10B.1, P10B.4 |
| P10B.3 | epa-rebuild-9 (+ reviewer) | import_epa_master follows the single ingestion path (D-ER1) | code | backend/scripts/import_epa_master.py, backend/tests/test_import_epa_master_append.py (new) | S | A | P10A.8 |
| P10B.4 | epa-rebuild-7 (+ reviewer) | Local backups, provenance backfill and verify (MAIN) | data_repair | workspace/backups/ | S | B | P10A.6 |
| P10B.5 | epa-rebuild missing (vehicles.csv rows) | Relink the cars on the 461 vehicles.csv-appended rows; archive the unreferenced ones (a mode of the P10B.6 script) | code | backend/scripts/reresolve_legacy_catalog_links.py, backend/tests/test_reresolve_legacy_catalog_links.py | S | B | P10B.6 (same new file) |
| P10B.6 | epa-rebuild-11 (replaced) + epa-rebuild missing (rollback) (+ reviewer) | Re-resolve legacy-linked cars; archive unreferenced legacy rows; `--undo` | code | backend/scripts/reresolve_legacy_catalog_links.py (new), backend/tests/test_reresolve_legacy_catalog_links.py (new) | M | A | P10A.7 |
| P10B.7 | epa-rebuild-13 (+ reviewer) | Catalog invariants (provenance, orphan specs, dangling everywhere, cross-source twins) | code | backend/scripts/data_quality_invariants.py, baseline json (+ history), backend/tests/test_data_quality_invariants.py | S | C | P10B.4 |
| P10B.8 | epa-rebuild-12 (+ reviewer) | Local: builder check, legacy re-resolve, stratified sample, invariants (MAIN) | data_repair | workspace/backups/, run log | M | D | P10B.2, P10B.5, P10B.6, P10B.7 |

**P10B.1: Loader and diff.**
- Change:
  - Switch to the `inventory_db.get_conn` adapter, so SQLite tests work.
  - Read `EPA_DIR` recursively and map every column, including engineDisplay, forcedInduction, epaId, atvType, cityE and highwayE.
  - Compute keys via epa_key. Match against the `source='dictionary_epa'` rows, by key and then by value assignment.
  - Report insert / update / unchanged / missing. No writes.
- Tests: a diff fixture.
- Accept: on local after P10B.4, the dry run reports about 11 inserts (CSV keys absent from the dump rows) and a small update count.

**P10B.2: Builder apply.**
- Change:
  - INSERT new keys, with flags and labels from `forced_induction_from_epa`.
  - UPDATE by id only the value columns that changed: city08, highway08, body_style, engine_display, forced_induction, t/s flags, city_e, highway_e. Never engine_description, which is part of the key.
  - Never null a non-null vid, atv_type or city_e. Set atv_type only when the stored value is null; never downgrade an EPA 'Hybrid' or 'FFV' to NULL.
  - `--archive-missing` moves only unreferenced missing rows (no cars link, no extended-specs row) into `epa_master_archive`, then deletes them. Referenced missing rows are kept and listed.
  - No `ON CONFLICT DO NOTHING` anywhere.
  - Guards: refuse when the V030 columns are absent; when CSV rows are under 90% of the active `dictionary_epa` rows; when updates exceed 5% unless `--allow-large-update` is given.
  - One transaction, with pre-commit assertions: the extended-specs count is unchanged, 0 dangling links, 0 duplicate (source, natural_key).
  - `--rebuild` keeps refusing.
- Tests: a second run is a no-op and ids are stable; an MPG change updates the same id; a removed CSV row that a car links to is kept and reported; archive touches only unreferenced rows; atv_type is never downgraded; display and label are loaded; the guards refuse.
- Accept: the tests pass.

**P10B.3: Single ingestion path.**
- Change: under D-ER1(a), `--append-years` becomes a wrapper that runs `import_epa_to_dictionary --years <y>` and then `build_epa_master_pg --apply`. The vehicles.csv full replace is removed, except for the empty-catalog bootstrap. The docstring is corrected ("into SQLite table" is wrong). Under (b), append writes `source` and `natural_key='vid:<id>'` and dedupes on natural_key. Same file as P10A.8, so this runs after it.
- Tests: an appended row carries its provenance, and re-running the append inserts 0 rows.
- Accept: the tests pass.

**P10B.4: Local provenance backfill.**
- Change (MAIN):
  1. Quiescence: `pg_stat_activity` shows no scanner or idle-in-transaction sessions from any client, and the mini's shards are confirmed stopped.
  2. Backup: `pg_dump -Fc -t epa_master -t epa_extended_specs -t epa_master_dump_id_map` into `workspace/backups/local_epa_catalog_<date>.dump`, plus `CREATE TABLE cars_epa_link_snapshot_<date> AS SELECT id, vin, listing_active, epa_master_id, epa_match_confidence, epa_match_method FROM cars WHERE epa_master_id IS NOT NULL` (expect 313,454 rows).
  3. Backfill dry run: expect `dictionary_epa` 50,029, `dictionary_legacy_root` 20,006, `epa_vehicles_csv` 461, and 0 collisions.
  4. `--apply`.
  5. Verify, then record the P10B.7 baselines.
  Rollback SQL: reset the provenance columns, and restore links from the snapshot.
- Accept:
  - `natural_key IS NULL` count is 0.
  - A diff of cars against the snapshot returns 0 rows.
  - `epa_extended_specs` still has 49,912 rows.

**P10B.5: vehicles.csv rows.** Under D-ER1(a), the 461 vehicles.csv-appended rows (5,261 car links on 26 ids) duplicate vehicles once the dictionary path covers those years. Add a `--source epa_vehicles_csv` mode to the P10B.6 script. It re-resolves the cars linked to those rows with `exclude_sources={'epa_vehicles_csv'}`, archives the unreferenced rows with their ids, and supports the same snapshot table and `--undo`. It refuses to run unless `dictionary_epa` rows already exist for every model year of the 461 rows; otherwise every car would simply be cleared. Code only here. The run (local P10D.4 step 6, prod P10D.6 step 8) happens after a `--years` import plus a builder `--apply` has put those years into the dictionary path. Tests: a fixture where a `dictionary_epa` twin exists, so the car relinks and the row is archived; the refusal when no year coverage exists; undo round-trips. Accept: the tests pass; the dry run reports counts.

**P10B.6: Legacy re-resolve.**
- Change: re-resolve EVERY car linked to a `dictionary_legacy_root` row (229,060 links, 136,001 active) through `relink_cars(exclude_sources={'dictionary_legacy_root'})`. That is what future scans do anyway: active cars are re-resolved on every scan (post_write.py:80-88). The CPU cost is minutes, so run on AC power.
  - Write per-car changes to a snapshot table `(run_id, car_id, old_id, new_id, method)`, not a markdown file.
  - Report how many cars landed on their content twin (verification only), relinked versus cleared, and atv-class transitions (Hybrid, FFV, Diesel, FCV). FCV counts as electric drive at read time (verified_specs/electrification.py:37).
  - Extend the dry run to UNLINKED cars whose LIMIT-1 fuzzy lookup row would change once legacy rows are archived (`knowledge_engine.lookup_epa_by_trim`, ~:640-660).
  - Then copy the unreferenced legacy rows into `epa_master_archive`, keeping their original ids (reason `legacy_root`, `superseded_by` = twin or NULL), and delete them. Referenced rows stay.
  - One transaction per make, resumable.
  - `--undo <run_id>`: reinsert the archived rows with their original ids, then restore links from the snapshot table.
- Tests: re-resolve excludes legacy rows; an archived row is restorable with the same id; a referenced row is never deleted; the dry run writes nothing; undo round-trips.
- Accept: the tests pass.

**P10B.7: Catalog invariants.** Add `epa_master_missing_provenance` (0 after the backfill), `epa_extended_specs_orphan`, `epa_link_dangling_all_listings` (the existing SQL without the `listing_active` filter) and `epa_master_cross_source_twin` (record its current value now; P10B.8 lowers it). Record the baselines AFTER P10B.4 by a reviewed JSON edit plus a history line; recording 0 before the backfill would fail the local nightly. Never raise an existing entry. Tests use the monkeypatched-runner pattern. Accept: the local `--sql-only` run is green against the new baselines.

**P10B.8: Local repair.**
- Change (MAIN):
  1. Refresh the dump and take a new link snapshot.
  2. Builder dry run: expect about 11 inserts, a small update count, 0 atv downgrades, 0 referenced rows archived. Apply if the owner agrees.
  3. Re-resolve dry run; the owner reviews it.
  4. Stratified sample: 100 active cars whose link moves (every atv class: Hybrid, FFV, Diesel, FCV), 50 inactive cars, and 50 unlinked cars from affected (year, make, model) groups. Render `serialize_car_for_api` and the generated spec sheet before and after, with a scratch script outside the repo. Engine, drivetrain and electrification must not change (vPIC precedence). Note: about 90.5k of the 136k active legacy-linked cars already get a LIMIT-1 extended-specs row through `row_by_ymmt` (extended_specs.py:86-98); the re-resolve swaps that arbitrary row for the exact variant's, and only about 45k cars gain a row they lack.
  5. Apply.
  6. `link_cars_to_catalog --dangling` (expect 0).
  7. Run the invariants and lower `cross_source_twin`.
  8. Restart the local web to clear the lru caches. Grid cards are unaffected; `epa_master_id` is not in `LISTINGS_GRID_CAR_COLUMNS`.
  Rollback: `--undo`.
- Accept:
  - The remaining legacy rows are exactly the still-referenced ones the report listed.
  - `epa_link_dangling` (active and all listings) is 0.
  - The year, make and engine-text link invariants are no higher than their baselines.
  - The sample diff is attached to the run log.

**Workflow shape.** Wave A: P10B.1, P10B.3, P10B.6 (code). Wave B: P10B.4 (MAIN), P10B.5 (code; extends P10B.6's script). Wave C: P10B.2, P10B.7. Wave D: P10B.8 (MAIN). P10B.5's run waits for 10D (P10D.4 step 6 locally, P10D.6 step 8 on prod), after the missing years have been imported into the dictionary path. Review lens: ids are never reassigned; every repair has an undo; FCV, FFV and Hybrid cars are never repointed blindly.

**Data repair.** P10B.4 and P10B.8, local only.

**Exit gate.**
- pytest: test_build_epa_master_pg.py, test_import_epa_master_append.py, test_reresolve_legacy_catalog_links.py, test_data_quality_invariants.py, test_catalog_resolver.py, test_catalog_linker.py.
- The local repair is verified, and the invariant baselines are recorded.

**Release step.** Minor: the builder's writes are enabled again, guarded. No prod work here (10D).

## Phase 10C: Forced-induction resolution, consumers, invariants, heal tool

**Goal.** Resolve each car's induction label by evidence. Order: strong dealer text, then vPIC's boosted flag typed by the EPA family, then family-unanimous EPA flags, then description text, then a tightened heuristic. Plug that into precedence and the linker. Make every consumer understand 'Naturally Aspirated'. Stop the upsert from re-guessing over repaired labels. Add invariants and a restorable heal tool.

**Entry gate.** Phase 10B released. D-ET2 and D-ET4 answered. Phase 8 done (precedence.py exists).

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P10C.1 | epa-turbo-6a | vPIC 'boosted' key (Turbo=Yes means boosted, not necessarily turbo) | code | backend/enrichment/knowledge_engine.py (:1271, :1364), backend/tests/test_vpic_boosted_key.py (new) | S | A | none |
| P10C.2 | epa-turbo-6b (+ reviewer) | `resolve_car_forced_induction`, registered as precedence's FI rule | code | backend/utils/forced_induction.py, backend/vehicle_facts/precedence.py, backend/tests/test_forced_induction_resolve.py (new) | M | B | P10C.1, P10C.5 |
| P10C.3 | epa-turbo-6c (+ reviewer) | Linker reconciles FI after linking, in its own transaction | code | backend/catalog/linker.py, backend/catalog/resolver.py (`_CANDIDATE_COLS`), backend/tests/test_catalog_linker_forced_induction.py (new) | M | C | P10C.2 |
| P10C.4 | epa-turbo-3 (+ reviewer) | `enrich_car` uses the resolver | code | backend/dictionary/enrich_from_dictionary.py, backend/tests/test_enrich_dictionary_forced_induction.py (new) | S | C | P10C.2 |
| P10C.5 | epa-turbo-7 (+ reviewer) | Tighten the 2016+ heuristic; fix false negatives, the PHEV miss, Raptor R | code | backend/utils/forced_induction.py, backend/tests/test_forced_induction_era_rules.py (new), fixtures/forced_induction_epa_truth_2016plus.csv (new), test_epa_engine_resolve.py, test_engine_display.py | M | A | none |
| P10C.6 | epa-turbo-8 (+ reviewer) | Consumers treat 'Naturally Aspirated' as not boosted; facet per D-ET2; Phase B fixed | code | backend/enrichment/knowledge_engine_specs.py, backend/utils/car_serialize/api_specs.py, backend/enrichment/generated_spec_sheet.py, backend/scripts/heal_cylinders_from_vpic.py, backend/db/repositories/facets/queries.py, backend/utils/hybrid_search.py, frontend/templates/listings.html, backend/tests/test_forced_induction_consumers.py (new) | M | C | P10C.2 |
| P10C.7 | epa-turbo missing (upsert guard) | Upsert never lets the heuristic overwrite an evidence-backed label | code | backend/scanner/upsert/sql.py, backend/scanner/upsert/serialize.py, backend/tests/test_upsert_forced_induction.py (new) | M | C | P10C.2, P8B.1 |
| P10C.8 | epa-turbo-9 (+ reviewer) | Induction invariants; fix the latent Postgres `\b` patterns | code | backend/scripts/data_quality_invariants.py, baseline json (+ history), backend/tests/test_data_quality_invariants.py | S | A | none |
| P10C.9 | epa-turbo-10 + epa-turbo missing (rollback, measurement) (+ reviewer) | `heal_forced_induction_from_evidence.py` with restore and a conflict sample | code | backend/scripts/heal_forced_induction_from_evidence.py (new), backend/tests/test_heal_forced_induction_from_evidence.py (new) | M | D | P10C.2, P10C.3 |

**P10C.1: vPIC boosted.** `_empty_vpic` (:1271) and `_normalize_vpic_response` (:1364) add `boosted`: Turbo 'Yes' → True, 'No' → False, anything else → None. Name it "boosted": vPIC Turbo=Yes appears on 20 of 343 supercharged active cars, including 2018-19 Range Rover 5.0 S/C and the twin-charged Volvo T6. Tests: Yes, No and blank. Accept: the tests pass.

**P10C.2: Resolver.**
- Change: `resolve_car_forced_induction(car, *, vpic_boosted=None, family_flags=None) -> (label, source)`, in order:
  1. Strong dealer text (engine_description, trim, title, T-displacement such as '2.0T') → `dealer_text`.
  2. vPIC boosted=True, typed by the unanimous EPA family: S → Supercharged, T → Turbocharged, unknown → Turbocharged; source `vpic`. vPIC False counts as negative only when there is no text marker.
  3. Family-unanimous EPA flags over id'd rows (year, make, model variants, displacement ±0.15, cylinders) → `epa`. A family that disagrees falls through. This guards against the 1.7% of wrong-engine links.
  4. Free-text description markers → `dealer_description` (weak; boilerplate is common).
  5. The P10C.5 heuristic → `heuristic`.
  Register it as precedence's `forced_induction` rule (P8A.2's placeholder).
- Tests:
  - 2017 RDX, no text, blank family → (NA, epa).
  - '2.0T' → Turbocharged (dealer_text).
  - vPIC Yes + family S → Supercharged; vPIC Yes + NA family → Turbocharged (vpic).
  - Disagreeing family → heuristic.
  - A description-only 'ecoboost' on an F-150 whose engine_description reads '5.0L 8 Cylinder' → NA (epa).
- Accept: the tests pass.

**P10C.3: Linker reconcile.**
- Change: after the link UPDATEs in `link_cars_by_vins`, reconcile forced_induction in a SEPARATE try/commit, so an FI bug never rolls back catalog linking (linker.py:44-79). Read `t_charger`/`s_charger` through `_CANDIDATE_COLS`, and family flags with one query per (year, make, model), cached like `cand_cache`; vPIC comes from the primed cache. Overwrite only when the stored value is empty, or differs and has no strong-text support. Write provenance. This runs on the existing post-upsert hook, so new VINs get corrected right away.
- Tests (SQLite): a heuristic 'Twin Turbocharged' on an NA family is rewritten to NA; dealer-text turbo is kept; an FI exception leaves the links committed.
- Accept: the tests pass.

**P10C.4: enrich_car.** `enrich_car` calls the resolver. FI already left FILLABLE_FIELDS in P8B.5, and there was the second precedence chain this unit's spec warned about. `fill_all=True` never overwrites strong-text values. Tests: '2.0L Turbo I4' with an NA row → Turbocharged; no text with an NA row → NA; an existing value is untouched with `fill_all=False`; the `fill_all=True` case. Accept: the tests pass.

**P10C.5: Heuristic tightening.**
- Change: a fixture of about 70 rows taken from the measured disagreements, plus true-positive controls. It covers:
  - naturally aspirated engines labelled boosted: RDX ≤2018, TLX ≤2020, Lamborghini non-Urus, Ferrari V12, Phantom VII, GranTurismo 4.7, Lincoln 3.7, Genesis 3.8/5.0, Infiniti 3.5/3.7, Hyundai/Kia 1.6 non-T, Lexus/Honda hybrids, Ford NA engines, GM 2.5 I4, Jeep 2.0/2.4, VW VR6, 2016 911 Carrera;
  - supercharged engines labelled turbo: Audi 3.0 TFSI 2016-18, JLR 3.0 V6 and 5.0 V8 S/C, CT5-V Blackwing, Escalade-V;
  - the PHEV miss: fuel strings like 'Premium Gasoline / Electricity' are PHEV, not EV (:455);
  - Raptor R 5.2 S/C, where the nameplate rule at :128 shadows the branch at :213;
  - the top false negatives that matter for unlinked cars: Tundra 3.4TT (4,912 cars), Tacoma 2.4T, Honda 1.5T, Nissan VC-Turbo, Ascent 2.4T, CX-90 3.3T.
  Keep the 2016 era guard. test_epa_engine_resolve.py's 2017 A6 3.0 expectation changes from '3.0L V6 Turbo' to Supercharged (EPA sCharger='S'), explained line by line.
- Accept: every fixture row matches. A scratch measurement against the pinned vehicles.csv shows 2016+ false positives drop from 831 to under 100 and true positives (6,450) fall by no more than 2%. Golden diffs (test_serialize_car_for_api_golden, test_search_golden) are explained line by line.

**P10C.6: Consumers.**
- Change: grep every reader first (backend, plus frontend/static main.js, listings/filters.js, facets.js, sc-helpers.js). Then:
  - `knowledge_engine_specs._has_forced_induction` (:270-279) uses `has_forced_induction()`, so a false turbo no longer skips the heavy-body NA 0-60 guard.
  - `api_specs.apply_forced_induction` (:27-36) returns the stored NA label (or None under D-ET2(b)) and never re-guesses.
  - generated_spec_sheet.py:1130 shows the stored source instead of the hard-coded 'listing'.
  - heal_cylinders_from_vpic Phase B skips NA and EPA/vPIC-backed values, or is retired in favour of P10C.9 after a grep. Either way, remove the P1A.5 flag.
  - The Engine type facet shows or hides NA per D-ET2, in `facets/queries.py:49 distinct_values` and hybrid_search.py:351/492.
- Tests: a heavy-body NA car with a 0-60 below the floor is flagged, and the same car labelled turbo is not; the serializer gives an NA car no 'Turbo'; Phase B keeps NA.
- Accept: the tests pass, and the golden and search diffs are explained.

**P10C.7: Upsert guard.**
- Change: today `sql.py:143 COALESCE(excluded.forced_induction, cars.forced_induction)` plus `sql.py:207 v.get(fi) or classify(v)` overwrite a repaired label whenever the same-dealer prefetch merge did not run (a VIN moved dealer, `SCANNER_VDP_DB_MERGE=0`, or a prefetch exception swallowed at enrich.py:145). Pass a classifier-only value with a heuristic marker, so the P8B.1 `decide()` path keeps an evidence-backed stored label (the heuristic ranks lowest). Postgres/SQLite neutral. Serial with other upsert edits.
- Tests: a stored NA from epa provenance survives an upsert whose feed has no FI text; feed text 'turbo' wins.
- Accept: the tests pass, and test_upsert_vehicles_golden passes with explained diffs.

**P10C.8: Invariants.**
- Change:
  - (a) `epa_master_fi_contradicts_flags`: flags non-NULL and the label disagrees, including a blank label on a non-EV row.
  - (b) `car_fi_boost_on_na_family` and (c) `car_fi_missing_on_boosted_family`. The family comes from the car's LINKED epa_master row (year, make, model), with displacement ±0.15 and cylinders, because `cars.model` ('F-150') differs from the EPA model ('F150').
  - Text markers use Postgres word boundaries: `~* '(turbo|supercharg|kompressor|ecoboost|tfsi|tsi|tdi|\m\d\.\dt\M)'`. `\b` is backspace in Postgres; `'2.0T engine' ~* '\d\.\dt\b'` is false.
  - Baselines go in by a reviewed hand edit plus a history line. The CLI refuses `write_baseline` with `--sql-only` and would rewrite every invariant anyway.
  - Also fix the latent `\b` patterns at data_quality_invariants.py:264/272 (`\bRS\b`, `\bSTI\b`, `\bZL1\b` never match in Postgres), with a history line for the changed counts.
- Accept: the `--sql-only` run takes seconds; the ids are registered with baselines; the history lines are present.

**P10C.9: Heal tool.**
- Change:
  - Dry run by default. It writes `workspace/data_quality/forced_induction_heal_<date>.csv` (id, vin, ymm, engine_description, old, new, source), a transition matrix and counts per source.
  - `--sample N` reports on the 1,640 text-vs-family conflicts, which are mostly catalog mislinks, for owner review.
  - `--apply` refuses until a backup CSV (id, vin, forced_induction) is written, then runs a set-based temp-table UPDATE on Postgres. It never overwrites a value backed by strong text.
  - `--restore <backup.csv>`.
  - Expected local volumes, from the reviewer's fleet join: about 77k NA writes, about 19.8k boost fills, about 1.8k boost→NA flips. The spec's 444 / 2,825 covered only id'd links.
- Tests (SQLite): the transitions are correct; dry run writes nothing; apply refuses without a backup; restore round-trips.
- Accept: the tests pass.

**Workflow shape.** Wave A: P10C.1, P10C.5, P10C.8. Wave B: P10C.2. Wave C: P10C.3, P10C.4, P10C.6, P10C.7. Wave D: P10C.9. Review lens: one precedence chain only; no consumer re-guesses over NA; Postgres regex syntax; the upsert stays Postgres/SQLite neutral.

**Data repair.** None. Run P10C.9's dry run once locally and attach the report; it is D-ET2's numbers.

**Exit gate.**
- pytest: test_vpic_boosted_key.py, test_forced_induction_resolve.py, test_catalog_linker_forced_induction.py, test_enrich_dictionary_forced_induction.py, test_forced_induction_era_rules.py, test_forced_induction_consumers.py, test_upsert_forced_induction.py, test_data_quality_invariants.py, test_heal_forced_induction_from_evidence.py, test_forced_induction_display.py, test_epa_engine_resolve.py, test_engine_display.py, test_serialize_car_for_api_golden.py, test_search_golden.py, test_upsert_vehicles_golden.py.
- A local `--sql-only` invariants run.

**Release step.** Minor. Deploy web and scanner-nightly, and pull on the MBP and the mini. All code units ship and run on every node BEFORE any CSV or data repair (10D), so post-scan enrichment, the plausibility guard and the facet understand NA before NA labels appear.

## Phase 10D: EPA data repairs, prod rollout, constraints, DICTIONARY/ deletion

**Goal.**
- Patch the dictionary CSVs from the pinned source, and repair the Complete_Options and trim-ladder copies of the same labels.
- Repair `epa_master` and `cars` labels locally, then roll the whole EPA catalog repair out to prod in stages.
- Add the RESTRICT foreign keys.
- Delete the stale root `DICTIONARY/` folder.
- Document the id contract.

**Entry gate.** Phase 10C released and running on every scanning node. D-EPA1, D-ER4, D-ER5 and D-ER6 answered. Owner windows available.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P10D.1 | epa-turbo-11 (+ reviewer) | Patch the 12,126 dictionary EPA CSVs from the pinned source | data_repair (tracked files) | backend/dictionary/epa/**/*_EPA.csv | M | A | P10C |
| P10D.2 | epa-turbo missing (Complete_Options) | Patch or blank FI/engineDisplay in the 10,614 Complete_Options CSVs | data_repair (tracked files) | backend/dictionary/**/*_Complete_Options.csv, backend/scripts/build_dictionary_options.py | S | A | P10C |
| P10D.3 | epa-turbo missing (trim ladder artifact) | Regenerate or retire `curated/trim_ladders_epa.json` | data_repair | backend/dictionary/curated/trim_ladders_epa.json, backend/scripts/build_trim_ladders_from_epa.py | S | C | P10D.1, P10D.4 (regenerate after its step-6 year import) |
| P10D.4 | epa-turbo-12 (local) (+ reviewer) | epa_master label refresh, local (MAIN) | data_repair | workspace/backups/ | M | B | P10D.1 |
| P10D.5 | epa-turbo-13 (local) (+ reviewer) | cars label heal from evidence, local (MAIN) | data_repair | workspace/backups/, workspace/data_quality/ | M | C | P10D.4 |
| P10D.6 | epa-rebuild-16 + epa-turbo-12/13 prod parts (+ reviewer) | Staged prod rollout of the EPA catalog repairs | data_repair, needs prod | workspace/backups/ | M | D | P10D.5, release |
| P10D.7 | epa-rebuild-5 (+ reviewer) | V031 unique key and RESTRICT FKs (NOT VALID); V032 VALIDATE in a later release | code | migrations/V031__epa_catalog_constraints.sql (new), migrations/V032__validate_epa_catalog_fks.sql (new, later release), backend/db/inventory_pg.py (:481), backend/scripts/migrate_inventory_sqlite_to_postgres.py (`_TABLES` order), backend/tests/test_epa_catalog_schema.py | S | E | P10D.6 |
| P10D.8 | epa-rebuild-14 + hygiene-5 (+ reviewer) | Delete root `DICTIONARY/` after the content audit | deletion | DICTIONARY/ (10,200 files), .dockerignore, .railwayignore, backend/scripts/clean_vehicle_csvs.py, backend/scripts/import_epa_to_dictionary.py, backend/enrichment/verified_specs/{result,sources}.py, backend/enrichment/knowledge_engine.py (comments), README.md:34 | S | E | P10D.6 |
| P10D.9 | epa-turbo-14 + epa-rebuild-15 (+ reviewer) | Docs: induction precedence, catalog id contract, importer usage, README | docs | docs/data_architecture_plan.md, README.md, backend/scripts/build_trim_ladders_from_epa.py, backend/scripts/import_epa_to_dictionary.py (docstring), docs/NETWORK_SCAN_PROCESS.md, backend/dictionary/README.md, docs/monolith_audit_2026_10_01/datascripts.md | S | F | P10D.6, P10D.8 (both edit README.md and import_epa_to_dictionary.py) |

**P10D.1: CSV patch.**
- Change:
  1. Pin the source (zip and sha256 in `workspace/backups/`). Start from a clean tree.
  2. `import_epa_to_dictionary.py --patch-induction-only --epa-csv <pinned>` dry run. Expect about 9.1k wrong positives corrected, about 3.4k missing labels filled, about 29k rows set to NA (D-ET2a), and about 227 unmatched or ambiguous rows (135 unmatched, 92 with conflicting flags) reported.
  3. `--apply`, then `--verify`: 0 disagreements.
  4. A column-diff checker shows only forcedInduction, engineDisplay and the D-EPA1 columns changed; base-display changes are listed. manifest.json's `epa_row_count` is unchanged.
  5. Spot checks: 2000 Integra ('1.8L I4', NA), 2016 RDX, 2016 A6 3.0 (Supercharged), 2019 XC90 T8, 2003 C230 Kompressor, 1984 Jetta diesel TRBO.
  6. Commit separately, with the sha256 and the counts in the message.
  The CSVs ship in the image, so prod receives them through the release deploy. Rollback: `git revert` plus a redeploy.
- Tests: test_import_epa_to_dictionary.py, test_dictionary_catalog.py, trim_ladder/test_epa_csv_revoked.py, test_forced_induction_display.py, test_epa_engine_resolve.py, test_engine_display.py.
- Accept: the verify step shows 0 disagreements and the column checker passes.

**P10D.2: Complete_Options.** The Complete_Options CSVs carry the same heuristic labels: 52,701 rows; 7,059 Turbocharged, 2,473 Twin, 101 Supercharged; 4,717 pre-2016 labels with no textual marker, e.g. 2007 VW Rabbit '2.5L V5 Turbo'. Patch them through the same verifier where a (year, make, model, engine) key matches the pinned source; otherwise blank the two columns. Extend `--verify` to cover them. Runtime use is nil today (`dictionary_options` is empty and `fetch_options_rows` has no callers), but build_dictionary_options.py:61-62 would propagate the labels on the next build. Separate commit. Accept: verify passes on matched rows, and every unmatched label is blank.

**P10D.3: Trim ladder artifact.** Grep the consumers: the loader `trim_ladder/loaders.py:73` has no callers, and scripts/merge_trim_ladders.py:47 merges the file. Then either regenerate it with build_trim_ladders_from_epa.py after P10D.1, or delete it. It carries 1,862 synthesized boost bullets, 16 of them '1.8L I4 Turbo'. Accept: no bullet contradicts the patched CSVs, or the file is gone with a clean grep.

**P10D.4: epa_master refresh (local).**
- Change (MAIN):
  1. Backup: `\copy (SELECT id, epa_vehicle_id, forced_induction, engine_display, t_charger, s_charger FROM epa_master)`; its row count must equal the table's.
  2. `import_epa_master.py --refresh-induction --csv <pinned>` dry run. Expect about 9,079 labels corrected, about 3,478 filled, and 32 id-less labelled rows reported.
  3. Review, then `--apply` (set-based).
  4. Invariant (a) = 0; spot SELECTs.
  5. Restart the local web (the `fetch_epa_rows` lru cache).
  6. The vehicles.csv-row cleanup (P10B.5's mode), under D-ER1(a):
     - list the model years of the 461 `epa_vehicles_csv` rows;
     - run `import_epa_to_dictionary --years <those years>` against the pinned source as its own tracked-CSV commit, with a dry run and a reviewed diff first (these years are regenerated, not patched);
     - run the builder dry run, then `--apply` (new `dictionary_epa` rows);
     - take a link snapshot;
     - run `reresolve_legacy_catalog_links --source epa_vehicles_csv` as a dry run, owner OK, then apply;
     - verify: `link_cars_to_catalog --dangling` reports 0, and the archived ids are restorable (`--undo` tested on a copy).
     Under D-ER1(b), skip this step.
  The family tier reads flags from id'd rows only. Rollback: `--restore`; for step 6, `--undo` plus `git revert` of the CSV commit.
- Accept: the dry-run transitions equal the applied ones; the `epa_master` row count is unchanged after steps 1-5; after step 6, the only rows left with `source='epa_vehicles_csv'` are the still-referenced ones the report listed.

**P10D.5: cars heal (local).**
- Change (MAIN):
  - Preconditions: no scanner lock on any host; every node runs the 10C release; `pg_stat_activity` is clean.
  1. `heal_forced_induction_from_evidence` dry run.
  2. The owner reviews the transitions and the conflict sample.
  3. `--apply` (backup first).
  4. Invariants (b) and (c) drop. Record the new baselines with a history line; this unit owns that closing step.
  5. Check with a query that no car whose engine_description says turbo or supercharged lost its label.
  6. Rebuild the grid cards offline with `FLASK_ENV=production`; about 100k row writes move `xmin`.
  7. Write a `_learning` lesson. Following the project's dealer-logs rule and the P8A.7 precedent, group the changed rows by dealer_id and append one dated line per dealer to its summary.md (counts per transition; the heal report CSV is linked). No per-dealer scan blocks are needed, because this is a label repair, not a scan.
  After the next fleet night, re-run the dry run; it should show about 0 transitions.
- Accept: the backup exists before any UPDATE; the applied transitions equal the reviewed ones; every dealer with a changed row has a dated summary.md line.

**P10D.6: Prod rollout.**
- Change: the owner approves each dry-run → apply step. The Railway window is closed for the night.
  1. Read-only checks compared with local: `schema_migrations`, `epa_master` count/min/max, the provenance split via `epa_master_dump_id_map`, the extended-specs count, dangling links across all listings. Use prod's numbers, not local's.
  2. Full `pg_dump -Fc`, verified non-empty, with a client version at least the server's major (Docker postgres:18 if needed, owner OK). Plus a prod link snapshot table.
  3. Confirm V030 is applied; otherwise run `--target 30`.
  4. Provenance backfill: dry run → apply.
  5. Builder dry run (expect about 0 changes).
  6. Legacy re-resolve: dry run → owner review → apply.
  7. `link_cars_to_catalog --dangling`.
  8. `epa_master` label refresh (P10D.4 steps 1-5), then the vehicles.csv-row cleanup (P10D.4 step 6). The CSV commit for those years is already deployed with the 10D patch release, so prod runs only the builder `--apply` and the re-resolve, each dry run → owner review → apply.
  9. cars heal (P10D.5 steps; set-based over the public DSN), including the grouped per-dealer summary lines.
  10. Invariants.
  11. Restart web to clear the lru caches (`railway redeploy` is fine here because the code is already deployed). Rebuild the grid cards. Spot-check 10 VDPs, including 2 hybrids.
  Every step has its rollback ready: `--undo`, `--restore`, the snapshot tables.
- Accept: on prod, `epa_link_dangling_all_listings` = 0; the extended-specs count is unchanged from step 1; the backups exist before the first write; every dry-run output was shown to the owner.

**P10D.7: Constraints.**
- Change: V031, with `SET LOCAL lock_timeout '5s'`:
  - `CREATE UNIQUE INDEX ux_epa_master_source_natural_key ON epa_master(source, natural_key)`;
  - a cars FK on `epa_master_id → epa_master(id) ON DELETE RESTRICT NOT VALID`;
  - drop the CASCADE extended-specs FK and re-add it as `RESTRICT NOT VALID`.
  Change inventory_pg.py:481 from CASCADE to RESTRICT for parity until P13B.1. Reorder `migrate_inventory_sqlite_to_postgres._TABLES` (:38-47) so epa_master comes before cars; today cars is copied first and the FK would break it.
  V032 (`VALIDATE CONSTRAINT` on both) ships in a LATER release, only after a read-only prod check shows 0 dangling links across all listings.
  Merge V031 only after P10D.6, because every `migrate --apply` and every prod boot applies all pending files.
  Rollback note: drop the FKs before any `pg_restore` of `epa_master`.
- Tests (scratch Postgres): deleting a linked row raises; deleting an extended-specs row's parent is refused, not cascaded; V032 fails clearly when a dangling link exists.
- Accept: the tests pass.

**P10D.8: Delete DICTIONARY/.**
- Change, after the prod provenance backfill (it re-reads `DICTIONARY/`):
  1. Content pre-flight. EPA: 31,825 of 31,830 rows are identical; the other 5 are superseded MPG values. Options: list the 13 files richer than their counterparts for the owner (e.g. 2020 MINI Clubman, 15 rows vs 2; the one inspected was Wikipedia junk).
  2. `git rm -r DICTIONARY`; remove `.dockerignore:88` and `.railwayignore:98`.
  3. Reword the docstrings and comments (clean_vehicle_csvs.py:6, import_epa_to_dictionary.py:10/202/215, verified_specs/result.py:75, verified_specs/sources.py:80, knowledge_engine.py:506) and README.md:34.
  Not parallel with other knowledge_engine or verified_specs edits.
- Accept: git grep finds no live path reference; `build_epa_master_pg --dry-run` works; an import smoke of the touched modules passes.

**P10D.9: Docs.**
- Change:
  - data_architecture_plan.md:
    - the induction precedence, next to the engine-facts precedence, matching `resolve_car_forced_induction`;
    - the catalog id contract: ids are permanent; rows are archived, never deleted while referenced; RESTRICT FKs; one ingestion path (D-ER1); the relink procedure; restart web after catalog loads.
  - The importer docstring: `--patch-induction-only`, `--verify`, `--years`, the pinned-vintage practice.
  - README.md:63-70: the Postgres builder (dry run, then `--apply`). Today it names a nonexistent `backend.scripts.build_epa_master` and SQLite. Never recommend `--rebuild`.
  - build_trim_ladders_from_epa.py:19.
  - NETWORK_SCAN_PROCESS.md:154 and backend/dictionary/README.md "Import new data".
  - Mark datascripts.md P3 ("three EPA ingestion paths") resolved.
  - `_learning` notes (MAIN, post-merge): synthesized catalog facts must come from source columns; the provenance correction (`backfill_epa_master_fields.py`, not `build_epa_master.py`); the Postgres `\b` pitfall.
- Accept: every command in the docs runs `--help`; no doc tells anyone to run a full reload.

**Workflow shape.** Wave A: P10D.1, P10D.2 (tracked files, worktrees). Wave B: P10D.4 (MAIN). Wave C: P10D.3 (worktree; reads the CSVs as patched and as regenerated in P10D.4 step 6), P10D.5 (MAIN). Then a patch release with deploy, so the CSVs reach the image. Wave D: P10D.6 (owner). Wave E: P10D.7, P10D.8. Wave F: P10D.9 (it shares README.md and import_epa_to_dictionary.py with P10D.8). Review lens: CSV diffs touch only the declared columns; every repair can be rolled back; FK migrations merge only after the repairs.

**Data repair.** P10D.1-P10D.6, each backup → dry run → owner OK → apply → verify.

**Exit gate.**
- Invariant (a) is 0 locally and in prod. (b) and (c) are at their new baselines.
- `epa_link_dangling_all_listings` = 0 in prod.
- `DICTIONARY/` is gone, the docs are merged, and V031 is applied everywhere.

**Release steps.** (1) A patch release after P10D.1-P10D.5 (data commits), deployed before P10D.6. (2) A minor release with V031, P10D.8 and P10D.9, with V031 applied to prod first. (3) V032 rides the next release (Phase 11A) after the 0-dangling check.

## Phase 11A: Scanner runtime: lock exit codes, cross-host dealer lock, upsert telemetry

**Goal.** A scanner refused by its lock exits with code 75 instead of 0 plus a log line, and every caller handles it. Lock-skipped dealers are never routed to discovery as errors. One dealer cannot be written by two processes or hosts at once. The lock file is atomic (flock). The write path stops using `utcnow` without changing its stored format, reports where upsert time goes, and retries transient connection errors.

**Entry gate.** Phase 10D released. D-SR1 (P11A.5) and D-SR2 (P11A.3) answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P11A.1 | scanner-runtime-2 (+ reviewer) | Scan lock: path resolved once per run, one liveness rule, `ScanLockHeld`, exit 75 | code | backend/scanner/scan_lock.py, backend/scanner/orchestrator.py, backend/scanner/cli.py, backend/scanner/pipeline/runner.py, backend/tests/test_scan_lock_exit_code.py (new) | M | A | none |
| P11A.2 | scanner-runtime-3 (+ reviewer) | Pipeline: shared launcher that retries on 75; `scan_skipped` carried through assess, lifecycle and triage | code | backend/scanner/pipeline/{runner,run,assess,lifecycle,triage}.py, backend/scripts/dealer_pipeline.py, backend/tests/test_pipeline_lock_retry.py (new), test_dealer_pipeline_surface.py | M | B | P11A.1 |
| P11A.3 | scanner-runtime-4 (reframed) + scanner-runtime missing + infra-5 (+ reviewers) | Per-dealer Postgres advisory lock in dealer_run and delta_scan; delta takes the file lock | code | backend/scanner/phases/dealer_run.py, backend/scanner/delta_scan.py, backend/scanner/cli.py, backend/scanner/pipeline/{run,assess,lifecycle,triage}.py, backend/tests/test_dealer_advisory_lock.py (new), test_delta_scan_lock.py (new), docs/RAILWAY_SCANNING.md (kill switches) | M | C | P11A.1, P11A.2, P6A.1 |
| P11A.4 | scanner-runtime-5 (+ reviewer) | Atomic lock with `fcntl.flock` | code | backend/scanner/scan_lock.py, backend/tests/test_scan_lock_flock.py (new) | S | B | P11A.1 |
| P11A.5 | scanner-runtime-21 | Worker loop: a lock-held exit is not a successful job | code | scripts/scanner_worker_loop.py, backend/tests/test_scanner_worker_lock_held.py (new) | S | B | P11A.1 |
| P11A.6 | db-layer-14 + scanner-runtime-14 | Replace `datetime.utcnow()`, keeping the stored format byte-identical | code | backend/scanner/upsert/serialize.py, backend/scanner/database.py, backend/scanner/upsert/guard.py, backend/routes/home_dashboard.py, backend/tests/test_upsert_vin_owner_guard.py, test_utc_now_z.py (new) | S | A | none |
| P11A.7 | scanner-runtime-15 (+ reviewer) | Upsert timing breakdown, wall and thread CPU | code | backend/scanner/database.py, backend/scanner/upsert/post_write.py, phases/dealer_run_steps/persist.py, backend/scanner/inventory_write.py, backend/tests/test_upsert_timing_breakdown.py (new) | S | B | P11A.6 (both edit database.py) |
| P11A.8 | scanner-runtime-19 (+ reviewer) | Coordinator retries transient connection errors with longer backoff; retry counts | code | backend/scanner/inventory_write.py, backend/tests/test_upsert_retry_transient_20261007.py (new) | S | C | P11A.7 |

**P11A.1: Lock exit code.**
- Change:
  - `EXIT_LOCK_HELD = 75` (EX_TEMPFAIL; no other script uses 75) and `class ScanLockHeld(RuntimeError)` carrying the holder pid and path.
  - `acquire`, `release` and `current_lock_holder` call `default_lock_path()` when no path is passed. `LOCK_PATH` stays importable through a module `__getattr__`.
  - `lock_holder_alive(path=None)` uses `_pid_alive`, where PermissionError means alive. runner.py:40-43 reads that case as dead today.
  - `orchestrator.main` computes `lock_path` ONCE and passes it to acquire, release and the message, so a mid-run env change cannot split them. It keeps the refusal log line, since older pipelines scrape it during rollout, and raises `ScanLockHeld`. `cli.run_cli_entry` catches it and exits 75.
  - `runner._lock_holder_alive` delegates to the new function. `runner.LOCK_FILE` and the facade re-exports (dealer_pipeline.py:119-120) stay.
  - Grep the consumers: scripts/scanner_worker_loop.py, deploy/nightly_http_refresh.sh, dealer/admin/routes.py:365, dev/scan_lab.py:473, scripts/scan_irvinebmw_test.sh, and the k8s/car-scanner cronjobs (restartPolicy OnFailure; dead per D-IF9, deleted in P18A.7).
- Tests:
  - With the lock held, `main` raises before `_main_impl` and the CLI exits 75.
  - Changing `SCANNER_LOCK_PATH` after import takes effect without a reload.
  - A PermissionError pid counts as alive.
  - test_scan_lock_path_env.py and test_dealer_pipeline_surface.py pass.
- Accept: the tests pass, and the refusal log text is unchanged.

**P11A.2: Pipeline lock handling.**
- Change:
  - `run_scan_with_lock(batch, *, log_path, timeout_sec, lock_wait, max_attempts=6) -> {rc, attempts, lock_held, lock_wait_timeout}` replaces the inline loop at run.py:110-138 and its log scraping. It retries on 75 after 5 s plus jitter.
  - When the lock is never won, every dealer in the batch gets `recipe_info[did]['scan_skipped'] = 'scanner_lock_held'`, and the run goes on to the next batch. The flag is carried through:
    - assess reports "scan skipped: scanner lock held (N attempts)";
    - `route_verdict` returns action none for it, instead of the force re-synth plus browser capture that 'error' plus a `rejected:` status gets today (lifecycle.py:105-107);
    - `write_needs_discovery` skips it, where today every 'error' dealer goes to needs_discovery (triage.py:41-52);
    - triage shows it.
  - A lock-wait timeout aborts the remaining batches, as the message already says. Today, after a lost race with rc 0, the loop prints "aborting" but carries on.
  - `scan_retry_batch` uses the helper (its return shape plus `lock_held`). Re-export it from the facade and add it to the surface test.
- Tests: `[75, 75, 0]` → 3 launches and rc 0, without reading scanner.log; six 75s → the dealers are marked skipped and the next batch runs; a lock-held dealer is not in needs_discovery and gets no lifecycle action; the timeout-after-lost-race case really aborts.
- Accept: the tests pass, grep finds no "Scanner already running" in backend/scanner/pipeline/, and the lifecycle tests pass.

**P11A.3: Per-dealer exclusion.**
- Change:
  - `run_dealer` and `delta_scan_dealer` take `try_lock_dealers([dealer_id])` (P6A.1) around replay → write → reconcile, on one lock connection per scanner process, reused across dealers. Check the P0A.2 connection headroom (N shards × 1).
  - A dealer held elsewhere gets result `skipped_locked`, with the holder's application_name, and no write and no reconcile. `SCANNER_DEALER_LOCKS=0` disables the lock. This is the mechanical form of "shards never share dealers". It also covers ad-hoc `scanner.py --dealer-id` runs and the worker loop, which bypass the pipeline.
  - In dealer_pipeline, `skipped_locked` is handled exactly like P11A.2's `scan_skipped`: assess reports "scan skipped: locked by <holder>" instead of 'error', `route_verdict` takes no lifecycle action, `write_needs_discovery` skips the dealer, and triage shows it. Before `ensure_recipe`, the pipeline probes each dealer's lock (`try_lock_dealers`, then release) and skips recipe synthesis for held dealers, so a dealer another host is scanning never has its recipes rewritten underneath it.
  - `scanner.py --delta` also takes the file scan lock (cli.py:377-385) and exits 75 when it is held.
  - Extend the P6A.4 kill-switch table.
- Tests: a fake helper reporting a dealer held → skipped and not written; a held dealer gets no ensure_recipe call, no lifecycle action and no needs_discovery entry; SQLite is a no-op; a Postgres test with two processes contending, run once locally with the output in the PR; the delta lock.
- Accept: the tests pass.

**P11A.4: flock.**
- Change: `acquire` opens the file with O_CREAT and takes `flock(LOCK_EX|LOCK_NB)` on a descriptor kept open for the life of the process. It writes the PID only for diagnostics. Once flock succeeds, stale PID content is ignored; today a recycled PID after SIGKILL can wedge the lock. `lock_holder_alive` probes the flock (try, then release) and uses the PID only in messages. `release` unlocks, and removes the file only if this process holds it.
- Tests: two subprocesses racing for 50 rounds → exactly one winner per round; after SIGKILL of the holder, the next acquire succeeds immediately.
- Accept: the tests pass.

**P11A.5: Worker exit.** `_run_dealer_scan` (scanner_worker_loop.py:63-77) maps rc 75 to `(False, 'lock_held', result)`, so `record_catalog_after_success` is not called. The Railway worker services are gone (P0A.4), but docker-compose and P15A.3 still use the loop. Tests: rc 75 → `ok=False`, `err='lock_held'`; rc 0 behaviour is unchanged.

**P11A.6: utcnow.**
- Change: `utc_now_z()` returns `datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + 'Z'`, byte-identical to today's string. That matters because `scraped_at` is TEXT, compared lexicographically against the guard cutoff (guard.py:33) and `since_iso`. Use it at database.py:386 and guard.py:111, and in the test helpers (test_upsert_vin_owner_guard.py:88,103). home_dashboard.py:65 becomes `datetime.now(timezone.utc)`; its template reads only `now.year`. Do NOT use `spec_provenance.utc_now_iso`, which gives '+00:00' and drops microseconds.
- Tests: for a frozen instant, equality with the old string, with and without microseconds; no DeprecationWarning under `-W error::DeprecationWarning`.
- Accept: the goldens are unchanged, and grep finds no `utcnow()` outside tests.

**P11A.7: Timing breakdown.**
- Change: `stats['timing']` gets prefetch_s, write_rows_s, commit_s, rows, the per-row SELECT count, and a post_write block {incomplete_sync_s, spec_backfill_s, model_specs_s, catalog_link_s, cars, get_conn_calls, vpic_network_fetches}. Record both `perf_counter` and `time.thread_time()` per component, plus the dealer concurrency in effect. Upsert phase time is wall time, which includes vPIC network I/O and GIL contention, so it does not prove CPU cost. persist.py copies the block to `result['phase_secs']['upsert_breakdown']` (already whitelisted). Retry counts live in the coordinator, because `upsert_vehicles` resets stats on every attempt.
- Tests: every key is filled on a SQLite golden upsert, and the parts sum to within 10% of the wall time.
- Accept: the tests pass, and the goldens differ only in the new subkey.

**P11A.8: Transient retries.**
- Change: `_is_retryable_db_error` (inventory_write.py:74-91) adds SQLSTATE class 08, 53300 and 57P01, plus a psycopg OperationalError without a sqlstate whose message reads as a lost or refused connection. Connection-class errors back off 2, 4 then 8 s with jitter, capped, because 53300 under 8 shards will not clear in a second. Deadlocks keep `0.5*attempt + random(0, 0.5)`. 57014 (statement timeout) is not retried. Retry counts go to `stats['timing']['upsert_retries']` from the coordinator.
- Tests: a deadlock raised after the first 200-row commit, then retried, leaves rows identical to a clean run and one price-history entry per changed price; a connection loss is retried; 57014 is not.
- Accept: the tests pass, and test_upsert_deadlock_retry.py is unchanged.

**Workflow shape.** Wave A: P11A.1, P11A.6. Wave B: P11A.2, P11A.4, P11A.5, P11A.7 (after P11A.6; both edit database.py). Wave C: P11A.3, P11A.8. Review lens: exit codes are never swallowed; no dealer is reported as an error when it was only skipped; the lock connection is never idle in transaction; the timestamp format is byte-identical.

**Data repair.** None.

**Exit gate.** pytest: test_scan_lock_exit_code.py, test_scan_lock_path_env.py, test_pipeline_lock_retry.py, test_dealer_pipeline_surface.py, test_recipe_lifecycle.py, test_dealer_advisory_lock.py, test_delta_scan_lock.py, test_scan_lock_flock.py, test_scanner_worker_lock_held.py, test_utc_now_z.py, test_upsert_vehicles_golden.py, test_upsert_vin_owner_guard.py, test_upsert_timing_breakdown.py, test_upsert_retry_transient_20261007.py, test_upsert_deadlock_retry.py. The local Postgres advisory-lock output is attached.

**Release step.** Minor. It also carries V032 (`VALIDATE CONSTRAINT`), but only if a read-only prod check shows 0 dangling links across all listings; apply V032 on prod first. Deploy scanner-nightly outside the window, and pull on the mini.

## Phase 11B: Upsert cost

**Goal.** Measure where upsert time goes, then remove the per-car connection churn and the per-row price read, and batch the Postgres writes behind a parity gate. On 2026-09-28 the upsert phase took 173.6 ms per row and 26.7% of dealer wall time; small lots cost 556 ms per row.

**Entry gate.** Phase 11A released. D-SR6 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P11B.1 | scanner-runtime-16 | Measure the breakdown on 3 real dealers at batch 1 and batch 6 (MAIN) | investigation | dealer scan_runs.md | S | A | P11A.7 |
| P11B.2 | scanner-runtime-17 (+ reviewer) | Post-write enrichment borrows one read connection | code | backend/scanner/upsert/post_write.py, backend/tests/test_post_write_borrowed_conn.py (new) | S | B | P11B.1 |
| P11B.3 | scanner-runtime-18a | Previous price read moves into the prefetch | code | backend/scanner/upsert/guard.py, backend/scanner/upsert/write.py, backend/tests/test_upsert_prefetch_price.py (new) | S | B | P11B.1 |
| P11B.4 | scanner-runtime-18b (+ reviewer) | Batched Postgres upsert with `executemany(..., returning=True)` behind a parity gate | code | backend/scanner/upsert/{write,sql}.py, backend/scanner/database.py, backend/scripts/upsert_parity_check.py (new), backend/tests/test_upsert_batched_parity_20261007.py (new) | M | C | P11B.2, P11B.3 |

**P11B.1: Measure.** On the D-SR6 host, on AC power, with one shard, pick 3 dealers: under 200 rows, about 500, and over 1,000, none near the 40-page cap (or run with `--no-reconcile`). Run each with `dealer_pipeline --dealers <id> --no-lifecycle`, once at `--batch 1` and once at batch 6, so intrinsic cost and contention can be told apart. Read `phase_secs.upsert_breakdown` with read-only SELECTs. Compare against the baseline: 173.6 ms/row overall; 556 ms/row under 200 rows; 97 ms/row over 1,000; 26.7% of wall time. Dealer logs are written. Accept: a ms/row table per component (wall and thread CPU) with vPIC fetch counts, plus a recommended order for P11B.2-P11B.4 with expected savings.

**P11B.2: Borrowed connection.** Wrap the loop in `sync_incomplete_and_backfill_specs` (post_write.py:59-67) in `with inventory_pg.borrow_read_connection():`. That helper (inventory_pg.py:33-60) was built for exactly this "~3 Postgres connections per car" case and is already used at incomplete_listings_db.py:220. Its wrapped `close()` rolls back, so no transaction stays idle across vPIC I/O. Respect database.py:181's note: no `SET` on a borrowed connection. DDL is not repeated per car (`_PG_INC_SCHEMA_OK` is cached per process); the cost is connection churn. Tests: with a counter on a monkeypatched `pg_connect`, connections stay O(1) per upsert; rows are equal to before; the golden is unchanged. Accept: the P11B.1 re-run shows the post_write share dropping.

**P11B.3: Price in the prefetch.** `prefetch_existing` also returns `price` and `price_provenance_json` per VIN, in the same chunked SELECT. That removes the per-row `fetch_prev_price_row` at write.py:40. The price-history read-modify-write semantics are unchanged. Tests: SQLite parity of the cars rows and the price history. Accept: the cursor spy shows no per-row SELECT.

**P11B.4: Batched writes.**
- Change: on Postgres, send chunks of 200 (the existing commit cadence) through `cursor.executemany(UPSERT ... ON CONFLICT (vin) DO UPDATE ... WHERE <guard> RETURNING vin, returning=True)`. VINs missing from RETURNING are the backstop refusals; this replaces the per-row rowcount check at write.py:57-60.
  - Keep: the sorted-VIN order (the deadlock fix), `ON CONFLICT DO UPDATE` (never DO NOTHING), the price-history semantics (consider computing the append inside DO UPDATE so no read is needed), and the public signature `upsert_vehicles(vehicles, stats)`, which dealer/portal_sync.py:169 also calls.
  - SQLite keeps the per-row path; Python's sqlite3 `executemany` cannot return rows.
  - Required gate: `upsert_parity_check.py` writes the same 1,000-row fixture (with guard conflicts and price changes) per-row and batched into two scratch databases on local Postgres, never into `cars`, and diffs them. A pytest marker runs it when a scratch PG DSN is set.
- Accept: zero diffs; test_upsert_vehicles_golden, test_upsert_vin_owner_guard and test_upsert_deadlock_retry pass; the P11B.1 re-run shows write_rows ms/row reduced, with numbers.

**Workflow shape.** Wave A: P11B.1 (MAIN). Wave B: P11B.2, P11B.3. Wave C: P11B.4. Then re-run P11B.1. Review lens: Postgres parity is proven on scratch DBs; no DO NOTHING; the deadlock ordering is kept.

**Data repair.** None.

**Exit gate.** pytest: test_post_write_borrowed_conn.py, test_upsert_prefetch_price.py, test_upsert_batched_parity_20261007.py, test_upsert_vehicles_golden.py, test_upsert_vin_owner_guard.py, test_upsert_deadlock_retry.py. The parity script reports zero diffs, and the before/after measurement table is in the PR.

**Release step.** Minor. Deploy scanner-nightly outside the window.

## Phase 12A: One scanner fetch policy

**Goal.** Every scanner HTTP path follows named policies. One block set, profile rotation that starts from the host's last winner, matching UA and TLS profiles, challenge pages treated as their own outcome, and the scanner proxy applied to every dealer-site fetch. Feed replay gets synth's HTML-edge handling, so validation can use the scan's own fetcher.

**Entry gate.** Phase 11B released. P0B.1 numbers available. D-OD3, D-OD7, D-OD8 and D-OD13 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P12A.1 | owner-decisions-3 + hygiene-17 (net/client docstring) | Named `FetchPolicy` values; `HostProfileMemory`; stale docstring fixed (no behaviour change) | code | backend/scanner/net/client.py, backend/scanner/chain.py (alias), backend/tests/test_net_client_policy.py (new) | S | A | none |
| P12A.2 | owner-decisions-4 (+ reviewer) | Feed replay on the decided policy; HTML-edge parity with synth; challenge outcome | code | backend/scanner/recipes.py (`_replay_request`, `_replay_impersonated`), backend/tests/test_scanner_http_characterization.py, test_recipes_replay_html.py (new) | M | B | P12A.1 |
| P12A.3 | owner-decisions-7 (+ reviewer) | Reachable stray scanner HTTP sites onto net/client; requests fallback rejects challenges | code | backend/scanner/pipeline/recipes.py, synth/platforms/wp_vehicles.py, post_scan/gap_fill.py, scrapers/{autowall,dealer_eprocess}.py (if reachable), backend/tests/test_scanner_http_stray_sites.py (new) | M | B | P12A.1 |
| P12A.4 | owner-decisions-6 (+ reviewer) | Chain: requests path uses the proxy; impersonation rejects challenges | code | backend/scanner/chain.py, backend/tests/test_scanner_http_characterization.py, test_scraper_chain.py, test_listing_gap_fill.py | S | C | P12A.2, P12A.3 |
| P12A.5 | owner-decisions-5 (+ reviewer) | VDP prefetch and VDP JSON on the policy (rotation never inside slow-host serialization) | code | backend/scanner/vdp/prefetch.py, backend/scanner/vdp/vdp_recipes.py, backend/tests/test_scanner_http_characterization.py, test_vdp_prefetch.py, test_vdp_recipes.py | M | D | P12A.4 |
| P12A.6 | owner-decisions-8 | Script curl_cffi call sites onto net/client | code | backend/scripts/discovery_probe.py, backend/scripts/audit_unscannable_dealers.py, backend/tests/test_script_http_sites.py (new) | S | B | P12A.1 |

**P12A.1: Named policies.**
- Change:
  - A frozen `FetchPolicy(timeout, block_statuses, profiles, check_challenge, min_bytes, ua_when_impersonating, retry_statuses, retries, backoff_s)`.
  - Named policies that hold today's exact per-site values: FEED_REPLAY, SYNTH_HTML, SYNTH_JSON, VDP_HTML, VDP_JSON, CHAIN_IMPERSONATE, CHAIN_REQUESTS, STATUS_PROBE.
  - `FINGERPRINT_BLOCK_STATUSES = {403, 405, 429, 503}`, defined but not yet used by any site.
  - A process-wide, thread-safe `HostProfileMemory`, moved from `chain.ImpersonatingFetcher._winning` (alias kept). This changes gap_fill slightly: it builds a new fetcher per call (gap_fill.py:153), so its memory used to last one call. Say so in the PR.
  - Fix the stale module docstring (net/client.py:22-25): replay now passes `scanner_proxies()` (recipes.py:718), and VDP prefetch rejects challenge pages (vdp/prefetch.py:366-395).
- Tests: the policy values equal today's constants; thread safety.
- Accept: the tests pass, and test_scanner_http_characterization.py passes unchanged.

**P12A.2: Replay policy.**
- Change: `_replay_request` uses FEED_REPLAY with the D-OD7 values.
  - 403/405 rotate profiles, starting from the host's remembered winner. 429 gets backoff and one retry (per D-OD8), never rotation. 503 is retried once, then rotated. That avoids 1+3+1+3 requests for one rate-limited page.
  - When impersonating, the profile sets its own User-Agent, unless P0B.1 showed a loss. Today the explicit Chrome/124 UA is sent even under safari17_0 (recipes.py:735-750, :776-785).
  - HTML paginations send `synth.http._browser_headers()`, plus a same-site Referer and `Sec-Fetch-Site: same-origin`.
  - A 200 challenge page escalates to impersonation. If it is still a challenge, return a distinct outcome (status 0, reason `challenge`) that the stop-reason report records as `blocked:challenge`. It never triggers `mark_stale('http_403')`, because intermittent Cloudflare challenges must not stale recipes.
  - Add a `before_attempt` pacing hook, off by default; P12B.3 turns it on for validation.
  - Characterization edits are limited to 503/405 escalation, the HTML challenge outcome and the HTML header set, and each edit cites its decision id.
- Tests:
  - An El Cajon-shaped DEP fixture whose stubbed edge requires a same-site Referer: replay and synth get the same VINs.
  - A challenge never stales a recipe.
  - 429 is never rotated.
- Accept: the JSON replay wire format is unchanged, apart from the UA rule.

**P12A.3: Stray sites.**
- Change:
  - First establish reachability under HTTP-only. The dealer_inspire, dealer_venom and dealer_eprocess `*_from_page` paths need a page, which an HTTP-only scan never has; hand those to P17B and do not migrate them.
  - Migrate the reachable sites:
    - the pipeline status probe (pipeline/recipes.py:36-44) on STATUS_PROBE;
    - `wp_vehicles._fetch_wp_vehicles` (:25-37) on `rotate_impersonation` plus `synth.http._pace`;
    - gap_fill's `_requests_fetch_html` (:51-63), which also rejects `looks_like_challenge`; P12A.4 relies on that;
    - the autoWALL Sessions (:321, :373, :513) set `session.proxies`;
    - the dealer_eprocess bare urllib call (:825-828), via `http_fetch.open_url`, if reachable.
  - Per D-OD13, the DuckDuckGo spec search at gap_fill.py:235 is excluded from `SCANNER_HTTP_PROXY`.
  - post_scan/window_sticker.py is out of scope ("leave OEM sticker fetch alone").
- Tests: a per-site test that dealer-site fetches pass the proxy.
- Accept: `git grep "from curl_cffi" backend/scanner` matches only net/client.py; the reachability findings are in the PR.

**P12A.4: Chain.** `RequestsFetcher` passes `scanner_proxies()`. `ImpersonatingFetcher.accept()` also requires `not looks_like_challenge(text)`: it keeps rotating and raises FetchError when every profile fails. It uses HostProfileMemory. Characterization edits: the no-proxy test now expects the proxy; the challenge-accepted test now expects rotation and then FetchError. Accept: a gap_fill fixture where both fetchers get a challenge returns None; test_scraper_chain and test_listing_gap_fill pass.

**P12A.5: VDP policy.**
- Change: in prefetch `_fetch_html` (~:346-401) and `vdp_recipes._fetch_json` (~:248-288):
  - start from the host's remembered winner, so a healthy host costs 1 request per VDP;
  - rotate only on a host's first, non-slow attempt, and only on 403/405;
  - never rotate on 429 or inside the slow-host serialization, which holds 1 request per 1.5 s (prefetch.py:631-640);
  - 405 joins the block set;
  - vdp_recipes falls back to requests on a curl_cffi error;
  - the 15 s timeouts become VDP_HTML and VDP_JSON;
  - the slow-host serialization, the cooldown and the challenge rejection added in 1344cbd09 are untouched; `_LAST_STATUS` semantics are preserved for `_SLOW_HOST_MAX_FAILS`.
- Tests: 1 request per VDP on a healthy host; a slow host gets exactly 1 request per attempt; characterization edits stay within these items.
- Accept: test_vdp_prefetch.py and test_vdp_recipes.py pass.

**P12A.6: Scripts.** discovery_probe.py `_get` (:56-75) and audit_unscannable_dealers.py (:118-135) use `net_client.import_curl_cffi` / `rotate_impersonation` and `scanner_proxies()`. discovery_probe keeps a single 'chrome' probe if its output depends on it; otherwise it rotates and records the profile in the probe record. Accept: no `from curl_cffi` remains in either script; the probe record gains only a `profile` field.

**Workflow shape.** Wave A: P12A.1. Wave B: P12A.2, P12A.3, P12A.6. Wave C: P12A.4. Wave D: P12A.5. test_scanner_http_characterization.py is shared, so P12A.2, P12A.4 and P12A.5 run serially. Review lens: every characterization edit is listed and justified; no rotation on 429; no stale on challenge; window_sticker is untouched.

**Data repair.** None.

**Exit gate.** pytest: test_net_client_policy.py, test_scanner_http_characterization.py, test_recipes_replay_html.py, test_scanner_http_stray_sites.py, test_scraper_chain.py, test_listing_gap_fill.py, test_vdp_prefetch.py, test_vdp_recipes.py, test_script_http_sites.py, test_autowall_http_recipe_20260924.py, test_dealer_venom.py.

**Release step.** Minor: push and tag. Deployment waits for the P12B.9 canary.

## Phase 12B: One recipe validation rule; under-collection repair

**Goal.**
- Validation judges recipes with the scan's own store place, walker and fetcher.
- One walk decides pass/fail and `vehicle_rows` at save time.
- One minimum-VIN rule that keeps real small sides but not widgets.
- Cascade recipes go through the gate.
- Every live recipe is re-judged once.
- Release behind a canary shard, then rescan the dealers whose inventory was under-collected.

**Entry gate.** Phase 12A released. P6A.6 hygiene applied locally and on prod. D-OD1, D-OD2, D-OD4, D-OD5 and D-OD6 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P12B.1 | owner-decisions-10 (+ reviewer) | Validation uses the scan's store place (`DealerCtx` built once per call) | code | backend/scanner/recipe_validation.py, backend/tests/test_recipe_validation.py | S | A | none |
| P12B.2 | owner-decisions-9a + scanner-runtime-10b | `check_all`/`judge` split; raw-VIN termination and short_page in `_check_recipe` | code | backend/scanner/recipe_validation.py, backend/tests/test_recipe_validation_walker.py (new) | M | B | P12B.1 |
| P12B.3 | owner-decisions-9b (+ reviewer) | `validate_recipe` becomes a paced count over the replay walker; junk post_template mirrors the scan | code | backend/scanner/recipe_validation.py, backend/scanner/synth/validate.py, backend/scanner/recipe_synth.py, backend/tests/test_recipe_synth.py, test_recipe_synth_surface.py | M | C | P12B.2, P12A.2 |
| P12B.4 | owner-decisions-9c (+ reviewer) | Save paths do one full walk, then judge the cached checks | code | backend/scanner/pipeline/recipes.py, backend/scripts/synthesize_recipes.py, backend/scanner/recipes.py (`promote_from_ledger`), backend/tests/test_recipe_validation.py, test_recipe_lifecycle.py | M | D | P12B.3 |
| P12B.5 | owner-decisions-12 + scanner-runtime-20 (+ reviewer) | One minimum-VIN rule (D-OD1) at save and at scan | code | backend/scanner/recipe_totals.py, backend/scanner/pipeline/recipes.py, backend/scripts/{synthesize_recipes,cascade_recipes}.py, backend/scanner/recipe_cascade.py, backend/scanner/recipe_validation.py, backend/scanner/recipes.py, backend/tests/test_recipe_min_vins_rule.py (new) | M | E | P12B.4 |
| P12B.6 | owner-decisions-13 (+ reviewer) | Cascade recipes go through the gate, with fall-through, and get dealer logs | code | backend/scanner/recipe_cascade.py, backend/scripts/cascade_recipes.py, backend/tests/test_recipe_cascade.py (new) | M | F | P12B.5 (both edit recipe_cascade.py and cascade_recipes.py) |
| P12B.7 | owner-decisions missing (re-judge) | Re-judge every live recipe once under the unified gate (MAIN, then prod) | data_repair | backend/scripts/rejudge_live_recipes.py (`--all` mode), docs/monolith_audit_2026_10_01/measurements/rejudge_2026_10.md (new) | M | G | P12B.5, P12B.6 |
| P12B.8 | owner-decisions-14 | Docs and workflow text for the unified rule and fetch policy | docs | docs/HTTP_ONLY_SCANS_PLAN.md, docs/NETWORK_SCAN_PROCESS.md, .claude/workflows/dealer-discovery.js, docs/monolith_audit_2026_10_01/{OWNER_DECISIONS,README}.md | S | G | P12B.6 |
| P12B.9 | owner-decisions-24 (+ reviewer) | Release, local stage check, canary shard | ops, needs prod | VERSION, CHANGELOG.md, dealer logs | S | H | P12B.1-P12B.8 |
| P12B.10 | owner-decisions-25 (+ reviewer) | Rescan the under-collected dealers; audit prod cascade recipes (MAIN) | data_repair, needs prod | workspace/backups/, dealer logs | M | I | P12B.9 |

**P12B.1: Store place.** Build the `DealerCtx` ONCE per `validate_recipe_set` call, not per page; per page would be up to 40 × N registry and hint reads. Use `attribution.store_place(base_url, dealer_id)` (registry plus hinted street, read-only DB), merged key by key under any caller-supplied place, where the caller wins. Parse with `attribution.parse_page(provider, parsed, ctx, refused, trust_feed_scope=...)`, as the scan does at recipes.py:1095. When the DB is unavailable, fall back silently to the caller place or `{}`. Tests: the Honda of Huntersville shape (registry town only, street in hints) keeps rows with no caller place; the existing place tests; the DB fallback. Accept: the tests pass.

**P12B.2: Walker refactor.**
- Change: split `validate_recipe_set` into `check_all(recipes, pages) -> checks` and `judge(checks)`, with no behaviour change. `gate_recipes` gains `pages=` and `checks=`. `_check_recipe` terminates and judges short_page on RAW new VINs per page, kept plus refused (recipe_validation.py:640-665). It does not mark the walk exhausted when a page's kept rows are empty but refusals exist. `_parse_rows` returns the refused VINs, not a count (:640-641).
- Tests:
  - A verdict golden over the existing fixtures is unchanged.
  - A page of sibling rows no longer triggers short_page.
  - A used side with 30 VINs plus a new side with 0, where the probe shows new ≥ 5, is rejected as one_condition, as today. That rule relies on "ensure_recipe drops an empty side before the gate" (comment :770).
- Accept: the tests pass.

**P12B.3: validate_recipe as a count.**
- Change: `validate_recipe(...) -> int` returns `_check_recipe(recipe, fetch=paced(_replay_request), pages=max_pages, ...)[0].vins`. The paced wrapper honours `SCANNER_SYNTH_FETCH_DELAY`; operators set 4 s for sweeps (national_scan.py:28). `_validate_json_feed`, `_dep`, `_html_walk` and `_cosmos` become one-line delegates for one release, so test_recipe_synth_surface.py and .claude/workflows/dealer-discovery.js keep working; grep every consumer before deleting them later. D-OD6: a GET with a junk post_template replays without a body and the report notes it; a POST without a parseable body is an error, matching recipes.py:1036-1052.
- Tests: a recording fetch shows `validate_recipe` and `validate_recipe_set` use the same function with equal counts for cosmos, TV page_query, DEP, HTML page and JSON POST fixtures; every synth registry template emits a pagination the replay walker can page (iterate `PLATFORM_TEMPLATES`); test_recipe_synth.py's walker tests stub `fetch` instead of synth.http.
- Accept: the tests pass, and test_recipe_synth_surface.py passes unchanged.

**P12B.4: One walk at save.**
- Change: `ensure_recipe` (pipeline/recipes.py:60-92), `synthesize_recipes` (:219-252) and `promote_from_ledger` (recipes.py:462) walk each candidate once, with `pages=_VALIDATE_MAX_PAGES`. They filter with the current floors (P12B.5 changes the floors), then judge the cached checks; `vehicle_rows` is that walk's count. `lifecycle.validate_live_recipes` and discovery_probe keep `pages=2`. A full walk sets `exhausted=True`, which arms the coverage reject for group feeds; the P0B.1 verdict-flip list documents the expected changes.
- Tests: a counting fetch shows no more requests than today (a full walk plus a 2-page re-read becomes one walk); only the listed verdicts flip.
- Accept: the tests pass.

**P12B.5: Minimum-VIN rule.**
- Change: one constant, `RECIPE_MIN_VINS=5`, in recipe_totals.py.
  - Save paths keep candidates with more than 0 store VINs and require the kept set's union to reach 5.
  - At gate time, record gate provenance on each recipe (`gate_verdict`, `gate_at`).
  - The scan union admits a recipe below `SCANNER_RECIPE_MIN_VEHICLES` only when it is a condition-pinned side of a paginated inventory shape whose recorded verdict is ok or uncertain. Sub-floor recipes with `pagination=none` are refused outright. The union succeeds at 5 or more.
  - First-hit replay accepts at least `min(global floor, max(3, ceil(0.5 × vehicle_rows)))` VINs.
  - `SCANNER_RECIPE_MIN_VEHICLES` stays as an override.
  - promote's `min_vehicle_rows=3` is documented as a candidate prefilter. `OTHER_SIDE_MIN_VINS` is decided explicitly (keep 5).
  - Attach the per-dealer union diff from re-running P0B.1's offline listing for the retirement owner.
- Tests:
  - 3+3-VIN feeds are kept by ensure_recipe and by the CLI.
  - The McLaren shape (used 28 plus a condition-pinned, paginated, gate-ok new side of 8) gives a union of 36; today it gives 28.
  - A 1-VIN vehicles-recommendations recipe stays out.
  - A dealer whose only recipe yields 4 VINs is `validated_zero` at save and None at scan.
  - A recipe saved with `vehicle_rows=6` replaying 6 returns records; a 1-VIN replay of a 40-row recipe is refused.
  - grep finds no literal 3/5/10 floors outside the constant.
- Accept: the tests pass.

**P12B.6: Cascade gate.** `cascade_for_dealer` takes an accept/gate callback, so a rejected shape falls through to the next one; today the first winning shape ends the loop (recipe_cascade.py:244-251). cascade_recipes.py runs `gate_recipes(context='cascade', record=not dry_run)` before saving. A reject saves nothing and is logged to discovery.md; `--dry-run` writes nothing. Tests (none exist today): adapt_recipe's host substitution; the tenant-id refusal; the `_looks_like_donor_inventory` guard; a reject saves nothing and the next shape is tried; dry run writes nothing; a success writes discovery.md with context `cascade`. Accept: the tests pass.

**P12B.7: Re-judge live recipes.**
- Change: `rejudge_live_recipes.py --all` runs a paced `validate_recipe_set` over every live recipe and writes nothing; it produces a report. Recipes saved before the set gate existed (2026-09-28) are otherwise never re-validated: lifecycle re-validates only after a failing capture (pipeline/lifecycle.py:246). The owner approves the stale list. Then: backup (sha256 manifest plus `\copy`); apply marks each recipe stale with reason `rejudge:<verdict>` and writes discovery.md per dealer. Run locally (MAIN), then on prod from a home IP, owner-run.
- Accept: the report doc exists; only owner-approved ids change; backups exist before the writes.

**P12B.8: Docs.** HTTP_ONLY_SCANS_PLAN.md:95; the NETWORK_SCAN_PROCESS.md lifecycle paragraph (~:110-120); dealer-discovery.js step 3 (:70-77) judges candidates with `validate_recipe_set`/`gate_recipes` and uses `validate_recipe` only as the count; OWNER_DECISIONS.md §1 and §2 resolved with commit ids (§3 and §4 stay "pending P17C"); the audit README progress table; the fetch-policy table; `_learning/platform_playbook.md` (MAIN). Accept: no doc says `validate_recipe` decides pass/fail, or that cascade bypasses the gate.

**P12B.9: Release and canary.**
- Change:
  1. Record the current Railway deployment id and the rollback command.
  2. `\copy` the canary dealers' cars and dealer_recipes rows.
  3. Minor bump.
  4. Build the exact stage locally (on the mini, or the MBP on AC, with owner OK). Replay one dealer each of cosmos, TV, DEP, HTML, carscommerce and dealer.com, and check the VIN counts against P0B.1.
  5. Deploy scanner-nightly.
  6. Run one canary shard (`SCAN_DEALERS` = DEP/HTML-walk dealers, its own lock) before the nightly.
- Accept: the canary verdicts are no worse than the previous run, recorded in each dealer's scan_runs.md and summary.md.

**P12B.10: Under-collection repair.**
- Change (MAIN):
  - Targets: the HTML-walk dealers where P0B.1 found VINs beyond the stored total, and the dealers whose scan union changes under P12B.5.
  1. Backup: `psql \copy (SELECT * FROM cars WHERE dealer_id IN (...))`, plus the dealer_recipes rows. `pg_dump --data-only` cannot filter rows.
  2. Dry run: `validate_recipe_set(pages=40)`. It writes nothing; `try_fetch_via_recipes` would write recipe files and hints. Record VIN counts against the live listing counts.
  3. The normal `dealer_pipeline` scan for those dealers on one shard lock against the LOCAL database (MAIN), with the vPIC verification per the process doc.
  4. Verify, and write the dealer logs.
  5. Prod. The owner takes the same `\copy` backup on prod before the first nightly after P12B.9's deploy, and that nightly rescans these dealers. Verify with the same per-dealer counts in that night's triage, and merge the pulled dealer logs (P6A.5). A dealer that Railway cannot reach is recorded as "prod rescan pending (home lane)" in its summary.md, and P15A.6's first home-lane run covers it. This unit does not wait for P15A.
  6. Run the read-only prod query `SELECT dealer_id FROM dealer_recipes WHERE recipes_json LIKE '%+cascade%'` and re-judge any hits with `lifecycle.validate_live_recipes`.
- Accept: the backup row counts match (local and prod); each dealer's live count, local and then prod, is at least its replay count minus refusals, explained in scan_runs.md; summary.md and scan_instructions.md are updated; the cascade audit is recorded.

**Workflow shape.** Wave A: P12B.1. Wave B: P12B.2. Wave C: P12B.3. Wave D: P12B.4. Wave E: P12B.5. Wave F: P12B.6 (shares the cascade files with P12B.5). Wave G: P12B.8, then P12B.7 (MAIN). Wave H: P12B.9. Wave I: P12B.10. Review lens: one walker, one fetcher, one place; no junk recipe admitted by the floor change; dry runs really write nothing.

**Data repair.** P12B.7 and P12B.10.

**Exit gate.**
- pytest: test_recipe_validation.py, test_recipe_validation_walker.py, test_recipe_synth.py, test_recipe_synth_surface.py, test_recipe_lifecycle.py, test_recipe_min_vins_rule.py, test_recipe_cascade.py, test_recipes.py.
- The canary passed, the re-judge report exists, and the rescans are logged.

**Release step.** P12B.9 is the release (minor).

## Phase 13A: DB layer: loud adapter, DDL guard, strict mode, side-effect-free import

**Goal.**
- Every inventory statement uses portable SQL. The SQLite-to-Postgres adapter raises on anything unportable instead of degrading it silently.
- A test stops new runtime DDL from appearing.
- Strict schema mode is on everywhere.
- Importing `backend.main` does no database I/O.
- The snapshot scripts stop leaking fixed-name tables.
- The `cars.zip_code` disagreement is settled.

**Entry gate.** Phase 12B done. Every DB in the ledger is at the current migration. D-DB3, D-DB4 and D-DB8 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P13A.1 | db-layer-3 (+ reviewer) | Portable ON CONFLICT at every inventory `INSERT OR` site; delete dead `seed_cars` | code | backend/db/repositories/saved_cars_repo.py, backend/db/incomplete_listings_db.py, backend/db/repositories/schema_repo.py, backend/db/inventory_db.py, backend/scripts/db_admin.py, backend/scripts/geocode_dealers.py, backend/enrichment/service.py, backend/tests/test_inventory_upsert_dialect.py (new) | M | A | none |
| P13A.2 | db-layer-4 | Adapter raises `UnportableSqlError`; non-connection PRAGMAs raise | code | backend/db/inventory_pg.py, backend/tests/test_inventory_pg_adapter.py, test_no_sqlite_dialect_on_inventory.py (new) | S | B | P13A.1 |
| P13A.3 | db-layer missing (DDL guard) | Static guard against new Postgres runtime DDL | test | backend/tests/test_no_runtime_pg_ddl.py (new) | S | A | none |
| P13A.4 | db-layer-9 part 2 | Strict schema mode on every service and host (owner) | ops, needs prod | Railway env, local and mini env | S | C | P13A.1, P13A.2 |
| P13A.5 | db-layer-10a (+ reviewer) | Bootstrap module wired into every launcher; ADMIN_PASSWORD check moves | code | backend/web/db_bootstrap.py (new), scripts/bootstrap_site_admin.py, scripts/docker-entrypoint-web.sh, run.py, start.sh, scripts/run-web-local.sh, deploy/always-on/* (if kept), backend/utils/production_security.py, backend/db/admin_users_db.py, backend/tests/test_db_bootstrap.py (new) | M | A | none |
| P13A.6 | db-layer-10b (+ reviewer) | Remove import-time init; users.db bootstraps become lazy | code | backend/main.py, backend/db/search_analytics_db.py, backend/db/user_history_db.py, conftest.py, backend/tests/conftest.py, backend/tests/test_web_import_side_effects.py (new) | M | B | P13A.5 |
| P13A.7 | db-layer missing (snapshot hygiene) | Snapshot scripts write timestamped tables into a `backups` schema and refuse to overwrite | code | backend/scripts/{repair_vin_make_model,backfill_extended_specs,fix_extended_spec_outliers,heal_stock_code_contamination}.py, backend/utils/db_snapshot.py (new), backend/tests/test_db_snapshot.py (new) | S | A | none |
| P13A.8 | db-layer missing (`cars.zip_code`) | Settle `cars.zip_code` per D-DB8 | code | backend/scripts/drop_dead_zip_code_column.py, backend/db/repositories/listings_repo.py (comment), (if D-DB8a: next migration, schema_repo.py:53) | S | A | none |

**P13A.1: Portable SQL.**
- Change: rewrite every statement that reaches the adapter as SQL that runs unchanged on SQLite 3.24+ and Postgres:
  - saved_cars_repo.py:8 → `ON CONFLICT (user_id, car_id) DO NOTHING`;
  - incomplete_listings_db.py:277 → `ON CONFLICT (k) DO UPDATE SET v = excluded.v`;
  - db_admin.py:288 → `ON CONFLICT (make, model) DO UPDATE` on transmission, drivetrain and cylinders only. That intentionally keeps gears, body_style and fuel_type, which SQLite REPLACE used to null;
  - geocode_dealers.py:626 → `DO UPDATE SET ..., geocoded_at = CURRENT_TIMESTAMP` (in SQL; no third timestamp format in this TEXT column);
  - geocode_dealers.py:613, the failed-geocode row → `DO UPDATE ... WHERE dealer_geopoints.lat IS NULL`, so a failed retry under `--force` never nulls a good point (the script's own rule at :599);
  - service.py:167 → `ON CONFLICT (cache_key) DO UPDATE` (kept per D-PR8).
  Delete `seed_cars` and `SEED_DATA` (schema_repo.py:452-472, call sites :283 and :443, re-exports at inventory_db.py:35,41). Leave the SQLite-only DBs (admin_users_db, users_db/schema) alone.
- Tests: a saved car twice → 1 row; the meta upsert overwrites; the model_specs upsert keeps the other columns; a failed geocode keeps the prior lat/lon; the haiku cache upsert overwrites; the adapter output strings. The test DDL for dealer_geopoints declares the PRIMARY KEY.
- Accept: no `INSERT OR` remains in any module that writes through inventory_db; the adapter logs no warning for any converted statement.

**P13A.2: Loud adapter.**
- Change: `adapt_sql_for_postgres_execute` raises `UnportableSqlError` (quoting the first 200 characters) for any `INSERT OR IGNORE` or `INSERT OR REPLACE`. Delete `adapt_insert_or_ignore_pg` and `adapt_insert_or_replace_pg`; their only consumer is this module. `PRAGMA table_info` and any other non-connection PRAGMA raise, pointing to `pg_table_columns`. The connection PRAGMAs (journal_mode, synchronous, busy_timeout, foreign_keys) stay no-ops. Add a static guard test over tracked `backend/**/*.py` and `scripts/**/*.py`, with an allowlist: admin_users_db.py, users_db/, tests/, and the sqlite-to-postgres migration scripts.
- Tests: INSERT OR raises; PRAGMA table_info raises; PRAGMA journal_mode returns None; portable statements pass with only the `?`→`%s` and EXCLUDED rewrites.
- Accept: the tests pass, and the test_dealer_portal.py adapter assertions pass.

**P13A.3: DDL guard.** `test_no_runtime_pg_ddl.py` fails on any new CREATE, ALTER or DROP in non-allowlisted backend or scripts code that reaches Postgres. Allowlist: the SQLite-only modules (users_db, admin_users_db, ip_rate_limit, bmw_store, the dev/routes job store, dictionary_catalog) and named snapshot or repair scripts. The remaining module `ensure_*` branches are listed as known exceptions until P13B.2 and P13B.3 remove them; the list only shrinks. This exists because review_reports and haiku_spec_cache were added as runtime DDL rather than migrations. Accept: the test passes on the tree and fails on an injected `CREATE TABLE` in a non-allowlisted module.

**P13A.4: Strict on.** The owner:
1. Confirms every DB is current (`migrate --dry-run` against each).
2. Sets `INVENTORY_SCHEMA_CHECK=strict` on Railway web, on scanner-nightly and on any other service that opens inventory Postgres, and in the local and mini env (D-DB5).
3. `railway redeploy`: a variables-only change, the code is already deployed.
4. Checks that the logs show no SCHEMA CHECK warning and that one scanner shard completes without SchemaNotMigratedError.
Rollback: unset the variable and redeploy. Accept: strict is set everywhere, and web returns 200 on /login.

**P13A.5: Bootstrap module.**
- Change:
  - `python -m backend.web.db_bootstrap` runs `init_users_db`, `init_admin_db`, `init_inventory_db` (a check only, on Postgres), `init_dealer_portal_db` and `init_job_queue_schema`. It does NOT run the incomplete-index full rebuild, which stays with the scanner and worker init paths behind an explicit CLI flag.
  - Fold it into `scripts/bootstrap_site_admin.py`, which the entrypoint already runs and which already calls `init_users_db` (:113), or call it from there.
  - Wire it into EVERY launcher: docker-entrypoint-web.sh (after migrate, before gunicorn; non-zero exit in production on failure), run.py, start.sh:63, scripts/run-web-local.sh:23, and the always-on launchd/systemd units if D-IF9 keeps them. Alternatively, call it from `gunicorn.conf.py` at module level, which gunicorn auto-loads.
  - Move the production ADMIN_PASSWORD check from `init_admin_db` (admin_users_db.py:105-111) into `assert_production_security_config`, so an import still refuses a bad production config.
  - Import-time init remains in this unit; running twice is idempotent.
- Tests: the bootstrap is idempotent; a production import without ADMIN_PASSWORD still raises.
- Accept: the tests pass.

**P13A.6: Side-effect-free import.**
- Change: remove the `init_*` calls at backend/main.py:95-101, keeping `assert_production_security_config()` and `assert_inventory_backend_configured()`. Make the `search_analytics_db.py:20-53` and `user_history_db.py:11-58` bootstraps lazy; today they run users.db DDL at import and swallow exceptions (:49-50). The root conftest session fixture and the `backend/tests/conftest.py` `app_factory` call the bootstrap after `importlib.reload(main)`; 28 test files import backend.main.
- Tests: import `backend.main` with `get_conn`, `pg_connect` and `get_users_conn` patched to raise, and assert through call counters that they were NEVER called. A successful import alone proves nothing, because those modules swallow errors.
- Accept: test_app_surface_golden is unchanged; targeted auth/admin/billing chunks pass; a local Docker run of the stage boots and /login returns 200 (owner OK).

**P13A.7: Snapshot hygiene.** A helper writes timestamped snapshot tables into a `backups` schema (or to files) and fails if the snapshot already exists. Use it in repair_vin_make_model.py:70, whose fixed name plus IF NOT EXISTS means a re-run takes no backup while :75 overwrites the JSON backup, and in backfill_extended_specs.py:291, fix_extended_spec_outliers.py:146 and heal_stock_code_contamination.py:232. `--repair-provenance-from` accepts schema-qualified names. Tests: a second run fails instead of skipping; the names carry timestamps. Accept: the tests pass.

**P13A.8: zip_code.** With D-DB8(b), the recommendation: grep consumers, delete `drop_dead_zip_code_column.py` (it drops, outside the chain, a column that V001 created and V025 indexes), and fix the false "was dropped" comment at listings_repo.py:559. With (a): the next migration drops the column and its index in a quiet window (ACCESS EXCLUSIVE on `cars`), and the script and the SQLite copy at schema_repo.py:53 go. Under (a) this unit edits schema_repo.py, which P13A.1 also edits, so it moves to wave B after P13A.1. Under (b) it stays in wave A. Accept: the chain, the code and the scripts agree.

**Workflow shape.** Wave A: P13A.1, P13A.3, P13A.5, P13A.7, P13A.8. Wave B: P13A.2, P13A.6. Wave C: P13A.4 (owner). Review lens: no silent dialect degradation; import performs no DB I/O (proven by call counters); every launcher bootstraps.

**Data repair.** None.

**Exit gate.**
- pytest: test_inventory_upsert_dialect.py, test_inventory_pg_adapter.py, test_no_sqlite_dialect_on_inventory.py, test_no_runtime_pg_ddl.py, test_db_bootstrap.py, test_web_import_side_effects.py, test_app_surface_golden.py, test_db_snapshot.py, test_dealer_portal.py, plus the auth/admin/billing chunks.
- Strict mode has been live for at least a week with no SCHEMA CHECK warnings before Phase 13B starts.

**Release step.** Minor. If D-DB8(a) produced V033, the owner applies it to prod first, in a quiet window under the D-DB6 protocol (verified dump, dry run, `--apply --target 33`). Then deploy web and scanner-nightly; then P13A.4 sets strict.

## Phase 13B: Delete legacy DDL; drop backup tables

**Goal.** Delete the hand-mirrored legacy Postgres DDL and the Postgres branches of the per-module `ensure_*` helpers, so the migration chain is the only schema owner. Export and drop the backup tables baked into V001 and those created by this plan.

**Entry gate.** Phase 13A done, with strict live for a week. Phase 10D done; the EPA recovery tables are no longer needed. D-DB1 and D-DB3 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P13B.1 | db-layer-11 (+ reviewer) | Delete `_legacy_postgres_inventory_ddl`; `init_postgres_inventory` check-only; the SQLite→Postgres migrator builds via the chain | deletion | backend/db/inventory_pg.py, backend/db/schema_version.py, backend/scripts/migrate_inventory_sqlite_to_postgres.py, backend/tests/test_schema_from_migrations.py, backend/scripts/import_epa_master.py (comment), backend/scripts/dump_baseline_schema.py (comment), migrations/README.md, docs/CLOUD_RESTRUCTURE_PLAN.md | M | A | none |
| P13B.2 | db-layer-12a | Drop the Postgres DDL branches in `backend/db/*` | deletion | backend/db/{dealerships_db,dictionary_schema,comments_db,dealer_portal_db,incomplete_listings_db}.py, backend/db/repositories/grid_cards_repo.py, backend/db/inventory_pg.py (`pg_add_columns`), backend/tests/test_runtime_ddl_gating.py | M | B | P13B.1 |
| P13B.3 | db-layer-12b | Drop the Postgres DDL branches in scanner, reviews, catalog, intelligence, enrichment, vector | deletion | backend/scanner/{job_queue,recipe_store,database}.py, backend/reviews/store.py, backend/scanner/specials/{store,lease_matches_store}.py, backend/scanner/dealer/profile.py, backend/catalog/generations.py, backend/intelligence/market_pricing.py, backend/enrichment/service.py, backend/vector/pgvector_service.py (per D-DB7), backend/db/runtime_ddl.py (delete), backend/tests/test_no_runtime_pg_ddl.py | M | C | P13B.2 |
| P13B.4 | db-layer-15 (+ reviewer) | Export, then drop the backup tables (next migration, after dumps exist) | data_repair, needs prod | migrations/V0xx__drop_backup_tables.sql (new), docs/db_layer/SCHEMA_LEDGER_2026_10.md | M | A | none |

**P13B.1: Legacy DDL.** Delete inventory_pg.py:319-793. `init_postgres_inventory` becomes check-only and raises when the DB is behind (D-DB3). `migrate_inventory_sqlite_to_postgres.py:92` builds its schema with `migrate.main(['--apply'])` (the fresh path), and its `_TABLES` lists epa_master before cars (if P10D.7 has not already done it). Replace `test_init_postgres_inventory_warn_mode_uses_legacy_ddl` with a "behind means refuse" test. The parity integration test compares the chain against the frozen `runtime_ddl_schema_golden.json`; the static test (a) stays. Update the comments. Keep `ensure_cars_listings_indexes` for SQLite, and `pg_add_columns` until P13B.2. Accept: no CREATE remains in inventory_pg.py; a behind DB raises without running DDL; the migrator test passes with `migrate.main` patched.

**P13B.2 and P13B.3: Module DDL.** On Postgres, every helper becomes a no-op; the SQLite branches stay for the test suite. Before each deletion, grep tests, scripts, deploy files, docs and string or importlib imports for the `ensure_*` name. Delete `runtime_ddl.py` and `pg_add_columns` once grep shows no consumers. The P13A.3 allowlist shrinks to SQLite-only modules. Accept: `test_runtime_ddl_gating` / `test_no_runtime_pg_ddl` assert zero DDL on any Postgres path; the SQLite chunks for comments, dealer_portal, reviews, specials, recipe_store, job_queue and grid cards pass; the P4.1 drift report on local is identical before and after.

**P13B.4: Backup tables.**
- Change, per DB (prod, local, and the mini if live):
  1. The census from P0A.2/P4.1.
  2. `pg_dump -Fc -t` each table into `workspace/backups/backup_tables_<db>_<date>.dump`:
     - the 7 `cars_backup_*` (cars_backup_capacity_20260718 is 127 MB);
     - the 4 `epa_extended_specs_bak_*`;
     - `epa_master_dump_id_map`;
     - `cars_epa_link_backup_20260921`;
     - and, as the owner decides, the plan's own snapshots: `cars_retire_backup_*`, `cars_precedence_backup_*`, `cars_epa_link_snapshot_*`, `cars_owner_backup_*`. Live quarantine tables (`cars_trim_quarantine`, `package_values_msrp_quarantine`) are NOT backups; keep them.
  3. Verify each dump by restoring it into a scratch DB and comparing row counts.
  4. The dumps must exist BEFORE the drop migration merges into any checkout, because `MIGRATE_ON_BOOT` and `INVENTORY_AUTO_MIGRATE` apply it automatically.
  5. Dry run: `to_regclass` per table; print the exact DROP list for owner review.
  6. The migration runs `DROP TABLE IF EXISTS` on that list. Apply it locally, then on prod manually before the deploy.
  7. Verify: `to_regclass` is NULL for each table, the cars count is unchanged, the DB size dropped.
  8. Document how to restore a table from its dump, which `backfill_extended_specs --repair-provenance-from` needs.
- Accept: a verified dump exists per DB before the apply; no code path errors (a static grep finds no reader of the dropped names).

**Workflow shape.** Wave A: P13B.1, plus P13B.4's dump steps (MAIN, owner for prod). Wave B: P13B.2. Wave C: P13B.3, then the P13B.4 migration merge and apply. Review lens: consumer greps are attached; the drift report is unchanged; no merge happens before the dumps exist.

**Data repair.** P13B.4.

**Exit gate.** pytest: test_schema_from_migrations.py, test_runtime_ddl_gating.py, test_no_runtime_pg_ddl.py, test_migrate_inventory_sqlite_to_postgres.py, plus the SQLite chunks. The drift report on local is clean, and the backup tables are gone on every DB with verified dumps.

**Release step.** Minor (schema). Apply the drop migration to prod before deploying.

## Phase 14A: Postgres test harness and first Postgres tests

**Goal.** Build a self-provisioning Postgres test tier: a throwaway database per test, cloned from a template built by the migration chain. Run it in CI against a pgvector service container. Move the existing live-Postgres tests onto it, and cover the write path, concurrency, reconcile and the recipe store on real Postgres.

**Entry gate.** Phase 13B done, or earlier if D-PLAN1 pulls this forward. D-TC3, D-TC6 and D-TC9 answered. P4.2 (fresh-build path) and P4.3 (V026) exist.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P14A.1 | tests-ci-5 (+ reviewer) | `pg` marker, migration-built template, per-test clone, safety guards, full cache reset | code | backend/tests/pg_harness.py (new), backend/tests/conftest.py, pytest.ini, backend/tests/test_pg_harness.py (new) | L | A | none |
| P14A.2 | tests-ci verdict (shared seed helpers) | Shared seed helpers in pg_harness, added before the fan-out | test | backend/tests/pg_harness.py | S | B | P14A.1 |
| P14A.3 | tests-ci-6 (+ reviewer) | CI `pytest-pg` job with a pgvector service container (replaces pytest-integration) | ops | .github/workflows/ci.yml, GitHub ruleset contexts (owner) | S | C | P14A.2 |
| P14A.4 | tests-ci-7 (+ reviewer) | Move the live-Postgres tests onto the harness; prove the chain on a real server | test | backend/tests/pg_harness.py, test_schema_from_migrations.py, test_data_quality_invariants.py, test_migrate.py | M | C | P14A.2 |
| P14A.5 | tests-ci-16b | Adapted SQL executes on Postgres | test | backend/tests/test_inventory_pg_adapter.py | S | D | P14A.2 |
| P14A.6 | tests-ci-8 (+ reviewer) | upsert_vehicles golden parity on Postgres, plus write-path cases | test | backend/tests/test_upsert_vehicles_golden.py, test_upsert_pg.py (new) | M | D | P14A.2 |
| P14A.7 | tests-ci-9 (+ reviewer) | Concurrent upserts, deadlocks and lock_timeout retries on Postgres | test | backend/tests/test_inventory_write_pg.py (new) | M | D | P14A.2 |
| P14A.8 | tests-ci-10 | Listing retirement (the single writer, provenance, two-strike) on Postgres | test | backend/tests/test_reconcile_pg.py (new) | M | D | P14A.2 |
| P14A.9 | tests-ci-11 (+ reviewer) | recipe_store CAS and reads on Postgres, with a deterministic interleave | test | backend/tests/test_recipe_store_pg.py (new) | S | D | P14A.2 |

**P14A.1: Harness.**
- Change, in backend/tests/pg_harness.py:
  - `admin_dsn()` reads ONLY `TEST_PG_ADMIN_DSN`, never `INVENTORY_DATABASE_URL` or `.env`.
  - `assert_safe_admin_dsn()` accepts only localhost, 127.0.0.1, ::1, a unix-socket dir or the CI service alias. It refuses `*.rlwy.net`, `*.railway.app`, `*.railway.internal` and port 15432 (the mini→MBP tunnel), and requires dbname `postgres` or `template1`.
  - Before building, check `rolsuper` or the availability of the dblink and vector extensions. V001:34 runs `CREATE EXTENSION dblink`, which is untrusted, so a non-superuser fails; skip or fail with a clear reason.
  - `build_template()`, once per session: `CREATE DATABASE pytest_<epoch>_tmpl_<hex>`; `migrate.main(['--apply'])` (the fresh path from P4.2); assert `max(version) == expected_schema_version()`.
  - `clone()`: assert zero connections to the template, then `CREATE DATABASE pytest_<epoch>_<hex> TEMPLATE <tmpl> STRATEGY WAL_LOG`. Never FILE_COPY on the cluster that holds `cars`: it forces cluster-wide checkpoints while the fleet writes.
  - `drop()`: `DROP DATABASE ... WITH (FORCE)`.
  - `janitor()`: drops leftover `pytest_%` databases older than 6 h, using the epoch in the name, because `pg_database` has no creation time.
  - `reset_pg_process_caches()` covers:
    - `inventory_pg.reset_postgres_inventory_schema_cache` (and the schema_version state);
    - the thread-local `inventory_pg._shared_read` connection (inventory_pg.py:25-52), which would otherwise point at a dropped DB;
    - the incomplete_listings, grid_cards and recipe_store per-DB state, and `scanner.database._vin_owner_table_ready`;
    - comments_db `_COMMENT_TABLES_OK`, `_COMMENT_FLAGS_TABLE_OK`, `_COMMENT_ATTACHMENTS_TABLE_OK`, `_DEALER_RATINGS_TABLE_OK` (:292-295);
    - msrp_trust `_BAND_LOADED`/`_BAND_CACHE`; price_plausibility `_trim_year_medians`/`_model_medians` (:58-60); deal_score_cache `_bands` (:43); the market_price cache.
    Its self-check sweeps EVERY backend module for module-level `_*(_OK|_ready|_LOADED|_CACHE)` flags and inventory-reading `lru_cache`s, not only the listed ones.
- Change, in conftest.py:
  - A session fixture `pg_template`. It skips with the reason "TEST_PG_ADMIN_DSN not set (pg tier)", added to `_ENV_GATE_SKIP_REASONS`; `REQUIRE_PG=1` turns that skip into a failure.
  - A function fixture `pg_db` (not `pg_inventory`, which test_search_golden.py already defines). It sets `INVENTORY_DATABASE_URL` to the clone, and `DATABASE_URL=''`, `INVENTORY_SQLITE_TESTS=''`, `INVENTORY_SCHEMA_CHECK=strict` and `INVENTORY_AUTO_MIGRATE=''`. It resets the caches before and after, and yields `PgInventory(dsn)` with `connect()` and `add_cars(rows)`.
- Change, in pytest.ini: register the `pg` marker and add `--strict-markers`; every marker in use is already registered.
- Budget: measure clone plus drop; median under 0.5 s. The fallback is one DB per module plus TRUNCATE between tests.
- Tests: test_pg_harness.py.
- Accept:
  - With `TEST_PG_ADMIN_DSN='postgresql:///postgres?host=/tmp'`, `pytest -m pg backend/tests/test_pg_harness.py` passes and leaves 0 `pytest_%` databases.
  - Without the DSN the tests skip with the counted reason; with `REQUIRE_PG=1` they fail.
  - The safety refusals are tested.
  - A row written in one test is invisible in the next.
  - The offline pass count is unchanged.

**P14A.2: Seed helpers.** Add `add_dealerships`, `add_dealer_geopoints`, `add_dealer_catalog`, `add_jobs` and `add_recipes` to pg_harness. Rule for P14A.5-P14B.8: never edit pg_harness.py; keep local helpers in the test file and file a harness follow-up, which avoids worktree conflicts.

**P14A.3: CI job.**
- Change: replace `pytest-integration` (D-TC6) with `pytest-pg`:
  - `services.postgres` uses the pgvector/pgvector image at prod's major version (D-TC3), pinned to a 0.8.x release, with `POSTGRES_PASSWORD=postgres` and a `pg_isready` health check;
  - env: `TEST_PG_ADMIN_DSN`, `SCHEMA_PARITY_PG_DSN` (the same value) and `REQUIRE_PG=1`;
  - `python -m pytest backend/tests -m pg -q -p no:cacheprovider --timeout=300 -rs --junitxml=reports/pg.xml`; exit code 5 (nothing collected) fails the job; `timeout-minutes: 15`.
  In the same release, the owner updates the P2B.5 ruleset contexts (`pytest-integration` → `pytest-pg`). Pin any pre-existing parity difference as `xfail(strict)` with a filed finding, so the job does not start red for a product reason.
- Accept: green on a branch push with 0 skipped pg tests, in 8 minutes or less; a deliberately broken migration file on a scratch branch turns it red.

**P14A.4: Live-Postgres tests.** Move `_with_dbname` and `_build_from_migrations` into pg_harness. Mark the parity test `pg`. Port `test_live_seeded_regression_in_a_temp_table` (test_data_quality_invariants.py:441) to `pg_db`; it cannot run today because conftest blanks the DSN. New pg tests in test_migrate.py, on a template0 clone:
- `migrate --apply` reaches the chain head in one call;
- a second run says "up to date" and adds no rows;
- an edited applied file exits 1 with CHECKSUM DRIFT;
- the V001 search_path reset regression;
- `test_fresh_chain_has_every_migration_relation` (from P4.3).
`test_merge_verified_specs_golden.py:319` stays `integration` (it needs live data). Accept: all of these pass in pytest-pg, and none reads the shell's DSN.

**P14A.5: Adapter on Postgres.** Execute the adapted `UPSERT_CARS_SQL` and representative statements on `pg_db` (EXPLAIN, or real executes in a rolled-back transaction). Accept: the tests pass.

**P14A.6: Upsert parity.**
- Change: route the golden's `_rows` reader through `inventory_db.get_conn` with dict rows (test_upsert_vehicles_golden.py:50,118,136,152). A pg variant replays the three batches into `pg_db` and compares with the expected JSON after a documented normalization (numeric types, JSON text, timestamp rank tokens). test_upsert_pg.py covers:
  - the SQL backstop's rowcount 0 through InventoryCursor, with the prefetch predicate disabled, and the refusal recorded in vin_owner_conflicts;
  - keep-if-nonempty fields; `LIKE 'http%'` escaping; the data_quality_score max rule;
  - post_write against Postgres incomplete_listings;
  - P8B.1's NULL binding.
  Never edit the expected JSON. Pin each divergence as `xfail(strict)` with a finding id.
- Accept: the SQLite golden is byte-unchanged; the pg variant passes or every divergence is pinned and filed; runtime is under 30 s.

**P14A.7: Concurrency.** Use InventoryWriteCoordinator with `SCANNER_PARALLEL_UPSERT=1`. Cases:
- overlapping VIN sets written in opposite orders: a real 40P01 is retried;
- a held row lock with `PGOPTIONS='-c lock_timeout=100ms'`: 55P03 is retried (the southcoasttoyota 609-row loss class);
- two dealers claiming one VIN inside the guard window: one owner, conflict recorded.
Synchronize with `threading.Event`. Accept: 20 out of 20 runs pass, in a one-off CI repeat job or on AC power in chunks of 120 s or less; each test runs under 20 s.

**P14A.8: Retirement on Postgres.** Against `pg_db`, cover:
- the P5A single writer (`plan_retirement` through `inventory_reconcile`) and the pipeline's report-only path;
- the minimum-rows floor and the prior-baseline fallback;
- TEXT comparison of `scraped_at` ('...Z') against `since_iso` ('+00:00');
- dry mode; retired rows keep their id and get `listing_removed_at`;
- `listing_retirements` rows (V027) and two-strike state (V028); other dealers untouched.
Accept: the tests pass; any SQLite-vs-PG difference is filed.

**P14A.9: Recipe store on Postgres.** Round trip, including the alias fallback; the `mutate_scan_hints` merge; `_ensure_table` against the migration-created table; `recipes.last_known_vin_count` (Postgres-only, recipes.py:919-940) counts DISTINCT active VINs; the CAS lost-update proof on Postgres. For the race, a deterministic interleave: a `threading.Barrier` inside a wrapped execute makes both writers read before either writes, and the CAS retry resolves it every run. A nondeterministic strict xfail is dropped (Appendix B). Accept: the tests pass, and no product code changes.

**Workflow shape.** Wave A: P14A.1. Wave B: P14A.2. Wave C: P14A.3, P14A.4. Wave D: P14A.5-P14A.9. Review lens: the harness can never touch `cars` or a Railway host; product code is never changed in test units (divergences are pinned and filed); every cache that could leak between tests is reset.

**Data repair.** None.

**Exit gate.** The pytest-pg CI job is green with REQUIRE_PG=1, every test listed above passes, the ruleset contexts are updated, and 0 `pytest_%` databases remain after a local run.

**Release step.** Patch.

## Phase 14B: Postgres tests for queue, grid, search parity, stores, admin

**Goal.** Cover the remaining Postgres-only behaviour, measured against actual computed values: the job queue, grid-card row versions, search and facet parity on the 984-row fixture, the Postgres-only features that silently do nothing under SQLite, the web stores that live in Postgres in prod, and the admin, dealer and scanner-side store branches.

**Entry gate.** Phase 14A released. D-TC4 answered (P14B.6-P14B.8).

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P14B.1 | tests-ci-12 | Job queue on Postgres (SKIP LOCKED claims, reaper, scheduler) | test | backend/tests/test_job_queue_pg.py (new) | M | A | none |
| P14B.2 | tests-ci-13 | Grid cards on Postgres (xmin row versions) | test | backend/tests/test_grid_cards_pg.py (new) | M | A | none |
| P14B.3 | tests-ci-14a | Move the search golden helpers into a support module (goldens byte-identical) | test | backend/tests/search_golden_support.py (new), test_search_golden.py | S | A | none |
| P14B.4 | tests-ci-14b | Search and facet parity, SQLite vs Postgres, on the 984-row fixture | test | backend/tests/test_search_parity_pg.py (new) | M | B | P14B.3 |
| P14B.5 | tests-ci-15 | Postgres-only features that do nothing under SQLite | test | backend/tests/test_pg_only_features.py (new) | M | A | none |
| P14B.6 | tests-ci-18 | Web stores whose prod home is inventory Postgres | test | backend/tests/test_web_stores_pg.py (new) | M | A | none |
| P14B.7 | tests-ci-19a | Admin and dealer routes on Postgres through the app | test | backend/tests/test_admin_pg.py (new) | M | A | none |
| P14B.8 | tests-ci-19b | Scanner-side stores on Postgres (no scanning entry points) | test | backend/tests/test_scanner_stores_pg.py (new) | M | A | none |

**P14B.1: Job queue.** `enqueue_job` plus the `_has_active_job` dedupe; two concurrent `claim_next_job` calls get distinct jobs (FOR UPDATE SKIP LOCKED, job_queue.py:146); finish success and failure; the reaper; `schedule_due_refresh_jobs` with its dns-fail and redirect-offsite backoffs; retry; `list_recent_jobs`; under strict, `ensure_job_tables` issues no DDL. Accept: two claimers never get the same id over 50 iterations.

**P14B.2: Grid cards.** `ensure_grid_cards_table()` is True on the V023 table. `refresh_cards` stores rows through `executemany ... ON CONFLICT (car_id) DO UPDATE`, with `row_ver` from `CAST(c.xmin AS TEXT)` (grid_cards_repo.py:611-612); under SQLite that is always '', so staleness is never tested today. An UPDATE to a car changes xmin, the card is detected stale, and a refresh rewrites it. `_touch_versions` and `store_generation`. The card JSON equals the SQLite path's, apart from the version. Accept: a card is rebuilt after an UPDATE, and only then.

**P14B.3 and P14B.4: Search parity.** P14B.3 moves the fixture loader, build_combos, the SQL recorder and the jsonable/digest helpers into `search_golden_support.py`; the SQLite goldens stay byte-identical. P14B.4 seeds the fixture (cars, dealerships, dealer_geopoints) into `pg_db`, runs the 400 combos and `_build_filter_options_uncached`, and compares result id order and counts with `search_golden_sqlite.json.gz`. Known dialect differences (NULL ordering, collation and case, float rounding, distance) go in a reviewed allowlist, one reason per entry, reported to the owner as findings. The live-DB SEARCH_GOLDEN_PG test is unchanged. Accept: parity holds against the allowlist.

**P14B.5: Postgres-only features.** Assert computed values from seeded rows, not just "no exception", for:
- `price_plausibility._load` (percentile_cont medians and the fallback key; `{}` under SQLite, :97);
- `msrp_trust._load_bands` (:318); `deal_score_cache._load_bands` (:70);
- the `market_price` Postgres path (:68) and its TTL;
- the nearby_dealers RETURNING-id insert (:860-868);
- the `::timestamptz` cast in the inventory_queries stale-price clause (:26-31);
- catalog/generations DDL selection.
Reset caches through `reset_pg_process_caches`. Accept: each feature has at least one value assertion.

**P14B.6: Web stores (D-TC4).** First confirm, store by store, whether prod keeps it in inventory Postgres or a SQLite users DB. Then round-trip create, read, update and delete on `pg_db`, including the DO NOTHING dedupe and RETURNING-id paths, for: comments_db (flags ON CONFLICT :678), reviews/store (:94-109, :299), hidden_dealers_repo, search_history_repo, saved_searches_repo, user_history_db, dealer_portal_db, incomplete_listings_db, dealerships_db (lastrowid vs RETURNING, :327, :535-556) and data_quality_repo (:153-172). Accept: every Postgres-in-prod store has a passing round trip, and the SQLite-only stores are listed.

**P14B.7: Admin routes.** With `app_factory` and `pg_db`, hit the Postgres branches of routes/admin_dealer_api.py; dealer/admin/{platform_stats, onboard_api, inventory_queries, dealers_hub}.py; and routes/dealers_recalls.py. One happy-path data assertion each. Accept: each `is_inventory_postgres()` branch executes at least once, proven by coverage or a SQL-path assertion.

**P14B.8: Scanner-side stores.** specials/store, lease_matches_store, rooftop_ledger (:88-90), the Postgres branch of enrich_from_dictionary, and the lowest-level post_scan job function with the network stubbed. scanner/cli.py is left out: driving it launches scanning. Accept: each branch executes once.

**Workflow shape.** Wave A: P14B.1, P14B.2, P14B.3, P14B.5-P14B.8. Wave B: P14B.4. Review lens: no product-code changes; divergences are pinned and filed; pg_harness.py is not edited.

**Data repair.** None.

**Exit gate.** pytest-pg is green with every new file, and the SQLite goldens are unchanged.

**Release step.** Patch.

## Phase 14C: SQL corpus check, boot test, skip budget, coverage map, runtime, TESTING.md

**Goal.**
- A breadth net: every SQL statement the SQLite suite runs must plan on Postgres.
- Web boot on Postgres issues no DDL.
- A test that starts silently skipping fails CI.
- A coverage map of the Postgres-only branches.
- Runtime numbers.
- The test tiers documented.

**Entry gate.** Phase 14B released. D-TC8 for P14C.5.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P14C.1 | tests-ci-17 (+ reviewer) | SQL corpus check from a committed corpus snapshot | code | backend/tests/conftest.py (opt-in recorder), backend/tests/sql_corpus.py (new), backend/tests/fixtures/sql_corpus.jsonl (new), backend/tests/test_sql_corpus_pg.py (new) | L | A | none |
| P14C.2 | tests-ci-20 | Web boot on Postgres issues no schema DDL | test | backend/tests/test_boot_pg.py (new) | S | A | none |
| P14C.3 | tests-ci-27 | Gated-skip budget | code | backend/tests/conftest.py, .github/workflows/ci.yml, backend/tests/test_gated_skip_budget.py (new) | S | B | P14C.1 |
| P14C.4 | tests-ci missing (coverage map) | Coverage map of the Postgres-only branches | ops | .github/workflows/ci.yml, requirements-test.txt, docs/TESTING.md (new; coverage-map section only) | S | C | P14C.3 (both edit ci.yml) |
| P14C.5 | tests-ci-25 | Runtime budget and the xdist decision | investigation | requirements-test.txt, .github/workflows/ci.yml, docs/TESTING.md (budget table) | S | D | P14C.4 (shares ci.yml, requirements-test.txt and TESTING.md) |
| P14C.6 | tests-ci-26 | docs/TESTING.md completed; stale tier comments refreshed | docs | docs/TESTING.md (extends P14C.4's file), conftest.py (comment), docs/monolith_audit_2026_10_01/tests.md | S | E | P14C.5 |

**P14C.1: SQL corpus.** An opt-in recorder (`RECORD_SQL_CORPUS=<path>`) wraps `InventoryCursor.execute`/`executemany` on the SQLite backend and writes distinct records: SQL, parameter Python types, and the first caller as module:line. Commit the corpus snapshot and regenerate it in an opt-in run. Do not hand an artifact from the offline job to the pg job; that would make pytest-pg wait for the whole offline suite. `test_sql_corpus_pg` runs `EXPLAIN <adapted sql>` on `pg_db` for DML and SELECT only, inside a rolled-back transaction. Parameter types come from the target column catalog, not from Python values: SQLite-recorded Nones would otherwise make Postgres fail with "could not determine data type"; untyped NULL is its own bucket. Failures are grouped by module, with an allowlist keyed by SQL hash and a finding id per entry. Expected failure classes: bare GROUP BY columns, SQLite-only functions, text-vs-int comparisons, columns that exist only in hand-written SQLite schemas. Accept: the first report is triaged into findings for their owners; the pg job grows by 3 minutes or less; a new failure fails CI.

**P14C.2: Boot DDL.** Snapshot `information_schema.columns`, `pg_indexes` and `pg_constraint` before and after `app_factory()` boots on `pg_db` under strict. `/api/health` and `/api/ready` return 200. Any DDL left at import is pinned as `xfail(strict)` and filed. Accept: the snapshots are identical.

**P14C.3: Skip budget.** In `pytest_sessionfinish`, count the skips whose reasons match `_ASSET_GATE_SKIP_REASONS` or `_ENV_GATE_SKIP_REASONS`. When `MAX_GATED_SKIPS` is set and exceeded, fail the session and print the node ids. In CI, the offline job uses the P2A.7 count and the pg job uses 0. Accept: a pytester test shows a newly asset-gated test turns the job red, and the current suite stays green.

**P14C.4: Coverage map.** In pytest-pg, run coverage restricted to the 41 modules that have `is_inventory_postgres()` branches, and report which of the 80 sites executed. Keep a ranked module → unit → covered/uncovered table in TESTING.md. CI prints the number, so a drop is visible. Accept: the table exists, and CI prints the count.

**P14C.5: Runtime.** Take the 40 slowest tests and the per-job wall time from `--durations`. If the offline job exceeds 12 minutes, trial pytest-xdist on a branch, after checking xdist safety: per-process mkdtemp, the dictionary write guard, app_factory reloads, the JS node tests, and no repo writes. Adopt it only per D-TC8. Accept: the budget table is in TESTING.md, and ci.yml changes only if xdist is adopted.

**P14C.6: TESTING.md.** Document:
- the tiers: offline, slow, pg, integration (needs live data; dev machine only) and asset-gated;
- the commands, including the local pg tier: `TEST_PG_ADMIN_DSN='postgresql:///postgres?host=/tmp' .venv/bin/python -m pytest -m pg`;
- `REQUIRE_PG`, `REQUIRE_LOCAL_ASSETS` (the owner sets it on the asset-bearing mini), `TESTS_BLOCK_NETWORK` and `MAX_GATED_SKIPS`;
- the CI jobs and their budget;
- how to write a pg test (`pg_db`, `add_cars`, cache resets, no pg_harness edits);
- the 180 s chunking rule for agents.
Update the tier comment at root conftest.py:107-122, and mark F3 and F8 addressed in tests.md, with commits. Accept: the doc's commands run as written; no comment still says the integration tier is one test, or that CI has a Postgres it lacks.

**Workflow shape.** Wave A: P14C.1, P14C.2. Then serial, because P14C.3-P14C.5 all edit ci.yml and P14C.4-P14C.6 all edit TESTING.md: wave B P14C.3, wave C P14C.4, wave D P14C.5, wave E P14C.6. Review lens: the CI budget; no serialization between CI jobs; findings filed, not fixed in place.

**Data repair.** None.

**Exit gate.** CI is green, with the corpus check, the boot test and the skip budget active. TESTING.md is merged.

**Release step.** Patch.

## Phase 15A: Home-IP lane and onboarding

**Goal.** Scan the dealers that answer only from a residential IP on a home lane on the mini, from a dedicated prod-lane clone, with Railway excluding them and alarms for unassigned or stale dealers. Fix or retire admin onboarding. Port the retired nightly post steps that are worth keeping.

**Entry gate.** Phase 14 done. D-IF1, D-IF3 and D-IF6 answered. P11A.3 (per-dealer locks) and P6A (window and fleet lock) are live. P6B.3/P6B.4 evidence exists.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P15A.1 | infra-4 (+ reviewer) | Egress lanes: home-lane roster, Railway exclusion, alarms | code | deploy/railway/home_lane_dealers.txt (new), backend/scanner/manifest.py, backend/scripts/fleet_scan.py, backend/tests/test_fleet_lanes.py (new), test_fleet_roster_rule.py | M | A | none |
| P15A.2 | infra-6 + infra missing (workspace isolation) (+ reviewer) | Home-lane runner: dedicated clone, prod-DSN guards, empty-string env exports; launchd template | code | deploy/home_lane/{run_home_lane.sh,com.sarraficars.home-lane.plist.template,install_launchd.sh,README.md} (new), backend/tests/test_home_lane_runner.py (new) | M | B | P15A.1 |
| P15A.3 | infra-23 (+ reviewer) | Fix or retire the admin onboarding job path (D-IF3) | code | scripts/scanner_worker_loop.py, backend/scripts/discovery_probe.py, backend/scanner/scrape_confidence.py, backend/scanner/job_queue.py, backend/dealer/admin/dealers_hub.py, frontend/templates/admin/dealers.html, frontend/static/find_dealers.js, backend/tests/test_scanner_worker_onboard.py (new), test_job_queue.py, test_dealer_onboard_api.py | M | A | none |
| P15A.4 | infra-24 | Configurable fleet post steps (the ones D-IF6 keeps) | code | backend/scripts/fleet_scan.py, requirements-scanner.txt, backend/tests/test_fleet_scan_post_steps.py (new) | S | B | P15A.1 |
| P15A.5 | infra missing (kill switches, lanes) | Kill-switch matrix extended for lanes and dealer locks | test + docs | backend/tests/test_fleet_scan_safety.py, docs/RAILWAY_SCANNING.md | S | B | P15A.1 |
| P15A.6 | infra-17 (+ reviewer) | Stand up the home lane on the mini (owner) | ops, needs prod | deploy/railway/home_lane_dealers.txt, docs/SCANNING_OPS_LOG.md, dealer scan_runs.md | M | C | P15A.2, P15A.5 |

**P15A.1: Lanes.**
- Change:
  - A tracked `deploy/railway/home_lane_dealers.txt`, in do_not_scan.txt format, with a dated evidence comment per id ("403 from Railway <date>, ok from home <date>"). It starts empty.
  - `apply_scannable_roster_rule(lane=...)`: `lane=railway` drops home-lane ids (reported as `excluded_home_lane`).
  - `SCAN_LANE` defaults to `railway` when `SCANNER_EGRESS_TAG` is set, and to `all` otherwise (today's behaviour).
  - `SCAN_LANE=home` gives the home-lane ids minus do_not_scan; `SCAN_POST_STEPS` defaults to 0 there.
  - fleet_summary alarms:
    - `blocked_unassigned`: dealers blocked from Railway (P7.6's `recipe_blocked['railway']`, or the legacy `blocked:railway:`) that are not in the home list;
    - `home_lane_stale`: home-lane dealers whose newest active `scraped_at` is older than 36 h, inside the guard window.
- Tests: `lane=railway` excludes and reports; the home roster; an explicit `SCAN_DEALERS` is untouched; both alarm lists are correct on fixtures.
- Accept: scanner.py's `DEALERS_FROM_SCANNABLE` behaviour is unchanged when no lane is set.

**P15A.2: Runner.**
- Change, in run_home_lane.sh:
  - It runs only from a DEDICATED prod-lane clone (e.g. `~/sarraficars-prod-lane`), with its own `workspace/`, and refuses to run from the main checkout (clone marker). Otherwise the cwd-relative recipe cache and the dealer logs would mix local and prod state; today 8 MBP recipe files are newer than prod's.
  - `HOME_LANE_DATABASE_URL` comes from the env or `~/.config/sarraficars/home_lane.env`, which must not be group- or world-readable.
  - It refuses localhost, 127.0.0.1, ::1 and port 15432 unless `HOME_LANE_ALLOW_LOCAL=1`. The mini's `.env` points at its own Postgres; 668 rows went to the wrong DB on 09-26.
  - It prints the target host (never credentials) and the active-row count.
  - It runs under `PROJECT_DOTENV_DISABLE=1` with explicit exports. `SCANNER_EGRESS_TAG`, `SCANNER_HTTP_PROXY` and `SCANNER_ALLOW_BROWSER` are exported as EMPTY strings, because unset variables are refilled from `.env` (`load_project_dotenv(override=False)`).
  - It sets `SCAN_LANE=home`, `SCAN_POST_STEPS=0` and `SCAN_SHARDS=${HOME_LANE_SHARDS:-2}`.
  - `SCAN_PIPELINE_ARGS` defaults to `--no-discover`, because runner.py:47-58 launches Playwright wherever it is installed.
  - It logs to `workspace/scanlogs/home_lane_<date>.log`; `--dry-run` prints the roster.
- Change: `install_launchd.sh` renders the template (`__REPO_ROOT__`, `__HOME__`) into `~/Library/LaunchAgents` and loads it only with `--load`. The default time is 02:00 America/New_York, which ends before Railway's 09:00 UTC run in both EDT and EST.
- Tests: `bash -n`; refusals for a localhost DSN, a world-readable env file and the main checkout; the dry run lists the file's ids; no hardcoded `/Users/` path.
- Accept: the tests pass, and no secret enters the repo.

**P15A.3: Onboarding (D-IF3).**
- Change, with (b) (an onboard-only worker on the mini):
  - discovery_probe accepts `--url` (or `id=url`), and the worker passes `payload.url`; today it is dropped at scanner_worker_loop.py:47.
  - An onboarding job succeeds when a recipe validates (recipe_count > 0 and validation ok), not when vehicles > 0 (scrape_confidence.py:62-63).
  - Onboard jobs run only where `browser_allowed()`; otherwise they are HTTP-template only and labelled as such.
  - Refresh and rescan jobs run through dealer_pipeline instead of a bare `scanner.py --scan-only` (:49); otherwise enqueue and retry (job_queue.py:612) reject them with an admin message. No scheduler.
  - Fix the `./deploy/up.sh --full` hint (admin/dealers.html:13, find_dealers.js:42); that flag does not exist.
- Change, with (a): the admin hub stops enqueuing, links to the dealer-discovery workflow, and hides the refresh and rescan retry buttons.
- Tests: an onboard job for a dealer absent from dealers.json and from cars reaches discovery_probe with its URL; a validated recipe with 0 vehicles is reported done; refresh and rescan jobs take the pipeline path or are rejected.
- Accept: the tests pass.

**P15A.4: Post steps.** `SCAN_POST_STEPS` takes '0' or a comma list (default `market,cards`, today's behaviour). Step names map to the commands D-IF6 keeps from P0B.5. Each step's rc and seconds go into fleet_summary's `post`. Cards stay last. Imported packages are added to requirements-scanner.txt and verified by the import sweep. An unknown step name fails before any scan. Accept: the default runs exactly `compute_market_stats`, then `build_listings_grid_cards` with `FLASK_ENV=production`.

**P15A.5: Kill switches.** Extend P6A.4: `SCAN_LANE=all` and `SCANNER_DEALER_LOCKS=0` restore today's behaviour; tests plus the doc table.

**P15A.6: Stand up the home lane.**
- Change:
  1. On the mini: create the dedicated clone at the release. The owner creates the 0600 env file.
  2. Populate `home_lane_dealers.txt` only with dealers that got 403 from Railway (P6B.4/P6B.5 evidence) and were ok from home (P6B.3), each with dated evidence. Commit the file on the integration branch, with no VERSION bump inside the unit. It ships with the Phase 15A release step, which deploys scanner-nightly so Railway excludes these dealers (`excluded_home_lane`). Steps 3-5 run after that release.
  3. `run_home_lane.sh --dry-run`, then one supervised run: check verdicts and rows, nothing `skipped_locked`, and no new vin_owner_conflicts pairs with Railway-lane dealers.
  4. Merge the logs into the canonical tree.
  5. `install_launchd.sh --load`, then verify the first scheduled night.
- Accept: every home-lane dealer has a scan_runs.md block from the mini; the next Railway summary lists them as `excluded_home_lane` with `home_lane_stale` empty; the DSN file is 0600 and outside the repo.

**Workflow shape.** Wave A: P15A.1, P15A.3. Wave B: P15A.2, P15A.4, P15A.5. Wave C: P15A.6 steps 1-2 (owner), then the Phase 15A release step, then P15A.6 steps 3-5 against the released code. Review lens: no host can write the wrong DB; `.env` cannot re-enable the egress tag or the browser; no browser on an unattended run.

**Data repair.** None. P15A.6 is a normal scan, with dealer logs.

**Exit gate.** pytest: test_fleet_lanes.py, test_fleet_roster_rule.py, test_home_lane_runner.py, test_scanner_worker_onboard.py, test_job_queue.py, test_dealer_onboard_api.py, test_fleet_scan_post_steps.py, test_fleet_scan_safety.py. The home lane has run its first scheduled night.

**Release step.** Minor. It runs between P15A.6 step 2 and step 3, so the populated `home_lane_dealers.txt` ships with it. Deploy scanner-nightly outside the window, and pull to the prod-lane clone. The exit gate's "first scheduled night" is checked after this release.

## Phase 15B: Dependencies and images

**Goal.**
- Remove the orphan dependencies, crawl4ai above all: it pins lxml below 6 and keeps PYSEC-2026-87 ignored.
- Split requirements per image without breaking the Docker builds.
- Pin and bake the embedding model.
- CPU-only torch in the web image; a slimmer discovery image.
- Delete the dead worker and scheduler images.
- pip-audit in CI, and optionally lock files.

**Entry gate.** Phase 15A released. D-DEP1 and D-SEC7 answered. The owner approves the image builds (on the mini, or the MBP on AC).

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P15B.1 | security-1 + dead-code-10 + infra-8b (+ reviewers) | Remove orphan dependencies; lxml 6.1 or later; pip-audit without the ignore; dev venv rebuild | code | requirements.txt, requirements-scanner.txt, scripts/security_check.sh, backend/scripts/install_scraper_browsers.sh, scripts/scanner_import_sweep.py, Dockerfile.web (comment), backend/tests/test_requirements_hygiene.py | M | A | none |
| P15B.2 | infra-8a (+ reviewer) | requirements split per image; every Dockerfile `COPY requirements*.txt` in the same commit | code | requirements-web.txt (new), requirements-browser.txt (new), requirements.txt, Dockerfile.web, Dockerfile.discovery, Dockerfile.scanner, scripts/scanner_import_sweep.py (`--profile`), backend/tests/test_requirements_split.py (new) | M | B | P15B.1 |
| P15B.3 | security-13 (+ reviewer) | Pin the embedding model revision; bake it into the web image; offline at runtime | code | backend/vector/{pgvector_service,catalog_service,ingest_master_specs}.py, Dockerfile.web, backend/tests/test_embedding_model_pin.py (new) | M | C | P15B.2 (both edit Dockerfile.web) |
| P15B.4 | infra-9 (+ reviewer) | Web image: CPU-only torch (amd64 measurement) | code | Dockerfile.web, backend/tests/test_dockerfiles.py (new) | M | D | P15B.2, P15B.3 |
| P15B.5 | infra-10 | Discovery image on the slim requirements | code | Dockerfile.discovery, backend/tests/test_dockerfiles.py | S | E | P15B.4 |
| P15B.6 | infra-11 + dead-code-9 (compose part) + tests-ci cross-cluster (compose image) | Delete the worker/scheduler Dockerfiles and tomls; compose cleanup, including a pgvector Postgres image | deletion | Dockerfile.scanner-worker, Dockerfile.scanner-scheduler, deploy/railway/railway.scanner-{worker,scheduler}.toml, deploy/docker-compose.yml, backend/tests/test_deploy_references.py (new) | S | B | P15B.1 |
| P15B.7 | security-17 (+ reviewer) | pip-audit in CI | ops | .github/workflows/ci.yml | S | C | P15B.2 |
| P15B.8 | D-SEC7 (optional) | Lock files per image with hashes, plus a CI drift check | code | requirements-*.lock (new), Dockerfiles, .github/workflows/ci.yml | M | F | P15B.4, P15B.5, P15B.7 |

**P15B.1: Dependency purge.**
- Change:
  - Remove crawl4ai, pyotp, segno, ollama, langchain-core, langchain-openai, langchain-text-splitters and langsmith (requirements.txt:45-48). Also remove the starlette, tornado and pygments floors, but only where `pip show` lists no requirer and nothing imports them. Keep the urllib3 and idna floors.
  - Declare httpx explicitly if crawl4ai was its only path; image_downloader.py:531 imports it.
  - In requirements-scanner.txt, set `lxml>=6.1.0`, or drop it with evidence: nothing imports lxml, and all 20 BeautifulSoup calls use html.parser.
  - Remove the patchright install (install_scraper_browsers.sh, requirements.txt:35) and the lxml SEC-076 comment.
  - security_check.sh: drop `--ignore-vuln PYSEC-2026-87` and use `pip-audit -r requirements.txt -r requirements-scanner.txt`. Environment mode would keep flagging stale venvs.
  - Tidy `ALLOWED_MISSING` in scanner_import_sweep.py and the comment at Dockerfile.web:25-29.
  - KEEP playwright-stealth: discovery_capture.py:96, image_downloader.py:746, car_data_scraper.py:51 and backfill_dealership_addresses.py:436 import it.
  - The P1A.6 hygiene test asserts crawl4ai is not declared and that no requirement caps lxml below 6. Add test_requirements_hygiene.py to security_check.sh's pytest list, so the security check runs it too.
  - Dev venvs on the MBP and the mini are rebuilt with the owner's OK: `pip uninstall crawl4ai patchright unclecode-litellm nltk && pip install -U lxml`, or recreate them.
  - The commit message records the consumer audit (rg over *.py, *.sh, Dockerfile* and runbooks, plus importlib strings).
- Accept: CI is green; `pip-audit` reports no PYSEC-2026-87 with no ignore; `bash scripts/security_check.sh` passes; Dockerfile.web and Dockerfile.discovery build from stages (owner OK).

**P15B.2: Requirements split.** `requirements-web.txt` holds what the web imports, derived with `scanner_import_sweep --profile web`. `requirements-browser.txt` holds playwright and playwright-stealth. `requirements.txt` becomes `-r requirements-web.txt -r requirements-scanner.txt -r requirements-browser.txt` plus offline-script extras. In the SAME commit, every Dockerfile's `COPY requirements.txt .` becomes `COPY requirements*.txt ./` (Dockerfile.web:17, Dockerfile.discovery:20, Dockerfile.scanner); otherwise the builds break on the `-r` includes. Add `scanner_import_sweep --profile scanner|web|discovery`. Tests: the scanner file holds none of torch, sentence-transformers, playwright, crawl4ai or flask. Accept: CI is green and every image builds.

**P15B.3: Embedding pin.**
- Change: one constant holds the model id and the pinned Hugging Face revision that produced the STORED vectors, read from the mini's HF cache or `huggingface_hub.model_info`. Every `SentenceTransformer(...)` call passes `revision=` (pgvector_service.py:61, catalog_service.py:33, ingest_master_specs.py:324). Dockerfile.web bakes the model into `/app/models` (`HF_HOME`, world-readable) and sets `HF_HUB_OFFLINE=1` and `TRANSFORMERS_OFFLINE=1`. A production `LISTING_EMBEDDING_MODEL` override without a revision is refused; confirm first from P0A.1 whether prod sets it, so semantic search does not break.
- Tests: a monkeypatched SentenceTransformer sees `revision=` at all 3 call sites; a production override without a revision raises.
- Accept: a local build serves `/api/search/smart` in semantic mode with huggingface.co blocked.

**P15B.4: CPU torch.** Install torch from the CPU index, pinned to a version sentence-transformers accepts, before requirements-web.txt. Chromium already left the image in P3.8 if D-SEC1=(a). Measure with `docker buildx build --platform linux/amd64`, or read the size from Railway's build log: an arm64 build on the mini has no CUDA wheels, so comparing it says nothing about Railway's amd64 image. Accept: `pip list | grep -i nvidia` in the image is empty; `scanner_import_sweep --profile web` reports 0 MISSING; `/health` returns 200; hybrid search returns results; embeddings for 20 sample texts match the vectors already stored in pgvector (cosine > 0.9999); a static Dockerfile test passes. Rollback: the deploymentRollback recipe.

**P15B.5: Discovery image.** Install `requirements-scanner.txt` plus `requirements-browser.txt` plus `playwright install chromium`. Keep `SCANNER_ALLOW_BROWSER=1` and `SCANNER_WORKER_JOB_TYPES=onboard`. Accept: it builds on the mini, with the size recorded (was 13.4 GB); `--profile discovery` reports 0 MISSING; `discovery_probe --browser-capture` for one known dealer against local PG appends to discovery.md.

**P15B.6: Dead images.** After a consumer audit, delete Dockerfile.scanner-worker, Dockerfile.scanner-scheduler and the two Railway tomls. In deploy/docker-compose.yml, drop the `scanner_node_modules` mount (:91) and volume (:131) and `SCANNER_BROWSER_PROFILE_DIR`. Also replace `image: postgres:16-alpine` (:22) with the pgvector/pgvector image at prod's major version (D-TC3, from P0A.2). Today V001:41 `CREATE EXTENSION vector` fails on a fresh compose stack, and the web entrypoint's `migrate --apply` then only WARNs and boots on legacy DDL. Keep Dockerfile.scanner's chmod of both entrypoints; compose still uses them. Validate compose with a YAML parse plus assertions, or `docker compose config -q` with the owner's OK. Accept: rg for the deleted names finds only CHANGELOG and docs marked superseded; the static reference test passes and also asserts that the compose Postgres image is a pgvector image.

**P15B.7: pip-audit in CI.** Add a step to the existing pytest job, after install: environment-mode `pip-audit`, no ignores. Add a light `pip-audit -r requirements-scanner.txt`. A separate `-r` job would rebuild torch for nothing. Accept: it fails on a known-vulnerable floor in a test commit.

**P15B.8: Lock files (optional, D-SEC7).** `uv pip compile` with hashes produces requirements-web.lock, requirements-scanner.lock and requirements-browser.lock. The images install from the locks, and CI checks for drift. One heavy local resolve, with the owner's OK. Accept: the images build from the locks and the drift check passes.

**Workflow shape.** Wave A: P15B.1. Wave B: P15B.2, P15B.6. Wave C: P15B.3, P15B.7. Wave D: P15B.4. Wave E: P15B.5. Wave F: P15B.8. Dockerfile.web is serial (P15B.2 → P15B.3 → P15B.4), as Appendix C.2 requires. Review lens: no image build breaks between units; no package is removed that something still imports; embeddings stay compatible with the stored vectors.

**Data repair.** None.

**Exit gate.** CI is green with pip-audit and no ignores. Every image builds from a stage. The web image has no CUDA and no Chromium, and its embeddings match.

**Release step.** Minor. Deploy web and scanner-nightly from tagged stages after the local builds.

## Phase 16: Security wave 2

**Goal.**
- Revoke other sessions on any password change.
- Drop the dead TOTP/phone columns (SEC-088), with an optional scrub.
- Guard every scanner URL entry point against non-public destinations.
- Inventory and gate the browser launch sites outside the scanner.
- Run the web container as non-root.
- Remove `style-src 'unsafe-inline'` in three steps.
- Map who holds the inventory DSN, for the users-PG isolation decision.
- Bring the security docs current.

**Entry gate.** Phase 15B released. D-SEC2 answered (P16.2, P16.3); D-SEC3 frames P16.10. The owner has the Resend key ready, to enable password reset after P16.1 deploys.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P16.1 | security-5 (redesigned) | Session revocation via `users.auth_epoch`; never bumped by rehash | code | backend/db/users_db/{schema,auth,_common}.py, migrations/V0xx__users_auth_epoch.sql (new), backend/auth/session.py, backend/billing/access.py, backend/web/security.py, backend/routes/account.py, backend/tests/test_session_revocation.py (new) | M | A | none |
| P16.2 | security-8 (+ reviewer) | Retire the dead TOTP/phone helpers; drop the dead users columns | code | backend/db/users_db/{auth,__init__,_common,schema,admin}.py, backend/db/admin_users_db.py, backend/scripts/app_users_status.py, migrations/V0xx__users_drop_dead_mfa_columns.sql (new), backend/tests/test_users_dead_mfa_removed.py (new) | M | B | P16.1 |
| P16.3 | security-9 (optional) (+ reviewer) | Scrub dead seeds and phones in users.db and dev_users.db (prod, owner) | data_repair, needs prod | backend/scripts/scrub_dead_mfa_secrets.py (new), backend/tests/test_scrub_dead_mfa_secrets.py (new) | S | B | none |
| P16.4 | security-15 (+ reviewer) | Scanner SSRF guard at the URL entry points | code | backend/scanner/net/client.py, backend/scanner/http_fetch.py, backend/scanner/scrapers/shopperexpress.py, backend/scanner/recipe_validation.py, backend/utils/outbound_url.py (resolver cache), backend/tests/test_scanner_net_ssrf.py (new) | M | A | none |
| P16.5 | dead-code missing (browser launch sites) | Inventory and gate the browser launch sites outside backend/scanner | code + deletion | backend/scripts/{image_downloader,car_data_scraper,backfill_dealership_addresses}.py, backend/scraping/cli.py, backend/scanner/browser_gate.py, backend/tests/test_browser_gate_outside_scanner.py (new) | M | A | none |
| P16.6 | security-19 (+ reviewer) | Web container runs as non-root | ops | Dockerfile.web, scripts/docker-entrypoint-web.sh | M | C | P16.1, P16.2 |
| P16.7 | security-14a | Static template style attributes become classes | code | frontend/templates/**, frontend/static/css/** | M | A | none |
| P16.8 | security-14b | Dynamic style attributes and JS style strings become data-* plus CSSOM | code | frontend/templates/{site_hub,car}.html, frontend/static/{car_dealer_map,find_dealers,compare,dev,main}.js, frontend/templates/admin/dealers.html | M | B | P16.7 |
| P16.9 | security-14c | CSP flip: `style-src-attr 'none'` after a report-only soak | code | backend/web/security.py, backend/tests/test_no_inline_style_elements.py | S | C | P16.8 |
| P16.10 | security-16 | Map who holds the inventory DSN and what each process runs (read-only) | investigation | docs/SCANNING_OPS_LOG.md or docs/USERS_PG_CUTOVER.md (privilege matrix) | S | A | none |
| P16.11 | security-18b | Security docs step for wave 2 | docs | docs/SECURITY_MASTER_TODO.md, docs/USERS_PG_CUTOVER.md, docs/CLOUD_RESTRUCTURE_PLAN.md | S | D | all |

**P16.1: Session revocation.**
- Change:
  - Add `users.auth_epoch INTEGER DEFAULT 0`: the SQLite ALTER ladder in users_db/schema.py, plus a Postgres migration at the next free number.
  - Bump it ONLY on a self-service password change (`account_password_page`), `complete_password_reset`, `admin_reset_user_password` and the bootstrap force-sync. NEVER in the rehash path. `_schedule_password_rehash` (_common.py:80-105) rewrites the hash in a background thread after a login when the bcrypt cost is low, so the spec's hash-fingerprint design would sign out a user right after they log in.
  - `finalize_app_session` stamps `session['ae']`.
  - A `before_request` in `register_security` compares it, using the request-cached AccessContext read. A mismatch clears the session; a DB error fails open (access.py:212-217).
  - Sessions from before the deploy get the stamp on their first request, so there is no mass logout.
  - The actor's own session is re-finalized after their change.
  - /dev admin sessions (dev_users.db) are out of scope.
- Tests:
  - Two clients: after a reset, client B's `/api/auth/me` returns 401.
  - The user who changed the password stays signed in.
  - An admin reset ends the target's sessions.
  - A login that triggers a rehash keeps its session.
  - A pre-deploy session is stamped.
- Accept: the tests pass; test_billing_gate, test_mobile_auth_api, test_login_session_fixes and test_admin_users pass.

**P16.2: Dead MFA columns.**
- Change:
  - Re-audit consumers. Delete `get_user_totp`, `set_user_totp` and `set_user_mfa_phone` (users_db/auth.py:27-85, :532-550) and their exports (__init__.py:74-80, 120-137), and `get_admin_totp`/`set_admin_totp` (admin_users_db.py ~:190-225).
  - Per D-SEC2:
    - (a) drop `totp_secret`, `mfa_phone` and `mfa_method` (V013:87), and keep `totp_enabled` as an always-0 SMALLINT;
    - (b) also drop `totp_enabled`, and rewrite its consumers: users_db/schema.py:15 (`totp_enabled = 0`), :63/:78 (INSERT); users_db/admin.py:90,121-122,152 (`mfa_label`); users_db/auth.py:285,304-305 (the mfa_phone read); scripts/app_users_status.py:55,75.
  - Update the `_users_select_columns` normalizers (_common.py:31-66). The SQLite ALTER ladder stays tolerant.
  - The migration (the number after P16.1's) drops the Postgres columns. Its header records the SEC-088 answer.
  - First read the prod PG users row count (P0A.2; local is 0). Add a raising DO block (any non-null `totp_secret` → raise) only if that count is 0. Otherwise make the check a pre-flight script: a raising migration at boot would block every later migration.
- Accept:
  - `git grep -n totp_secret` matches only the ALTER ladder, V013, the new migration, the admin_users_db DDL (dev_users.db keeps its schema) and docs.
  - test_billing_gate, test_admin_users, test_mobile_auth_* and test_mfa_* pass.
  - The local migrate dry run lists the migration.

**P16.3: Optional scrub.**
- Change: open users.db and dev_users.db through the app's SQLCipher openers. Dry run by default; it prints the counts of non-empty `totp_secret`, `mfa_phone` and `totp_enabled=1`. Stop if every count is 0. Otherwise, in a stated maintenance window:
  1. `railway ssh` into the running web container; it is the only access to the volume.
  2. Back up to `/data/backups/<ts>/` and confirm the backups open with the key.
  3. Apply one transaction per DB, with `PRAGMA secure_delete` instead of VACUUM against the live DB.
  4. Verify that admin and user logins work.
  5. A dated step deletes the backup after verification; otherwise the seeds survive in it.
- Accept: the counts are 0 after the apply; the logins work; the backup is deleted on schedule.

**P16.4: Scanner SSRF.**
- Change: one resolver helper applies the P3.3 non-global rule with an LRU DNS cache (about a 10-minute TTL), so the CPU-bound scanner keeps its throughput. Call it from:
  - `net/client.send` (:150-159), `http_fetch.open_url` (:86) and the shopperexpress aiohttp session (:283);
  - dealer_eprocess's urlopen (:827), if it survives P17B;
  - recipe_validation, so endpoint hosts must be public, and dealer-URL ingestion.
  It checks only target URLs; `SCANNER_HTTP_PROXY` itself may legitimately be a tailnet 100.x host. `SCANNER_ALLOW_PRIVATE_HOSTS=1` is the escape hatch. The DNS stub lives in the scanner test modules and patches the guard's resolver, never `socket.getaddrinfo` and never the shared conftest.
- Tests: literal and DNS-resolved private/CGNAT hosts are refused; the cache is hit on the second call; the escape hatch works; the scanner characterization tests pass.
- Accept: a read-only scan of dealer_recipes and `workspace/recipes` finds 0 non-public endpoint hosts (0 of 687 locally today); a 2-dealer replay (with dealer logs) gives identical row counts with no measurable slowdown.

**P16.5: Browser launch sites.** Inventory every browser launch outside backend/scanner:
- `backend/scripts/image_downloader.py` writes `cars.image_url` and the gallery (:584) after a Playwright/Stealth launch (:746), with no browser_gate. A browser must never write car rows: route it to discovery, or retire it after a consumer audit.
- `car_data_scraper.py`, `backfill_dealership_addresses.py:436` and `backend/scraping/cli.py`: each is either sanctioned with `require_browser()` and a named operator caller, or deleted after a consumer audit.
- `utils/web_researcher.py` was handled in P3.8.
Tests: `require_browser` refuses without `SCANNER_ALLOW_BROWSER`. Accept: every launch site is either gated or deleted.

**P16.6: Non-root container.**
- Change:
  - Add user `app` (uid 10001).
  - The entrypoint chowns first: `/data` (users.db, dev_users.db, dealer_portal.db and their WAL/SHM files) and `/app/data` (the rate-limit DB). The uploads move to `/data` or are chowned: `DEALER_UPLOAD_ROOT` (dealer/routes.py:61) and `COMMENT_UPLOAD_DIR` (utils/comment_images.py:99-138).
  - Then it runs migrate, the bootstrap AND gunicorn all through `setpriv --reuid=10001 --regid=10001 --init-groups`. Otherwise bootstrap_site_admin, which runs before gunicorn, creates root-owned DB or WAL files and logins break.
  - HOME and the caches are writable; `HF_HOME` is baked world-readable (P15B.3).
  - If D-SEC1=(b), enable `chromium_sandbox` after this unit.
- Accept: a local test with root-owned bind mounts holding an existing users.db plus WAL writes successfully; `ps` shows uid 10001; the Railway boot log shows migrate, bootstrap and the healthcheck passing. Rollback: redeploy the previous image.

**P16.7-P16.9: CSP styles.**
- P16.7: move the 50 static `style=""` attributes (24 templates) into classes in frontend/static/css.
- P16.8: dynamic values become data-* attributes plus a small CSSOM helper (pattern at main.js:425): the site_hub.html:170-227 bars, the car.html:34 rating fill, the data_quality/dev colors. The JS-emitted `style=` strings (car_dealer_map.js:44, find_dealers.js:357, compare.js:204, dev.js:858, admin/dealers.html:361) become classes or CSSOM. Leaflet uses CSSOM only, so the maps are not a blocker.
- P16.9: after a report-only soak in prod (`CSP_REPORT_ONLY`) with no violations, set `style-src 'self'; style-src-elem 'self'; style-src-attr 'none'` in both policies, and extend test_no_inline_style_elements.py to attributes and the header.
- Each of the three units gets a manual visual pass with the run skill: listings, car, compare, home, landing, admin site hub/users/dealers, inventory and the maps. The owner is sensitive to design regressions (no AI-template look).
- Accept: no unstyled elements and no CSP violations.

**P16.10: DSN map.** Read-only. List every holder of `INVENTORY_DATABASE_URL`/`DATABASE_URL`: the Railway services, the MBP and mini `.env`, the tunnel and the home-lane env file. List the SQL that scanner and discovery processes run (no DDL after P13B) and the tables they write. Produce a privilege matrix (role × table × verb) for a least-privilege scanner role and a web-only users-schema role, with an ordered rollout procedure for the owner (D-SEC3). Local catalog SELECTs only. Accept: the matrix and the procedure exist.

**P16.11: Docs.** In SECURITY_MASTER_TODO: SEC-076 Done (P15B.1); SEC-014/061/063 record the TOTP drop and scrub; new rows for P16.1-P16.9; the summary counts match the rows. USERS_PG_CUTOVER: rewrite the encryption section (TOTP is dead data) and record the D-SEC3 answer. CLOUD_RESTRUCTURE_PLAN.md:104: mark the crawl4ai note stale. Accept: the summary counts match the per-row statuses.

**Workflow shape.** Wave A: P16.1, P16.4, P16.5, P16.7, P16.10. Wave B: P16.2, P16.3 (owner), P16.8. Wave C: P16.6, P16.9 (after the soak). Wave D: P16.11. Review lens: no mass logout; no new root-owned files; nothing unsanctioned launches a browser; the visual pass is done.

**Data repair.** P16.3 (optional).

**Exit gate.**
- pytest: test_session_revocation.py, test_users_dead_mfa_removed.py, test_scrub_dead_mfa_secrets.py, test_scanner_net_ssrf.py, test_browser_gate_outside_scanner.py, test_no_inline_style_elements.py, test_csp_policy.py, test_billing_gate.py, test_admin_users.py.
- The non-root container boots on Railway, and the CSP is enforced with no violations.

**Release step.** Minor (users migrations, CSP). Build and run the stage locally first (non-root). The owner applies the two users migrations (auth_epoch; the MFA column drop) to prod Postgres BEFORE the deploy, per the D-DB6 protocol (verified dump, `migrate --dry-run`, `--apply --target N`). Prod users still live in SQLite on the volume, where P16.1's SQLite ALTER ladder adds `auth_epoch` at boot. After the deploy, the owner sets `PASSWORD_RESET_ENABLED=1` (Resend configured) and `EMAIL_VERIFICATION_ENABLED=1`.

## Phase 17A: Consolidation: shims, facades, dead functions

**Goal.**
- Move every importer off the ten `sys.modules` alias shims (about 88 import sites) and delete the shims, with a guard test so they cannot return.
- Delete the dead facade (`synth/validate.py`) and the `vdp/core.py` re-export.
- Remove the dead dealer_run helpers and the stale log line, the dead nav.py chain, functions referenced nowhere, and the empty providers package.
- Keep the recovery strategy names in one place, and correct the sister.tv TODO.

**Entry gate.** Phase 16 released. The unmerged remotes are archived (P2B.8). The mini will pull this release before its next fleet run, because the old import paths disappear.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P17A.1 | dead-code-1 (+ reviewer) | Move importers off 7 small shims (attribution golden included) | code | backend/parsers/dealer_dot_com.py, backend/scanner/phases/dealer_run_steps/{enrich,state}.py, backend/scanner/phases/inventory_scrape.py, backend/scanner/scrapers/dealer_inspire.py, backend/scanner/inventory_recovery.py, backend/enrichment/spec_backfill.py, backend/scanner/cli.py, backend/scanner/orchestrator.py, backend/vision/analyze_images.py; tests: test_dealer_location.py, test_dealer_profile.py, test_dealer_site_url.py, test_rooftop_match.py, test_vdp_html_recovery.py, test_cabin_vision_url_pick.py, test_scanner_vision_env_defaults.py, test_attribution_golden_20261001.py (:230-233) | M | A | none |
| P17A.2 | dead-code-2 | Move importers off the window_sticker, dealer_sticker_provider and listing_gap_fill shims | code | backend/enrichment/{knowledge_engine_specs,window_sticker_service,listing_packages_service}.py, backend/enrichment/verified_specs/electrification.py, backend/routes/car_detail/{assemble,sticker}.py, backend/routes/cars_pages.py, backend/utils/car_serialize/engine.py, backend/utils/msrp_trust.py, run.py; tests: test_car_page_perf.py, test_dealer_sticker_provider.py, test_engine_display.py, test_listing_sticker_ipacket.py, test_sticker_skip_vision.py, test_window_sticker_{parse,scan,service,ui_visibility,urls}.py | M | A | none |
| P17A.3 | dead-code-3 (+ reviewer) | Delete the 10 shims; AST guard test | deletion | backend/scanner/{dealer_location,dealer_profile,dealer_site_url,dealer_sticker_provider,listing_gap_fill,post_pipeline,rooftop_match,vdp_html_recovery,vdp_spec_extract,window_sticker}.py, backend/scanner/__init__.py, backend/attribution/{__init__,match}.py (docstrings), backend/tests/test_no_scanner_alias_shims.py (new) | S | B | P17A.1, P17A.2 |
| P17A.4 | dead-code-4 | Delete `synth/validate.py` (re-check for zero importers after P12B) | deletion | backend/scanner/synth/validate.py, backend/scanner/recipe_synth.py (docstring) | S | A | none |
| P17A.5 | dead-code-5 | Delete the `vdp/core.py` re-export; repoint its 4 consumers | deletion | backend/scanner/vdp/{core,__init__,prefetch}.py, backend/scanner/orchestrator.py, backend/tests/test_monroney_merge.py, test_vdp_spec_gap.py, pyproject.toml (comment) | S | B | P17A.1 |
| P17A.6 | dead-code-6 (+ reviewer) | dealer_run: delete the dead timeout helpers and the stale "Warmup" log (39 fixture entries) | deletion | backend/scanner/phases/dealer_run.py, backend/tests/fixtures/dealer_run_golden_20261001.json, backend/scanner/cli.py (docstring) | S | B | P17A.1 |
| P17A.7 | dead-code-7 | nav.py: delete the unreferenced failure-HAR/warmup chain and `truncate_url` | deletion | backend/scanner/phases/nav.py, debug/README.md | S | A | none |
| P17A.8 | dead-code-8 (+ reviewer) | Token-scan sweep of functions referenced nowhere | deletion | backend/scanner/dealer/{location,sticker_provider}.py, backend/scanner/post_scan/{pipeline,window_sticker}.py, backend/scanner/scan_log.py, backend/scanner/scrapers/{autowall,dealer_inspire}.py, backend/scanner/vdp/extract.py, backend/vision/{analyze_images,claude_rate_limit,claude_vision}.py, backend/utils/llm_client.py | M | C | P17A.1, P17A.2 |
| P17A.9 | dead-code-13 | Delete the empty `intelligence/llm/providers` package | deletion | backend/intelligence/llm/providers/__init__.py | S | A | none |
| P17A.10 | dead-code-12 (+ reviewer) | One source for the recovery strategy names (a stated behaviour change) | code | backend/scanner/dealer/profile.py, backend/scanner/inventory_recovery.py, backend/tests/test_dealer_profile.py | S | B | P17A.1 |
| P17A.11 | dead-code-20 (+ reviewer) | sister.tv TODO replaced with neutral observed state | docs | backend/scanner/synth/platforms/sister_tv.py | S | A | none |

**P17A.1: Seven shims.**
- Change: pure import-path renames (each shim is the same module object):
  - `dealer_location` → `dealer.location`;
  - `dealer_profile` → `dealer.profile`;
  - `dealer_site_url` → `dealer.site_url`;
  - `rooftop_match` → `backend.attribution.match`;
  - `vdp_html_recovery` → `vdp.html_recovery`;
  - `vdp_spec_extract` → `vdp.spec_fetch`;
  - `post_pipeline` → `post_scan.pipeline`, keeping the alias name `scanner_post_pipeline`.
  The import sites, as measured 2026-10-07 (re-grep before editing):
  - `dealer_location`: dealer_dot_com.py:713, dealer_run_steps/enrich.py:111, inventory_scrape.py:177, scrapers/dealer_inspire.py:357, test_dealer_location.py:5.
  - `dealer_profile`: inventory_recovery.py:23, test_dealer_profile.py:7.
  - `dealer_site_url`: dealer_run_steps/state.py:9, test_dealer_site_url.py:5.
  - `rooftop_match`: test_rooftop_match.py:11 becomes `from backend.attribution import match as rm`.
  - `vdp_html_recovery`: test_vdp_html_recovery.py:4.
  - `vdp_spec_extract`: enrichment/spec_backfill.py:232 (if P8C.1 retired spec_backfill, skip).
  - `post_pipeline`: cli.py:58, orchestrator.py:27, dealer_run_steps/enrich.py:13, vision/analyze_images.py:68, :79 and :90, test_cabin_vision_url_pick.py:5 and :16, test_scanner_vision_env_defaults.py:7-8.
  Do NOT touch test_window_sticker_scan.py; P17A.2 owns it. Leave the shim files in place; P17A.3 deletes them. test_attribution_golden_20261001.py:230-233 pins the old path on purpose: drop `rooftop_match` from its import and assert against `backend.attribution.match` instead, after confirming with the attribution owner.
- Accept:
  - The grep matches both `backend[./]scanner[./](names)` and `from backend\.scanner import .*\b(names)\b`, and finds only the shim files and test_window_sticker_scan.py (owned by P17A.2).
  - The diff contains import lines only.
  - The listed tests and the dealer_run golden pass.

**P17A.2: Three shims.** `backend.scanner.window_sticker` → `post_scan.window_sticker` (about 55 references in 19 files: run.py:24, routes/car_detail/sticker.py:40, routes/cars_pages.py:330, window_sticker_service.py (×14), knowledge_engine_specs.py (×8), and the files in the row above; `from backend.scanner import window_sticker` at test_listing_sticker_ipacket.py:26 and test_window_sticker_urls.py:65; `import backend.scanner.window_sticker as ws` at test_car_page_perf.py:398). `dealer_sticker_provider` → `dealer.sticker_provider` (window_sticker_service.py:657, :794, :802; test_dealer_sticker_provider.py:5). `listing_gap_fill` → `post_scan.gap_fill` (listing_packages_service.py:113, window_sticker_service.py:645). test_window_sticker_scan.py:5 moves from `post_pipeline` to `post_scan.pipeline`. The string patch targets change too (test_window_sticker_scan.py:45 → `backend.scanner.post_scan.window_sticker.get_window_sticker_url`; test_dealer_sticker_provider.py:37). Use a word-bounded regex so `window_sticker_service` is never touched. Accept: grep finds only the shim files; the web goldens (car_detail_context, app_surface, serialize_car_for_api) pass.

**P17A.3: Delete the shims.**
- Change: re-run the consumer audit across .py, .sh, .js, .toml, Dockerfile*, deploy/ and scripts/, plus importlib strings; historical docs may still match. Delete the 10 files and update the docstrings. KEEP the logger name `backend.scanner.rooftop_match` at attribution/match.py:64, for log-channel continuity. The guard test walks the AST for Import/ImportFrom nodes, including `ImportFrom(module='backend.scanner', names=[... 'rooftop_match' ...])`, plus mock.patch/monkeypatch string targets. It allowlists the logger literal, which the spec's string check would have flagged.
- Accept: the files are gone; `python scripts/scanner_import_sweep.py` exits 0 (with a timeout, on AC power); the guard fails when a shim is re-added locally; the rooftop corpus and attribution goldens pass.

**P17A.4: synth/validate.** After P12B, re-run `git grep -nE "synth[./]validate|synth import .*validate"`. P12B.3 listed this file only to keep its names, and the verdict found zero importers. If still none, delete it and fix the recipe_synth.py docstring (:29-35). Accept: test_recipe_synth_surface.py, test_recipe_validation.py and test_recipe_synth.py pass.

**P17A.5: vdp/core.** orchestrator.py:68 imports from `vdp.config`, prefetch.py:307 from `vdp.queue`, and the two tests from `vdp.extract` and `vdp.queue`. `vdp/__init__.py` drops the core re-export block (:13-20). Accept: grep is clean; test_monroney_merge.py, test_vdp_spec_gap.py, test_vdp_prefetch.py and test_scan_timing.py pass.

**P17A.6: dealer_run dead code.**
- Change: delete `_warmup_phase_timeout_sec` (:43-50), `_vdp_phase_timeout_sec` (:53-66), `import os` (:10) and the log line at :88.
- Fixture: a scripted transform loads the golden JSON, drops the `["INFO", "Warmup: <name> — navigating to base URL"]` pairs from every scenario's logs (39 entries, one per scenario except missing_url, each a 4-line JSON block), and dumps with the recorder's exact `json.dumps` parameters. Never re-record.
- Rewrite the cli.py module docstring (:2-16) for the HTTP-only flow.
- Accept: the diff removes exactly 39 entries (156 lines); a Python check shows every other key equal; the golden passes without RECORD.

**P17A.7: nav.py.** Delete `capture_scanner_failure_har` (:289) and the helpers only it uses (`_failure_har_enabled` :66, `warmup_delays` :99, `_warmup_signal_timeout_ms` :111, `warmup_settle_after_base_goto` :355), plus `truncate_url` (:427) and any env reads left orphaned. Update debug/README.md:10-12. Accept: grep for those names and `SCANNER_FAILURE_HAR` is empty; `import backend.scanner.discovery_capture, backend.scanner.phases.inventory_scrape` works; test_browser_gate_20260926, test_network_observer and test_dealer_locator pass.

**P17A.8: Sweep.** The candidate list below holds names that each occurred exactly once (the def itself) across tracked .py/.sh/.js/.toml/.yml/.json/.html files on 2026-10-07:
- dealer/location.py:438 `apply_vdp_location_verdict`; dealer/sticker_provider.py:106 `note_sticker_signals_from_vdp`;
- post_scan/pipeline.py:684 `_classify_category_is_cabin` and :696 `_interior_vision_max_gallery_classify`;
- post_scan/window_sticker.py:236 `fetch_window_sticker_text`, :1076 `sticker_option_groups_for_display`, :1354 `car_has_turbo_signal`;
- scan_log.py:35 `active_log_path`;
- scrapers/autowall.py:536 `scrape_autowall_from_page`; scrapers/dealer_inspire.py:134 `_query_algolia_inventory` (the HTTP Algolia replay lives in recipes `PAGINATION_ALGOLIA`);
- vdp/extract.py:119 `_merge_vdp_sticker_url`, :195 `_response_maybe_gallery_image_url`, :237 `_vdp_wants_json_network_capture`, :554 `_vdp_count_gallery_signals`;
- vision/analyze_images.py:66 `pick_image_for_interior_vision`; vision/claude_rate_limit.py:33 `_max_retries` and :77 `run_with_vision_limit`; vision/claude_vision.py:438 `_equipment_batch_enabled`;
- utils/llm_client.py:108 `_claude_model`.
Line numbers will have moved by Phase 17. Re-run the token scan (a small Python script counting identifier tokens over `git ls-files`) right before editing, and delete only names that still have exactly one occurrence and no getattr/importlib/string use. Do one follow-up pass for helpers that lost their last user; stop after 2 passes. Excluded: `reload_scanner_intercept_policy` (owner hook), `platform_registry.entry_to_dict` (P17B.2), the vehicle_reference menu helpers (P17B.10). Deleting `claude_rate_limit._max_retries` orphans `ANTHROPIC_VISION_MAX_RETRIES`, which goes on the owner's env cleanup list. Accept: the token scan of backend/scanner, backend/vision and utils/llm_client.py lists only the excluded names; each deleted name is in the commit message with file:line; these tests pass (chunked): test_window_sticker_*.py, test_claude_vision_finalize.py, test_claude_rate_limit.py, test_llm_client.py, test_llm_call_site_parity.py, test_vdp_recipes.py, test_autowall_http_recipe_20260924.py, test_dealer_location.py, test_dealer_sticker_provider.py, test_analyze_images_cli.py.

**P17A.9: Empty package.** Delete `intelligence/llm/providers` (`__all__ = []`, no importers). Accept: `import backend.intelligence.pipeline` works.

**P17A.10: Strategy names.** Move `RECOVERY_STRATEGY_ORDER` and `HTTP_SAFE_STRATEGIES` into dealer/profile.py and derive `_VALID_STRATEGIES = frozenset(RECOVERY_STRATEGY_ORDER)`. inventory_recovery re-imports both, so its public names stay. State the behaviour change: shopperexpress_api wins will now be cached (profile.py:76-84) and moved to the front of the chain (inventory_recovery.py:185-216). Whether html_next_data and jsonld_listing_html survive is decided in P17B.4. Tests: a cached shopperexpress_api moves first and nothing else reorders. Accept: no literal copy of the set remains.

**P17A.11: sister.tv comment.** Replace the TODO (synth/platforms/sister_tv.py:9-17) with neutral wording: sister.tv is recognized but not synthesizable; 3 dealers (audiofcostamesa-com, audifletcherjones-com, fjmercedes-com) carry a captured recipe with the same library_id 202405220922, replayed alongside their carscommerce recipes; ownership is under investigation by the attribution owner (Appendix B). Comment only. Accept: comment lines only in the diff; test_recipe_synth_surface.py passes.

**Workflow shape.** Wave A: P17A.1, P17A.2, P17A.4, P17A.7, P17A.9, P17A.11. Wave B: P17A.3, P17A.5, P17A.6, P17A.10. Wave C: P17A.8. Review lens: pure renames stay pure; the guard test fails on a re-added shim; golden fixtures are edited only by scripted transforms.

**Data repair.** None.

**Exit gate (includes the dead-code-24 close-out).**
- Chunked offline suite counts equal the P0A.6 baseline, minus deleted tests and plus the new guards.
- `ruff check .` is clean; `scanner_import_sweep.py` exits 0.
- All listed tests pass.

**Release step.** Patch. The mini pulls the release before its next run.

## Phase 17B: Consolidation: browser path retirement, platform registry, LLM/OEM leftovers

**Goal.**
- Retire the `scanner.py --allow-browser` full-scan path, the recovery page strategies, the scraper page halves and the chain/gap_fill Playwright stages. Afterwards, discovery is the only browser entry point in `backend/scanner`.
- Remove the Node/Puppeteer leftovers.
- Reuse synth in the platform registry and fix its seed drift.
- Settle requires_browser, LLM adjudication and vehicle_reference.
- Close out the HTTP-only plan doc.

**Entry gate.** Phase 17A released. D-DC1, D-DC5, D-DC6, D-DC7 and D-DC9 answered. P0A.1 confirmed `SCANNER_ALLOW_BROWSER` is unset on Railway web and on scanner-nightly. P0B.6 classification and P12A.3 reachability findings available.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P17B.1 | dead-code-9 (non-compose part) (+ reviewer) | Remove the Node/Puppeteer leftovers from job_diagnosis and the docs | deletion | backend/scanner/job_diagnosis.py, backend/tests/test_job_diagnosis.py, backend/tests/fixtures/llm_call_site_goldens.json (4 keys), backend/scripts/install_scraper_browsers.sh, docs/CLOUD_RESTRUCTURE_PLAN.md (:37-51) | M | A | none |
| P17B.2 | dead-code-11 (+ reviewer) | platform_registry reuses synth; seed drift guard; template-backed fallback | code | backend/scanner/platform_registry.py, backend/tests/test_platform_registry.py | M | A | none |
| P17B.3 | dead-code-16 (+ reviewer) | Retire the `--allow-browser` full-scan path | deletion | backend/scanner/{cli,orchestrator,browser_gate}.py, phases/dealer_run.py, phases/dealer_run_steps/{session,__init__}.py, backend/tests/test_dealer_run_golden_20261001.py, fixtures/dealer_run_golden_20261001.json, test_browser_gate_20260926.py | L | A | none |
| P17B.4 | dead-code-17a + dead-code missing (dead HTML strategies) (+ reviewer) | HTTP-only recovery chain; decide the HTML strategies | deletion | backend/scanner/inventory_recovery.py, backend/scanner/dealer/profile.py, phases/dealer_run_steps/recovery.py, backend/tests/test_inventory_recovery.py, test_dealer_profile.py, test_browser_gate_20260926.py | M | B | P17B.3 |
| P17B.5 | dead-code-17b (+ reviewer) | Remove the scrapers' page halves (keep their HTTP parsers) | deletion | backend/scanner/scrapers/{dealer_inspire,dealer_venom,dealer_on,pixel_motion,dealer_eprocess}.py, backend/tests/test_dealer_venom.py, test_pixel_motion*.py | M | C | P17B.4 |
| P17B.6 | dead-code-18 (+ reviewer) | Remove the Playwright stage from chain.py and gap_fill (including the legacy path) | deletion | backend/scanner/chain.py, backend/scanner/post_scan/gap_fill.py, backend/tests/test_scraper_chain.py, test_browser_gate_20260926.py | M | C | P17B.3, P17B.4 (both edit test_browser_gate_20260926.py) |
| P17B.7 | dead-code-19 (revised per D-DC9) | requires_browser: drop the special skip branch; fix the hint text | code | backend/scanner/delta_scan.py, backend/scanner/recipe_store.py (doc), docs/data_architecture_plan.md, backend/tests/test_delta_scan_hints.py (new) | S | A | none |
| P17B.8 | dead-code-23 (optional) (+ reviewer) | Clear cached winning strategies that no longer exist (local; prod with the owner) | data_repair | workspace/backups/ | S | D | P17B.4 |
| P17B.9 | dead-code-14 (revised per D-DC6) | Delete the unreachable LLM adjudication branch (or the opt-in adapter, if D-DC6=a) | deletion | backend/intelligence/{agents/adjudicator_agent.py,pipeline/adjudication.py,pipeline/orchestrator.py,llm/client.py}, backend/scraping/cli.py, related tests | S | A | none |
| P17B.10 | dead-code-15 | Move the EPA web-service client; delete the rest of `backend/oem` | deletion | backend/oem/**, backend/enrichment/epa_ws_client.py (new), backend/tests/test_epa_ws_client.py (new), backend/scripts/build_trim_ladders_from_epa.py (docstring), docs/VEHICLE_REFERENCE_BMW.md | M | A | none |
| P17B.11 | dead-code-21 (+ reviewer) | HTTP_ONLY_SCANS_PLAN.md decisions and Phase 2 table; audit note correction | docs | docs/HTTP_ONLY_SCANS_PLAN.md, docs/monolith_audit_2026_10_01/README.md | S | D | P17B.1-P17B.10 |

**P17B.1: Node leftovers.**
- Change: in job_diagnosis:
  - Keep the x86-emulation rule, but drop its `PUPPETEER_EXECUTABLE_PATH` action and point its hint at the Playwright image.
  - Delete the npm "cannot find module" rule (:70-80), the `retry_with_python_scanner` strategy (:27) and the `use_python_scanner` handling (:220-221, :297-298), which nothing consumes.
  - Update the LLM prompt text (:170-172).
- Regenerate ONLY the 4 `job_diagnosis.llm|*` golden keys, with a small script that calls the test module's `_run(site, scenario)` and writes back using `indent=1, sort_keys=True, ensure_ascii=False`. A full `LLM_GOLDEN_REGEN=1` rewrites every key and breaks the CHAT_SITES timeout assertion (test_llm_call_site_parity.py:393).
- Replace the PUPPETEER assertions in test_job_diagnosis.py. install_scraper_browsers.sh installs Playwright Chromium only. Mark the CLOUD_RESTRUCTURE_PLAN.md:37-51 rows done; this unit is that file's only editor in this phase. Locally, `rmdir backend/scanner/node_modules`.
- Accept: grep finds no node_modules, puppeteer, use_python_scanner or patchright outside ignore files and history; the golden diff touches only those 4 keys; test_job_diagnosis, test_job_queue and test_llm_call_site_parity pass.

**P17B.2: Platform registry.**
- Change:
  - Import lazily from `synth.http` (`looks_like_challenge`, `_pace`) and `synth.registry` (`fingerprint_platform`, `harvest_html_vehicles`), so an ImportError fails loudly. Keep the `_recipe_synth_fingerprint` seam, which tests patch.
  - Delete `_BROWSER_UA` (:418) and `_CHALLENGE_MARKERS` (:425). Build the headers from `synth.http._browser_headers`, keeping `Accept-Encoding: identity`.
  - Add seed entries for autowall, wp_vehicles_index, oneaudi and html_cards with EMPTY `html_markers`. Substring markers cannot mirror structural detects (html_cards needs `parsers.html_cards.detect` or data-vin plus an inventory path), and would misclassify.
  - In `classify_dealer` (:633-635), when the fingerprint names a template that has no seed entry, build the result from the template (strategy `synthesize` if `template.synth`, else discovery) instead of falling through to unknown.
  - Delete `entry_to_dict`.
  - State the routing change for national_scan.py and synthesize_recipes.py.
  - Optional data step: back up `workspace/unclassified_platforms.json`, dry-run a reclassification of its hosts with `do_http=False`, and prune the entries that are now classifiable.
- Tests: a drift test (every `PLATFORM_TEMPLATES` name maps to a seed or the alias `{dealer_alchemist: typesense}`, and `seed.synthesizable == (template.synth is not None)`; shopperexpress stays an html_harvest seed with no template); a stub fingerprint of 'autowall' returns platform autowall with strategy synthesize, not unknown.
- Accept: the tests pass; test_recipe_synth_surface and test_platform_fingerprint pass.

**P17B.3: Retire --allow-browser (D-DC5).**
- Change:
  - Remove `--allow-browser` (cli.py:249-252, :349-350). Keep `--http-only` as an accepted no-op for old command lines.
  - orchestrator.py:299-424: no Playwright/Stealth import ladder, no `chromium.launch`, no failure-screenshot `new_page`.
  - `run_dealer` drops the `browser` parameter in the same commit, and the golden scenarios `s_browser_allowed` and `s_browser_given_but_http_only` are removed by targeted edit; every other scenario stays byte-identical. If the parameter is kept for one release, keep `s_browser_given_but_http_only`.
  - `session.open_session` loses its browser branch (:33-38).
  - Rewrite test_browser_gate_20260926.py:30 as "no context even with ALLOW=1".
  - browser_gate's docstring: inside backend/scanner, discovery_capture is the only launcher (plus chain/gap_fill until P17B.6). Launch sites outside the scanner were handled in P16.5.
  Today such a run writes cars with a browser (session.py:33-38 plus inventory_recovery.py:657-692), so the monolith audit note at scanner.md:315 was wrong.
- Accept: `git grep -n "chromium.launch\|async_playwright" backend/scanner` lists only discovery_capture.py (and chain/gap_fill until P17B.6); the golden passes with only the two scenarios removed.

**P17B.4: Recovery chain.**
- Change: the strategy chain keeps the HTTP-safe strategies that can actually run: shopperexpress_api and dealer_eprocess_json. html_next_data and jsonld_listing_html are dead under HTTP-only (recovery.py:58-59 passes `path_htmls=[]` and `page=None`; test_browser_gate_20260926.py:52-56 asserts it). Decide here with the owner: either delete them (and the HTML branch of `detect_platform_hints`, :133-135), or rebuild them on an HTTP-fetched SRP so the capability is real ("rebuild better, never lose capability"; 10 dealers still have html_next_data cached, last 2026-08-05). Drop `ctx.page` and the `if ctx.page is None` filter (:723). recovery.py:51 stops passing a page. Removed names in manifests or caches are dropped with one log line.
- Tests: the chain expectations in test_inventory_recovery.py (:156, :170) use the surviving names, and test_dealer_profile.py likewise.
- Accept: `git grep -n "from_page\|ctx.page" backend/scanner/inventory_recovery.py` is empty.

**P17B.5: Scraper page halves.** Audit consumers per function, recorded in the commit message, and use the P12A.3 reachability findings:
- dealer_inspire: delete the page, browser-Algolia and SRP-intercept functions; delete the module if nothing it holds is imported.
- dealer_venom: delete the page scraper. Keep or merge `_extract_typesense_config_from_html` and `_map_typesense_document` only after comparing them with parsers/typesense.py and synth/platforms/typesense.py; never lose a field mapping.
- dealer_on: delete the page scraper and the SRP pager. KEEP `_extract_vehicles_from_srp_body` and `_map_vehicle_card` (parsers/__init__.py:59, heal_from_recipes.py:136).
- pixel_motion: delete the page scraper and its pagination. KEEP `parse_pixel_motion_inventory_html`, `_is_pixel_motion_html`, `_dismiss_cookie_banner` and `_PIXEL_PATHS`, which discovery's inventory_scrape and site_profile use.
- dealer_eprocess: the page function becomes the results-API call. Delete the browser-only helpers once each is confirmed to need a page.
Accept: test_pixel_motion, test_pixel_motion_stock_leak, test_platform_carfax_packages, test_dep_store_scope_20260926, the dealer_run golden, test_network_observer and test_dealer_locator pass; `import backend.scanner.discovery_capture` works.

**P17B.6: Chain and gap_fill.**
- Change:
  - Delete `chain.PlaywrightFetcher` (:288-~360) and its `DEFAULT_FETCHERS` entry (:426); `default_chain` no longer branches on `browser_allowed`.
  - In gap_fill, delete `_playwright_fetch_html` (:66-105), `_PlaywrightListingFetcher` (:140-155) AND `_fetch_listing_html_legacy` (:164-169). The legacy function calls `_playwright_fetch_html`, and it is the path `deploy/nightly_http_refresh.sh:44` forces with `SCANNER_LISTING_FETCH_CHAIN=0`. It becomes HTTP-only and returns None when the requests result is insufficient.
  - Fix the docstrings at :2, :121, :174 and :208.
  - Tests:
    - test_scraper_chain.py:273-291: the order becomes `['impersonate', 'requests']`;
    - TestFetchListingHtmlIntegration (:345-435; 8 tests that patch `gap_fill._playwright_fetch_html` with `raising=True`) is rewritten or deleted;
    - TestPlaywrightFetcherAvailability (:437-480) is rewritten or deleted;
    - test_browser_gate_20260926.py:39-48 and :74-79 are updated.
  - Precondition (D-DC5): the on-demand listing and sticker fetch from the web (packages_ensure.py:287) loses its browser fallback, which only ran under `SCANNER_ALLOW_BROWSER` (unset in prod per P0A.1). The owner explicitly accepts this against the "leave OEM sticker fetch alone" rule.
- Accept: `git grep -n "sync_playwright\|PlaywrightFetcher\|_playwright_fetch_html" backend/scanner` is empty; ruff F821 is clean; under ALLOW=1, `fetch_listing_html` with a stubbed HTTP 200 still returns the HTML.

**P17B.7: requires_browser (D-DC9 c).** Drop the special branch at delta_scan.py:159-160. requires_browser dealers get the normal no_recipe_yield and hint note, and the note at :165 no longer says "... or browser". This is what HTTP_ONLY_SCANS_PLAN.md:91 asked. The key stays in `SCAN_HINT_KEYS`; no data change. recipe_store.py:56 and data_architecture_plan.md:45-49 describe it as "informational; no code routes on it". Tests: a stubbed replay returning None with `{requires_browser: true}` gets the standard handling. Accept: `git grep -n "go straight to Playwright"` is empty.

**P17B.8: Cached strategy hygiene (optional).**
- Change: local first.
  1. Back up with `\copy (SELECT * FROM dealer_scan_profile)` to CSV.
  2. Dry run: count the removed strategy names (locally: dealer_venom_typesense 10, dealer_on_cosmos 6, dealer_inspire_algolia 5, plus html_next_data 10 if P17B.4 deleted it).
  3. One transaction: `UPDATE dealer_scan_profile SET last_winning_strategy = NULL WHERE last_winning_strategy IN (...)`. Leave `updated_at` untouched, or write it in isoformat with `to_char`: `now()::text` would mix formats with profile.py:72.
  4. Verify.
  Prod only with the owner present.
- Accept: the dry-run query returns 0 rows afterwards, and the total row count is unchanged.

**P17B.9: LLM adjudication (D-DC6).** With the recommended (b): after a consumer audit (backend/scripts/dealer_group_copyright.py:21 is the only entry point), delete adjudicator_agent.py, `pipeline/adjudication.run_llm_adjudication`, the `LLMClient` ABC, `default_llm_client` and the `use_adjudicator` branch (orchestrator.py:114-116). Keep the rules-only hybrid batch. With (a): add an opt-in adapter over backend/llm, behind a `--llm` flag, adding the `adjudicate` role to `ROLE_MODELS` in backend/llm/client.py plus a call-site parity entry. That is new paid-feature scope. Accept: `backend.scraping.cli` runs rules-only with no network.

**P17B.10: vehicle_reference (D-DC7).** Move `sources/epa_client.py`, the only fueleconomy.gov web-service client, to `backend/enrichment/epa_ws_client.py`, with a stubbed-HTTP test. Delete the rest of `backend/oem` (about 1,560 lines) and docs/VEHICLE_REFERENCE_BMW.md, and repoint build_trim_ladders_from_epa.py:21. `data/vehicle_reference/` (untracked) is left alone. Accept: `git grep -n "oem[./]vehicle_reference\|backend\.oem"` in code is empty; the test passes offline.

**P17B.11: HTTP-only plan doc.**
- Change, in HTTP_ONLY_SCANS_PLAN.md:
  - Close D1-D4 as decided on 2026-09-26 (:120): D1 now follows D-SEC1; D3 is verified done; D4 is superseded by D-DC5.
  - Add the D-DC5 to D-DC9 answers.
  - Mark every Phase 2 row done, kept-for-discovery or open, each with its unit id.
  - Append the P0B.6 classification table and progress-log lines.
  - In the monolith audit README: the scanner.md:315 note was wrong (session.py:33-38 plus inventory_recovery.py:657-692 under ALLOW=1), and it was addressed by P17B.3-P17B.5.
  This unit is the only editor of HTTP_ONLY_SCANS_PLAN.md in this phase.
- Accept: no open item lacks either an answer or an explicit "open" tag.

**Workflow shape.** Wave A: P17B.1, P17B.2, P17B.3, P17B.7, P17B.9, P17B.10. Wave B: P17B.4. Wave C: P17B.5, P17B.6 (P17B.3, P17B.4 and P17B.6 all edit test_browser_gate_20260926.py, so they are serial). Wave D: P17B.8 (optional, owner), P17B.11. Review lens: never lose a field mapping or a discovery helper; the web listing/sticker fetch still works over HTTP; the golden edits are scripted.

**Data repair.** P17B.8 (optional).

**Exit gate.** pytest: test_job_diagnosis.py, test_llm_call_site_parity.py, test_platform_registry.py, test_platform_fingerprint.py, test_dealer_run_golden_20261001.py, test_browser_gate_20260926.py, test_inventory_recovery.py, test_dealer_profile.py, test_pixel_motion.py, test_dealer_venom.py, test_scraper_chain.py, test_listing_packages_service.py, test_delta_scan_hints.py, test_epa_ws_client.py, test_scraping_cli_package_imports.py. Discovery imports resolve.

**Release step.** Minor: the browser path is removed. Deploy scanner-nightly and web.

## Phase 17C: Consolidation: vPIC getter, brochure CLI and split, comments_db split, env knobs, ScanConfig

**Goal.** Close the remaining OWNER_DECISIONS items:
- §4: the vPIC getter becomes a patchable module function;
- §3: the brochure CLI rules;
- split brochure_extract (2,403 lines, six jobs) and comments_db (1,074 lines) behind facades;
- register every scanner env knob, retire the dead ones, and introduce a frozen ScanConfig.

**Entry gate.** Phase 17B released. D-OD9, D-OD10 and D-OD11 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P17C.1 | owner-decisions-15 (+ reviewer) | vPIC HTTP getter at module level; the offline stub patches it | code | backend/enrichment/nhtsa_vpic.py, backend/tests/conftest.py, backend/tests/test_vpic_offline_stub.py, test_nhtsa_vpic.py | S | A | none |
| P17C.2 | owner-decisions-16 | Brochure acquisition CLI per D-OD9 | code | backend/enrichment/brochure_acquisition/{reachability,plan,html_specs}.py, backend/scripts/fetch_oem_brochures.py, backend/tests/test_fetch_oem_brochures_surface.py, test_brochure_acquisition_plan.py | S | A | none |
| P17C.3 | owner-decisions-18 | brochure_extract split, step 1: surface golden plus the provenance policy module | code | backend/tests/test_brochure_extract_surface.py (new), backend/enrichment/provenance/{__init__,ladder_stores}.py (new), backend/enrichment/brochure_extract.py | M | A | none |
| P17C.4 | owner-decisions-19 | brochure_extract split, step 2: archive, PDF text, matrix parse, overlay store | code | backend/enrichment/brochure_pdf/{__init__,archive,pdf_text,matrix_parse,overlay_store}.py (new), backend/enrichment/brochure_extract.py (facade), backend/tests/test_brochure_extract.py | M | B | P17C.3 |
| P17C.5 | owner-decisions-20 | comments_db split: comments, attachments, dealer ratings | code | backend/db/comments_db.py, backend/db/comment_attachments_db.py (new), backend/db/dealer_ratings_db.py (new), backend/tests/test_comments_db_surface.py (new) | M | A | none |
| P17C.6 | owner-decisions-21 | Scanner env-knob registry plus an AST lint test | code | backend/scanner/env_knobs.py (new), backend/tests/test_scanner_env_knob_registry.py (new), docs/monolith_audit_2026_10_01/scanner_env_knobs.md (generated) | M | A | none |
| P17C.7 | owner-decisions-22 | Retire browser-era and unread knobs (batches of 10 or fewer) | deletion | backend/scanner/env_knobs.py, per-knob readers, tests | M | B | P17C.6 |
| P17C.8 | owner-decisions-23 | ScanConfig: cli.py builds one frozen run config | code | backend/scanner/scan_config.py (new), backend/scanner/cli.py, backend/scanner/orchestrator.py, backend/tests/test_scan_config.py (new) | L | C | P17C.7 |

**P17C.1: vPIC getter.** A module-level `_http_get_json(url, timeout_s)` that looks up urllib at call time, so tests marked `real_vpic_client` that mock `urllib.request.urlopen` still work. `fetch_decode_vin_values_extended` resolves `_http_get_json` through the module global at call time; never bind it as a default argument, or the conftest patch would not reach it. The autouse conftest stub patches `nhtsa_vpic._http_get_json` to raise URLError, instead of swapping the module's urllib. Tests: test_vpic_offline_stub.py asserts on the new hook. A socket-connect-counting fixture over the upsert tests named at conftest.py:87-91 shows 0 network attempts; `pytest -p no:cacheprovider` blocks nothing, and pytest_socket is not installed. Accept: the error contract `(None, None, 'url_error')` holds offline.

**P17C.2: Brochure CLI.**
- Change:
  - `report_reachability` iterates every `--brand` (None means all).
  - `--user-agent both` outside `--probe-reachability` is an argparse error.
  - `--quarantine-unidentified`'s help says it also sweeps the derived stores.
  - The ledger "fetcher" provenance strings stay, and no subcommands are added.
- Accept: the surface golden is edited only for the new error and the help text; `--probe-reachability --brand Toyota --brand Honda` probes both; the default and `--html-specs` modes with `--user-agent both` exit 2 with a clear message.

**P17C.3 and P17C.4: brochure_extract split.**
- P17C.3: write the surface golden first (public and underscore names, signatures, constant values). Then move the provenance-policy block (L1347-2207) verbatim into `provenance/ladder_stores.py`, with re-exports from the facade, so all 27 non-test importers are untouched. Lazy trim_ladder imports stay lazy, so no cycle forms. Importing `provenance.ladder_stores` must not load pdfplumber (checked through `sys.modules`).
- P17C.4: move the remaining blocks behind the facade, into `brochure_pdf/` (named so, to avoid confusion with brochure_sources and brochure_acquisition): archive (L29-357), pdf_text (L358-761), matrix_parse (L762-1133), overlay_store (L1134-1346 plus L2208-2403). The 8 `monkeypatch.setattr(be, ...)` sites in test_brochure_extract.py move to the owning modules ("patch the submodule, not the facade"). The lru_cache on `_rung_order_from_brochure_text` moves with its function. The 'P' marker semantics are unchanged (D-OD12 is deferred).
- Accept: the golden is green before and after; `git diff --color-moved` shows pure moves; brochure_extract.py is under 150 lines; process_brochure_queue.py and reingest_brochures.py run `--help`.

**P17C.5: comments_db split.** A pure move, with re-exports from comments_db: attachments (L720-891) and dealer ratings (L928-1074, plus `ensure_dealer_ratings_table`). routes/community_api.py (21 references), enrichment/dealer_ratings.py, dealer_portal_db.py and comment_images.py are untouched. The SQLite `ensure_*` DDL stays; P13B.2 already removed the Postgres branches. Accept: the surface test pins names and signatures; test_comments_db, test_comment_attachments, test_community_api_routes, test_dealer_reviews and test_schema_from_migrations pass; the diff is a pure move.

**P17C.6: Knob registry.** `env_knobs.KNOBS` maps each name to its type, default, category (live, operator, browser_era, test_only or internal), readers and a one-line doc. The test walks the AST of backend/scanner for `os.environ.get`, `os.getenv`, `os.environ[...]` and the helper wrappers (`_env_flag` and similar). It fails on an unregistered name, and on a registered name that has no reader. Categories come from git grep: names set in deploy files, scripts, plists or Dockerfiles, or by cli.py/scan_efficiency setdefaults, are operator or internal; names read only by browser code paths are browser_era. Do not name the file `scanner/config/*`, which holds the intercept policy JSON. Accept: the test is green; every knob has a category; the generated table lists each knob's readers and where it is set.

**P17C.7: Retire knobs.** For each browser_era or unread knob: grep tests, scripts, deploy files, docs, .claude/workflows and string uses; confirm it is not in the owner's Railway or mini variable export (P0A.1); then delete it together with the dead branch it guards. Batches of 10 or fewer per commit. Accept: the registry test is green with fewer names; each retired name has 0 grep hits outside CHANGELOG and audit docs.

**P17C.8: ScanConfig.** `ScanConfig.from_args_env(args, environ)` returns a frozen dataclass covering the operator knobs and the CLI flags. cli.py still exports the values to `os.environ`, for compatibility with the subprocesses and per-module readers. The orchestrator and `run_dealer` receive the config. Module readers migrate in later work. Accept: the argparse surface golden is unchanged; the env after `run_cli_entry` is identical to today's for a matrix of flags (`--fast`, `--scan-only`, shard flags); ScanConfig round-trips to the env.

**Workflow shape.** Wave A: P17C.1, P17C.2, P17C.3, P17C.5, P17C.6. Wave B: P17C.4, P17C.7. Wave C: P17C.8. Review lens: moves are pure (color-moved diffs); goldens hold; no knob is removed while still set in the owner's environments.

**Data repair.** None.

**Exit gate.** pytest: test_vpic_offline_stub.py, test_nhtsa_vpic.py, test_fetch_oem_brochures_surface.py, test_brochure_acquisition_plan.py, test_brochure_extract_surface.py, test_brochure_extract.py, test_comments_db_surface.py, test_comments_db.py, test_scanner_env_knob_registry.py, test_scan_config.py. OWNER_DECISIONS.md §3 and §4 are marked resolved with commits (finishing P12B.8's pending items).

**Release step.** Minor (ScanConfig wiring). Deploy scanner-nightly.

## Phase 18A: Hygiene: junk, runtime state, workspace scratch, nightly scripts, k8s, ruff

**Goal.**
- Remove tracked junk, and move runtime state off tracked paths (with a race-safe counter).
- Untrack the force-added scratch, and track only the curated `_learning` files.
- Retire the laptop nightly refresh, and make the report wrappers portable.
- Delete the dead k8s and home-machine deploy assets.
- Tighten ruff in measured steps.

**Entry gate.** Phase 17C released. D-HY3, D-HY4, D-IF6, D-IF7, D-IF9 and D-TC7 answered.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P18A.1 | hygiene-8 | Remove dead tracked junk; ignore rules | deletion | backend/debug/, docs/GUEST_USERS.txt, backend/dictionary/derived/dictionary_catalog.sqlite3, dealers_28173_50mi.json, backend/dealers.json, .idea/, .gitignore | S | A | none |
| P18A.2 | hygiene-9 (+ reviewer) | Runtime state (gas prices, scan counter) off tracked paths; counter is race-safe | code | backend/utils/runtime_state.py (new), backend/cron/{scan_run_counter,sync_gas_prices}.py, backend/routes/fuel_api.py, .gitignore, docs/RAILWAY_SCANNING.md, backend/tests/test_runtime_state_paths.py (new) | S | B | P18A.1 (both edit .gitignore) |
| P18A.3 | hygiene-10 | Untrack the force-added workspace scratch; move the one real fixture | deletion | workspace/ (tracked files), backend/dev/fixtures/manifest_92694_25mi.json, backend/dev/scan_lab.py, backend/scanner/cli.py (help), scripts/scan_irvinebmw_test.sh, .gitignore | S | C | P18A.2 |
| P18A.4 | hygiene-20 (+ reviewer) | Track only the curated `_learning` files (MAIN) | code | .gitignore, workspace/dealer_logs/_learning/{platform_playbook,location_patterns}.md | S | D | P18A.3 |
| P18A.5 | infra-19 (+ reviewer) | Retire the laptop nightly HTTP refresh; NIGHTLY_OPERATIONS.md with a capability table | deletion + docs | deploy/nightly_http_refresh.sh, deploy/com.sarraficars.nightly-http-refresh.plist, deploy/NIGHTLY_REFRESH.md, docs/NIGHTLY_OPERATIONS.md (new) | M | A | none |
| P18A.6 | infra-20 | Portable report wrappers with an explicit DB target | code | deploy/nightly_{data_quality_invariants,rooftop_refusals}.sh, deploy/com.sarraficars.nightly-{data-quality,rooftop-refusals}.plist (templates), deploy/home_lane/install_launchd.sh, backend/tests/test_deploy_scripts_portable.py (new) | M | B | P18A.5 |
| P18A.7 | infra-21 + hygiene-21 | Delete the k8s, car-scanner and postgres manifests, the build.sh k8s block, and deploy/always-on (D-IF9) | deletion | deploy/k8s/, deploy/car-scanner/, deploy/postgres/, deploy/build.sh, deploy/always-on/, docs/INVENTORY_POSTGRES.md (:50), docs/PLATFORM_MASTER_PLAN.md (:578), backend/tests/test_deploy_references.py | M | B | P18A.5 |
| P18A.8 | tests-ci-21 | Ruff: exclude vendored skills; F541 autofix; inventory_db facade `__all__` | code | pyproject.toml, backend/db/inventory_db.py, the 8 F541 files | S | A | none |
| P18A.9 | tests-ci-22 | Ruff F401: consumer-audited removal, then enforce | code | pyproject.toml, about 60 files | M | D | P18A.3, P18A.8 (touches .py files across the tree, so no other unit that edits .py files runs beside it) |
| P18A.10 | tests-ci-23 | Ruff F841: review the 17 dead stores, then enforce | code | pyproject.toml, flagged files | S | E | P18A.9 |
| P18A.11 | tests-ci-24 | Ruff B023/PLE/E7(-E741)/B904 adopted with every site fixed | code | pyproject.toml, flagged files (html_spec_sources.py:304, generic_csv.py, structured.py, import_epa_master.py, ...) | S | F | P18A.10 |

**P18A.1: Junk.** `git rm`:
- `backend/debug/` (15 PNGs plus `last_scrape_samples.json`; no reader, the scanner uses the root `debug/`);
- `docs/GUEST_USERS.txt` (0 bytes);
- `derived/dictionary_catalog.sqlite3` (0 bytes, no references);
- `dealers_28173_50mi.json` and `backend/dealers.json`, after listing the 5+1 dealer ids that are only in them. The owner either drops them or routes them through discovery, never by hand-edit.

Then `git rm --cached -r .idea`, add the ignore rules, correct the stale chroma comment in .gitignore, and `rmdir` the empty preview3/ and preview4/ locally. Accept: grep finds no live reference; `git check-ignore .idea/x backend/debug/x.png` reports both ignored.

**P18A.2: Runtime state.** `state_path(name)` returns `$RUNTIME_STATE_DIR`, or else `<repo>/workspace/state/`; on Railway, `/app/workspace` symlinks to `/data`. Use it for COUNTER_PATH, OUTPUT_PATH and `_LIVE_GAS_PRICES_PATH`. Untrack both JSON files and ignore them; copy the local counter once. Writers use a per-process tmp name plus an `fcntl` lock: shards on one host race the counter's fixed `.json.tmp` (scan_run_counter.py:39) today. Prod web output is unchanged, because the committed gas file equals `fallback_payload()`. Accept: `/api/fuel/lookup` is unchanged with the file absent; a local post-scan leaves `git status` clean; 2 concurrent writers both count.

**P18A.3: Workspace scratch (D-HY4a).** `git rm --cached` the one-off investigate_*.py, *_investigation/results JSON, the chattanooga and 92694 manifests, and the discovery dumps; the local copies stay. Move `manifest_92694_25mi.json` to `backend/dev/fixtures/` and update scan_lab.py:35, the cli.py:156 help and scripts/scan_irvinebmw_test.sh:8. Untrack `unclassified_platforms.json` (a runtime log). `dealers_backup.json` stays tracked until its 43,105 extra dealers exist elsewhere. Accept: `git ls-files workspace` lists only what D-HY4 keeps; scan_lab resolves the fixture.

**P18A.4: Curated _learning (D-HY3a).** Replace `workspace/` in .gitignore with this sequence: `workspace/*`, `!workspace/dealer_logs/`, `workspace/dealer_logs/*`, `!workspace/dealer_logs/_learning/`, `workspace/dealer_logs/_learning/*`, `!workspace/dealer_logs/_learning/platform_playbook.md`, `!workspace/dealer_logs/_learning/location_patterns.md`. `errors_index.md` and `platform_candidates.md` stay ignored: they are machine-written by lifecycle.py:63-70, recipe_validation.py:879, discovery_probe.py:429 and platform_candidates.py:245, and tracking them would dirty every tree. .railwayignore keeps excluding workspace. Commit the 2 curated files (MAIN). Accept: after a local `dealer_pipeline` run, `git status` is clean.

**P18A.5: Retire the nightly refresh.** Audit consumers. Delete the script and its plist. Write docs/NIGHTLY_OPERATIONS.md:
- the Railway lane (schedule, window, shards and locks, cost);
- the home lane;
- the report jobs;
- where logs land;
- a capability table mapping each of the 7 old laptop steps and the 3 k8s cronjobs (schedules copied in P0B.5) to its replacement or its D-IF6 decision. Note that laptop step 7 built cards without `FLASK_ENV=production`, unlike fleet_scan.py:360.
NIGHTLY_REFRESH.md becomes a pointer, or is deleted once references are updated. scripts/scan_irvinebmw_test.sh is out of scope; it never used `--delta` (refuted claim). The `--delta` code path stays. Accept: rg finds no live reference to the deleted script; the capability table covers all 10 items.

**P18A.6: Report wrappers (D-IF7).** `REPO_ROOT` comes from the script's own path. `REPORTS_DATABASE_URL` is required, with no `.env` fallback unless `REPORTS_ALLOW_LOCAL=1`, so a report can no longer silently grade a frozen DB. Add a statement_timeout if the target is prod or a replica. The plists become templates rendered by the P15A.2 installer. Accept: a static test finds no `/Users/` under deploy/ (docs excepted); `bash -n` passes; the scripts exit non-zero without the variable.

**P18A.7: Deploy assets (D-IF9).** P18A.5 copies the k8s schedules first. Then delete deploy/k8s, deploy/car-scanner, deploy/postgres, the k8s block of deploy/build.sh (L44-50) and, per D-IF9, deploy/always-on. Fix the links at INVENTORY_POSTGRES.md:50 and PLATFORM_MASTER_PLAN.md:578. The banners and SCANNER_NODE.md are Phase 18B. Keep post_scan.py and run_nationwide_discovery.py, which have non-k8s consumers. Accept: rg for `deploy/k8s|car-scanner|deploy/postgres|kubectl` matches only banners and CHANGELOG; deploy/build.sh still builds (`bash -n`).

**P18A.8-P18A.11: Ruff (D-TC7).**
- P18A.8:
  - add `extend-exclude` for `.agents/skills`;
  - apply the safe F541 autofix (8 sites) and stop ignoring F541;
  - give the backend/db/inventory_db.py facade an explicit `__all__` listing all 90 re-exports as a contract. Attributes stay, so string monkeypatch targets keep working.
- P18A.9: F401 in per-directory batches. For each unused import, grep `from <module> import <name>`, `<module>.<name>`, string monkeypatch targets and importlib/getattr lookups. Imports with consumers become explicit re-exports; the rest are removed. Run each batch's tests, then the chunked offline suite. Enforce F401. The commit message lists every kept re-export and why.
- P18A.10: review the 17 F841 sites by hand. Delete true dead stores; where one marks a half-finished path, file a finding instead. Enforce F841.
- P18A.11: select B023 (5 sites, loop-closure bugs), PLE (1: a zero-width space at html_spec_sources.py:304), E7 without E741 (about 7) and B904 (7), and fix every site. Each B023 fix gets a test or a one-line justification. Do not adopt BLE001, S608 or PLW0603.
- Accept: `ruff check .` is green after each unit, and the offline pass count matches the P2A.7 baseline.

**Workflow shape.** Wave A: P18A.1, P18A.5, P18A.8. Wave B: P18A.2, P18A.6, P18A.7. Wave C: P18A.3. Wave D: P18A.4 (MAIN; only .gitignore and the two `_learning` files), P18A.9. Wave E: P18A.10. Wave F: P18A.11. `.gitignore` is serial (P18A.1 → P18A.2 → P18A.3 → P18A.4), and the ruff units run with no other code unit beside them. Review lens: nothing deleted that has a consumer; runtime behaviour unchanged; git status clean after scans.

**Data repair.** None.

**Exit gate.** pytest: test_runtime_state_paths.py, test_deploy_scripts_portable.py, test_deploy_references.py, plus the chunked offline suite after P18A.9. Ruff is green with the new rules. `git status` is clean after a local scan.

**Release step.** Patch.

## Phase 18B: Docs

**Goal.** One current entry page, and a README without dead paths. Superseded plans carry banners and recorded outcomes. Security doc scopes point at modules that exist. Docstrings match the code. The location-logging contract matches what the pipeline writes.

**Entry gate.** Phase 18A released. D-HY6 and D-HY7 answered; the owner approves the CLAUDE.md wording for P18B.7.

| ID | From | Title | Kind | Files | Size | Grp | Depends |
|---|---|---|---|---|---|---|---|
| P18B.1 | hygiene-11 | RESUME_HERE as a short pointer page; master-todo retired into EXECUTION_PLAN | docs | RESUME_HERE.md, master-todo.md, docs/EXECUTION_PLAN_2026_09.md, docs/CLOUD_RESTRUCTURE_PLAN.md (inbound links) | S | A | none |
| P18B.2 | hygiene-12 | README cleanup; delete docs/SCANNER_NODE.md; fix debug/README.md | docs | README.md, docs/SCANNER_NODE.md, debug/README.md | S | B | P18B.1 |
| P18B.3 | hygiene-13 | CLOUD_RESTRUCTURE_PLAN.md banner and decision outcomes | docs | docs/CLOUD_RESTRUCTURE_PLAN.md | S | B | P18B.1 |
| P18B.4 | hygiene-15 | Historical banners on SCANNER_CHANGES.md and FULL_AUDIT_MAY2026.md | docs | docs/scanner/SCANNER_CHANGES.md, docs/FULL_AUDIT_MAY2026.md | S | A | none |
| P18B.5 | hygiene-16 | Re-scope SECURITY_MASTER_TODO to the current module layout | docs | docs/SECURITY_MASTER_TODO.md | M | A | none |
| P18B.6 | hygiene-17 (remainder) | Docstrings: discovery tiers, hybrid_search | docs | discovery.py, backend/discovery/cli.py, backend/utils/hybrid_search.py | S | A | none |
| P18B.7 | hygiene-18 | Location-logging contract aligned; seed location_patterns.md | docs | docs/NETWORK_SCAN_PROCESS.md, CLAUDE.md (owner), workspace/dealer_logs/_learning/location_patterns.md | S | A | none |

**P18B.1: Entry page (D-HY6a).** RESUME_HERE.md becomes a short pointer page:
- VERSION read from the file, not hard-coded;
- the branch policy (RELEASING.md);
- run commands checked against run.py;
- prod = Railway;
- links to EXECUTION_PLAN, OWNER_DECISIONS, NETWORK_SCAN_PROCESS, CHANGELOG and this plan.
In master-todo.md, C1, A2 and B2 are done with evidence (ff76d4e58; main.py at 192 lines; migrations V001 onward). B1 splits: inventory done (SEC-102), users cutover pending. The remaining rows fold into EXECUTION_PLAN and the file is retired. Fix the inbound links (CLOUD_RESTRUCTURE_PLAN.md:33, 54, 308, 316, 328). Accept: no version, branch or hosting statement contradicts the repo; there are 0 dead paths.

**P18B.2: README and SCANNER_NODE.** In README.md: L58 (trim ladders live in backend/dictionary/curated/); delete the Node section (L101-108) and the SCANNER_NODE link (L148); check section 3 against today (users, dev_users and dealer_portal are still SQLite; inventory is Postgres). L63-70 was rewritten in P10D.9. `git rm docs/SCANNER_NODE.md` after a grep (master-todo.md:34 was handled in P18B.1). In debug/README.md, drop the scanner.js and e2e_deep_browser.py rows. Accept: 0 dead paths; `git grep SCANNER_NODE` matches only history.

**P18B.3: CLOUD_RESTRUCTURE banner.** "Status 2026-10: superseded by EXECUTION_PLAN_2026_09.md, RAILWAY_SCANNING.md and docs/REMEDIATION_PLAN_2026_10.md." Record the outcomes: D1 decided 2026-09-28 (Railway, no k3s); D3 (persisted grid cards, V023); D2 (moot after SEC-014, or open); D4 and D5 open. Phase 0 is done (Node scanner deleted in ff76d4e58). Section 3.2 (k8s) is "not pursued" (D-IF9). The 19 dead path references are struck through, with the removal commit. Accept: every D item has a dated outcome or "open".

**P18B.4: Banners.** SCANNER_CHANGES.md: "Historical, June 2026, browser-era scanner; current loop: docs/NETWORK_SCAN_PROCESS.md". FULL_AUDIT_MAY2026.md: "Snapshot 2026-05-22; not maintained". Neither file is moved, so inbound links keep working.

**P18B.5: Security doc re-scope.** For each of the 43 `backend/main.py` references in SEC rows, point to the module that defines the route or function today (git grep). The 29 references to deleted files (users_db.py ×13, dev_routes.py ×6, auth/mfa.py ×3, ...) become "(removed <date>, <commit>)" or point at the successor. The Changelog section is left as history. Accept: dead paths remain only in the Changelog; the test files named in Validation collect in under 60 s.

**P18B.6: Docstrings.** discovery.py:5-6 and discovery/cli.py:2: the tiers are DMV → Google Places → OSM (Overpass) → DDG URL gap-fill (pipeline.py:142-172). hybrid_search.py:2: "then SQL filters on the inventory DB (Postgres in production)". Docstrings only.

**P18B.7: Location contract (D-HY7a).** NETWORK_SCAN_PROCESS.md L38-41 and L77-83 name where location evidence is actually written: the scan_runs.md `- location:` line (pipeline/dealer_logs.py:68) and the discovery.md store-location line (:44). A per-dealer location.md is created only for a location quirk; today 0 of 587 dealers have one. The owner edits or approves the matching CLAUDE.md wording, which lists location.md as required today. Seed location_patterns.md (MAIN, tracked since P18A.4) with at least 5 lessons rolled up from the 465 scan_runs.md location lines, each citing its dealer log. Accept: the doc, CLAUDE.md and dealer_logs.py agree.

**Workflow shape.** Wave A: P18B.1, P18B.4, P18B.5, P18B.6, P18B.7. Wave B: P18B.2, P18B.3. Review lens: every command and path in the docs exists; history is kept, not rewritten.

**Data repair.** None.

**Exit gate.** The deadrefs check reports 0 dead paths in README.md, RESUME_HERE.md, debug/README.md and CLOUD_RESTRUCTURE_PLAN.md (outside struck lines), and in SECURITY_MASTER_TODO.md outside its Changelog.

**Release step.** Patch. Close out by marking every phase in Section 5 DONE, DROPPED or DEFERRED.

---

# Part 3: Appendices

## Appendix A: Claim verification table

The status is the final one, after the reviewer verdicts. A verdict that disputed a spec's status overrides it. "Partial" means the claim is true only in part or needs the stated correction; the correction is folded into the units.

### A.1 release

| Claim | Status | Evidence |
|---|---|---|
| main and origin/main sit at 0.2.0, last commit 2026-06-21 | confirmed | main = origin/main = 70c3dbb57; VERSION 0.2.0; tag v0.2.0 → 45a886f41 |
| main is 318 commits behind HEAD and can be fast-forwarded | confirmed | `rev-list` 318/0; ancestor check true; 4 merge commits (662b195e7, fa3b8c814, bacd5f03f, 8b3449164) |
| Releases 1.4.0-1.5.2 exist only on feature/http-only-scans; no tags | confirmed | branch --contains; release SHAs on the first-parent line |
| CI triggers only on main/master pushes and PRs; branch pushes never tested | confirmed (stronger) | ci.yml:3-6; 0 workflow runs, 0 workflows, 0 PRs ever; ci.yml never existed on main |
| Branch is 6 ahead; the pre-push hook refuses an unbumped push | confirmed | pre-push:34 (only checks that VERSION differs) |
| CHANGELOG [Unreleased] is empty | confirmed | CHANGELOG.md:3-5; no 1.3.2 entry; 0.2.0 dated 07-08 vs a 06-14 tag |
| deploy/railway/README.md says deploy from main | confirmed | README.md:35 |
| A Railway service autodeploys from main | partial | setup-full-stack.sh@1746932b7:10,24 used `railway add --branch main`; the stale June worker is running against prod NOW (RAILWAY_SCANNING.md:289-290; dealers_hub.py:40 enqueues) |
| Stale local and remote branches | partial | inventory-consolidation has 15 unmerged commits (2 authors); crawler-llm has 1 |
| The 4 worktrees are fully merged | confirmed | about 1.75 GB; clean status |
| Gitignored files reach the web image, e.g. dictionary_catalog.db | partial (the .db part refuted) | .railwayignore:20 `*.db`; manifest.json and spec_pages do reach it; manifest has no runtime reader |
| GitHub refuses anything but a fast-forward to main | refuted | rulesets [] and main unprotected; only the client refuses |
| Repo is public, made so on 08-24 | partial | PUBLIC confirmed; updatedAt does not date a visibility flip |

### A.2 reconcile (listing retirement)

| Claim | Status | Evidence |
|---|---|---|
| Pipeline reconcile retires by scraped_at with no per-condition protection | confirmed | pipeline/reconcile.py:14-58, :28 allows `inaccurate` and `thin` (assess.py:139-141, :152-161) |
| Scanner reconcile is VIN-set based with a zero-only condition guard and 50% coverage | confirmed | inventory_reconcile.py:86-87, :98-110, :208-248, :257-263 |
| `--no-reconcile` only dry-runs the pipeline path | confirmed | runner.py:105-107; run.py:160-161; lifecycle.py:330-331 |
| The scanner path runs first and the pipeline then undoes its guard | confirmed | dealer_run.py:119-120 → after_write.py:124-136; run.py:147-164 |
| Definition of "since" | confirmed | run.py:35-37; reconcile.py:35-40 |
| 1,595 unexplained retirements on 09-28 | refuted | explained by 28173f712; 1,786 restored (HTTP_ONLY_SCANS_PLAN.md:130); 12-13 one-condition batches (1,399-1,480 rows) never restored; 6 dealers with a bucket left empty |
| Gunn Honda 8-row run retired 382; floor added | confirmed | reconcile.py:31-43 |
| The 40-page cap truncates silently | partial | five silent truncation paths (recipes.py:507, :1050-1157) |
| Union, section-scoped and egress-blocked recipes interact badly | confirmed | exposure 346/423 dealers, 38,532 minority rows (window 09-07) |
| The delta scan reconciles with no condition guard | confirmed | delta_scan.py:324-349 |
| "Two writers" | partial | five writers: plus disown.py:72-81, portal_sync.py:202, hand-run SQL |
| Shard race on the id-only UPDATE | partial (narrow) | upsert/sql.py:150-156; guard still worth adding |

### A.3 recipes

| Claim | Status | Evidence |
|---|---|---|
| save_recipes writes in place, then a best-effort DB write | confirmed | recipes.py:272-282; recipe_store.py:161 logs failures at DEBUG |
| mark_stale and last_ok/coverage updates do not bump saved_at | confirmed | recipes.py:285-291, :1131-1144 |
| load_recipes adopts the DB copy only if its saved_at is newer | confirmed | recipes.py:248-269; 12 dealers with equal saved_at (9 have the DB newer, scottclarkhonda the reverse) |
| Saves are whole-array last-writer-wins | confirmed | recipe_store.py:129-162 |
| Shards rewrite the same recipe files concurrently | partial → confirmed | disjoint rosters, but every shard loads the synth reference dealers through load_recipes (synth/common.py:24-35), which writes |
| Store I/O runs synchronously on the event loop | confirmed | recipes.py:1135/:1144; feed.py:42; discovery_capture.py:86/:169/:179; delta_scan.py:165/213/266/273 |
| ensure_recipe(force) replaces the set; synthesized sets carry saved_at=0 | confirmed | pipeline/recipes.py:68-71, :89; recipe_cascade.py:180; 201 rows locally |
| Egress-tag "blocked:" status is shared across hosts | confirmed | recipes.py:334-367; lifecycle.py:188 |
| recipe_store races (SELECT then INSERT; set_scan_hints read-modify-write) | confirmed | recipe_store.py:141-155, :288-321 |
| CLOUD_RESTRUCTURE_PLAN.md:295 says "prefers the newer copy" | refuted | saved_at is rarely bumped and 0 for synthesized sets |
| Validator and lifecycle state lives only in the Railway store | partial | the MBP wrote to an unreachable DB on 09-29 (scottclarkhonda discovery.md:5) |
| updated_at stands in for the last recipe write | refuted | every hint write bumps it (683/687 rows carry hints) |
| The home-IP prod repair is safe as written | refuted | the MBP cache would push local sets into prod (recipes.py:262-268) |
| The 09-29 repair needs the Phase 1 store work | partial | Phase 0 code plus the cache override is enough (P6B.2) |

### A.4 epa-turbo (forced induction)

| Claim | Status | Evidence |
|---|---|---|
| 2000 Integra mislabelled turbo | confirmed | backend/dictionary/epa/Acura/2000_Acura_Integra_EPA.csv rows 2-4 |
| About 13k of 50k rows mislabelled | confirmed (within 1%) | 8,654 false positives, 473 wrong type, 3,286 missed; 92 ambiguous by flag conflict |
| The importer is import_epa_to_dictionary.py | confirmed | :168-174, bare except; catalog_engine_fields epa_engine.py:87 |
| Root cause is tCharger/sCharger parsing | refuted | those flags were never read; heuristic plus era timing (7d3c76e1c vs 67bb52695) |
| epa_master.forced_induction was filled by build_epa_master.py | partial (provenance refuted) | it was backfill_epa_master_fields.py (fd7ccfddf) |
| cars.forced_induction is affected | confirmed, under-scoped | family join: 19,807 missing, 1,844 false positives without text, 1,640 conflicts, 77,116 NULL on NA families |
| resolver.py scoring uses FI | refuted | resolver.py:159-292 |
| verified_specs consumes FI | refuted | no reader |
| VDP display consumes FI | confirmed | engine.py:566-581; api_specs.py:27-36 |
| Trim ladders consume engineDisplay | partial | plus curated/trim_ladders_epa.json, 1,862 bullets |
| The spec sheet shows FI | confirmed | generated_spec_sheet.py:1130 (wrong source label) |
| The plausibility guard reads FI | confirmed | knowledge_engine_specs.py:270-279, :391 |
| data_quality_invariants is the right home | confirmed | plus the Postgres `\b` bug at :264/:272 |
| Prod has the same corruption | cannot_assess | read-only check in P10D.6 |
| An NA label sticks through COALESCE and prefetch | partial | excluded wins when non-NULL; prefetch is same-dealer only (prefetch.py:197-199) |
| About 70 badge rows need the supplement | partial | 32 are Taycan EVs; about 35 ICE rows |
| vPIC Turbo=Yes means turbocharged | partial | it also flags supercharged JLR V8s and twin-charged Volvo T6 |
| Description text is the strongest evidence | refuted | boilerplate (F-150 V8 'ecoboost'); Raptor R shadowed at forced_induction.py:128 |

### A.5 epa-rebuild (catalog safety)

| Claim | Status | Evidence |
|---|---|---|
| `--rebuild` runs DELETE FROM epa_master | confirmed | build_epa_master_pg.py:97-100, non-atomic (:108-122) |
| The DELETE cascades to epa_extended_specs | confirmed | V001:2201-2205; 49,912 rows; nothing in the repo produces them |
| cars.epa_master_id has no FK | confirmed | V001:192; no index |
| After a rebuild every link dangles | confirmed | new ids; 313,454 links |
| The builder reads ROOT/"DICTIONARY" | confirmed | :40, :86 |
| Root DICTIONARY/ is a stale copy | confirmed | 10,200 files, old schema |
| Pointing at backend/dictionary/epa fixes the load | partial | the insert never writes display/FI/vid; dedupe drops variants |
| Another destructive variant exists | partial | import_epa_master.py:97 (atomic, `--yes`) |
| epa_master_store depends on catalog ids | refuted | reads by (year, make, model) |
| link_cars_to_catalog cannot repair dangling links | partial | full mode repairs dangling active cars that still resolve |
| MIN_CONFIDENCE is involved | partial | the real defect is heap-order tie breaking (:84-86, :335) |
| epa_link_dangling guards this | partial | active-only, after the fact |
| Catalog ids are stable | refuted | fresh ids on reload; no natural key |
| Consumers of DICTIONARY/ | confirmed | only the builder; README.md:34 also mentions it |
| 231,980 links point at legacy twins | refuted | 229,060 (136,001 active) |
| 7,391 legacy ids miss the extended-specs join | partial | 90,524 active cars already get a LIMIT-1 row via row_by_ymmt |
| The CSVs are curated; regenerating them for an EPA id is costly | partial | they are machine output; an EPA id column is cheap |
| Collapse AGREE about 17,360 / DISAGREE about 2,640 | refuted | the rule contradicts its own numbers; replaced by a full re-resolve |
| Extended specs are recoverable only from dumps | partial | in-DB _bak tables also exist |
| The runbook can apply V026 alone | refuted | no `--target`; prod boot applies everything (entrypoint :21-24) |

### A.6 precedence (vehicle facts)

| Claim | Status | Evidence |
|---|---|---|
| vPIC overrides run at scan write, post-scan and in heal scripts | confirmed | vpic_facts.py:65-137, :360-371 |
| Window sticker force-overwrites with no vPIC check | confirmed | window_sticker_service.py:407-444; 1 local conflict (a 4xe) |
| Dictionary writes EPA fallbacks; `--all` overwrites | confirmed (worse) | engine correction overwrites even without --all (:447-480) |
| service.py batch-writes enrichment | confirmed | :997-1004 |
| Read-time and write-time precedence differ | confirmed | verified_specs/drivetrain.py:20-34 |
| First-scan rows carry feed values | confirmed | pipeline heals once per shard (run.py:139-144) |
| Scans outside the pipeline never decode | partial | only scan-only and fast modes |
| "4 spec writers" | confirmed (understated) | nine or more writers |
| Rescans revert healed values | confirmed | upsert/sql.py:62-66; 73 rows |
| Provenance reflects the stored value | refuted | override provenance is dropped (vpic_facts.py:110) |
| Size of current conflicts | confirmed | 1,409 Hybrid rows decode as PHEV |
| Scan-only disables model_specs and EPA writes | partial | post_write runs them on every upsert (database.py:410-416) |
| The tiebreak `A and B or C` is a bug | refuted | intended (:334-337); cosmetic |
| Legacy labels inventory_repair→catalog, listing_gap_fill→dealer | refuted | both are mixed sources |
| prior_spec_src prefetch is enough | partial | the stored values must be prefetched too (guard.py:54-57) |
| The vPIC cache is never refreshed | partial | heal_cylinders_from_vpic is the only live re-decode path |
| `--decode --dry-run` is a dry run | refuted | it writes the cache (heal_from_vpic.py:38-47) |
| D5(b) env change helps scan-only runs | refuted | scan-only returns before the tail (orchestrator.py:462-465) |

### A.7 db-layer

| Claim | Status | Evidence |
|---|---|---|
| inventory_pg regex-rewrites SQLite SQL | confirmed | :117-264 |
| Unmapped INSERT OR IGNORE loses rows silently | partial | a hazard only; none live |
| Unmapped INSERT OR REPLACE fails at runtime | confirmed | service.py:167 |
| DDL lives in 3+ places | partial (understated) | about 17 modules, 10 of them ungated, plus scripts |
| V019 re-creates V001; baseline 19 handles it | partial | the chain cannot replay from empty; 5 V019-only objects; car_move_log_pkey missing |
| Prod is at V018 | refuted locally / cannot_assess prod | local holds V001-V025; prod per P0A.2 |
| init_* runs at web import | confirmed | main.py:95-101 |
| Baseline backup tables | partial | 7 cars_backup_* tables, not 6 |
| datetime.utcnow in the upsert | confirmed | database.py:386; guard.py:111 |
| Strict mode never refuses | partial | the web fails late; the scanner swallows it on every upsert |
| pgvector is a separate store | partial | falls back to DATABASE_URL (pgvector_service.py:48) |
| InventoryEnricher fails on PG (dead path) | confirmed, understated | takes ACCESS EXCLUSIVE on cars with no lock_timeout (service.py:99-102) |

### A.8 tests-ci

| Claim | Status | Evidence |
|---|---|---|
| Lint selects F/E9 and ignores F401/F841/F541; gate is red | confirmed | pyproject.toml:22-35; 3x F811 at test_csrf_delete_routes.py:7,15,24 |
| An offline pytest job exists | confirmed | ci.yml:27-50 |
| The integration job has one test | partial | it has 3 |
| Triggers are main/PR only | confirmed | CI has never run |
| conftest blanks the DSN | confirmed | 3 places; the DQ live test is dead |
| 41 modules have untested PG branches | confirmed | 80 sites |
| sqlcipher3 blocks CI | refuted | the prod image installs a bundled wheel |
| Two conftests | confirmed | plus trim_ladder |
| About 325 files and 3,438 tests | partial | 336 files |
| Markers are barely used | confirmed | 3 integration, 2 slow, 0 regression |
| The replay needs V019 recorded | partial | 5 V019-only objects would be missing |
| No PG test fixture exists | partial | only opt-in live-DB tests |
| CPU torch gives image parity | refuted | prod installs CUDA torch |
| Every push needs a bump | partial | a new branch passes against origin/main (0.2.0) |
| Two shards race on recipe_store | partial | shards never share dealers; low severity |
| UPSERT_CARS_SQL has 3 apostrophes in comments | confirmed | 4 characters; they balance |

### A.9 infra

| Claim | Status | Evidence |
|---|---|---|
| scanner-nightly has no cron | confirmed | RAILWAY_SCANNING.md:7-9, :68 |
| The unstale repair has not run on prod | confirmed | 31 auth-staled dealers (10-04 dump) |
| The conflicts are unread | partial | read from the dump: 12,903 rows, 146 claimants |
| Prod needs idle_in_transaction_session_timeout set | refuted | already 5 min (68ce8433c) |
| About 25 Cloudflare-blocked dealers | partial | 31; some are home-blocked too |
| The home lane contradicts EXECUTION_PLAN | confirmed | EXECUTION_PLAN_2026_09.md:8, :15 |
| Worker/scheduler Dockerfiles install full requirements | confirmed | :18-20 |
| The worker loop runs `--browser-capture` | partial | onboarding is broken (URL dropped; success needs vehicles) |
| Legacy services exist | confirmed | |
| Dockerfile.discovery has no service | confirmed | |
| k8s manifests are dead | confirmed | the only record of the post-scan schedules |
| NIGHTLY_REFRESH step 1 uses --delta | partial | script drift (7 steps, hardcoded psql) |
| Hardcoded /Users paths; launchd still running | confirmed | pid 7112 grading a frozen DB |
| requirements pins playwright; orphan packages | confirmed | |
| Dockerfile.web ships Chromium | confirmed | plus CUDA torch |
| railway.scanner-nightly.json is ignored | partial | unverified on the staged path |
| scan_irvinebmw_test.sh uses --delta | refuted | scan_irvinebmw_test.sh:27 uses --scan-only |
| Pipeline-level locks cover all paths | partial | worker, delta and ad-hoc runs bypass them |
| A median-based guard extension is enough | partial | owners missed by the last run stay exposed |
| The requirements split keeps Docker working | partial | `COPY requirements.txt` alone breaks the `-r` includes |
| un-staled + still-blocked = candidates | refuted | a third "skipped" outcome exists |
| `unset` keeps a var unset | partial | dotenv refills it |
| Railway lists only 3 services | partial | kmac-vault exists |
| The lxml bump risks the scanner image | partial | the scanner image already floats lxml 6 |

### A.10 hygiene and dictionary

| Claim | Status | Evidence |
|---|---|---|
| backend/dictionary is 35,526 files, about 395 MB | partial | about 245 MB of blobs, about 55 MB packed |
| epa/raw/stubs counts | confirmed | 12,126 / 7,996 / 2,004 |
| derived/ is about 276 MB | partial | 211 MB |
| 620 flat root CSVs | partial | 614 options CSVs plus 6 code files |
| 4 mojibake Mazda files | confirmed | |
| Root DICTIONARY/ duplicate | confirmed | |
| Untracked indexes break fresh clones | partial | the catalog DB is load-bearing (223 vs 54 groups); manifest unread |
| Runtime state is committed | confirmed | gas file equals the fallback |
| 0-byte sqlite3 tracked | confirmed | |
| .idea tracked | confirmed | |
| dealers.json tracked | partial | it is the live manifest; the two others are orphans |
| Debug PNGs tracked | confirmed | |
| preview dirs | partial | empty dirs |
| 47 force-added workspace files | confirmed | |
| RESUME_HERE, master-todo, SCANNER_NODE, CLOUD_RESTRUCTURE, SCANNER_CHANGES stale | confirmed | plus the master-todo.md:34 link |
| deploy/railway README stale | partial | proxy-hops hazard (sync-vault :98) |
| FULL_AUDIT stale | partial | needs a banner only |
| net/client, discovery and hybrid_search docstrings stale | confirmed | |
| SECURITY_MASTER_TODO scopes stale | confirmed | 43 + 29 references |
| location_patterns.md stub | confirmed | CLAUDE.md also requires location.md |
| platform_candidates contradictions | confirmed | |
| History rewrite not recommended | confirmed | also commits c40bed8fe.. |
| A filename index reaches 223/300 | refuted | 203/300 (hyphens, aliases) |
| .dockerignore `*.db` excludes nested DBs | refuted | root-anchored |
| brochure_text is script-only | refuted | dormant request-path reader (brochure_extract.py:2213-2245) |
| Deleting root CSVs loses nothing | partial | 97 summary rows exist only in root copies |
| build_epa_master_pg built epa_master | partial | it did not; `--rebuild` cascades |
| `railway redeploy` ships new code | refuted | it reuses the build |

### A.11 dead-code

| Claim | Status | Evidence |
|---|---|---|
| Ten scanner alias shims | partial | about 88 importers; the attribution golden pins old paths |
| Facades are dead | partial | synth/validate is dead; vdp/core has 4 consumers; recipe_synth and dealer_pipeline are live |
| dealer_run helpers dead; stale log line | confirmed | 39 fixture entries, not 8 |
| Browser code is reachable only via discovery | partial | the `--allow-browser` scan path; the web gap_fill path |
| HTTP_ONLY Phase 2 pending D1-D4 | refuted | decided at :120 |
| delta honours requires_browser | partial | it only relabels |
| platform_registry overlaps synth | partial | seed drift (4 templates missing) |
| Three LLM layers | partial | one transport, one router, one dead ABC |
| claude_vision never executes | refuted | reachable via post_scan and enrichment |
| vehicle_reference used only by one script | refuted | no importers; holds the only EPA web-service client |
| node_modules empty | confirmed | stale compose and job_diagnosis references |
| sister_tv synth=None TODO | partial | 3 dealers carry a live captured recipe |
| html_next_data and jsonld_listing_html are live | refuted | path_htmls=[] under HTTP-only |
| No lxml use | partial | requirements-scanner.txt:17 pins it |
| playwright-stealth unneeded | refuted | 4 importers |
| A scoped LLM golden regen is possible | refuted | a full regen rewrites every key |
| No unit changes fleet behaviour | refuted | P17A.10 and P17B.2 change routing and caching |

### A.12 owner-decisions (validation and fetch)

| Claim | Status | Evidence |
|---|---|---|
| §1 VIN floors differ | confirmed | five floors (pipeline/recipes.py:68, synthesize_recipes.py:219-240, recipe_cascade.py:51/240, recipes.py:500-504/:1129, :375/:403) |
| validate_recipe walks 40 pages, the set gate 2 | confirmed | recipe_validation.py:960, :87 |
| Two fetch stacks | confirmed | :1047/:1079/:1111/:1137 vs :688 |
| parse_kept lacks roster place items | refuted | parsers/__init__.py:302-311 |
| Stop conditions disagree | confirmed | a third total reader (parsers/base.py:16-17) |
| Pass semantics differ | confirmed | |
| Bad post_template handling differs | confirmed | |
| RequestsFetcher sends no proxy | confirmed (dormant) | chain.py:255-270 |
| Block status sets differ | confirmed | |
| No VDP profile rotation | confirmed | prefetch.py:372; vdp_recipes.py:266 |
| vdp_recipes never falls back on error | confirmed | :269-281 |
| Timeouts differ | confirmed | |
| Only synth retries and paces | partial | VDP prefetch paces |
| UA and headers differ | confirmed | UA/TLS mismatch under safari17_0 |
| Stray curl_cffi sites | confirmed | |
| net/client is the shared layer | confirmed | docstring stale |
| §3 provenance strings | confirmed | harmless |
| Brochure CLI quirks | confirmed | |
| §4 vPIC closure | confirmed | nhtsa_vpic.py:342-350 |
| brochure_extract still open | confirmed | 2,403 lines |
| comments_db still open | confirmed | 1,074 lines |
| 139 env knobs | partial | 131 |
| Spec-column writers | cannot_assess here | precedence cluster |
| jordanford-net has a 6-VIN CPO feed | refuted | it is a specials widget with 32 hard-coded VINs |
| Junk sets are caught by the gate, not the floor | refuted | 38 live sub-10 recipes, mostly junk |
| 32/592 dealers; 52 HTML walks | partial | 33 and 53 |
| D2-A changes no verdicts | partial | the coverage reject arms more often |
| The git-status no-write check works | refuted | workspace is gitignored |
| try_fetch_via_recipes can do a dry run | refuted | it writes recipes and hints |
| HostProfileMemory is no behaviour change | partial | changes gap_fill's profile order |
| backend/data/brochure_text path | refuted | it is backend/dictionary/derived/brochure_text |
| `-p no:cacheprovider` blocks sockets | refuted | it does not |

### A.13 security

| Claim | Status | Evidence |
|---|---|---|
| crawl4ai holds lxml below 6 | confirmed | requirements.txt:25, :52 |
| PYSEC-2026-87 is blocked | confirmed | security_check.sh:56-59 |
| crawl4ai can be isolated to discovery | refuted | zero importers; remove it (keep brotli) |
| CSP has style 'unsafe-inline' | confirmed | security.py:159/:176; plus an unused esm.sh |
| SEC-096/097/099 off or in progress | confirmed | plus no session revocation; the summary table says 0 open |
| SEC-088 crown jewel is TOTP in PG | partial | TOTP is dead data; mfa_method too |
| Rate limiting is in-process | confirmed | |
| MiniLM is downloaded at runtime | confirmed | no revision pin |
| Chromium runs as root without a sandbox | confirmed | coreBundle.js:41681-41682 |
| TRUSTED_PROXY_HOPS=2 gap | confirmed (understated) | also defeats DEV_IP_ALLOWLIST (dev/routes.py:356-361) |
| Owner still owes DNS/password/Resend | partial | www DNS confirmed missing |
| Discovery isolation leak still open | refuted | fixed in e13d4c3c3 |
| Fail-closed SSRF guard landed | confirmed | CGNAT gap |
| Guarding send() closes scanner SSRF | partial | open_url, aiohttp, urllib bypass it |
| requirements.txt scope | partial | worker/scheduler Dockerfiles too |
| Removing the *.up.railway.app domain closes direct-origin spoofing | refuted | the edge routes by Host |

### A.14 scanner-runtime

| Claim | Status | Evidence |
|---|---|---|
| LOCK_PATH/LOCK_FILE captured at import | confirmed | scan_lock.py:32; runner.py:25 |
| Lock races detected by log scraping | confirmed | run.py:110-135; plus the wait-timeout-after-lost-race bug |
| The 40-page cap is silent | confirmed | 28 cap hits; retirement happens scanner-side first |
| Group-feed walk ends on kept rows | partial | code defect confirmed; autosavvy and terrylabonte were misread |
| validate_recipe >=5 floor | confirmed | |
| utcnow used twice | confirmed | |
| platform_candidates stale from a test leak | confirmed | bmwofmurrieta file dated Aug 4 |
| Upsert per-row cost is CPU | partial | the measurement is wall time |
| InventoryWriteCoordinator retry semantics | partial | connection errors are not retried |
| Post-write runs _ensure_schema per car | partial | DDL is cached; the connection churn is real |
| errors_index leak is 17+1 lines | partial | 17 lines in total (:119-135) |

## Appendix B: Dropped and deferred items

### B.1 Dropped (refuted premise, superseded design, or replaced by a verdict fix)

| # | Source | Item | Reason |
|---|---|---|---|
| B-1 | reconcile | Investigate the "1,595 unexplained retirements" | Refuted: explained by 28173f712. The unrestored residue is handled by P5B.5-P5B.8 and P6B.6 |
| B-2 | infra | Set `idle_in_transaction_session_timeout` on prod | Already 5 min (68ce8433c); only re-checked in P0A.2 |
| B-3 | security | Isolate crawl4ai into the discovery image | Refuted: zero importers, so it is removed instead (P15B.1) |
| B-4 | security | Fix the "open" discovery isolation leak | Fixed in e13d4c3c3 |
| B-5 | epa-turbo | Fix tCharger/sCharger NaN parsing | Refuted root cause: the flags were never read. Replaced by `forced_induction_from_epa` (P10A.1) |
| B-6 | epa-turbo | FI changes in resolver scoring and verified_specs | Refuted consumers |
| B-7 | epa-rebuild-11 | Repoint legacy twins by MPG or ordinal; the AGREE/DISAGREE split | Not resolver-consistent (links would flip on the next scan), and the rule contradicted its own numbers. Replaced by a full re-resolve (P10B.6) |
| B-8 | epa-rebuild-7/16 | "migrate --apply for V026 only" | Not executable: no `--target`, and boot applies every pending file. Replaced by `--target` (P4.2) and staged releases |
| B-9 | epa-rebuild D2(a) | Positional ordinals as the matching key | Unstable (236 ambiguous groups). Replaced by EPA id plus a sticky key plus value matching (P10A.2) |
| B-10 | hygiene-1 | Filename-glob fallback (fnmatch on `_filename_token`) | Reached 203/300, not 223. Replaced by the normalized index (P9.2) |
| B-11 | hygiene-7 | Unconditional exclusion of brochure_text from images | Dormant request-path reader exists. Now conditional on a CI gate (P9.7) |
| B-12 | hygiene-22 | `railway redeploy` to ship new scanner code | Redeploy reuses the old build. Use deploy_scanner_nightly.sh |
| B-13 | release-3 | Per-file-ignore for F811 | pyproject forbids new ignores. Replaced by tests-ci-1's helper (P2A.1) |
| B-14 | release-4 | `if:` dedupe of PR runs | Skipped jobs report success and would mask a red run |
| B-15 | release-10 | Rely on "GitHub refuses non-FF" | Refuted. Never force-push until P2B.5 lands |
| B-16 | release-7 | Premise that dictionary_catalog.db ships in the image | Refuted (`.railwayignore *.db`). The unit now uses ground truth |
| B-17 | dead-code | Retire claude_vision.py as "never executed" | Refuted: reachable from post_scan and enrichment |
| B-18 | dead-code-14 | D6a opt-in LLM adapter as the default | New paid-feature scope. The default is to delete (D-DC6 b) |
| B-19 | dead-code-19 | D9a relabel of requires_browser | Replaced by D9c (drop the special branch) |
| B-20 | dead-code-6 | Remove "8" fixture entries | There are 39. Done by scripted transform |
| B-21 | dead-code-9 | Full `LLM_GOLDEN_REGEN=1` | Would break the CHAT_SITES timeout goldens. Replaced by a 4-key regeneration |
| B-22 | dead-code | Claim that no unit changes fleet behaviour | Refuted. The changes are stated in P17A.10 and P17B.2 |
| B-23 | dead-code-16 | Delete `s_browser_given_but_http_only` while keeping the parameter | Now conditional on the parameter being removed in the same commit |
| B-24 | owner-decisions | D1-A "union admits every recipe with more than 0 VINs" | The rationale is refuted (38 live junk recipes). Replaced by the revised D-OD1, after hygiene (P6A.6) |
| B-25 | owner-decisions-1 | git-status as the no-write proof | workspace is gitignored. Replaced by a sha256 manifest |
| B-26 | owner-decisions-25 | Dry run through `try_fetch_via_recipes`; `pg_dump --data-only` with WHERE | Both write or cannot filter. Replaced (P12B.10) |
| B-27 | owner-decisions-15 | `-p no:cacheprovider` as a socket block | Blocks nothing. Replaced by a connect counter |
| B-28 | owner-decisions-17 | Path `backend/data/brochure_text` | Wrong path. Corrected to `backend/dictionary/derived/brochure_text` |
| B-29 | owner-decisions-4 | A challenge page returns `(403, None)` | Would stale recipes on intermittent challenges. Replaced by a distinct non-stale outcome |
| B-30 | precedence | Parenthesizing the tiebreak as a bug fix | Intended behaviour. Cosmetic edit folded into P8A.4 |
| B-31 | precedence-2 | Legacy labels mapped to catalog/dealer | Mixed sources. Mapped to `unknown` instead |
| B-32 | precedence-2 | Global SOURCE_RANK | Would freeze vPIC text fills. Replaced by per-(field, source) rank |
| B-33 | precedence-16 | `--decode --dry-run` as a dry run | It writes the cache. Split into separate steps (P8A.5, P8C.7) |
| B-34 | precedence-10 | D5(b) as an env-only change | Does nothing under scan-only. Corrected in D-PR5 |
| B-35 | precedence-12 | Delegate Phase A to the cache-only heal | Would lose the only fresh re-decode. Replaced by decode refresh (P8C.2) |
| B-36 | recipes-4 | Backfill saved_at from `updated_at` | Hint writes pollute it, and it flips scottclarkhonda. Replaced by `max(last_ok_at)` plus a skip rule |
| B-37 | recipes-6 | "saved_at untouched" by mutations | Breaks mixed-version hosts. saved_at stays a per-write stamp |
| B-38 | recipes-8 | Archived recipes inside `recipes_json` | Key collisions and old-code revival. Replaced by `superseded_json` |
| B-39 | recipes-11 | Replaced-key rule based on `DB.updated_at` | Replaced by `max_saved_at` and `recipes_written_at` |
| B-40 | recipes-13 | 09-29 prod repair gated behind Phase 3 | Phase 0 is enough. Moved to P6B.2 |
| B-41 | reconcile-1 | Count-based coverage; fresh baseline without an upper bound | Retires on unmatched VINs and counts the run's own rows. Replaced by VIN-matched prior coverage |
| B-42 | reconcile-3 | Change `split_refusals`' return shape | Pinned by callers and tests. Replaced by a new helper |
| B-43 | reconcile-8 | Edit V026 / add strike columns to cars | Unsafe once applied, and touches the upsert. Replaced by a side table in its own migration |
| B-44 | reconcile-10 | HTTP VDP verification before rescans | A rescan re-lists live cars itself. Now rescan-first |
| B-45 | tests-ci-3 | macOS runs as the CI oracle | Cannot reproduce Linux/case issues. CI junit is the oracle |
| B-46 | tests-ci-11 | Nondeterministic strict-xfail race | Flaky. Replaced by a barrier interleave |
| B-47 | tests-ci-17 | Corpus artifact hand-off between jobs | Serializes the pg job. Replaced by a committed corpus |
| B-48 | tests-ci-19 | Cover scanner/cli.py | Would launch scanning. Dropped |
| B-49 | tests-ci D5 | "CPU torch = image parity" rationale | Refuted. CPU torch is kept as a choice, not as parity |
| B-50 | infra-19 | Update scan_irvinebmw_test.sh for --delta | It never used --delta |
| B-51 | infra-3 | Median-based guard extension | Leaves unscanned owners exposed. Replaced by the oldest owner |
| B-52 | infra-5 | Pipeline-only dealer locks | Other paths bypass it. Superseded by locks in dealer_run and delta (P11A.3) |
| B-53 | infra-13 | "Only 3 services remain" acceptance | kmac-vault exists. Corrected |
| B-54 | db-layer-6 | Unconditional record-only V019 | Silent loss on the old prod lineage. Now conditional |
| B-55 | db-layer-5 | DELETE inside the V026 migration | Boot-applied data change with no backup. Moved to P4.4 |
| B-56 | db-layer-8 | Gate all 12 modules, then delete them in db-layer-12 | Double work. Limited to the hot paths |
| B-57 | security-5 | HMAC of the password hash as the session fingerprint | The rehash path would log users out. Replaced by `auth_epoch` |
| B-58 | security D-proxy-origin (b) | Remove the Railway domain as the fix | The edge routes by Host. Hygiene only |
| B-59 | scanner-runtime | autosavvy/terrylabonte as evidence of early termination | Misread. Investigated in P0B.2 instead |
| B-60 | scanner-runtime-6 | Pins (b) and (d) | Duplicate existing tests. Replaced by the replay→persist parity pin |
| B-61 | hygiene-18 | Location option (a) without a CLAUDE.md change | CLAUDE.md also requires location.md. Owner edit added |
| B-62 | hygiene-20 | Track all of `_learning` | Machine-written files dirty every tree. Curated files only |
| B-63 | dead-code-23 | `updated_at = now()::text` | Mixed timestamp format. Corrected |

### B.2 Deferred (kept for later, outside this plan's phases)

| # | Item | Reason / where it goes |
|---|---|---|
| F-1 | Brochure 'P' marker parser fix and overlay re-derivation (D-OD12) | Data is collected in P0B.4. The fix belongs to an enrichment follow-up |
| F-2 | Live EIA gas prices served from Postgres (D-HY8 b) | Status quo accepted |
| F-3 | Users-to-Postgres cutover and the D-SEC3 role isolation | Users cutover cluster. P16.10 provides the privilege matrix |
| F-4 | Refuse known (owner, claimant) VIN pairs in the upsert (D-IF5 d) | Scanner write-path follow-up after the claimant recipes are scoped |
| F-5 | Prod as the single recipe store for every scanner (D-RS1 a) | Once Railway owns all scanning |
| F-6 | `build_engine_desc` layout bug (I{n}/V{n}: 'BMW 2.7 V6', 'Audi 2.2L V5') | Importer follow-up after P10D.1. Base-display changes are reported there |
| F-7 | Audit of other synthesized catalog facts (same class as FI) | Follow-up, per the P10D.9 lesson |
| F-8 | Salesperson names used as dealer_name in the rooftop name tier (northparktoyota, capitolchevy) | Attribution owner |
| F-9 | Alias identities still scanned and written (aaronfordofescondido-com, hughwhitehonda-com) | Recipes/alias owner |
| F-10 | Shared sister.tv library 202405220922 and carscommerce ccid 5379783 across audiofcostamesa/audifletcherjones | Attribution investigation (P17A.11 records it neutrally) |
| F-11 | `dictionary_options` table migration (no callers of `fetch_options_rows`) | Data-model follow-up |
| F-12 | `/dev/api/audit-last-scrape` reads a file nothing writes | Web cluster |
| F-13 | DNS-rebinding window in `fetch_page_text_http` | Pin the resolved address in the connection, as a follow-up |
| F-14 | 4 active cars with an empty dealer_id | Data hygiene follow-up |
| F-15 | Port HD-truck MPG and EV motor count from the archived inventory-consolidation branch (D-REL4 c) | Separate investigation |
| F-16 | Split recipes.py into recipes/{model,store,lifecycle,replay,coverage,promote} (monolith F-7) | After Phases 7 and 12, keeping the facade names |
| F-17 | Dealer-group LLM adjudication as a real feature (D-DC6 a) | New scope |
| F-18 | Optional P16.3 MFA scrub and P17B.8 profile hygiene, if skipped | Low value |
| F-19 | quarantine_package_value_msrps.py `CREATE ... LIKE` drift vs the V019 shape | Script owner (noted in P4.3) |
| F-20 | Lock files (D-SEC7), if skipped | Revisit after the first pip-audit failure |
| F-21 | D-RC4 revisit: retire covered buckets when one recipe fails | After 2 fleet runs with the alarms live |
| F-22 | Web image without the playwright/torch pip packages | When nothing web-side imports them (after P3.8 and P15B.2) |
| F-23 | Delete the `scanner.py --delta` path (delta_scan.py) once the laptop nightly is retired (infra cross-cluster note) | Scanner owner decision. Until then P1A.1, P5A.6 and P11A.3 keep it policy-correct and locked; P18A.5 retires only its caller |
| F-24 | Parse-time cleaning of 'Other'/'n/a' drivetrain, transmission and fuel values (DATA_COMPLETENESS F10), and the cumberlandchryslercenter feed putting a price into drivetrain (F19) (precedence cross-cluster note) | Parser/feed-quality follow-up. Meanwhile P8A.2's rule that a less specific feed value never replaces a stored specific end limits the damage |
| F-25 | The recovery chain (`inventory_recovery` 'replaced', winner dealer_eprocess_json) dropped or relabelled a condition the replay had: hyundaiofcookeville-com on 09-28 replayed 106 used + 180 new, recovery recorded 374 new / 0 used (reconcile cross-cluster note) | Scanner/recovery owner. P5A.1 rule 2 already refuses to retire on recovery-replaced runs |
| F-26 | tests.md test-hygiene items F4, F5, F7 and F10 (tests-ci cross-cluster note) | Test-hygiene owner. Serialize any conftest edits with P14A.1, P14C.1 and P14C.3 (Appendix C.2) |

## Appendix C: Cross-cluster dependency notes

### C.1 Migration number registry and rollout protocol

Numbers follow merge order. The runner refuses a file numbered below the highest one already applied, and applied files are never edited. If a decision drops a migration (for example D-RC2 rejects two-strike, or D-DB8 keeps `cars.zip_code`), renumber only the later, still-unmerged files.

| Reserved | Content | Unit | Phase | Prod apply |
|---|---|---|---|---|
| V026 | Chain completeness: V019-only objects (+ car_move_log_pkey), review_reports, haiku_spec_cache, uq_option_rejections_standing (+ pgvector tables per D-DB7) | P4.3 | 4 | P4.8 |
| V027 | listing_retirements | P5B.3 | 5B | P6B.1 |
| V028 | listing_miss_strikes (if D-RC2) | P5B.4 | 5B | P6B.1 |
| V029 | dealer_recipes rev, recipes_written_at, superseded_json | P7.1 | 7 | Phase 7 release |
| V030 | epa_master t/s flags, provenance, archive tables, idx_epa_master_vehicle_id, idx_cars_epa_master_id | P10A.3 | 10A | 10A release (quiet window) |
| V031 | Unique (source, natural_key); cars FK and extended-specs FK as RESTRICT NOT VALID | P10D.7 | 10D | 10D second release |
| V032 | VALIDATE both FKs | P10D.7 | ships with 11A | after a 0-dangling prod check |
| V033 | Drop cars.zip_code (only if D-DB8 a) | P13A.8 | 13A | quiet window |
| next | Drop backup tables | P13B.4 | 13B | after verified dumps |
| next | users.auth_epoch | P16.1 | 16 | 16 release |
| next | Drop dead MFA columns | P16.2 | 16 | 16 release |

Protocol (D-DB6), for every migration:
1. Apply locally (and to the mini if D-DB5=a) as part of the merge, before any local process runs the new code. Otherwise warn-mode processes fall back to the legacy runtime DDL.
2. Before any deploy that contains the migration: take a prod `pg_dump -Fc` and verify it (`pg_restore -l` TOC check); run `migrate --dry-run`; then `migrate --apply --target N`. The owner runs this.
3. Only then deploy. `MIGRATE_ON_BOOT` is then a no-op.
4. A migration that drops or constrains data merges only after its dumps or prerequisite repairs exist, because every checkout's `migrate --apply` and every boot applies all pending files.
5. Migrations that lock `cars` (indexes, FKs) run in a quiet window, after `pg_stat_activity` shows no idle-in-transaction session, with `SET LOCAL lock_timeout`.

### C.2 Shared files: serialize these

| File | Units, in order |
|---|---|
| backend/scanner/recipes.py | P1B.2 → P1B.3 → P1B.4 → P1B.5 → P1B.6; P5A.2; P5B.1 → P5B.2; P6A.6; P7.2 → P7.3 → P7.4 → P7.5 → P7.6; P12A.2; P12B.4; P12B.5 |
| backend/scanner/recipe_store.py | P1B.2 (log level), P1B.3; P4.6; P7.1; P13B.3 |
| backend/scanner/recipe_validation.py | P1B.8; P5A.3; P5B.2; P7.6; P12B.1 → P12B.2 → P12B.3; P12B.5; P16.4 |
| backend/scanner/inventory_reconcile.py | P1A.1; P5A.4; P5B.3 → P5B.4 |
| backend/scanner/delta_scan.py | P1A.1; P1B.4; P5A.2; P5A.6; P11A.3; P17B.7 |
| backend/scanner/upsert/{sql,guard,write,serialize}.py, backend/scanner/database.py | P4.5 (`_ensure_schema`); P5A.5 (dropped VINs); P6A.3 (database.py:53); P8B.1 → P8B.8 (incl. IN-list chunking); P10A.3 (SQLite DDL); P10C.7; P11A.6 → P11A.7; P11B.3 → P11B.4 |
| backend/enrichment/vpic_facts.py | P8A.4 → P8A.5; P8B.4 |
| backend/dictionary/enrich_from_dictionary.py | P1A.5; P8B.5; P10C.4 |
| backend/utils/forced_induction.py | P10A.1; P10C.5 → P10C.2 |
| backend/scripts/data_quality_invariants.py + baseline json | P8A.1; P8A.7 (baseline); P8C.6; P10B.7; P10C.8; P10D.5. Baselines are always hand edits plus a history line, never a CLI rewrite of every invariant |
| backend/scripts/import_epa_master.py | P1A.2; P10A.8 → P10B.3 |
| backend/scripts/build_epa_master_pg.py | P1A.2; P10B.1 → P10B.2 |
| backend/catalog/{resolver,linker}.py | P1A.3; P10C.3 |
| backend/db/inventory_pg.py | P4.5 (docstring), P4.6 (:338), P4.7 (:117-161, :252-260); P10A.3 (legacy list); P10D.7 (:481); P13A.2; P13B.1 → P13B.2 |
| backend/db/repositories/schema_repo.py | P4.5; P5B.3; P5B.4; P10A.3; P13A.1; P13A.8 |
| .github/workflows/ci.yml | P2A.2 → P2A.4; P14A.3; P14C.1; P14C.3; P14C.4; P15B.7; P15B.8 |
| conftest.py and backend/tests/conftest.py | P1B.1 (root); P2A.3; P8B.4 (decode default off); P13A.6; P14A.1; P14C.1; P14C.3; P17C.1 |
| Dockerfile.web | P3.8 → P9.6 (only if the in-image catalog is chosen) → P15B.2 → P15B.3 → P15B.4 → P16.6 |
| requirements*.txt | P1A.6; P2A.2 (requirements-test.txt); P15A.4; P15B.1 → P15B.2; P15B.8 |
| backend/tests/fixtures/dealer_run_golden_20261001.json | P5A.5; P5B.1 (page events only); P17A.6; P17B.3. Always scripted, targeted edits |
| backend/tests/test_scanner_http_characterization.py | P12A.2 → P12A.4 → P12A.5 |
| backend/web/security.py | P3.4; P16.1; P16.9 |
| docs/SECURITY_MASTER_TODO.md | P3.9; P16.11; P18B.5 |
| docs/HTTP_ONLY_SCANS_PLAN.md | P12B.8; P17B.11 |
| docs/CLOUD_RESTRUCTURE_PLAN.md | P1C.6; P13B.1; P16.11; P17B.1; P18B.1; P18B.3 |
| docs/NETWORK_SCAN_PROCESS.md | P5B.9; P7.8; P8C.4; P10D.9; P12B.8; P18B.7 |
| docs/RAILWAY_SCANNING.md | P1B.6; P1C.6; P2B.7; P5B.9; P6A.3 → P6A.4 → P6A.7; P6B.4; P6B.8; P7.8; P11A.3; P15A.5; P18A.2 |
| backend/tests/test_schema_from_migrations.py | P4.2 → P4.3 (P4.5 uses its own test_schema_strict_mode.py); P7.1; P10A.3; P13B.1; P14A.4 |
| backend/tests/test_browser_gate_20260926.py | P17B.3 → P17B.4 → P17B.6 |
| backend/scanner/recipe_cascade.py, backend/scripts/cascade_recipes.py | P7.5; P12B.5 → P12B.6 |
| backend/scripts/reresolve_legacy_catalog_links.py | P10B.6 → P10B.5 |
| .gitignore | P18A.1 → P18A.2 → P18A.3 → P18A.4 |
| README.md | P10D.8 → P10D.9; P18B.2 |
| docs/data_architecture_plan.md | P1C.6; P7.7 → P7.8; P10D.9; P17B.7 |
| docs/TESTING.md | P14C.4 → P14C.5 → P14C.6 |
| docs/SCANNING_OPS_LOG.md | Append-only. Same-wave writers use `docs/ops_log/<unit id>.md` blocks (Section 2) |
| VERSION, CHANGELOG.md | Release step of each phase only |

### C.3 Traceability: every cluster unit → plan unit

- **release**: 0→P2A.5, 1→P0A.1, 2→P0A.4, 3→P2A.1, 4→P2A.2, 5→P2A.4, 6→P2B.1, 7→P0A.3, 8→P2B.2, 9→P2B.7, 10→P2B.3, 11→P2B.4, 12→P2B.8, 13→P2B.5, 14→P2B.6. Missing: "stop the running worker now"→P0A.4; CHANGELOG backfill→P2B.3.
- **reconcile**: 0 and the missing 0b→P1A.1, 1→P5A.1, 2→P5A.2, 3a→P5A.4, 3b→P5A.5, 4→P5A.6, 5→P5A.7, 6→P5B.1+P5B.2, 7→P5B.3, 8→P5B.4, 9a→P5B.5, 9b/9c→P5B.6, 10→P5B.8, 11→P6B.6, 12→P5A.3, 13→P5B.9. Missing: host parity→P6A.7; shadow run→P5A.8; stale-bucket alarm→P5A.7.
- **recipes**: 1→P1B.2, 2→P1B.3, 3→P1B.4, 4→P1C.3 (prod: P6B.2), 5→P7.1, 6a→P7.2, 6b→P7.3, 7→P7.4, 8→P7.5, 9→P7.6, 10→P1B.5, 11→P1C.2, 12→P1C.4, 13a→P6B.2, 13b→P7.9, 14a→P1C.6 (+P1B.3 docstrings), 14b→P7.8. Missing: cache identity→P1B.6; schema and Postgres CAS test→P7.1; backcompat and rollback→P7.7.
- **epa-turbo**: 1→P10A.1, 2a→P10A.4, 2b→P10A.5, 3→P10C.4, 4→P10A.3, 5→P10A.8, 6a→P10C.1, 6b→P10C.2, 6c→P10C.3, 7→P10C.5, 8→P10C.6, 9→P10C.8, 10→P10C.9, 11→P10D.1, 12→P10D.4+P10D.6, 13→P10D.5+P10D.6, 14→P10D.9. Missing: Complete_Options→P10D.2; retire the heuristic backfill→P1A.5; upsert guard→P10C.7; fleet measurement→P10C.9 dry run (numbers in D-ET2); trim_ladders_epa.json→P10D.3; rollback tooling→P10A.8/P10C.9.
- **epa-rebuild**: 1→P1A.2, 2→P1A.3, 3→P10A.2, 4→P10A.3, 5→P10D.7, 6→P10A.6, 7→P10B.4, 8a→P10B.1, 8b→P10B.2, 9→P10B.3, 10→P10A.7, 11→P10B.6, 12→P10B.8, 13→P10B.7, 14→P10D.8, 15→P10D.9, 16→P10D.6. Missing: dictionary columns→P10A.5; vehicles.csv rows→P10B.5; rollback→P10B.6; migrate target→P4.2; quiescence→P10A.6/P10B.4.
- **precedence**: 1→P8A.1, 2→P8A.2, 3→P8A.4, 4→P8B.1, 5→P8B.2, 6→P8B.5, 7a→P8B.6, 7b→P8B.7, 8→P8B.8, 9→P8B.3, 10→P8B.4, 11→P8A.6, 12→P8C.2, 13→P8C.3, 14→P8C.4, 15→P8A.7+P8C.6, 16→P8C.7. Missing: gap_fill→P8B.9; spec_backfill/persist_enrichment→P8C.1; apply helper→P8A.3; decode refresh→P8A.5; legacy relabel→P8C.5.
- **db-layer**: 1→P0A.2, 2→P4.1, 3→P13A.1, 4→P13A.2, 5→P4.3, 6→P4.2, 7→P4.5, 8→P4.6 (+P13B.2/P13B.3), 9→P4.8+P13A.4, 10a→P13A.5, 10b→P13A.6, 11→P13B.1, 12a→P13B.2, 12b→P13B.3, 13a→P1A.4, 13b→P8B.8, 14→P11A.6, 15→P13B.4. Missing: DDL guard→P13A.3; zip_code→P13A.8; dedupe→P4.4; pgvector census→P0A.2 (+D-DB7); snapshot hygiene→P13A.7.
- **tests-ci**: 1→P2A.1, 2→P2A.2, 3a→P2A.6, 3b..n→P2A.7, 4→P4.2, 5→P14A.1, 6→P14A.3, 7→P14A.4, 8→P14A.6, 9→P14A.7, 10→P14A.8, 11→P14A.9, 12→P14B.1, 13→P14B.2, 14a→P14B.3, 14b→P14B.4, 15→P14B.5, 16a→P4.7, 16b→P14A.5, 17→P14C.1, 18→P14B.6, 19a→P14B.7, 19b→P14B.8, 20→P14C.2, 21→P18A.8, 22→P18A.9, 23→P18A.10, 24→P18A.11, 25→P14C.5, 26→P14C.6, 27→P14C.3. Missing: V026→P4.3; coverage map→P14C.4; network guard→P2A.3; fast-forward main→P2B.3; shared seed helpers→P14A.2.
- **infra**: 1→P0A.1+P0A.2, 2→P6A.1, 3a→P6A.2, 3b→P6A.3, 4→P15A.1, 5→P11A.3, 6→P15A.2, 7→P6A.5, 8a→P15B.2, 8b→P15B.1, 9→P15B.4, 10→P15B.5, 11→P15B.6, 12→P6B.2, 13→P0A.4, 14→P6B.1+P6B.4, 15→P6B.5, 16→P6B.7, 17→P15A.6, 18→P0B.5, 19→P18A.5, 20→P18A.6, 21→P18A.7, 22→P6B.8, 23→P15A.3, 24→P15A.4. Missing: workspace isolation→P1B.2+P15A.2; 72 h guard default→P6A.3; home verification scan→P6B.3; launchd inventory→P0A.5; kill switches→P6A.4+P15A.5.
- **hygiene**: 1→P9.2, 2→P9.6, 3→P9.3, 4→P9.4, 5→P1A.2 (repoint)+P10D.8 (deletion), 6→P9.5, 7→P9.7, 8→P18A.1, 9→P18A.2, 10→P18A.3, 11→P18B.1, 12→P18B.2, 13→P18B.3, 14→P3.7, 15→P18B.4, 16→P18B.5, 17→P12A.1 (net/client part)+P18B.6, 18→P18B.7, 19→P1B.7, 20→P18A.4, 21→P18A.7, 22→P9.8. Missing: hygiene-23 summary-row merge→P9.1; catalog staleness guard→P9.2.
- **dead-code**: 1→P17A.1, 2→P17A.2, 3→P17A.3, 4→P17A.4, 5→P17A.5, 6→P17A.6, 7→P17A.7, 8→P17A.8, 9→P17B.1+P15B.6, 10→P15B.1, 11→P17B.2, 12→P17A.10, 13→P17A.9, 14→P17B.9, 15→P17B.10, 16→P17B.3, 17a→P17B.4, 17b→P17B.5, 18→P17B.6, 19→P17B.7, 20→P17A.11, 21→P17B.11, 22→P0B.6, 23→P17B.8, 24→Phase 17A exit gate. Missing: browser launch sites outside the scanner→P16.5; dead HTML strategies→P17B.4; Railway env precondition→P0A.1; baseline→P0A.6; declared-vs-imported→P1A.6+P15B.1.
- **owner-decisions**: 1→P0B.1, 2→P5B.2, 3→P12A.1, 4→P12A.2, 5→P12A.5, 6→P12A.4, 7→P12A.3, 8→P12A.6, 9a→P12B.2, 9b→P12B.3, 9c→P12B.4, 10→P12B.1, 11→P1B.8, 12→P12B.5, 13→P12B.6, 14→P12B.8, 15→P17C.1, 16→P17C.2, 17→P0B.4, 18→P17C.3, 19→P17C.4, 20→P17C.5, 21→P17C.6, 22→P17C.7, 23→P17C.8, 24→P12B.9, 25→P12B.10. Missing: recipe hygiene→P6A.6; re-judge→P12B.7; DuckDuckGo proxy→D-OD13 in P12A.3.
- **security**: 0→P1A.6, 1→P15B.1, 2→P3.4, 3→P3.3, 4→P3.1, 5→P16.1, 6→P3.6, 7→P3.5, 8→P16.2, 9→P16.3, 10→P0B.3, 11→P3.8, 12→P3.2, 13→P15B.3, 14a→P16.7, 14b→P16.8, 14c→P16.9, 15→P16.4, 16→P16.10, 17→P15B.7, 18a→P3.9, 18b→P16.11, 19→P16.6. Missing: web-research stopgap→Section 8 (Phase 0); venv cleanup→P15B.1.
- **scanner-runtime**: 1→P1B.1, 2→P11A.1, 3→P11A.2, 4→P11A.3, 5→P11A.4, 6→P5B.7 (reframed pin), 7→P1B.2, 8→P5A.2, 9→P5A.7, 10a→P5B.1, 10b→P12B.2, 11→P0B.2, 12→P1B.7, 13a→P1C.1, 13b→P1C.5, 14→P11A.6, 15→P11A.7, 16→P11B.1, 17→P11B.2, 18a→P11B.3, 18b→P11B.4, 19→P11A.8, 20→P12B.5, 21→P11A.5. Missing: misfiled/capped data repair→P0B.2+P5B.8+P6B.6; replay→persist telemetry→P5B.7; feed catch-all→P5A.2; per-dealer lock→P11A.3; release unit→each phase's Release step.

### C.4 Other cross-cluster notes

1. **Retirement and truncation ship together.** The truncation flag (P5A.2, P5A.7) and the scanner-side policy that refuses to retire on incomplete walks (P5A.1, P5A.4) are in the same phase and release. Telemetry alone protects nothing, because the scanner retires inside the scan, before assess.
2. **Forced induction plugs into precedence.** P8A.2 leaves a `forced_induction` placeholder; P10C.2 registers the resolver there. `enrich_car` is edited by P8B.5 (drops FI from FILLABLE) and then P10C.4 (uses the resolver), so only one precedence chain exists.
3. **EPA order.** The catalog provenance backfill and legacy re-resolve (10B) come before the label repairs (10D), because the family tier reads flags only from id'd rows. All 10C code is deployed on every scanning node before any CSV or data repair (10D), so NA labels are understood before they appear. The builder (P10B.2) carries labels via `forced_induction_from_epa` (P10A.1), so a later builder run cannot null them.
4. **Stale scanner services.** release-2 and infra-13 are one unit (P0A.4) under D-REL1.
5. **Dependency removal.** security-1, dead-code-10 and infra-8b are one unit (P15B.1) under D-DEP1. The undeclared `anthropic` SDK was split out as an early hotfix (P1A.6).
6. **Web Chromium.** D-SEC1 merges three clusters' views. If removal is chosen, P3.8 does it early. If hardening is chosen, it waits for non-root (P16.6).
7. **DICTIONARY/.** hygiene-5 and epa-rebuild-14 merge. The builder repoint ships early (P1A.2); the deletion waits until after the prod provenance backfill (P10D.8).
8. **chain.py.** The fetch-policy change (P12A.4) lands before the Playwright-stage deletion (P17B.6).
9. **Migration tooling before Postgres tests.** P4.2 and P4.3 (fresh-chain build) are prerequisites of the pg harness (P14A.1). tests-ci-4 and db-layer-6 are one unit.
10. **workspace/ is untracked.** Units that read or write `workspace/` (recipes cache, dealer_logs, backups, data_quality) are MAIN units. They run serially in the main checkout, and on the mini with the tunnel DSN prefix. Lock files are host-local, so quiescence checks use `pg_stat_activity` plus `pgrep` on both hosts.
11. **Attribution side findings** go to the attribution owner, not into these phases: terrylabonte 324→11 (pinned in P5B.7), the autosavvy possible misfile (P0B.2), the shared sister.tv/ccid recipes, salesperson strings as dealer_name, and alias identities (Appendix B.2).
12. **Golden fixtures** (dealer_run, attribution, upsert, llm_call_site, search, serializer) are shared. Edit them only by scripted, key-scoped transforms, and explain every diff line.
13. **Every push bumps VERSION** (pre-push hook), and only in the phase's release step. Phase 1 pushes once, at the end of 1C. Each later phase releases at least once.

## Appendix D: Risks register

| ID | Risk | Likelihood / impact | Mitigation | Where |
|---|---|---|---|---|
| R-01 | The stale June scanner-worker claims admin-enqueued jobs and writes prod outside the pipeline (no VIN guard, vPIC or logs) | High now / High | Stop and retire it before any push | P0A.4 |
| R-02 | A push to main (or any linked branch) starts Railway builds | Medium / High | Trigger audit; detach; 15-minute post-push check; never force-push | P0A.1, P0A.4, P2B.3 |
| R-03 | SQLite-only tests miss Postgres behaviour (CAS, ON CONFLICT, locks, regex syntax) | High / High | Each unit that changes SQL shows one local-PG check in its PR until Phase 14; adapter hardening (P4.7); then the pg tier | All, P14 |
| R-04 | The first CI runs are red (Linux, Python 3.12, missing assets) and each fix costs a VERSION bump | High / Low | `ci/*` shakedown branches; CI junit as the oracle | P2A.5-P2A.7 |
| R-05 | A merged but unapplied migration flips processes into legacy DDL; boot applies every pending file | Medium / High | Migration protocol (C.1); `--target`; dumps before drop migrations | P4.2, C.1 |
| R-06 | Stricter retirement keeps sold cars listed longer | Medium / Medium | Stale-bucket and no-retirement alarms; two-strike documented; watch `scraped_at` histograms | P5A.7, P5B.4 |
| R-07 | Longer walks trigger WAF 403s on group feeds | Medium / Medium | One-shard measurement; cap 150; pacing unchanged | P5B.2, P6B.4 |
| R-08 | Restores re-list sold cars or a sibling store's cars | Medium / High | Rescan first; verify live; host match; exclude disown batches; owner review | P5B.6, P5B.8, P6B.6 |
| R-09 | The first fleet after the gap reassigns contested VINs (12,903 conflicts outside the 48 h guard) | High / High | Guard gap cover from the oldest owner; 72 h default; refresh the 6 owners first; ownership snapshots and a repair transaction | P6A.3, P6B.3, P6B.5 |
| R-10 | With `SCAN_FLEET=1`, any redeploy or variable change starts a full fleet scan | Medium / High | Run window; capped idle hold; runbook freeze 08:30-12:30 UTC | P6A.2, P6B.7 |
| R-11 | Wrong-DB writes from the mini or the home lane (its `.env` points at its own Postgres) | Medium / High | DSN guards; dedicated prod-lane clone; store fingerprint on the recipe cache; empty-string env exports | P1B.6, P15A.2 |
| R-12 | Mixed-version hosts during the recipe store rollout lose updates | Medium / Medium | Coordinated rollout with fleets paused; backcompat test; max_saved_at maintained | P7, P7.7 |
| R-13 | The first authoritative load destroys cache-side evidence | Low / Medium | Tar caches first; `_reconciled` guard backs up differing caches | P1C.4, P7.2 |
| R-14 | The arbiter freezes stale or junk values under high-rank labels | Medium / Medium | Stale-provenance rule; legacy labels map to unknown; decode refresh | P8A.2, P8A.5 |
| R-15 | 'Naturally Aspirated' asserted on a turbo car via a wrong catalog link | Medium / Medium | Family-unanimous rule; strong text wins; owner reviews the 1,640-conflict sample | P10C.2, P10C.9 |
| R-16 | The catalog re-resolve changes rendered specs for about 136k cars | Medium / Medium | Stratified 200-car sample; undo tool; vPIC precedence intact | P10B.6, P10B.8 |
| R-17 | RESTRICT FKs block restores or the SQLite→Postgres migrator | Low / Medium | Rollback notes (drop FKs before pg_restore); `_TABLES` reorder | P10D.7, P13B.1 |
| R-18 | Golden fixture churn hides real drift | Medium / Medium | Scripted, key-scoped edits only; reviewer reads every diff line | All |
| R-19 | Heavy local work on battery (Docker, full suites, fleet relinks) | Medium / Low | Owner approval; prefer the mini; `run_in_background` | All |
| R-20 | Removing crawl4ai drops transitive packages something still imports (brotli, httpx) | Medium / Medium | Declared-vs-imported hygiene test; explicit declarations | P1A.6, P15B.1 |
| R-21 | CPU torch or an unpinned model changes embeddings and quietly degrades semantic search | Low / High | Revision pin; cosine check against stored vectors | P15B.3, P15B.4 |
| R-22 | The non-root container cannot write users.db, so every login fails | Medium / High | chown first; setpriv for migrate, bootstrap and gunicorn; local root-owned bind-mount test | P16.6 |
| R-23 | Session revocation logs everyone out, or logs a user out after login (rehash) | Low / Medium | auth_epoch; lazy stamp; no bump on rehash | P16.1 |
| R-24 | Shim deletion breaks other checkouts (the mini, collaborators) | Medium / Medium | Mini pulls before its next run; archive tags; guard test | P17A.3, P2B.8 |
| R-25 | The public repo publishes a secret, or user data in history | Low / High | Secrets scan before every push; push protection; D-REL5; D-HY2 rotation | Every release |
| R-26 | Agent runs killed at 180 s per tool call | High / Low | Chunked tests; `run_in_background` with checkpoints | All |
| R-27 | Prod has drifted from local since 2026-09-28, so local numbers mislead prod repairs | High / Medium | Read-only prod checks before every prod write; use prod numbers | P0A.2, every prod unit |
| R-28 | The plan's own backup tables pile up in the public schema | Medium / Low | Listed for P13B.4; snapshot helper writes to a `backups` schema | P13A.7, P13B.4 |
| R-29 | MAIN units collide with concurrent scans | Medium / Medium | Quiescence checks; fleet pause; host lock files plus `pg_stat_activity` | All MAIN units |
| R-30 | CSP and style changes regress the design | Medium / Medium | Visual pass with the run skill; report-only soak first | P16.7-P16.9 |
| R-31 | The web on-demand listing/sticker fetch loses its browser fallback | Low / Medium | Owner decision D-DC5 with the "leave sticker fetch alone" rule stated; `SCANNER_ALLOW_BROWSER` unset in prod today | P17B.6 |
| R-32 | Recipe hygiene stales a legitimate small feed | Low / Medium | Dry-run list reviewed by the owner; restore from backup | P6A.6, P12B.7 |

