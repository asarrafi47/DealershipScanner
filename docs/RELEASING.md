# Releasing

How a change gets from a branch to `main`, to a release tag, and onto Railway.
The rules come from the owner decisions D-REL1 to D-REL10
(`docs/remediation/OWNER_DECISIONS_LOG.md`, 2026-10-07 and 2026-10-08) and plan units
P2B.1 to P2B.8 (`docs/REMEDIATION_PLAN_2026_10.md`, Phase 2B).

The short version: work happens on short-lived branches. A release fast-forwards `main`
and tags it, and Railway gets code only from that tag, through the guarded deploy
scripts.

Every step below has a command that proves it worked. Do not go on to the next step until
that command shows the expected result.

## One-time setup on every clone

```bash
git config core.hooksPath scripts/git-hooks
```

Verify: `git config --get core.hooksPath` prints `scripts/git-hooks`. Every clone needs
this (the MBP, the mini, any fresh checkout). Worktrees share their clone's config.
Without it the pre-push hook never runs, and nothing on your machine stops an unbumped
push or an unreleasable push to `main`.

For Railway: `railway whoami` shows the owner's account, and `railway status` in the
checkout shows project `dealership-scanner`, environment `production`. The CLI acts on
whichever project the current directory is linked to, so run `railway status` before any
Railway command.

**Always pass `-s <service>`** (or `--service <service>`) on every Railway command that
acts on a service: `variable`, `deployment`, `logs`, `redeploy`, `down`, `run`, `ssh`,
`up`. The main checkout is linked to **scanner-nightly** (2026-10-08; still so at the
2026-10-16 N1.5 check), so a command without `-s` acts on the fleet service. On
2026-10-08 an unflagged bare `railway up` uploaded the checkout to scanner-nightly
([Railway source policy](#railway-source-policy)). Relinking the checkout to web is not
the fix: it moves the same hazard onto prod web, where an upload of the working tree
would replace the site. Keep the link, run `railway status` first, and pass `-s` every
time. Commands in this document and in every owner command pack follow this rule.

## Branching model (D-REL2 (a): trunk)

- `main` is the only long-lived branch. It moves only by fast-forward, and only to a
  released commit.
- Each phase works on a short-lived `phase/<id>` branch cut from `origin/main`. `<id>` is
  the phase id, for example `phase/N4`; `docs/NEXT_PHASES_PLAN_2026_10.md` writes the
  same name as `phase/<NID>`.
- Each unit of a phase works on its own branch `unit/<unit id>` (for example `unit/N4.3`
  or `unit/P15B.6`), in its own worktree `.claude/worktrees/<unit id>`, cut from the
  current tip of the phase branch. The unit merges back into the phase branch, and only
  the phase branch is released. Unit branches stay local: a pushed branch needs a new
  VERSION ([bump rule](#bump-rule-d-rel3-a)), and units never bump it. The 0A-2B phase
  runner's unit-branch name, `remediation/<unit>`, is retired (LE-5): new units always
  use `unit/<unit id>`.
- When the main checkout is busy (another phase's workflow, a MAIN scan), the phase
  branch is checked out in an integration worktree, `.claude/worktrees/phase-<id>`, and
  units merge there. Git checks a branch out in only one worktree at a time. So before
  `git switch phase/<id>` in the main checkout (for a MAIN unit or the release
  checklist), remove that worktree with `git worktree remove .claude/worktrees/phase-<id>`.
  That command refuses a dirty tree.
- A phase releases with `git push origin phase/<id>:main` (a fast-forward). The phase
  branch is deleted afterwards. A phase branch is rebased on `origin/main` only when
  another release moved `main` after the branch was cut (release checklist step 1 then
  does not print `fast-forward-ok`). Do not rebase otherwise: a plain
  `git rebase origin/main` rewrites a phase branch that holds merge commits even when
  `main` has not moved. It drops the merges and replays the unit commits in a line.
- After the release, from a checkout of `main` at the release, the phase's unit
  worktrees are removed (`git worktree remove .claude/worktrees/<unit id>`) and its unit
  branches deleted with `git branch -d unit/<unit id>`, which refuses a branch that is
  not merged. If the phase was not rebased, every unit tip is in the release and `-d`
  succeeds. After a rebase the unit commits were rewritten, so `-d` refuses each unit
  branch whose commits were replayed. That refusal is expected. For each refused
  branch, run `git cherry v<VERSION> unit/<unit id>`. A line starting with `-` is a
  commit whose change is in the release unchanged. A line starting with `+` is one whose
  change is not (the rebase hit a conflict there, or the commit was lost). Delete the
  branch with `git branch -D unit/<unit id>` only when no `+` line is left, or after
  comparing each `+` commit with the release by hand (`git show <sha>`).
- `feature/http-only-scans` was the integration branch until the first release to `main`
  (P2B.3, v1.5.4). Its tip on origin is still `486f9012d`, the `v1.5.4` tag commit, which
  `main` already contains. No new work goes to it. The owner retires it once four gates
  hold ([Retiring `feature/http-only-scans`](#retiring-featurehttp-only-scans)). No
  agent deletes it.
- `ci/shakedown-<n>` branches are throwaway CI shakedowns (see the bump rule). Delete them
  once green.
- Never force-push, on any branch. Until the P2B.5 ruleset is active, GitHub itself would
  accept a force push to `main`. Only the person pushing prevents it.

Start a phase, in the main checkout:

```bash
git fetch origin
git switch -c phase/<id> origin/main
```

Verify: `git merge-base --is-ancestor origin/main HEAD && echo based-on-main` prints
`based-on-main`.

Or, when the main checkout is busy, create the phase branch in its integration worktree
and leave the main checkout where it is:

```bash
git fetch origin
git worktree add .claude/worktrees/phase-<id> -b phase/<id> origin/main
```

If `phase/<id>` already exists (for example after the worktree was removed for a MAIN
unit), drop `-b`: `git worktree add .claude/worktrees/phase-<id> phase/<id>`.

Verify: `git -C .claude/worktrees/phase-<id> branch --show-current` prints `phase/<id>`,
and
`git -C .claude/worktrees/phase-<id> merge-base --is-ancestor origin/main HEAD && echo based-on-main`
prints `based-on-main`.

Start a unit, from the main checkout:

```bash
git worktree add .claude/worktrees/<unit id> -b unit/<unit id> phase/<id>
```

Verify: `git -C .claude/worktrees/<unit id> branch --show-current` prints
`unit/<unit id>`, and
`git -C .claude/worktrees/<unit id> merge-base --is-ancestor phase/<id> HEAD && echo based-on-phase`
prints `based-on-phase`.

### Retiring `feature/http-only-scans`

D-REL2 (a) retires this branch after the first release. P2B.8 step 7 adds two
conditions: the next release must start from `main`, and the mini's clone must show no
unpushed work. As for the two D-REL4 (a) branches, ksarrafi hears about it first.
NEXT_PHASES_PLAN unit N4.3 collects these conditions here. The owner runs every command
in this section; an agent only prepares them. Do not start until all four gates hold.

Gates 1 and 3 are met by a log row with a fixed decision text, and each Verify greps for
exactly that text, so it prints nothing while the gate is open. Other rows already name
ksarrafi (the 2026-10-08 D-REL4 row, about the two D-REL4 branches) or this branch
(D-REL2, the 1.5.3 push, N4.4's record), so a plain grep for either word proves nothing.
Do not quote the two texts in any other row of the log.

1. **ksarrafi has been told** that `feature/http-only-scans` will be deleted. Record a
   dated row in `docs/remediation/OWNER_DECISIONS_LOG.md` whose decision cell reads
   `N4.3 gate 1: ksarrafi told feature/http-only-scans will be deleted`, or tell the
   agent, which adds that row.

   Verify: `grep -nF 'N4.3 gate 1: ksarrafi told' docs/remediation/OWNER_DECISIONS_LOG.md`
   prints that one dated row. It prints nothing until the row exists.
2. **The first phase release is on `main`.** That means `main` has moved past
   `486f9012d` to a release cut from a `phase/<id>` branch, and that release's tag is on
   origin.

   ```bash
   git fetch origin --tags --prune
   git merge-base --is-ancestor origin/feature/http-only-scans origin/main && echo merged-into-main
   git show origin/main:VERSION
   git ls-remote --tags origin "refs/tags/v$(git show origin/main:VERSION)"
   ```

   Verify: `merged-into-main` is printed, the VERSION is above `1.5.4`, and `ls-remote`
   prints one line.
3. **The mini's clone has been checked, or the owner has waived the check.** P2B.8 step
   7 asks that `git status` and `git log @{u}..` on the mini show no unpushed work.
   Under D-N2 (b) the mini has no role and there is no SSH approval, so this check runs
   only if the owner runs it on the mini (N4.4 (a)). Otherwise the owner waives it: a
   dated row in `docs/remediation/OWNER_DECISIONS_LOG.md` whose decision cell reads
   `N4.3 gate 3 waived: retire feature/http-only-scans without the mini check`, or a word
   to the agent, which adds that row. A waiver loses nothing on origin, because the
   branch tip is the `v1.5.4` commit, which `main` already contains. Any unpushed commits
   on the mini stay in the mini's clone.

   Verify: either a dated block at the end of `docs/ops_log/N4.4.md` holds the owner's
   output of N4.4's mini command (`git status --short --branch`, `git log --oneline
   --branches --not --remotes`, `git stash list`), with a clean status and both lists
   empty, or `grep -nF 'N4.3 gate 3 waived' docs/remediation/OWNER_DECISIONS_LOG.md`
   prints that dated row. N4.4's command covers every local branch and the stash, not
   only the checked-out branch that `git log @{u}..` checks.
4. **No checkout or worktree is on the branch.**

   ```bash
   git worktree list | grep -F '[feature/http-only-scans]'
   ```

   Verify: it prints nothing. If the main checkout is still on the branch, switch it
   to `main` or a phase branch first (an owner step while another workflow uses the
   checkout). Nothing else needs the branch name: CI runs on every branch
   (`.github/workflows/ci.yml`), and P0A.1 found no Railway deployment trigger at all
   (the trigger query in [How to verify](#how-to-verify-read-only) returns
   `"edges":[]`).

Then delete the branch, on origin and locally. The pre-push hook lets branch deletions
through ([bump rule](#bump-rule-d-rel3-a)). The N1.7 ruleset
(`workspace/phase_runs/N1/ruleset.json`) targets only `refs/heads/main`, so it does not
block this delete.

```bash
git push origin --delete feature/http-only-scans
git branch -d feature/http-only-scans
git fetch --prune origin
```

Verify: `git ls-remote --heads origin feature/http-only-scans` prints nothing.
`git rev-parse 'v1.5.4^{commit}'` still prints `486f9012d…`. The branch needs no
`archive/*` tag: unlike the D-REL4 branches it is fully merged, the `v1.5.4` tag already
names its tip, and the N1.7 ref bundle
(`workspace/backups/repo_all_refs_20261016T224046Z.bundle`) holds it too. If the owner
wants an archive tag anyway, create and push an annotated
`archive/http-only-scans` at `486f9012d` before the delete. That follows N1.7's
naming, which drops the `feature/` prefix.

If the mini returns (N4.4 (a)), a clone still on the old branch moves to the trunk
with `git fetch --prune origin` and then `git switch main`. Do this only after its
unpushed-work check is clean. If it has no local `main` yet, `git switch` creates one
tracking `origin/main`.

## Bump rule (D-REL3 (a))

- **Every push carries a new VERSION.** `scripts/git-hooks/pre-push` compares VERSION at
  the pushed commit with VERSION at the remote's current commit for that branch. For a new
  branch it compares with your local `origin/main`. If the two match, it refuses. It lets
  three kinds of push through: a push whose only changes are VERSION and CHANGELOG.md,
  tag pushes, and branch deletions. There is no exemption by branch name: `phase/*` and
  `ci/*` follow the same rule.
- **Pushes to `main`** are judged by `scripts/release_guard.py` instead, in the hook and in
  CI. VERSION must rise above the remote `main`'s VERSION by semver (1.5.10 > 1.5.9), and
  CHANGELOG.md must have a `## [X.Y.Z]` section with at least one line of notes. If
  `[Unreleased]` was empty at the bump, the new section is empty too, and the guard
  refuses it.
- **The CI `release-guard` job** runs only on pushes to `main` and on PRs to `main`. It
  answers "is this releasable?". On any other branch it shows as skipped, by design.
  `lint`, `pytest` and `pytest-pg` run on every branch push.
- **To bump**, run `scripts/bump_version.sh patch` (or `minor`, `major`, or an explicit
  `x.y.z`). It writes VERSION, renames `## [Unreleased]` to `## [X.Y.Z] - <today>`, opens
  a new empty `[Unreleased]` above it, and stages both files. It does not commit.
- Work units never bump VERSION. The release step bumps it once per phase. A phase branch
  pushed before its release would need a bump for every push, so normally a phase branch
  is pushed for the first time at its release.
- **CI shakedowns** (P2A.5): push the already-bumped integration HEAD to a new name each
  time, `git push origin HEAD:refs/heads/ci/shakedown-<n>`. A new branch is compared with
  `origin/main`, so it needs no extra bump. Re-pushing the same name with more work would
  need one.

Verify the rule: `python -m pytest backend/tests/test_release_tooling.py -q -p no:cacheprovider`
passes. It drives the real hook and guard in scratch repositories.

## CI runs per release

A branch with an open PR to `main` runs CI twice per push: once as `push` (concurrency
group `ci-refs/heads/<branch>`) and once as `pull_request` (group `ci-<PR number>`). This is
deliberate (see the comment in `.github/workflows/ci.yml`). An `if:` that skipped one of
the two would leave skipped jobs on the same SHA, and GitHub counts a skipped job as a
success, so a red run could be hidden. The push to `main` runs CI a third time on the
same SHA. Expect about three runs per release SHA. The repo is public (D-REL5), and
GitHub-hosted runners cost nothing for public repos, so the duplication costs wall time,
not minutes.

## Release checklist

Run these from the branch being released, in the main checkout, with a clean tree. That
branch is `phase/<id>` (the first release, 1.5.4, ran from `feature/http-only-scans`).
If the phase branch is checked out in `.claude/worktrees/phase-<id>`, remove that
worktree first ([Branching model](#branching-model-d-rel2-a-trunk)). `<branch>`
below stands for its name. `V` is the new version, set in step 3.

1. **Preconditions.**

   ```bash
   git fetch origin
   git status --porcelain --untracked-files=no
   git merge-base --is-ancestor origin/main HEAD && echo fast-forward-ok
   git diff --name-only origin/main..HEAD -- migrations/
   ```

   Verify: `git status` prints nothing and `fast-forward-ok` prints. The phase's exit gate
   has passed (see the plan). If the last command lists migration files, do
   [Migrations](#migrations) before the deploy in step 12. For the first release (P2B.3,
   done as v1.5.4) it listed `migrations/README.md` and V001 to V025, because
   `origin/main` was still the June 0.2.0 commit; prod was already at V025 (P0A.1).

   **Then compare the release's migrations with prod's applied max version** (added
   2026-10-18). The `origin/main..HEAD` diff shows only what this release adds over the
   last release. Prod can be further behind: a no-deploy release still moves `main`, so
   migrations it merged (for example N5's V026, N9's V027/V028, N8's V029) reach prod only
   when a later deploy ships them, and every web boot applies every pending file
   (`MIGRATE_ON_BOOT`, default `1`, `scripts/docker-entrypoint-web.sh:21`). So whenever
   this release will deploy (step 12 or step 15), the owner runs the "Targeting prod"
   `migrate --dry-run` from [Migrations](#migrations) and compares it with the highest
   file the release carries:

   ```bash
   git ls-tree --name-only HEAD migrations/ | grep -E '/V[0-9]+__' | sort | tail -1
   ```

   Verify: run from this release checkout, the prod dry run prints `target: remote`, then
   "… already applied, 0 pending" and `database is up to date`. Because it reads the
   release's own `migrations/`, 0 pending means prod has applied every file up to the
   version the command above prints. If it lists pending files, prod's applied max
   version is below the release's: run the [Migrations](#migrations) protocol first
   (PG18 `pg_dump -Fc` by full path, `pg_restore -l`, then
   `migrate --apply --target <max>`), or skip the deploy and write "no deploy" in the
   release-log row. A plain `migrate --dry-run` in a checkout reads the local database
   and proves nothing about prod.

2. **CHANGELOG.** `## [Unreleased]` lists everything since the last release. Units add
   their lines as they merge.

   Verify: `awk '/^## \[Unreleased\]/{f=1;next} /^## \[/{f=0} f && NF' CHANGELOG.md`
   prints the notes. If it prints nothing, the bump in step 3 would produce an empty
   section.

3. **Bump and commit.**

   ```bash
   scripts/bump_version.sh patch        # or minor / major
   V=$(cat VERSION)
   git commit -m "Release $V"
   ```

   Verify: `python3 scripts/release_guard.py --base origin/main` prints
   `release_guard: OK: ...`.

4. **Secrets scan.** Everything in `origin/main..HEAD` becomes public on `main`. GitHub
   push protection is on, but scan anyway.

   ```bash
   git log -p origin/main..HEAD | grep -nE 'AIza[0-9A-Za-z_-]{35}|sk-ant-[0-9A-Za-z_-]{20,}|sk_live_[0-9A-Za-z]{16,}|AKIA[0-9A-Z]{16}|gh[pousr]_[0-9A-Za-z]{36}|-----BEGIN [A-Z ]*PRIVATE KEY-----|postgres(ql)?://[^:/@ ]+:[^@/ ]+@|eyJ[0-9A-Za-z_-]{10,}\.eyJ[0-9A-Za-z_-]{10,}'
   git diff --no-renames --diff-filter=AM --name-only origin/main..HEAD | grep -E '(^|/)\.env$|\.(db|sqlite3?|pem|key)$'
   ```

   The second command lists files the range adds or changes (`--no-renames`, so a file
   that git pairs with an unrelated deleted file still shows as added). A deleted file
   was already public on `main`.

   Verify: look at every hit of both commands before going on. Each one must be a
   placeholder or an empty file, so the output is reviewed, not expected to be empty. On
   2026-10-08 the range up to 1.5.3 held only these. Content: an
   `sk-ant-api03-parity-test-...` test key, localhost DSNs in tests and in
   `docs/monolith_audit_2026_10_01/tests.md`, a `prod-db.example.net` DSN in a test,
   commented `user:pass@localhost` examples in `.env.example`, and the plan's own
   `user:pw` example. Files: `backend/dictionary/derived/dictionary_catalog.sqlite3`, which
   is empty (`git cat-file -s HEAD:backend/dictionary/derived/dictionary_catalog.sqlite3`
   prints `0`). A real secret stops the release. Deleting it is not enough, because it
   stays in the history: it has to be rotated.

5. **Lint and targeted tests.** The full offline suite runs in CI.

   ```bash
   ruff check . --no-cache
   ```

   Verify: `ruff check . --no-cache` exits 0, and the phase's targeted test chunks pass
   (`python -m pytest <files> -q -p no:cacheprovider`, files from the phase's exit gate).

6. **Push the branch and wait for CI.**

   ```bash
   git push origin <branch>
   gh run list --commit "$(git rev-parse HEAD)" --json databaseId,event,headBranch,status,conclusion
   gh run view <databaseId> --json jobs --jq '.jobs[] | [.name, .conclusion] | @tsv'
   ```

   Verify: in the push run, `lint`, `pytest` and `pytest-pg` show `success`, and
   `release-guard` shows `skipped`, as designed on a branch push. If the run is red, fix
   forward, never force-push. Commit the fix with a line for it under `## [Unreleased]`
   in CHANGELOG.md, then repeat step 3 (bump, `V=$(cat VERSION)`, commit) and push again.
   Without that line the new section is empty and the guard refuses it. The earlier
   version is never released on its own, and its section stays in CHANGELOG.md as it is.

7. **Open the release PR, so `release-guard` runs in CI before `main` moves.**

   ```bash
   gh pr create --base main --head <branch> --title "Release $V" --body "<the CHANGELOG section for $V>"
   gh pr checks <branch> --watch
   ```

   Verify: `gh pr checks <branch>` exits 0, with `lint`, `pytest`, `pytest-pg` and
   `release-guard` passing. The skipped `release-guard` entry from the push run is
   expected. Leave the PR open. GitHub marks it merged by itself once `main` reaches its
   head in step 8.

8. **Fast-forward `main`.** First take a Railway snapshot, which step 11 compares
   against. The snapshot asks for up to 1000 deployments per service, so it holds each
   service's whole list (web had 70 on 2026-10-08), sorted, with the count first, one
   line per service. A new deployment then shows up in the diff whatever order the CLI
   lists them in. It fails closed: if any `railway deployment list` call fails, returns
   no deployments, returns 1000 (the list may be cut off) or prints JSON of another shape
   (expired token, wrong linked project, a renamed service), the function stops with
   `SNAPSHOT-FAILED <service>` and `snapshot-ok` does not print.

   ```bash
   railway_snapshot() {
     for s in web scanner-nightly Postgres; do
       railway deployment list -s "$s" --limit 1000 --json \
         | python3 -c 'import json,sys; ids=sorted(d["id"] for d in json.load(sys.stdin)); assert ids and len(ids) < 1000; print(sys.argv[1], len(ids), *ids)' "$s" \
         || { echo "SNAPSHOT-FAILED $s" >&2; return 1; }
     done
   }
   railway_snapshot > /tmp/railway_deploys_before.txt && echo snapshot-ok
   ```

   The JSON shape (a list of objects with an `id`) has not been seen live yet. On the
   first release (P2B.3), check that each line's count matches the service's deployment
   count in the dashboard. If the shape differs, the snapshot fails closed: use the
   GraphQL deployments query from the P0A.1 "Reproducing the checks" block in
   `docs/SCANNING_OPS_LOG.md` instead, and fix this function.

   Do not push until `snapshot-ok` prints. Then:

   ```bash
   git push origin <branch>:main
   ```

   Never pass `--force`. git refuses a non-fast-forward on its own. The pre-push hook runs
   `scripts/release_guard.py` against the `main` that origin reports.

   Verify: `snapshot-ok` printed before the push, and
   `test "$(git ls-remote origin refs/heads/main | cut -f1)" = "$(git rev-parse HEAD)" && echo main-released`
   prints `main-released`.

9. **Tag the release.** The tag must be annotated, sit at the release commit, and be on
   origin. The deploy scripts refuse a lightweight tag, a tag that is not at HEAD, and a
   tag that origin does not have.

   ```bash
   git tag -a "v$V" -m "Release $V"
   git push origin "v$V"
   git fetch origin main:main          # skip this one if main is the checked-out branch
   ```

   Verify:

   ```bash
   git cat-file -t "v$V"                # prints: tag
   test "$(git rev-parse "v$V^{commit}")" = "$(git rev-parse HEAD)" && echo tag-at-head
   test "$(git ls-remote origin "refs/tags/v$V" | cut -f1)" = "$(git rev-parse "v$V")" && echo tag-on-origin
   test "$(git rev-parse main)" = "$(git rev-parse origin/main)" && echo local-main-current
   ```

10. **Check CI on `main`.** Here `release-guard` runs for real, against the `main` this
    push replaced.

    ```bash
    gh run list --branch main --commit "$(git rev-parse HEAD)" --json databaseId,event,status,conclusion
    gh run view <databaseId> --json jobs --jq '.jobs[] | [.name, .conclusion] | @tsv'
    ```

    Verify: all four jobs show `success`.

11. **Check that no Railway build fired.** Do this within 15 minutes of step 8.

    In a new shell, paste the `railway_snapshot` definition from step 8 first.

    ```bash
    railway_snapshot > /tmp/railway_deploys_after.txt \
      && test "$(grep -c . /tmp/railway_deploys_before.txt)" = 3 \
      && test "$(grep -c . /tmp/railway_deploys_after.txt)" = 3 \
      && diff /tmp/railway_deploys_before.txt /tmp/railway_deploys_after.txt \
      && echo no-new-deployment
    ```

    Verify: `no-new-deployment` prints. Without it nothing was proven. `SNAPSHOT-FAILED`
    or a line count other than 3 means a snapshot is missing or partial: fix the CLI
    (`railway whoami`, `railway status`) and rerun this step. Never replace the step 8
    snapshot after the push, because it is the only record of the state before it. A
    `diff` that shows a new id means some service builds from GitHub. For web, roll it
    back ([Rollback](#rollback), web). For scanner-nightly, never use a Railway rollback.
    The new deployment runs with the current variables, so it is a fleet run if
    `SCAN_FLEET` is on: the owner stops it at once with `railway down -s scanner-nightly`
    ([Rollback](#rollback), scanner-nightly), then run step 15's variable check. Then fix
    the source ([Railway source policy](#railway-source-policy)).

12. **Deploy web from the tag.** D-REL8 (b) decided that the first release deploys web and
    that scanner-nightly waits for P6B.1. Deploying web from the tag on every later
    release is the plan's release checklist (P2B.7), not an owner decision. Always do the
    dry run first.

    ```bash
    deploy/railway/deploy_web.sh --dry-run
    deploy/railway/deploy_web.sh
    ```

    Verify the dry run before the real deploy. It exits 0, and its plan shows
    `settings  match deploy/railway/service_settings.json at refs/tags/v$V`,
    `release   v$V (annotated, on origin; HEAD == origin main)` and
    `dry run: nothing was uploaded; railway ran only the read-only settings query.` It
    must show no `warning   the railway CLI printed a deprecation notice` line (N4.1
    acceptance; if one appears, record it in the release-log row). Exit 1 means a refusal,
    with the reasons listed and nothing staged: either the release guard refused (railway
    was not called) or web's live service settings differ from
    `deploy/railway/service_settings.json` (the table marks each `MISMATCH`; apply the
    settings pack, [Service settings](#service-settings-n41-d-n24)). `ALLOW_UNRELEASED_DEPLOY=1`
    never overrides the settings check. Exit 2 is a usage or staging error. The real run
    checks the settings again and uploads with
    `railway up <stage> --path-as-root --service web --detach`. The stage is
    `git archive` of the tag plus `BUILD_COMMIT` (the full SHA) and `BUILD_TAG`, with no
    config-as-code file (a release that carries a root `railway.toml` or `railway.json` is
    refused). Railway builds it with web's own service settings: `Dockerfile.web`,
    healthcheck `/health` within 300 s, restart `ON_FAILURE` up to 5 times, 1 replica.

    Optional local check of the exact stage before the real deploy. It is a Docker build,
    so only with the owner's OK and on AC power. P2B.6 lists the full local smoke test.
    Add `--no-settings-check` to stage it without asking Railway (dry runs only; the real
    deploy always checks).

    ```bash
    deploy/railway/deploy_web.sh --dry-run --keep-stage /tmp/web-stage-$V
    docker build -f /tmp/web-stage-$V/Dockerfile.web -t dealership-scanner-web:$V /tmp/web-stage-$V
    docker run --rm --entrypoint cat dealership-scanner-web:$V /app/BUILD_COMMIT /app/BUILD_TAG
    ```

    Verify: it prints the full SHA of `v$V`, then `v$V`. `--keep-stage` needs a new or
    empty directory.

13. **Verify prod.** `deploy_web.sh` uploads with `--detach`, so it returns before Railway
    has built anything. Railway marks the new deployment SUCCESS only after `/health`
    answers within 300 s. Until then the old deployment serves, and the check below shows
    the old version and exits 1. Wait until `railway deployment list -s web --limit 3`
    shows the new deployment (newest first) as SUCCESS, then run:

    ```bash
    curl -fsS https://sarraficars.com/api/health | python3 -c 'import json,sys; h=json.load(sys.stdin); got=(h.get("version"), h.get("commit")); print(got); sys.exit(got != (sys.argv[1], sys.argv[2]))' "$V" "$(git rev-parse "v$V^{commit}")"
    ```

    Verify: exit 0. The printed version is `$V` and the commit is the tag's full SHA.
    `/health` returns the same body. It is Railway's healthcheck path (web's
    `healthcheckPath` service setting), and the healthcheck only needs a 200. If the deployment ends FAILED instead, the old one
    keeps serving. Read its logs by id (without an id, `railway logs` shows the last
    successful deployment): `railway logs -s web --build --lines 200 <deployment id>`, or
    `--deployment` for the runtime log. Then fix forward.

    **Card builder.** If this release changes the card revision `CARD_REV`, also deploy
    `web-cards` at this tag and run the builder (150 to 170 s), away from 00:00 UTC. See
    [The card builder and the web-cards service](#the-card-builder-and-the-web-cards-service).

14. **Record the deployment.** Take the id of the new SUCCESS deployment from
    `railway deployment list -s web --limit 3` (newest first). Add a row to the
    [Release log](#release-log) on the next phase branch, so it ships with the next
    release. Write the version column bare, as in the table (`| 1.5.4 |`, no `v`). A
    docs-only push to `main` would need a bump of its own.

    Verify: `grep -nF "| $V |" docs/RELEASING.md` shows the row with the deployment id.

15. **scanner-nightly**, from P6B.1 on only (D-REL8 (b)). Before then, do not deploy it.

    ```bash
    railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='
    deploy/railway/deploy_scanner_nightly.sh --dry-run
    deploy/railway/deploy_scanner_nightly.sh
    ```

    A deploy starts the container once. It scans if `SCAN_FLEET` is `1`, `true` or `yes`,
    or if `SCAN_DEALERS` is non-empty (`backend/scripts/fleet_scan.py`, the same at
    `89c13ad52`). P0A.1 found `SCAN_FLEET=1` still set on 2026-10-07. Unless you intend a
    run, the first command must print nothing or only `SCAN_FLEET=0`. To turn the fleet
    off without starting the container, run
    `railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys`.
    `railway variable delete` has no `--skip-deploys` in CLI 5.57.2. `--kv` prints raw
    values, and the grep keeps only these two non-secret lines. The script stages the tag
    with no config-as-code file; Railway builds it with scanner-nightly's own service
    settings (`Dockerfile.scanner`, no healthcheck, restart `NEVER`, 1 replica,
    16 vCPU / 16 GB, no cron). Like step 12 it refuses unless those live settings equal
    `deploy/railway/service_settings.json`; before the first scanner-nightly deploy (N12.2)
    the owner applies the scanner-nightly settings pack
    ([Service settings](#service-settings-n41-d-n24)). It refuses `SERVICE=web`. Run it only
    from this release checkout (HEAD == the tag, step 9), never from a checkout of an older
    tag ([why](#scanner-nightly)).

    Verify: the dry run passes as in step 12. Afterwards,
    `railway deployment list -s scanner-nightly --limit 3` shows the new deployment, and
    `railway logs -s scanner-nightly --lines 50` shows `idle`. While the idle container
    holds, `railway ssh --service scanner-nightly -- cat /app/BUILD_COMMIT /app/BUILD_TAG`
    prints the tag's SHA and `v$V`. The hold lasts `SCAN_IDLE_HOLD_SECONDS`, which
    `backend/scripts/fleet_scan.py` defaults to 0 (exit at once). P0A.1 found it set on
    scanner-nightly; if it is unset or 0, there is no container to `ssh` into and this
    check cannot run. Record the id in the release log too.

## Hotfixes (D-REL10)

- **scanner-nightly, mid-fleet trouble:** the owner stops the fleet with
  `railway down -s scanner-nightly`, and the fix ships as a PATCH release from `main`
  through the current guarded script (steps 1 to 15). [Rollback](#rollback),
  scanner-nightly, has the steps.
- **scanner-nightly, only if that patch release cannot be cut in time (D-REL10):**
  first run both checks in "Check first" below and stop if either fails. Then run
  `ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh`, in a checkout
  whose HEAD is the fix commit on a branch cut from `main`. This deploys even though the
  guard failed, and prints a loud warning. It ships the committed tree of HEAD
  (`git archive HEAD`, never uncommitted changes) with `BUILD_TAG=unreleased`, or with the
  tag if HEAD carries one. As soon as the fleet allows, cut a release and redeploy from
  its tag. Only the exact value `1` overrides. Never point it at an older release by
  checking out its tag: that runs the old tag's own script, which deploys at once
  ([Rollback](#rollback), scanner-nightly).

  Check first, in this order, before the override deploy above:
  `grep -q _guarded_deploy.sh deploy/railway/deploy_scanner_nightly.sh && echo guarded-script`
  prints `guarded-script` (the script in this checkout is the guarded one, which honours
  `--dry-run`), then
  `ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh --dry-run` exits 0
  and its plan says `GUARD OVERRIDDEN` and shows the `BUILD_TAG` that will ship.
- **web:** do not use the override. Cut a patch release (steps 1 to 13).

## Rollback

`main` only moves forward. Never force-push it back. Either fix forward with a new patch
release, or roll web back to an earlier deployment. Never roll scanner-nightly back, and
never deploy an older release by checking out its tag and running its scripts
([below](#scanner-nightly)).

**A Railway rollback restores the target deployment's variables, not only its image.**
Railway's docs (https://docs.railway.com/guides/deployment-actions, "Rollback") say: "A
deployment rollback will revert to the previously successful deployment. Both the Docker
image and custom variables are restored during the rollback process." So the rolled-back
service runs with the variable values the target deployment had, not today's. Any
variable changed since then (a rotated secret, `TRUSTED_PROXY_HOPS`, a security switch)
quietly reverts. Railway does not say whether the restore also writes those values back
to the service's variables, where every later deploy would pick them up. Assume it may,
and check the variables after every rollback. The same page says a deployment older than
the plan's retention policy cannot be rolled back (the option is not shown), so an old id
in the release log may no longer be a target.

### web

1. Check web's source first. Step 5 changes web variables, and no web variable may change
   while web records a GitHub source ([Web's GitHub source](#webs-github-source)). Paste
   the `web_source_check` definition from that section, then run `web_source_check`. If
   it prints `WEB-SOURCE-SET`, the owner disconnects the source (Settings → Source →
   Disconnect) before the rollback, so that step 5 can run straight after it.

   Verify: `web_source_check` prints `web-source-clear`.
2. Find the last good deployment: `railway deployment list -s web --limit 10`. The
   release log has the id. Then list every web variable changed since that deployment
   (the dated blocks in `docs/SCANNING_OPS_LOG.md` and the release log notes record
   them). Example: a rollback to `c9872e66` would bring back
   `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` and drop `CAR_CHAT_WEB_RESEARCH=0`, the two
   2026-10-08 changes that `d9aaa75e` was redeployed to apply.
3. Fingerprint the current variables, so you can prove later that they are back. This
   writes only names and a hash, never a value:

   ```bash
   railway variable list -s web --kv | grep -v '^RAILWAY_' | sort | shasum -a 256 > /tmp/web_vars_before.sha
   railway variable list -s web --kv | grep -v '^RAILWAY_' | cut -d= -f1 | sort > /tmp/web_var_names_before.txt
   ```

   Verify: `cat /tmp/web_var_names_before.txt` lists the variable names you expect
   (names only, no values), and `/tmp/web_vars_before.sha` holds one hash.
4. Roll back. In the dashboard: web → Deployments → that deployment → ⋯ → Rollback. Or
   use GraphQL, since the CLI (5.57.2) has no rollback command:

   ```bash
   TOKEN=$(python3 -c 'import json,os; print(json.load(open(os.path.expanduser("~/.railway/config.json")))["user"]["accessToken"])')
   curl -sS https://backboard.railway.com/graphql/v2 \
     -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
     -H "User-Agent: railway-cli/$(railway --version | awk '{print $2}')" \
     --data '{"query":"mutation($id: String!) { deploymentRollback(id: $id) }","variables":{"id":"<deployment id>"}}'
   ```

   Never print the token. Send the `User-Agent: railway-cli/...` header, or Cloudflare
   answers with error 1010. If the token is refused (`Not Authorized`), run
   `railway whoami` to refresh it.

   Verify: `railway deployment list -s web --limit 3` shows the rollback deployment as
   SUCCESS, and `curl -fsS https://sarraficars.com/api/health` shows the earlier version
   and commit.
5. Put today's variables back on the rolled-back image. Only after step 1's
   `web_source_check` printed `web-source-clear`; run it again if anything may have
   changed since. Compare first:

   ```bash
   railway variable list -s web --kv | grep -v '^RAILWAY_' | cut -d= -f1 | sort | diff /tmp/web_var_names_before.txt -
   railway variable list -s web --kv | grep -v '^RAILWAY_' | sort | shasum -a 256 | diff /tmp/web_vars_before.sha - && echo vars-match
   ```

   If they differ, the rollback wrote the old values back. Re-apply each changed
   variable without a deploy: `read -rs VALUE`, then
   `railway variable set "NAME=$VALUE" -s web --skip-deploys`, so a secret never reaches
   the screen or the shell history. To remove a variable the rollback brought back, use
   the dashboard's Variables tab or `railway variable delete NAME -s web`. Either may
   start a deployment of its own (the CLI delete has no `--skip-deploys` in CLI 5.57.2),
   so check `railway deployment list -s web --limit 3` afterwards. Repeat the compare
   until it prints `vars-match`. Then, even if it matched the first time (the running
   deployment still has the target's snapshot):

   ```bash
   railway redeploy -s web -y
   ```

   It redeploys the latest deployment, which is now the rollback's image, with the
   current variables. This is web only. On scanner-nightly a redeploy starts the
   container, and that runs a fleet if `SCAN_FLEET` is on (below).

   Verify: `vars-match` prints, the new deployment is SUCCESS in
   `railway deployment list -s web --limit 3`, `/api/health` still shows the earlier
   version and commit, and any switch you re-applied behaves as it did before the
   rollback.

A rollback does not undo a migration. Migrations only go forward. If the bad release
applied one, the older code must still run on the new schema. Otherwise the owner
restores the dump taken before the deploy ([Migrations](#migrations)).

### scanner-nightly

Do not use a Railway rollback on scanner-nightly. A rollback starts the container once
with the target deployment's variables, so setting `SCAN_FLEET=0` beforehand does not
help. Never roll back to a deployment whose snapshot had `SCAN_FLEET` on or
`SCAN_DEALERS` set. `90a3d2a0` (2026-09-29) ran the 561-dealer fleet, so its snapshot
has `SCAN_FLEET=1`: a rollback to it starts a full fleet against prod Postgres on the
`89c13ad52` image, which stales shared recipes on Railway 401/403s. This doc has no
verified way to read an older deployment's variable snapshot, so treat every earlier
scanner-nightly deployment the same way.

**Never run an old tag's deploy script either.** Every release before the first one
P2B.3 cuts (0.2.0 to 1.5.3, so every retro tag P2B.4 adds, v1.3.2 to v1.5.2) predates
the guarded scripts of P2B.2. From 1.4.2 to 1.5.3, `deploy/railway/deploy_scanner_nightly.sh`
is a 24-line script that reads no arguments: it ignores `--dry-run`, `--help` and
`ALLOW_UNRELEASED_DEPLOY`, and uploads `git archive HEAD` to scanner-nightly the moment it
runs. Releases up to 1.4.1 have no such script, and no old tag has `deploy_web.sh`. So
never check out an older tag, or add a worktree at one, to deploy from it: its "dry run"
is a real deploy, which starts the container with the current variables, so with
`SCAN_FLEET` on it is a full fleet on the old code. Known-good code comes back as a
revert, shipped in a new patch release.

Before P6B.1, scanner-nightly is not deployed at all (D-REL8 (b)). From P6B.1 on,
mid-fleet trouble goes like this:

1. **Stop the fleet.** The owner runs `railway down -s scanner-nightly` (it asks for
   confirmation; `-y` skips that). It removes the service's most recent deployment, which
   stops the running container. On 2026-10-08 `railway down -s scanner-scheduler -y`
   removed the running `50ac4514` the same way, and no other deployment started
   (`docs/SCANNING_OPS_LOG.md`, the owner's 2026-10-08 block). Agents never run it.

   Verify: `railway deployment list -s scanner-nightly --limit 3` shows that deployment
   as REMOVED, with nothing newer.
2. **Keep it stopped until the fix ships.** Run step 15's variable check
   (`railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='`).
   Unless the fixed release should resume the fleet, turn it off without a deploy:
   `railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys`.

   Verify: the check prints nothing or only `SCAN_FLEET=0`, unless a resumed run is
   intended.
3. **Ship a fixed PATCH release from `main`.** On a branch cut from `main`, fix the bad
   change or `git revert` it, then run the release checklist (steps 1 to 15), which
   deploys through the current guarded script from the new tag. If that release cannot
   be cut in time, D-REL10's override ships the fix commit instead
   ([Hotfixes](#hotfixes-d-rel10)), never an older tag.

   Verify: step 15's checks pass, and the deployment is recorded in the release log.

After anything that touched scanner-nightly, including a rollback someone ran by
mistake, re-run step 15's check
(`railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='`),
because the service's values may have been reverted.

Never use `railway redeploy --from-source` or a bare `railway up` to roll back
([Railway source policy](#railway-source-policy)).

## Railway source policy

No Railway service builds from GitHub. Code reaches Railway only as a CLI upload of a
`git archive` stage of a release tag. Only `deploy/railway/deploy_web.sh` and
`deploy/railway/deploy_scanner_nightly.sh` make those uploads.

- **Never attach a GitHub source to a service.** Never turn on autodeploy or "Wait for
  CI", and never run `railway redeploy --from-source`. On 2026-09-11 a GitHub build of
  `main` replaced prod with the June 0.2.0 app. A `main` build on a legacy worker would
  have booted that same app against prod (P0A.1 answer 4). Web recorded one until the
  owner disconnected it on 2026-10-16 ([Web's GitHub source](#webs-github-source)).
- **Never run a bare `railway up` from a checkout.** It uploads the working tree,
  uncommitted files included, to whatever service the directory is linked to. On
  2026-10-08 a bare `railway up` uploaded a checkout to scanner-nightly by accident. The
  upload failed before anything ran.
- **To apply a variable change**, on web first run `web_source_check`
  ([below](#webs-github-source)). Unless it prints `web-source-clear`, change no web
  variable at all, in the dashboard or with the CLI, `--skip-deploys` included. Then
  stage the change with `railway variable set NAME=value -s <service> --skip-deploys`,
  and run `railway redeploy -s <service> -y`. That redeploys the existing latest
  deployment with the same image and the current variables (`railway redeploy --help`;
  only `--from-source` pulls new code). The 2026-10-08 `d9aaa75e` redeploy of web did
  exactly this. Once web's source is disconnected, a deploying `railway variable set` on
  web (one without `--skip-deploys`) also deploys the current image; staging several
  changes and redeploying once just makes one deployment instead of one per change.
  `--skip-deploys` followed by `railway restart` does not apply the change, because a
  restart relaunches the same deployment with its old variables. Always pass `-s`. On
  2026-10-08 the main checkout was linked to scanner-nightly, so a command without `-s`
  acted on it.

  **On scanner-nightly, a redeploy or a deploying variable change starts the container
  once**, and that runs a full fleet scan against prod Postgres if `SCAN_FLEET=1` or
  `SCAN_DEALERS` is set. P0A.1 found `SCAN_FLEET=1` still set, on an image that stales
  shared recipes on Railway 401/403s. Run the release step 15 check first
  (`railway variable list -s scanner-nightly --kv | grep -E '^SCAN_(FLEET|DEALERS)='`),
  and unless a run is intended, turn the fleet off without a deploy:
  `railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys`. Do not use
  `railway variable delete` for this: it has no `--skip-deploys` in CLI 5.57.2. The same
  check comes before deleting `SCAN_DEALERS` after a manual run: if that change deploys
  while `SCAN_FLEET=1` is set, the container runs the whole fleet. None of this protects
  against a Railway rollback, which restores the target deployment's own variables
  ([Rollback](#rollback), scanner-nightly).

### Web's GitHub source

**Never change a web variable, in the dashboard or with the CLI, while web's
`serviceInstance.source.repo` is set.** That covers sets with or without `--skip-deploys`,
deletes, the dashboard's Variables tab and step 5 of the web rollback. On 2026-09-11 a
web variable change rebuilt web from GitHub `main` (`09dfabb7`, `main@70c3dbb57`) and
replaced prod with the June 0.2.0 app. Check the source first, every time, with
`web_source_check` below. It is read-only: one GraphQL query, never a `mutation`, and it
never prints the token.

```bash
web_source_check() {
  local token
  token=$(python3 -c 'import json,os; print(json.load(open(os.path.expanduser("~/.railway/config.json")))["user"]["accessToken"])') || return 1
  curl -sS https://backboard.railway.com/graphql/v2 \
    -H "Authorization: Bearer $token" -H "Content-Type: application/json" \
    -H "User-Agent: railway-cli/$(railway --version | awk '{print $2}')" \
    --data '{"query":"query($eid: String!, $sid: String!) { serviceInstance(environmentId: $eid, serviceId: $sid) { source { repo image } } }","variables":{"eid":"abe0fc9d-2dd5-4217-b303-12c6c4ac498a","sid":"b711c414-86c3-4b1d-a458-17168d11e439"}}' \
    | python3 -c 'import json,sys; s=json.load(sys.stdin)["data"]["serviceInstance"]["source"] or {}; r=s.get("repo"); print("web-source-clear" if r is None else "WEB-SOURCE-SET " + r); sys.exit(r is not None)' \
    || { echo "web_source_check: not clear; change no web variable" >&2; return 1; }
}
web_source_check
```

`eid` is the production environment and `sid` is the web service, both from the P0A.1
per-service table in `docs/SCANNING_OPS_LOG.md`. The function works in bash and zsh.

- `web-source-clear` (exit 0): web has no GitHub source, so web variable changes are
  safe. Each one deploys the current image with the new variables, as the 2026-10-08
  redeploy `d9aaa75e` did (the same 1.5.1 image, with that day's variable changes).
  Apply them as the variable-change bullet above says.
- `WEB-SOURCE-SET <repo>` (exit 1): stop. Change no web variable until the owner has
  disconnected the source and the check prints `web-source-clear`.
- Anything else (a Python traceback, `Not Authorized`, a Cloudflare error page) is a
  failed check, also exit 1. Refresh the login (`railway whoami`) and run it again. A
  failed check never counts as clear.

The rule is about variables. Uploads through `deploy/railway/deploy_web.sh` deploy the
uploaded stage whether or not the source is recorded: `62f93905` (1.5.0) and `c9872e66`
(1.5.1) were CLI uploads made while it was.

**Status 2026-10-09: the owner is disconnecting web's source.** The decision is the "Web
GitHub source" row of `docs/remediation/OWNER_DECISIONS_LOG.md`. A read-only run of the
query above on 2026-10-09 still returned `source.repo = asarrafi47/DealershipScanner`, so
until the check prints `web-source-clear`, no web variable may change. The owner
disconnects it in the dashboard: web → Settings → Source → Disconnect.

**Status 2026-10-16: disconnected.** The owner disconnected the source in the dashboard;
`web_source_check` printed `web-source-clear`, `848895c1` was still the newest SUCCESS
deployment, and `/api/health` still reported 1.5.4 at `486f9012d` (`docs/SCANNING_OPS_LOG.md`,
N1.5 block). Web variable changes are now allowed under the variable-change bullet; keep
running `web_source_check` before every one. The github.com/settings/installations check
is not recorded yet.

Verify the disconnect: `web_source_check` prints `web-source-clear`, and
`railway deployment list -s web --limit 3` shows the same newest deployment as before the
disconnect, still SUCCESS. P0A.4 step 6 expects no new deployment and the running one
unaffected. `curl -fsS https://sarraficars.com/api/health` still answers with the same
version.

### State as of the 2026-10-08 ops log (P0A.1 and P0A.4, `docs/SCANNING_OPS_LOG.md`)

- No service, and no environment-level setting, has a deployment trigger. Autodeploy is
  off everywhere: web reports `NO_INSTALLATION`, Postgres and scanner-nightly report
  `NO_REPO`. The last GitHub-built deployment was web `09dfabb7` on 2026-09-11.
- **The legacy services are gone.** scanner-worker and scanner-scheduler were deleted on
  2026-10-08 (D-REL1 (a)). First, `railway down` removed scanner-scheduler's running June
  deployment `50ac4514`. Neither service had a volume. The project now holds Postgres,
  scanner-nightly and web. Their Dockerfiles and per-service Railway tomls stayed in the
  repo until P15B.6 (N4) deleted them, together with the docker-compose worker and
  scheduler services.
- **Open owner items** (P0A.4 step 6). Web still records `asarrafi47/DealershipScanner`
  as its source (rechecked read-only on 2026-10-09). If the Railway GitHub App is given
  access to the repo again, or someone runs `redeploy --from-source`, Railway can build
  `main` again. The fix is to disconnect web's source (Settings → Source → Disconnect),
  which the owner is doing as of 2026-10-09 ([Web's GitHub source](#webs-github-source)),
  and to confirm at github.com/settings/installations that the Railway app cannot see
  this repo. Until `web_source_check` prints `web-source-clear`, no web variable may
  change.
  **Update 2026-10-16:** disconnected; `web_source_check` prints `web-source-clear`
  (N1.5 block in `docs/SCANNING_OPS_LOG.md`). The installations check is still owed.
- **`SCAN_FLEET=1` is still set on scanner-nightly** (with `SCAN_SHARDS=8` and
  `SCAN_BATCH=6`; P0A.1). The owner's 2026-10-08 actions did not change it. The deployed
  image is `89c13ad52` (deployment `90a3d2a0`, 2026-09-29), which predates the egress tag
  `1707e1349`, so a run on it stales shared recipes on Railway 401/403s (P0A.1 answer
  3). Any new scanner-nightly deployment that runs with the current variables (a
  variable change without `--skip-deploys`, a redeploy, a deploy) starts a full fleet
  run against prod Postgres (561 dealers on 2026-09-29) until `SCAN_FLEET` is turned
  off. See the variable-change bullet above. A rollback is different: it restores the
  target deployment's variables (Railway's deployment-actions docs), and `90a3d2a0`'s
  have `SCAN_FLEET=1`, so turning `SCAN_FLEET` off does not make a rollback to it safe.
  Never roll scanner-nightly back to it ([Rollback](#rollback)). After any rollback,
  re-run release step 15's variable check, since the service's values may have been
  reverted.
  **Update 2026-10-16 (N1.5, `docs/SCANNING_OPS_LOG.md`):** the owner ran
  `railway variable set SCAN_FLEET=0 -s scanner-nightly --skip-deploys`; the variable list
  shows `SCAN_FLEET=0` and the deployment list is unchanged (newest `152fb03c` FAILED,
  then `90a3d2a0` SUCCESS). The staged value applies only to the next new deployment: a
  restart or a rollback of `90a3d2a0` still runs with its own `SCAN_FLEET=1`, a full fleet
  on the 09-29 image. Never restart, roll back or redeploy scanner-nightly until N12.2
  deploys v1.6.0; stop trouble with `railway down -s scanner-nightly`.
- **Config as code is deprecated.** The root `railway.toml` (web) and the staged
  `railway.json` (scanner-nightly) were config-as-code files. Railway stops reading them
  on 2026-12-01 (P0A.1). N4.1 deleted both and moved their settings onto the services:
  [Service settings](#service-settings-n41-d-n24).

### How to verify (read-only)

- After every push to `main`, run release step 11.
- Before any web variable change, run `web_source_check`
  ([Web's GitHub source](#webs-github-source)). It must print `web-source-clear`.
- To check for triggers, run the trigger query from the P0A.1 block in
  `docs/SCANNING_OPS_LOG.md` ("Reproducing the checks"). The environment-wide form:

  ```bash
  TOKEN=$(python3 -c 'import json,os; print(json.load(open(os.path.expanduser("~/.railway/config.json")))["user"]["accessToken"])')
  curl -sS https://backboard.railway.com/graphql/v2 \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -H "User-Agent: railway-cli/$(railway --version | awk '{print $2}')" \
    --data '{"query":"query($eid: String!) { environment(id: $eid) { deploymentTriggers { edges { node { serviceId repository branch } } } } }","variables":{"eid":"abe0fc9d-2dd5-4217-b303-12c6c4ac498a"}}'
  ```

  Expect `"edges":[]`. The per-service query in the ops log also shows autodeploy status
  and the recorded `source.repo`. Never send a `mutation` while checking.

### Service settings (N4.1, D-N24)

**Mechanism.** D-N24 says: use whatever Railway documents as supported after
2026-12-01. N4.1 checked Railway's official docs on 2026-10-18:

- [Config as Code](https://docs.railway.com/config-as-code) is deprecated. Existing
  `railway.json` / `railway.toml` files keep working only until **2026-12-01 (hard
  cutoff)**. While they are read, they override the dashboard settings.
- The replacement Railway names is
  [Infrastructure as Code](https://docs.railway.com/infrastructure-as-code)
  (`.railway/railway.ts`, applied with `railway config plan` / `railway config apply`;
  Railway does not read `.railway/` during deploys). Its
  [reference](https://docs.railway.com/infrastructure-as-code/reference) has `build`
  (a build command), `start`, `healthcheck`, `healthcheckTimeout`, `replicas`, variables,
  domains and volumes. It has **no** field for the builder or the Dockerfile path, the
  restart policy, a cron schedule or CPU/memory limits, and it manages the whole project
  (Postgres and variables included). The
  [10-16 changelog](https://railway.com/changelog/2026-10-16-activity-feed-filters) says
  `railway config migrate cutover` warns about redeployments. So IaC cannot hold these
  settings today, and the repo does not adopt it.
- The settings stay on the **service instance**. The
  [Manage Services guide](https://docs.railway.com/integrations/api/manage-services)
  documents a `serviceInstance` query, `serviceInstanceLimits`, and `serviceInstanceUpdate`
  inputs `dockerfilePath`, `healthcheckPath`, `healthcheckTimeout`, `restartPolicyType`,
  `restartPolicyMaxRetries`, `numReplicas` and `cronSchedule`.
  [`railway api`](https://docs.railway.com/cli/api) runs GraphQL with the CLI's own login.
  Dashboard edits become [staged changes](https://docs.railway.com/deployments/staged-changes)
  that redeploy the affected services when deployed, unless you hold Alt while clicking
  Deploy.
- Not settled by the docs, so not relied on until the owner read pack
  (`workspace/phase_runs/N4/N4.1_read.md`) shows them: whether `serviceInstanceUpdate`
  creates a deployment; the JSON shape of `serviceInstanceLimits` (the check accepts the
  old `limitOverride` shape and `vCPUs`/`memoryGB`, and refuses anything else); the limits
  mutation. `serviceInstanceLimitsUpdate(input: {serviceId, environmentId, vCPUs, memoryGB})`
  is not in the docs, but it set scanner-nightly's 16 vCPU / 16 GB on 2026-09-28
  (`docs/RAILWAY_SCANNING.md`, Service configuration).
- The old files' `builder = "DOCKERFILE"` is not a checked setting. The instance `builder`
  field reads `RAILPACK` on services that built from a Dockerfile through the toml (P0A.3,
  `docs/SCANNING_OPS_LOG.md`). On 2026-09-28, setting `dockerfilePath` through the API is
  what made scanner-nightly build its Dockerfile after a Railpack build failed
  (`docs/RAILWAY_SCANNING.md`). So the check compares `dockerfilePath`, and the first
  deploy without a `railway.toml` (N4.5) confirms the build used `Dockerfile.web`
  (deployment `meta`, or the build log). A Railpack build of the web stage fails its
  healthcheck, and the old deployment keeps serving.

**The expected values** live in `deploy/railway/service_settings.json`. Each key is a
`ServiceInstance` field name, `null` means the setting must be unset, and `limits` is
compared with `serviceInstanceLimits`. The values are those of the two deleted files,
plus explicit "unset" for the cron schedule and the config-file path on both services,
and for the healthcheck on scanner-nightly (`backend/tests/test_railway_service_settings.py`
pins the equality):

| Setting | web (was root `railway.toml`) | scanner-nightly (was `railway.scanner-nightly.json`) |
|---|---|---|
| `dockerfilePath` (the Dockerfile build) | `Dockerfile.web` | `Dockerfile.scanner` |
| `healthcheckPath` / `healthcheckTimeout` | `/health` / 300 | unset / not checked |
| `restartPolicyType` / `restartPolicyMaxRetries` | `ON_FAILURE` / 5 | `NEVER` / not checked |
| `numReplicas` | 1 | 1 |
| `cronSchedule`, `railwayConfigFile` | unset | unset |
| limits | not checked | 16 vCPU, 17179869184 bytes (16 GB) |

**The check.** Before every deploy and every dry run, both deploy scripts take the file
from the deployed ref (never the work tree) and run
`python3 -I deploy/railway/service_settings.py check <service> --settings-file -`. That
sends one read-only `railway api` query (the helper refuses to send anything other than a
plain `query`). Any difference, a field Railway does not return, or an answer it cannot
read (not logged in, a GraphQL error, a CLI without `railway api`) refuses the deploy with
exit 1, and nothing is staged. `ALLOW_UNRELEASED_DEPLOY=1` never overrides it.
`DEPLOY_PYTHON` picks the interpreter (default `python3`). Run the check on its own,
read-only:

```bash
python3 -I deploy/railway/service_settings.py check web
python3 -I deploy/railway/service_settings.py check scanner-nightly
python3 -I deploy/railway/service_settings.py print-query web   # the exact railway api command
```

Verify: exit 0 and `all N checked settings match`. Exit 1 lists each `MISMATCH`. Exit 2 is
`CANNOT CHECK` with the reason.

**Writing the settings** is an owner step from a prepared pack. Never write them from a
script, and never with a dashboard Deploy click that redeploys.

- web: `workspace/phase_runs/N4/N4.1_web_settings.md`. Gated on `web_source_check`
  printing `web-source-clear` ([Web's GitHub source](#webs-github-source)), because a web
  settings change may create a deployment. It records `railway deployment list -s web
  --limit 3` before and after. The settings take effect on web's next deployment that
  ships no `railway.toml`: the first `deploy_web.sh` run from a release that contains N4.1
  (N4.5).
- scanner-nightly: `workspace/phase_runs/N4/N4.1_scanner_settings.md`. **No
  scanner-nightly deployment before N12.2** (D-REL8 (b)). The pack is applied either
  (a) only through a mechanism that the web pack first proved creates no deployment, with
  the owner's explicit OK, or (b) inside N12.2, immediately before
  `deploy_scanner_nightly.sh --dry-run`. If a new scanner-nightly deployment appears
  anyway, stop it at once with `railway down -s scanner-nightly`.
- `print-update <service>` prints the `serviceInstanceUpdate` command for the documented
  inputs and runs nothing. Its stderr names what it leaves out (`railwayConfigFile`, and
  limits, which take the separate `serviceInstanceLimitsUpdate`). `--with-undocumented`
  adds `railwayConfigFile`, for use only after
  `railway api describe ServiceInstanceUpdateInput` shows it.
- When a setting changes on purpose, change `service_settings.json` in the same release
  as the pack that applies it. Otherwise the next deploy refuses.

**Cron path for P6B.7 (N12.11).** The nightly schedule is the scanner-nightly service
setting `cronSchedule`, not a config file (IaC has no cron field). Per
[Cron Jobs](https://docs.railway.com/cron-jobs), it is a crontab expression in UTC. Runs
must be at least 5 minutes apart. A run is skipped while the previous one still runs. The
service must exit when done, which `fleet_scan.py` does. N12.11 does three things in one
step: it sets `"cronSchedule": "0 9 * * *"` in `service_settings.json` in its release; it
applies the same value with `serviceInstanceUpdate` (`input.cronSchedule`, through
`railway api`, with `railway deployment list -s scanner-nightly --limit 3` before and
after); and it runs `python3 -I deploy/railway/service_settings.py check scanner-nightly`.
Read the runs with `railway cron list --service scanner-nightly`. Each scheduled run
starts the latest deployment again. Railway's docs do not say whether a run uses that
deployment's variables or the service's current ones, so treat it like a restart (the
deployment's own variables, [scanner-nightly](#scanner-nightly)). Set the schedule only
when the latest deployment carries the intended `SCAN_FLEET` and `SCAN_FLEET_WINDOW_UTC`.
Remove the schedule by setting `cronSchedule` to `null` the same way, then revert the file.

## The card builder and the web-cards service

The radius search (`GET /api/listings/cars`) answers from stored cards in the table
`listings_grid_cards`. Each card carries a revision, `CARD_REV`: a hash of the serializer
source files (`_REV_SOURCES` in `backend/db/repositories/grid_cards_repo.py`), folded
into every card's key together with the market-stats generation and the attribution
state. A deploy that edits one of those files, or a market-stats recompute, makes every
stored card stale until

```bash
python -m backend.scripts.build_listings_grid_cards
```

has run. It takes 150 to 170 s for the whole fleet (0.6 ms per card, 268,868 active cars
when it was timed), runs in its own process, and a re-run on a current store is a
read-only pass. Since web speed P19 the web worker no longer re-serializes a large stale
batch itself (it serves the stale cards and logs a warning that names this command), so
the builder is what brings the store current. Cards that only aged (older than a day) but
still match their row are served as complete cards; the builder rewrites them nightly so
the date-dependent "price dropped N days ago" label is rebuilt from source.

**After any deploy that changes the card revision, run the builder** (it does no harm
after any other deploy). To know whether this release changes it, print the revision of
the old and the new tag; different values mean the builder must run:

```bash
for t in "v$OLD" "v$V"; do
  git worktree add -q "/tmp/rev-$t" "$t" &&
    (cd "/tmp/rev-$t" && python3 -c 'from backend.db.repositories.grid_cards_repo import CARD_REV; print(CARD_REV)')
  git worktree remove --force "/tmp/rev-$t"
done
```

Run it away from the UTC day roll (00:00 UTC, 19:00 Eastern in winter), while the site
serves stale cards without waiting. The nightly job below also runs it at 08:30 UTC.

### The web-cards service

`web-cards` is a Railway cron service that runs the builder every day at **08:30 UTC**.
It is built from `Dockerfile.web` (the same image as `web`, so the same serializer code and
the same `CARD_REV`), with the start command
`python -m backend.scripts.build_listings_grid_cards`, no health check (it exits when it is
done), restart policy `NEVER` (a failed run waits for the next day instead of looping) and
one replica. The expected settings are the `web-cards` entry of
`deploy/railway/service_settings.json`; `deploy/railway/railway.web-cards.json` holds the
same values as a reference copy (Railway stops reading config files on 2026-12-01, so the
service settings are what counts, as for `web` and `scanner-nightly`).

`scripts/docker-entrypoint-web.sh` runs a command given to the container instead of the web
server, so the start command skips the vault check, the migrations and gunicorn.

`deploy/railway/deploy_web_cards.sh` deploys `git archive v$(cat VERSION)`, exactly like
`deploy_web.sh` and with the same guard (clean tree, HEAD is `main` on origin, annotated
tag on origin) and the same settings check. Deploy it **at the same tag as web, right after
`deploy_web.sh`**: a builder from another tag would write cards for a revision the site does
not serve. Cron runs use the latest deployment, so redeploy it with every release.

**Owner steps, once** (nothing here is done by a script or a session):

1. **Create the service.** Railway dashboard, project `dealership-scanner`, environment
   `production`: New, Empty Service, name it exactly `web-cards`. Do not connect a GitHub
   repository ([Railway source policy](#railway-source-policy)). Copy its service id
   (Settings, Copy Service ID) and export it where you run the scripts; the id is not
   stored in the repository:

   ```bash
   export RAILWAY_WEB_CARDS_SERVICE_ID=<the service id>
   ```

2. **Copy the database variables from web with reference variables.** In `web-cards`,
   Variables, add every database variable web has as a reference to the `Postgres` service,
   for example `INVENTORY_DATABASE_URL=${{Postgres.DATABASE_URL}}` (web's own definition;
   read the names with `railway variable list -s web --kv | cut -d= -f1`, which prints
   names only, never values). A reference keeps the cron service on the same database when
   the database password rotates. Never paste a value. Also give it the variables that
   change what a card says, with the same values as web, or the builder writes cards the
   site then treats as changed: `LISTINGS_INCLUDE_INCOMPLETE_CARS`, `LISTINGS_GRID_GALLERY_MAX`,
   `PRICE_DROP_RECENT_DAYS`, `FLASK_ENV`/`ENV` and the `MARKET_PRICE_*` ones, whichever web sets.

3. **Apply the service settings** in the dashboard (Settings): Dockerfile path
   `Dockerfile.web`, Custom Start Command `python -m backend.scripts.build_listings_grid_cards`,
   Cron Schedule `30 8 * * *`, Restart Policy Never, no Healthcheck Path, one replica, no
   config file path. Deploy the staged changes. Then confirm them read-only (exit 0 and
   `all 8 checked settings match`):

   ```bash
   python3 -I deploy/railway/service_settings.py check web-cards
   ```

4. **Deploy with the script**, from the release checkout that deployed web (tag `v$V`):

   ```bash
   deploy/railway/deploy_web_cards.sh --dry-run
   deploy/railway/deploy_web_cards.sh
   ```

   Verify: the dry run ends `dry run: nothing was uploaded; railway ran only the read-only
   settings query.` and the second command ends `deployed <sha> (v$V) to web-cards`.

5. **Trigger the first run** instead of waiting for 08:30 UTC (a deploy may already have
   started one): `railway redeploy -s web-cards` (dashboard: the latest deployment, Redeploy).
   Run it away from 00:00 UTC. This redeploys only `web-cards`. It does not touch `web` (so
   `web_source_check` does not apply to it) or `scanner-nightly` (whose `SCAN_FLEET` setting
   is what makes a redeploy there dangerous); the builder never scans.

6. **Check the log** for the line that starts with `done in`:

   ```bash
   railway logs -s web-cards --lines 50 | grep '^done in'
   ```

   Verify: it prints `done in 1xx.xs: {...}` with `active`, `fresh`, `rebuilt` and `pruned`
   counts; the first run after a revision change shows `rebuilt` close to `active`, a run
   on a current store shows `rebuilt` of a few hundred at most. No `done in` line, or a
   traceback, means the run failed: nothing was half-applied (the builder stores in
   batches), and the next run continues. Then open a search for a large metro on the site
   and check the response header: no `X-Listings-Partial: 1`.

Read the runs with `railway cron list --service web-cards`. Remove the schedule by setting
`cronSchedule` to `null` the same way, and change `service_settings.json` in the same
release as the pack that applies the change.

## Migrations

Every migration follows the D-DB6 protocol in Appendix C.1 of
`docs/REMEDIATION_PLAN_2026_10.md`:

1. Apply it locally when it merges.
2. Before any deploy that contains it, the owner takes a prod `pg_dump -Fc` and verifies
   it (`pg_restore -l`). Prod runs PG 18.6, so use the PG18 client by full path:
   `/opt/homebrew/opt/postgresql@18/bin/pg_dump` and `.../pg_restore` (18.6, installed
   2026-10-16, N1.5). It is not on PATH: on the MBP, PATH resolves `pg_dump` to 17.10
   (`postgresql@17`), or to 16.14 where `/opt/homebrew/bin` comes first, and neither
   can dump a PG18 server. Then the owner runs
   `python -m backend.scripts.migrate --dry-run` and
   `python -m backend.scripts.migrate --apply --target N` against prod, in the form
   under "Targeting prod" below. N is the highest migration file the release carries
   (release step 1).
3. Only then deploy (release step 12).
4. A migration that drops or constrains data merges only after its dumps or prerequisite
   repairs exist: every checkout's `--apply` and every web boot apply all pending files.
5. A migration that locks `cars` (indexes, foreign keys) runs in a quiet window, after
   `pg_stat_activity` shows no idle-in-transaction session, with `SET LOCAL lock_timeout`.

**What `migrate.py` does (N5: P4.2, P4.3b).** The full rules are in `migrations/README.md`.
- `--target N` handles pending files up to and including VN. Later files stay pending,
  and the run lists them. When only files above the target are pending it logs
  `nothing pending at or below --target N`, never `database is up to date`.
- The first log line names the target: `target: host=... port=... db=...`, then
  `(dry run, no writes)` or `(apply)`. The user, password and connection options are
  never logged. Read that line before you trust a `database is up to date`.
- Each file runs under `SET LOCAL lock_timeout`, 5 s by default. `--lock-timeout`
  changes it (`0` means no limit). A file that waits longer for a lock rolls back, and
  the run exits 1. A file that sets its own `SET LOCAL lock_timeout` overrides the
  flag; `migrations/V026__chain_completeness.sql` sets none.
- Every write transaction takes a Postgres advisory lock and re-reads `schema_migrations`
  under it. A second `migrate` (a web boot during an operator run, say) waits up to
  `--lock-wait` (default `10min`, `0` means no limit), then skips what the first one
  applied. If the wait runs out it exits 1 and writes nothing.
- It never writes to `cars_cleanroom`, the frozen clean-room baseline (D-N1). There,
  `--apply` logs `REFUSE --apply: database cars_cleanroom is the frozen clean-room
  baseline ...` and exits 2 before any lock or write, whatever the other flags
  (`--baseline` and `--record-only` included). A dry run there still reports. Scratch
  copies named `cars_cr_*` (`scripts/cleanroom/build_db.sh`) migrate as usual.

**Targeting prod.** `migrate.py` reads `INVENTORY_DATABASE_URL` from the environment,
and the checkout's `.env` fills it in when it is unset (a value set in the shell wins).
Run plainly in a checkout, it checks the local Postgres: its first line shows
`host=unix-socket:/tmp` or `localhost`, and its `database is up to date` proves nothing
about prod. That matters because every web boot applies pending migrations
(`MIGRATE_ON_BOOT`, default `1`, in `scripts/docker-entrypoint-web.sh`), so a false "up to
date" ships the release's migrations at boot, without the dump. Point it at prod
explicitly, the way the ops log reaches prod Postgres (P0A.4), with the DSN never
printed:

```bash
railway run -s Postgres -- sh -c 'export INVENTORY_DATABASE_URL="$DATABASE_PUBLIC_URL"; case "$INVENTORY_DATABASE_URL" in ""|*@localhost*|*@127.0.0.1*) echo "target: LOCAL, stopping"; exit 1 ;; *) echo "target: remote" ;; esac; .venv/bin/python -m backend.scripts.migrate --dry-run'
```

Verify: it prints `target: remote`, then the migrate log, whose first line names the
Railway proxy host and `db=railway`, never `unix-socket:` or `localhost`. `railway run`
loads the Postgres service's variables into this one local command, and the explicit
`INVENTORY_DATABASE_URL` wins over `.env`. For `--apply`, run the same command with
`--apply --target N` in place of `--dry-run`, and only after the dump. The first release (P2B.3)
carries V001 to V025, which prod already has, so its dry run must log
`database is up to date` and nothing needs applying. The first planned migration prod
does not have is V026 (P4.8, which spells out its prod rollout).

Release step 1 lists the migrations a release carries and, before any deploy, compares
them with prod's applied max version through this dry run (no-deploy releases leave prod
behind `main`). Verify: a second
`python -m backend.scripts.migrate --dry-run` against prod, in the form above, logs
`database is up to date`.

## Release log

Add a row per deployment, on the next phase branch (release step 14). The notes column
records: the step 8 and step 11 Railway snapshots, the last-known-good web deployment id,
whether a rollback below this release is schema-safe (additive migrations only: yes/no),
every web variable changed since the previous row, the web-facing changes the deploy
carries, and the Docker smoke result (NEXT_PHASES_PLAN Section 7.0, release step 8).
Write "not recorded" for anything nobody captured.

| version | tag commit | date | web deployment | scanner-nightly deployment | notes |
|---|---|---|---|---|---|
| 1.5.4 | `486f9012d` | 2026-10-09 (tag 17:56 EDT; web SUCCESS 18:26 EDT) | `848895c1` | none: stays on `90a3d2a0` (09-29) per D-REL8 (b) | First release through this flow (P2B.3; earlier releases tagged retroactively by P2B.4). **Docker smoke: not recorded** (no P2B.6 local stage smoke appears in any log). Railway snapshots (steps 8 and 11): not recorded. Previous web deployment: `d9aaa75e` (1.5.1 redeploy, 2026-10-08), REMOVED status on 2026-10-15; whether Railway still allows a rollback to it is unverified. Rollback below 1.5.4 schema-safe: yes (no file under `migrations/` changed from v1.5.0 to v1.5.4; prod at V025). Web variables changed since the 1.5.1 row: none recorded (cannot_assess between `d9aaa75e` and `848895c1`; the 2026-10-16 N1.5 pack changed no web variable). Row added 2026-10-18 by N1.9 (LE-6). |
| 1.5.6 | `164096b33` | 2026-10-18 (main + tag pushed about 18:24Z) | none: no deploy (N1.10 is a no-deploy release); web stays on `848895c1` (1.5.4) | none: stays on `90a3d2a0` (09-29) per D-REL8 (b) | Phase N1 release (N1.10). 1.5.5 (`4c78b04ec`) was never released on its own: its release PR went red on `pytest` (test-isolation leak in `test_cleanroom_shapes_2026_10.py` hid `test_listings_perf`'s seeded cars; a migration path in step 1 of this doc), fixed forward in `381ae4811` and bumped to 1.5.6 per step 6. PR #1 checks green (lint, pytest, pytest-integration, release-guard). Owner pushed main by exact SHA (`164096b33:refs/heads/main`); tag `v1.5.6` annotated at `164096b33`. Railway snapshots (steps 8 and 11): before 18:23Z and after 18:24:40Z, web 72 / scanner-nightly 8 / Postgres 2 deployments, diff empty (`no-new-deployment`). Docker smoke: not applicable (no deploy). Rollback below 1.5.6 schema-safe: yes (no file under `migrations/` changed since v1.5.4). Web variables changed since the 1.5.4 row: none. Pre-push public-exposure audit: two independent checkers (credentials; private data) clean. |
| 1.5.7 | `715312066` | 2026-10-22 (main fast-forwarded and tag `v1.5.7` pushed about 18:33Z) | none: no deploy yet (the N2.3 deploy waits on N1.6 (ask 3) and the owner's Docker OK); web stays on `848895c1` (1.5.4) | none: stays on `90a3d2a0` (09-29) per D-REL8 (b) | Phase N2 release (N2.1, N2.2, N2.4) plus phase/N1's four docs commits (merged as `a35d73013`). Release PR #2 green (lint, pytest, pytest-integration, release-guard); CI on main run 39832403790 all four jobs success. Railway snapshots (steps 8 and 11): web 72, scanner-nightly 8, Postgres 2 deployments, 3 lines each, identical after the push (no new deployment). No migrations in the range. Rollback schema-safe: not applicable (no deploy). Web variables changed since the previous row: none. Docker smoke: not run (no deploy). Run from `.claude/worktrees/phase-N2` (the main checkout is on the owner's `home-sync-setup`); the `main` push was run by the agent on the owner's explicit chat instruction, because the desktop app sent the owner's `!` line as a message. |
| 1.5.8 | `0fd91872b` | 2026-11-02 (tag `v1.5.8` annotated 10:53 EST; owner pushed main by exact SHA, CI run 41475962311 success; web deploy `deploy_web.sh` 17:47 EST, SUCCESS) | `228eedff` (v1.5.8 build, replaced the 1.5.4 deployment `848895c1`); then `c0fc6580` (redeploy 18:21 EST, SUCCESS, same source with `TRUSTED_PROXY_CIDRS` read at boot), the last-known-good web deployment | none: stays on `90a3d2a0` (09-29) per D-REL8 (b); `152fb03c` FAILED (10-08) is the newest row and no new nightly deployment appeared; `SCAN_FLEET=1` stays staged, never restart before N12.2 | Phase N3 release (security wave 1); deploy pack N3.13 run through Part C4. Railway snapshots (steps 8 and 11): the step 8 snapshot was not taken before the owner's push (the push instruction omitted it). Substitute: after the push, the newest deployments of web (`848895c1`), scanner-nightly (`152fb03c`) and Postgres (`61bdd7bb`) all predated it. The deploy pack's B3 snapshot of web and scanner-nightly, compared with a re-read after the deploy, shows no new scanner-nightly deployment. **Docker smoke: run** against a scratch copy of the cars database (`createdb -T cars`, V025), with these deviations: (1) B17 prod `migrate --dry-run` could not connect from the owner's Mac (psycopg connection timeout: TCP connects, TLS handshake stalls), so the substitute is `git diff 486f9012d..v1.5.8 -- migrations` empty (25 files, V001 to V025) and, after the deploy, the prod boot log of `228eedff` (23:20:26Z), where migrate on boot ran against the prod database and printed "25 migration file(s), 25 already applied, 0 pending" (the entrypoint only warns when migrate fails and boots anyway, so the log line is the proof, not the boot); (2) the smoke container needs a writable database, because `backend/main.py` runs `init_job_queue_schema()` (CREATE TABLE IF NOT EXISTS) at import, which fails on a read-only transaction (also true of 1.5.4); (3) image 11.8 GB against the pack's 3 to 4 GB estimate (GPU torch wheels, before N29). Smoke results: no Chromium in the image, BUILD_COMMIT/TAG/VERSION baked, CSP without esm.sh, register-post and operator-delete 403, smart-search 200 with 500 results (observed in the session; the saved smoke log holds only the boot lines). Live after deploy (observed at 18:20 EST, re-captured on `c0fc6580` on 2026-11-03 in `workspace/phase_runs/N3/N3.13_post_deploy_recheck.txt`): `/api/health` 1.5.8 / `0fd91872b`, `/api/ready` db ok, CSP `script-src 'self' 'nonce-...'` with no esm.sh. Web variables changed since the 1.5.4 row: `TRUSTED_PROXY_CIDRS=cloudflare` (set `--skip-deploys` then `railway redeploy -s web -y`; log line `client_ip: X-Forwarded-For has 2 entries; TRUSTED_PROXY_HOPS=2; TRUSTED_PROXY_CIDRS has 22 networks`); `TRUST_PROXY_HEADERS=1`, `TRUSTED_PROXY_HOPS=2`, `GUNICORN_WORKERS=1` and `CAR_CHAT_WEB_RESEARCH=0` were already set. Rollback below 1.5.8 schema-safe: yes (no file under `migrations/` changed). Web-facing changes carried include: CSP tightened (no esm.sh), `css_url` filter, pending OAuth link, "Inventory last checked" line, `TRUSTED_PROXY_CIDRS`; the [1.5.8] CHANGELOG section has the full list. Left for the owner: N3.13 C5 (remove the `*.up.railway.app` domain, needs the A5 Cloudflare/Railway routing check) and C7b (optional `DEV_IP_ALLOWLIST`). |
| 1.5.9 | `e2219a76b` | 2026-11-02 (owner pushed main by exact SHA, tag `v1.5.9` annotated at 18:43 EST, CI run 41547563741 success) | none: no deploy; web stays on `c0fc6580` (1.5.8) | none: stays on `90a3d2a0` (09-29) per D-REL8 (b) | Phase N4 release (N4.1 service settings as code, N4.3 branch naming docs, N4.4 ops log, P15B.6 deploy references). Railway snapshots (steps 8 and 11): web, scanner-nightly and Postgres each show the same two newest deployments before and after the push (`no-new-deployment`: diffed against the before file right after the tag push; the 18:52 EST snapshot taken before the 1.5.10 push is identical). No migrations in the range. Docker smoke: not applicable (no deploy). Rollback schema-safe: not applicable (no deploy). Web variables changed since the 1.5.8 row: none. Deploy scripts refuse from this tag on until the live Railway settings match `deploy/railway/service_settings.json` (N4.1; Railway stops reading `railway.toml` on 2026-12-01). Ran from the `main` checkout; branch CI on `phase/N4` was green first. |

Before this flow existed, deploys were `railway up` uploads of working trees, which may
have contained uncommitted files (P2B.4 tags these releases retroactively):

| version | commit | web deployment | scanner-nightly deployment | notes |
|---|---|---|---|---|
| 1.5.0 | `db4b6df8d` | `62f93905` (2026-10-04) | | applied V025 |
| 1.5.1 | `90c9d6160` | `c9872e66` (2026-10-05), then `d9aaa75e` (2026-10-08) | | `d9aaa75e` is `railway redeploy -s web` of the same image, done to apply variable changes |
| (none) | `89c13ad52` | | `90a3d2a0` (2026-09-29) | `git archive HEAD` upload, no release tag |

These ids are rollback targets only while Railway's retention policy keeps them, and a
rollback to one restores its variable snapshot too ([Rollback](#rollback)). `62f93905`
and `c9872e66` predate the 2026-10-08 web variable changes. `90a3d2a0` has
`SCAN_FLEET=1` and must never be a rollback target.
