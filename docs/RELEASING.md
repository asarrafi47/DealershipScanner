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

## Branching model (D-REL2 (a): trunk)

- `main` is the only long-lived branch. It moves only by fast-forward, and only to a
  released commit.
- Each phase works on a short-lived `phase/<id>` branch cut from `main`. Unit branches
  (`remediation/<unit>`, each in its own `.claude/worktrees/<unit>`) come off the phase
  branch and merge back into it.
- A phase releases with `git push origin phase/<id>:main` (a fast-forward). The phase
  branch is deleted afterwards.
- Until the first release to `main` (P2B.3), the integration branch is
  `feature/http-only-scans`. It is retired after the next release starts from `main`, and
  only once the mini's clone shows no unpushed work (P2B.8 step 7).
- `ci/shakedown-<n>` branches are throwaway CI shakedowns (see the bump rule). Delete them
  once green.
- Never force-push, on any branch. Until the P2B.5 ruleset is active, GitHub itself would
  accept a force push to `main`. Only the person pushing prevents it.

Start a phase:

```bash
git fetch origin
git switch -c phase/<id> origin/main
```

Verify: `git merge-base --is-ancestor origin/main HEAD && echo based-on-main` prints
`based-on-main`.

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
  `lint`, `pytest` and `pytest-integration` run on every branch push.
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
branch is `phase/<id>`, or `feature/http-only-scans` for the first release. `<branch>`
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
   [Migrations](#migrations) before the deploy in step 12. For the first release it lists
   `migrations/README.md` and V001 to V025, because `origin/main` is still the June 0.2.0
   commit. Prod is already at V025 (P0A.1), so the Migrations dry run should log
   `database is up to date`.

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

   Verify: in the push run, `lint`, `pytest` and `pytest-integration` show `success`, and
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

   Verify: `gh pr checks <branch>` exits 0, with `lint`, `pytest`, `pytest-integration` and
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
    back ([Rollback](#rollback), web). For scanner-nightly, never use a Railway rollback:
    follow Rollback, scanner-nightly, and run step 15's variable check at once. Then fix
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
    `release   v$V (annotated, on origin; HEAD == origin main)` and
    `dry run: railway was not called.` Exit 1 means the guard refused: the reasons are
    listed, nothing was staged, and railway was not called. Exit 2 is a usage or staging
    error. The real run uploads with
    `railway up <stage> --path-as-root --service web --detach`. The stage is
    `git archive` of the tag plus `BUILD_COMMIT` (the full SHA) and `BUILD_TAG`. The root
    `railway.toml` builds it with `Dockerfile.web`.

    Optional local check of the exact stage before the real deploy. It is a Docker build,
    so only with the owner's OK and on AC power. P2B.6 lists the full local smoke test.

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
    `/health` returns the same body. It is Railway's healthcheck path (`railway.toml`), and
    the healthcheck only needs a 200. If the deployment ends FAILED instead, the old one
    keeps serving. Read its logs by id (without an id, `railway logs` shows the last
    successful deployment): `railway logs -s web --build --lines 200 <deployment id>`, or
    `--deployment` for the runtime log. Then fix forward.

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
    with the archive's own `deploy/railway/railway.scanner-nightly.json` as the stage's
    `railway.json` (`Dockerfile.scanner`). It refuses `SERVICE=web`.

    Verify: the dry run passes as in step 12. Afterwards,
    `railway deployment list -s scanner-nightly --limit 3` shows the new deployment, and
    `railway logs -s scanner-nightly --lines 50` shows `idle`. While the idle container
    holds, `railway ssh --service scanner-nightly -- cat /app/BUILD_COMMIT /app/BUILD_TAG`
    prints the tag's SHA and `v$V`. The hold lasts `SCAN_IDLE_HOLD_SECONDS`, which
    `backend/scripts/fleet_scan.py` defaults to 0 (exit at once). P0A.1 found it set on
    scanner-nightly; if it is unset or 0, there is no container to `ssh` into and this
    check cannot run. Record the id in the release log too.

## Hotfixes (D-REL10)

- **scanner-nightly, mid-fleet only:** `ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh`.
  This deploys even though the guard failed, and prints a loud warning. It ships the
  committed tree of HEAD (`git archive HEAD`, never uncommitted changes) with
  `BUILD_TAG=unreleased`, or with the tag if HEAD carries one. As soon as the fleet
  allows, cut a release and redeploy from its tag. Only the exact value `1` overrides.
  Verify first with
  `ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh --dry-run`. When the
  guard fails, its plan says `GUARD OVERRIDDEN` and shows the `BUILD_TAG` that will ship.
  Putting a known-good release back mid-fleet uses the same override from a checkout of
  that release's tag ([Rollback](#rollback), scanner-nightly).
- **web:** do not use the override. Cut a patch release (steps 1 to 13).

## Rollback

`main` only moves forward. Never force-push it back. Either fix forward with a new patch
release, or roll web back to an earlier deployment. Never roll scanner-nightly back
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

1. Find the last good deployment: `railway deployment list -s web --limit 10`. The
   release log has the id. Then list every web variable changed since that deployment
   (the dated blocks in `docs/SCANNING_OPS_LOG.md` and the release log notes record
   them). Example: a rollback to `c9872e66` would bring back
   `ALLOW_APP_ADMIN_DEV_PASS_THROUGH` and drop `CAR_CHAT_WEB_RESEARCH=0`, the two
   2026-10-08 changes that `d9aaa75e` was redeployed to apply.
2. Fingerprint the current variables, so you can prove later that they are back. This
   writes only names and a hash, never a value:

   ```bash
   railway variable list -s web --kv | grep -v '^RAILWAY_' | sort | shasum -a 256 > /tmp/web_vars_before.sha
   railway variable list -s web --kv | grep -v '^RAILWAY_' | cut -d= -f1 | sort > /tmp/web_var_names_before.txt
   ```

   Verify: `cat /tmp/web_var_names_before.txt` lists the variable names you expect
   (names only, no values), and `/tmp/web_vars_before.sha` holds one hash.
3. Roll back. In the dashboard: web → Deployments → that deployment → ⋯ → Rollback. Or
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
4. Put today's variables back on the rolled-back image. Compare first:

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

Before P6B.1, scanner-nightly is not deployed at all (D-REL8 (b)). From P6B.1 on, put
known-good code back with a deploy, which uses the current variables:

- Normally fix forward: revert the bad change on the next phase branch and cut a patch
  release (steps 1 to 15).
- Mid-fleet only (D-REL10): run step 15's variable check, then in the main checkout (the
  directory Railway is linked to) check out the known-good release tag and use the
  override:

  ```bash
  git switch --detach v<good version>
  ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh --dry-run
  ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/deploy_scanner_nightly.sh
  git switch -
  ```

  Verify: the dry run's plan says `GUARD OVERRIDDEN` with `BUILD_TAG=v<good version>`,
  and afterwards the step 15 checks pass. Record the deployment in the release log.

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
  have booted that same app against prod (P0A.1 answer 4).
- **Never run a bare `railway up` from a checkout.** It uploads the working tree,
  uncommitted files included, to whatever service the directory is linked to. On
  2026-10-08 a bare `railway up` uploaded a checkout to scanner-nightly by accident. The
  upload failed before anything ran.
- **To apply a variable change**, stage it with
  `railway variable set NAME=value -s <service> --skip-deploys`, then run
  `railway redeploy -s <service> -y`. That redeploys the existing latest deployment with
  the same image and the current variables (`railway redeploy --help`; only
  `--from-source` pulls new code). The 2026-10-08 `d9aaa75e` redeploy of web did exactly
  this. On web, do not apply a change with a deploying `railway variable set` (one
  without `--skip-deploys`) while web still records a GitHub source (open item below):
  on 2026-09-11 exactly that rebuilt web from GitHub `main`. `--skip-deploys` followed
  by `railway restart` does not apply the change, because a restart relaunches the same
  deployment with its old variables. Always pass `-s`. On 2026-10-08 the main checkout
  was linked to scanner-nightly, so a command without `-s` acted on it.

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

### State as of the 2026-10-08 ops log (P0A.1 and P0A.4, `docs/SCANNING_OPS_LOG.md`)

- No service, and no environment-level setting, has a deployment trigger. Autodeploy is
  off everywhere: web reports `NO_INSTALLATION`, Postgres and scanner-nightly report
  `NO_REPO`. The last GitHub-built deployment was web `09dfabb7` on 2026-09-11.
- **The legacy services are gone.** scanner-worker and scanner-scheduler were deleted on
  2026-10-08 (D-REL1 (a)). First, `railway down` removed scanner-scheduler's running June
  deployment `50ac4514`. Neither service had a volume. The project now holds Postgres,
  scanner-nightly and web. Their Dockerfiles and `deploy/railway/railway.scanner-*.toml`
  files are still in the repo; P15B.6 deletes them. docker-compose still uses the
  worker/scheduler entrypoints.
- **Open owner items** (P0A.4 step 6). Web still records `asarrafi47/DealershipScanner`
  as its source. If the Railway GitHub App is given access to the repo again, or someone
  runs `redeploy --from-source`, Railway can build `main` again. The fix is to disconnect
  web's source (Settings → Source → Disconnect) and to confirm at
  github.com/settings/installations that the Railway app cannot see this repo.
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
- **Config as code is deprecated.** The root `railway.toml` (web) and the staged
  `railway.json` (scanner-nightly) are config-as-code files. Railway says existing files
  keep working until 2026-12-01, so the deploy flow needs a replacement before then
  (P0A.1).

### How to verify (read-only)

- After every push to `main`, run release step 11.
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

## Migrations

Every migration follows the D-DB6 protocol in Appendix C.1 of
`docs/REMEDIATION_PLAN_2026_10.md`:

1. Apply it locally when it merges.
2. Before any deploy that contains it, the owner takes a prod `pg_dump -Fc` and verifies
   it (`pg_restore -l`). Then the owner runs `python -m backend.scripts.migrate --dry-run`
   and `python -m backend.scripts.migrate --apply` against prod, in the form under
   "Targeting prod" below.
3. Only then deploy (release step 12).
4. A migration that drops or constrains data merges only after its dumps or prerequisite
   repairs exist: every checkout's `--apply` and every web boot apply all pending files.
5. A migration that locks `cars` (indexes, foreign keys) runs in a quiet window, after
   `pg_stat_activity` shows no idle-in-transaction session, with `SET LOCAL lock_timeout`.

Appendix C.1 applies with `--apply --target N`. `migrate.py` has no `--target` until P4
adds it, so until then `--apply` applies every pending file. Update the command above
when P4 lands.

**Targeting prod.** `migrate.py` reads `INVENTORY_DATABASE_URL` from the environment,
and the checkout's `.env` fills it in when it is unset (a value set in the shell wins).
It does not log the host. Run plainly in a checkout, it checks the local Postgres,
which is also at V025, and prints `database is up to date` without proving anything
about prod. That matters because every web boot applies pending migrations
(`MIGRATE_ON_BOOT`, default `1`, in `scripts/docker-entrypoint-web.sh`), so a false "up to
date" ships the release's migrations at boot, without the dump. Point it at prod
explicitly, the way the ops log reaches prod Postgres (P0A.4), with the DSN never
printed:

```bash
railway run -s Postgres -- sh -c 'export INVENTORY_DATABASE_URL="$DATABASE_PUBLIC_URL"; case "$INVENTORY_DATABASE_URL" in ""|*@localhost*|*@127.0.0.1*) echo "target: LOCAL, stopping"; exit 1 ;; *) echo "target: remote" ;; esac; .venv/bin/python -m backend.scripts.migrate --dry-run'
```

Verify: it prints `target: remote`, then the migrate log. `railway run` loads the
Postgres service's variables into this one local command, and the explicit
`INVENTORY_DATABASE_URL` wins over `.env`. For `--apply`, run the same command with
`--apply` in place of `--dry-run`, and only after the dump. The first release (P2B.3)
carries V001 to V025, which prod already has, so its dry run must log
`database is up to date` and nothing needs applying. The first planned migration prod
does not have is V026 (P4.8, which spells out its prod rollout).

Release step 1 lists the migrations a release carries. Verify: a second
`python -m backend.scripts.migrate --dry-run` against prod, in the form above, logs
`database is up to date`.

## Release log

Add a row per deployment, on the next phase branch (release step 14).

| version | tag commit | date | web deployment | scanner-nightly deployment | notes |
|---|---|---|---|---|---|

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
