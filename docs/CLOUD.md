# Cloud sessions (Claude Code on the web)

How to run Claude Code sessions on this repository in Anthropic's cloud, so a long run does not
die when the Mac sleeps or loses its network. A cloud session is a fresh Ubuntu 24.04 VM (x86_64,
about 4 CPUs, 16 GB of memory and 30 GB of disk) with the repository cloned into it. It runs as
root and keeps working after you close the tab or the laptop, until Claude finishes the task in
front of it. (Remote Control is not a substitute: it only steers a session that still runs on
the Mac, so a sleeping Mac still stops the work.)

This repository is public. Nothing in this guide, in `.claude/settings.json` or in `scripts/cloud/`
may carry a secret, a private host name, an address or an id. Keys belong in the claude.ai
environment settings, and anyone who uses an environment can read its variables and its setup
script, so the environment below holds none.

What the setup gives a session: the CI toolchain (Python 3.12, CPU torch, the pinned requirements,
ruff, Node 20), the offline test tier, the Postgres tier on Postgres 18 with pgvector, and the
repo's push rules. What it cannot give a session is in [section 7](#7-what-cloud-sessions-cannot-do).

The setup lives in the repository: `scripts/cloud/setup.sh` (idempotent; `--check` only reports),
`scripts/cloud/with_pg.sh` (the Postgres tier as root), and a SessionStart hook in
`.claude/settings.json` that runs the setup only when `CLAUDE_CODE_REMOTE=true`, which the cloud VM
sets and your Mac never does. A local Claude Code session in this repository is not affected.

Every step has a **Verify** line. Do not go on until it shows what it says. The UI names below are
the ones in Anthropic's documentation on 2026-11-10; the places where the documentation is silent
are collected in [section 10](#10-not-confirmed-yet).

## 0. Before the first session

1. **Decide the history rewrite first.** If you are going to rewrite and force-push the history,
   do it before any cloud session pushes, or rebuild its branches afterwards
   ([section 9](#9-history-rewrite)). A branch pushed now carries the old commit ids.
2. **Protect `main` on GitHub.** Today `main` has no branch protection and no ruleset, and the
   cloud's GitHub proxy does not restrict which branch a session may update (it only refuses tags
   and branch deletions). In the repository: Settings, Rules, Rulesets, New branch ruleset.
   Target `main`. Turn on Restrict updates, Restrict deletions and Block force pushes, and put the
   Repository admin role on the bypass list so your own release push
   (`git push origin <branch>:main`, docs/RELEASING.md) still works. If a ruleset for `main`
   exists by the time you read this, add Restrict updates to it and run the test below against it.

   Whether a push made from a cloud session counts as you for that bypass is not documented. A
   session pushes with your GitHub credentials, through a proxy, so it may. Find out on a
   throwaway name before relying on it:

   1. Make a second ruleset that targets the branch name `cloud-ruleset-test`, with Restrict
      updates and the same bypass list. Create that branch on GitHub.
   2. In a cloud session ([section 3](#3-start-a-session)) ask for an empty commit pushed to
      `cloud-ruleset-test`.
   3. The push must be refused. If it goes through, your bypass covers the session: do not give
      yourself bypass on `main`, and switch the ruleset's enforcement off for the minute a
      release push takes.
   4. Delete the test ruleset and branch (a session cannot delete branches).

   Verify: `gh api repos/asarrafi47/DealershipScanner/rules/branches/main` no longer prints `[]`.
3. **Push the branch that carries the setup.** The hook and the scripts exist only on branches
   that contain them, and `phase/N37a` does not yet. From the Mac:

   ```bash
   git push -u origin chore/cloud-setup
   ```

   The first push of a new branch is compared with `origin/main`, so it needs no VERSION bump
   (docs/RELEASING.md, bump rule).

   Verify: `git ls-remote --heads origin chore/cloud-setup` prints one line.

## 1. Connect GitHub

Cloud sessions need a claude.ai account on Pro, Max, Ultra, Team or Enterprise (not an API key,
and not Bedrock, Vertex or Foundry). They count toward the plan's usage limits like any other use,
and parallel sessions use the limits in proportion. On Team and Enterprise an Owner may first have
to turn on the GitHub connector (Organization settings, Connectors).

**Option A, recommended: the Claude GitHub App.** At claude.ai/code, sign in, choose Sign in with
GitHub and approve the authorization. Then install the Claude GitHub App
(github.com/apps/claude/installations/new): on GitHub choose Only select repositories and pick
`asarrafi47/DealershipScanner`. A session can clone a public repository without the App, but
`claude --cloud` from the Mac clones from GitHub only when the App is installed on the repository.
Without it, Claude Code uploads your local repository as a bundle instead (all branches' history,
plus uncommitted changes to tracked files), and that applies even with `/web-setup`. With the
history rewrite pending, you do not want that upload. The documentation does not say whether the
App alone gives a session push access to a public repository you own (not confirmed, see
[section 10](#10-not-confirmed-yet)); the push test below settles it. Installing the App makes
Auto-fix available for pull requests in the repository. It is turned on per pull request, so leave
it off: it replies on GitHub under your name.

**Option B, only if A cannot push: `/web-setup`.** In the Claude Code CLI, signed in with
`claude auth login`, run `/web-setup`. It sends the token from `gh auth token` to Anthropic, which
stores it encrypted. Sessions can then reach every repository that token reaches, not just this
one, and their pushes are made as you. A narrower token is safer: a fine-grained token limited to
this repository (contents and pull requests, read and write) in `gh` before you run `/web-setup`
(whether `/web-setup` accepts one is not confirmed). If the CLI warns that the token lacks the
`workflow` scope, `gh auth refresh -s workflow` adds it; without it GitHub can refuse a push that
changes `.github/workflows/`. Remove the connection at claude.ai/customize/connectors.

**Push test** (once the environment of [section 2](#2-create-the-environment) exists): start a
session on `chore/cloud-setup`, and ask it to create a branch, make an empty commit and push it.

Verify: the branch is on GitHub (`git ls-remote --heads origin '*'` shows it). Delete it from the
Mac, because the proxy rejects branch deletions: `git push origin --delete <branch>`.

## 2. Create the environment

At claude.ai/code, the first control in the row under the message box is the environment
selector. Choose New environment at the bottom of its menu, fill in the rows below, and save with
Add environment. (Edit one later from the same menu: hover over its row and use the settings icon.)

**Name:** `dealership-code`.

**Network access:** Limited, with Allow package managers left checked, and these hosts in the
Allowed domains field, one per line. Limited with Allow package managers is the default list: it
covers PyPI, npm, the Ubuntu archives, GitHub (including `raw.githubusercontent.com`) and Docker
Hub. None of the hosts below is on it. (Older versions of the dialog and of the documentation
called these levels Trusted and Custom; an environment saved that way opens as Limited.)

| Host | Why | Without it |
|---|---|---|
| `download.pytorch.org`, `download-r2.pytorch.org` | CPU-only torch, installed first the way CI does. The index lists its wheels on the second host | The setup refuses to install the requirements and says so: pip would take PyPI's default torch, a multi-GB CUDA stack. The Python side is not ready |
| `apt.postgresql.org` | PostgreSQL 18 and pgvector for the pg tier, from the PGDG repository (its signing key comes with the image's `postgresql-common` package) | The pg tier runs on the image's PostgreSQL 16 with the Ubuntu archive's pgvector instead. That works, but it is not parity with CI and production, and the setup says so |
| `download.geonames.org` | The US postal table from GeoNames, the source CI uses. `backend/db/geo.py` loads it when any test imports it | The setup falls back to a mirror on `raw.githubusercontent.com` (already allowed) that can lag GeoNames slightly |
| `huggingface.co`, `*.hf.co` | The embedding model for the web app's smart search | Leave both out for code work |

The image ships PostgreSQL 16 and Redis 7.0 (installed, not running). The tests need neither as
they are: the pg tier starts its own throwaway cluster, and Redis is only an optional readiness
probe. Do not point any test or script at the image's own cluster.

Do not run live dealer scans in the cloud ([section 7](#7-what-cloud-sessions-cannot-do)). If you
want a throwaway session to look at a dealer's site, make a second environment with Full network
access, no variables and no setup script. Never give the code environment Full access.

### Environment variables

Paste these into the Environment variables row (`.env` format). None is a secret.

```
BASH_DEFAULT_TIMEOUT_MS=600000
BASH_MAX_TIMEOUT_MS=1800000
LOCAL_LLM_AUTOSTART=0
KMAC_VAULT_DISABLE=1
SMART_SEARCH_SKIP_VECTOR_RERANK=1
```

The two `BASH_` values let a command run for 10 minutes by default and up to 30, so the 7 to 12
minute offline test tier fits. (A command that reaches its timeout is moved to the background and
may run up to 30 more minutes.) `LOCAL_LLM_AUTOSTART=0` stops `run.py` from looking for a local
model runner the VM does not have. `KMAC_VAULT_DISABLE=1` is a safeguard: no secrets vault is
reachable from the cloud. `SMART_SEARCH_SKIP_VECTOR_RERANK=1` skips the embedding rerank until the
model host above is allowed.

Do not set any of these in a cloud environment (or anywhere a session can read):

- Database addresses: `INVENTORY_DATABASE_URL`, `DATABASE_URL`, `PGVECTOR_URL`, `REDIS_URL`,
  `SCAN_LAB_DATABASE_URL`, `SCAN_TEST_DATABASE_URL`, `SCHEMA_DRIFT_SCRATCH_DSN`,
  `MERGE_SPECS_GOLDEN_DB_URL`. A session works against its own throwaway cluster only, and
  `scripts/pg_test_up.sh` and the pg harness refuse a real database anyway.
- Deploy and vault credentials: `VAULT_TOKEN`, `VAULT_TOKEN_LOCAL`, `RAILWAY_VAULT_TOKEN`, and any
  `RAILWAY_*` variable or Railway token. Deploys stay on the Mac (docs/RELEASING.md).
- Application secrets: `SECRET_KEY`, `FLASK_SECRET_KEY`, `ADMIN_PASSWORD`, `USERS_DB_ENCRYPTION_KEY`,
  `DEV_USERS_DB_ENCRYPTION_KEY`, `STRIPE_SECRET_KEY`, `RESEND_API_KEY`, `GOOGLE_MAPS_API_KEY`.
  Code work and every test run without them.
- Test switches: `TEST_PG_ADMIN_DSN`, `REQUIRE_PG`, `TESTS_BLOCK_NETWORK`. Set them for one command
  ([section 5](#5-commands)), as CI does. `scripts/cloud/with_pg.sh` sets the first two itself.
- Scanner switches such as `SCAN_FLEET` and `SCAN_DEALERS`. Nothing scans in the cloud.
- `ANTHROPIC_API_KEY`. Only the web app's car chat and enrichment read it, and no test needs it.
  Network secrets are never attached to Anthropic's API host, so it could only be a plain variable
  that anyone using the environment can read. If you ever add one, do it in a personal
  environment, and check that it does not change how the session itself signs in (not confirmed).

### Setup script

Paste this into the Setup script row. It is optional: it pre-warms the cached environment, so a
new session starts in seconds instead of minutes. The repo's SessionStart hook runs the same
script at every session start and finishes whatever this leaves undone, so this one never fails
the session (a non-zero setup script stops the session from starting, or starts it without what the
script failed to install).

```bash
#!/bin/bash
# DealershipScanner: pre-warm the cached cloud environment (docs/CLOUD.md). It never fails the session.
REF=chore/cloud-setup
SRC=""
top="$(git rev-parse --show-toplevel 2>/dev/null)"
if [ -n "$top" ] && [ -f "$top/scripts/cloud/setup.sh" ]; then
  SRC="$top"
else
  SRC=/opt/ds-cloud-src
  rm -rf "$SRC"
  if ! git clone --quiet --depth 1 --filter=blob:none --sparse --branch "$REF" \
         https://github.com/asarrafi47/DealershipScanner.git "$SRC" \
     || ! git -C "$SRC" sparse-checkout set scripts; then
    echo "could not fetch $REF: the SessionStart hook will do the setup"
    SRC=""
  fi
fi
if [ -n "$SRC" ]; then
  bash "$SRC/scripts/cloud/setup.sh" --skip git,env \
    || echo "pre-warm incomplete: the SessionStart hook will finish it"
fi
exit 0
```

Per the documentation the repository is cloned before the setup script runs, but the script's
working directory is not stated, so the script uses the session's clone when it finds one and
fetches `scripts/` and the requirements files itself when it does not. `REF` is the branch it
fetches from then. Keep it equal to the branch you start sessions from
([section 6](#6-branches-pushes-and-version)) and edit it when that changes. The script runs before
the environment's variables are set, so it relies on none.

How the cache behaves (from the documentation): the script runs as root before Claude starts, on
the first session in an environment. If it finishes in about five minutes the filesystem is
snapshotted and reused, and the script is skipped for later sessions. The snapshot is rebuilt when
the script or the allowed hosts change, and after about a week. A script change reaches an existing
session only when its VM is rebuilt. The pre-warm overlaps the Postgres install with the Python
install to stay inside that budget. If it does not, every new session runs the install again in the
hook, so the result is the same, only slower (a few minutes).

The hook installs into `/opt/ds-venv` (a venv outside the clone, with a `.venv` link inside it),
puts the venv and Node 20 first on the session's `PATH`, and exports `PG_TEST_BIN` and
`PG_TEST_EXT` for `scripts/pg_test_up.sh`.

**Personal instructions (optional).** A setup script can write `~/.claude/CLAUDE.md` inside the VM
(the documentation describes this pattern; `/context` lists `/root/.claude/CLAUDE.md` under Memory
files). Put your own preferences there, in this environment's script only. Do not commit them: the
repo is public, and your auto-memory holds private details of your infrastructure.

Verify: the environment is saved, and a session started in it shows `cloud-setup: environment
ready` ([section 4](#4-first-session-checks)).

## 3. Start a session

At claude.ai/code choose the repository `asarrafi47/DealershipScanner`, the branch (the control
after the repository selector) and the environment `dealership-code`. This link preselects the
repository and the environment (the branch is still yours to pick):
`https://claude.ai/code?repositories=asarrafi47/DealershipScanner&environment=dealership-code`.
Use one repository per session, started from claude.ai/code or `claude --cloud`: per the
documentation, repo hooks do not run in a cloud thread inside a Claude project or in a session
with several repositories, so the setup would not run there.

| Start from | When |
|---|---|
| `chore/cloud-setup` | Until it is merged. It is `phase/N37a` plus the cloud setup, and the hook only exists on branches that contain it |
| `phase/N37a`, or the newest tip of the release line | After `chore/cloud-setup` is merged into it |
| never `main` | `main` is release 1.5.10, behind the staged 1.5.11 to 1.5.16, and moves only by fast-forward to a tagged release (docs/RELEASING.md) |

Each task gets its own session and its own branch. Permission modes in the cloud are Auto, Accept
edits and Plan. Accept edits pushes without stopping to ask, and the cloud's proxy would let a push
to `main` through. So finish [section 0](#0-before-the-first-session) first, and use Plan for any
session started before the ruleset exists. The commit-and-push checks of
[section 4](#4-first-session-checks) need Auto or Accept edits, so they come after it.

From the terminal: `git switch chore/cloud-setup && claude --cloud "<task>"` starts a session on
the pushed branch, once the Claude GitHub App is installed on the repository (see
[section 1](#1-connect-github)); otherwise it uploads a bundle. Push first: if the branch is not on
GitHub the CLI says so and starts from an ancestor. `/remote-env` picks the default environment for
`--cloud`. `claude -p "<message>" --cloud <session id>` queues a follow-up into a running session
from any machine you are signed in on.

## 4. First-session checks

Start the first session with: "Run `scripts/cloud/setup.sh --check` and answer the checks in
section 4 of docs/CLOUD.md." Each answer settles something the documentation leaves open.

| Check | Expect |
|---|---|
| The session's first context lines | `cloud-setup: environment ready in ...s` (a first session takes several minutes), then the rules line |
| `python --version`; `which python` | `Python 3.12.x`; `/opt/ds-venv/bin/python` |
| `node --version` | `v20.x` |
| `git config core.hooksPath`; `git rev-parse --verify origin/main` | `scripts/git-hooks`; a commit id |
| `env \| grep -i _proxy` | Note what is set. If lowercase `http_proxy`, `https_proxy` or `all_proxy` appear, use the `env -u` form in [section 5](#5-commands) |
| `check-tools` | Records the image's Python minor and the other preinstalled versions |
| `python -c "import backend.db.geo"` | No output and no download (the US postal table is cached) |
| `scripts/cloud/with_pg.sh -- python -m pytest backend/tests/test_pg_harness.py -q -p no:cacheprovider` | Passes, with no "not parity" line on stderr: PostgreSQL 18 and pgvector work as root |
| Make a commit on a new branch and push it | The branch appears on GitHub; note its name; the pre-push hook ran (a second push with an unchanged VERSION is refused) |

Write what you learn into [section 10](#10-not-confirmed-yet).

## 5. Commands

Run them from the repository root. The venv is on `PATH` already.

```bash
# Lint (ruff is pinned to the version CI uses)
ruff check .

# Offline tier, the CI `pytest` job minus the junit file. 7 to 12 minutes: run it in the background.
TESTS_BLOCK_NETWORK=1 python -m pytest backend/tests \
  -m "not integration and not slow and not pg" -q -p no:cacheprovider --timeout=300 --durations=40

# Postgres tier, the CI `pytest-pg` job. 5 to 6 minutes. REQUIRE_PG=1 and the DSNs are set for you.
scripts/cloud/with_pg.sh -- python -m pytest backend/tests -m pg -q -p no:cacheprovider --timeout=300 -rs
```

If the VM exports lowercase proxy variables, tests that assume no proxy can fail
(`test_scanner_http_fetch.py`, `test_scanner_http_characterization.py`, `test_network_guard.py`: they
clear only the uppercase names). Prefix the command with
`env -u http_proxy -u https_proxy -u all_proxy -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY`.

The same 36 or so asset-gated and environment-gated skips as in CI appear (brochure PDFs, the
catalog database, the dev inventory). `PYTHONHASHSEED=0` turns on the six car-detail golden tests
CI skips.

When you change requirements: `pip install pip-audit==2.9.0` then
`pip-audit --ignore-vuln PYSEC-2026-3447 --ignore-vuln GHSA-xmj3-9gf7-h52w` and
`pip-audit -r requirements-scanner.txt`, as the CI `pytest` job does.

`scripts/cloud/with_pg.sh` exists because `initdb` and `pg_ctl` refuse to run as root and a cloud
session is root. It starts the cluster of `scripts/pg_test_up.sh` as the `postgres` user, runs your
command as you, and removes the cluster afterwards. `scripts/pg_test_up.sh` keeps refusing any real
database. It uses PostgreSQL 18 when it is installed; otherwise the first of 17, 16 and 15 that has
pgvector (the pg harness needs 15 or newer), and then it prints a "not parity" line. If a run is
killed hard (SIGKILL), its cluster can outlive it: `pkill -u postgres postgres; rm -rf /tmp/pgtu.*`
clears it.

If neither `apt.postgresql.org` nor the Ubuntu archive route works, the CI image
`pgvector/pgvector:0.8.1-pg18` could run in the VM's Docker instead (Docker Hub is on the default
list): start `dockerd`, run it with `-p 5432:5432` and the same `POSTGRES_PASSWORD` the `pytest-pg`
job in `.github/workflows/ci.yml` uses, and give the pytest command that job's `TEST_PG_ADMIN_DSN`
and `REQUIRE_PG=1`. That path is not tested.

Long runs. After a few idle minutes the VM pauses with its files kept, and a paused VM can later be
reclaimed. When a reclaimed session is reopened its conversation comes back but background commands
and subagents that were running do not. So drive a long job from inside a turn (a background
command that Claude polls), make it resumable in chunks, and commit results as it goes.

## 6. Branches, pushes and VERSION

- **A session pushes to a branch it names.** The documentation says each task gets its own branch
  and states no prefix (not confirmed); expect `claude/...`. It must never be `main`, and never a
  `phase/*` branch. If the first session pushes to the branch it started from instead of a new one,
  stop and tell the session.
- **Never `main`.** Not even a fast-forward. Tags cannot be pushed from the cloud (the proxy rejects
  them), so releases, tags and `main` stay on the Mac (docs/RELEASING.md). The ruleset of section 0
  is the enforcement. The pre-push hook refuses a push to `main` whose VERSION does not rise or
  whose CHANGELOG section is empty (if hooks run in the cloud at all; not confirmed), so it does
  not stop a well-formed one.
- **VERSION.** The tracked pre-push hook compares VERSION with the remote branch, and for a new
  branch with `origin/main`. The first push of a branch cut from `phase/N37a` (1.5.16) passes
  without a bump. Every later push needs `scripts/bump_version.sh patch` first. Sessions stop at
  several points, so expect a bump each time. The numbers are provisional: parallel sessions all
  pick 1.5.17. You renumber at release, as the 1.5.11 to 1.5.16 split already needed. Work units
  never release (docs/RELEASING.md), so drop their bumps when you merge them.
- **CHANGELOG.** A session adds its lines under `## [Unreleased]` and nowhere else; the bump moves
  them under the new version.
- **Pull requests.** GitHub's GraphQL is blocked in the cloud, so `gh pr create` and `gh issue`
  fail. Use the session's Create PR button, or the REST form:
  `gh api repos/asarrafi47/DealershipScanner/pulls -f title=... -f head=<branch> -f base=<phase branch>`.
  Base a PR on the release line, never on `main`: a PR to `main` runs the release guard.
- **Setup files are code that runs as root.** The setup script block above, `scripts/cloud/` and
  `.claude/settings.json` run in every later session of the environment, with whatever the
  environment holds. Review a diff to them like a deploy script before you merge it or point `REF`
  at a branch that changed them.
- **Workflow files.** A `/web-setup` token without the `workflow` scope may be refused when a push
  changes `.github/workflows/`. Refresh it with `gh auth refresh -s workflow` on the Mac.
- **Session links in commits.** Commits made in the cloud carry a `Claude-Session:` trailer with the
  session's URL, and pull request bodies carry it too. The repository is public, so decide whether
  you want that. `attribution.sessionUrl` set to `false` in the settings turns it off. It is not set here.

## 7. What cloud sessions cannot do

| Not possible | Why | Instead |
|---|---|---|
| Live dealer scans (`dealer_pipeline`, `scanner.py`, `fleet_scan`) | The recipes live in the gitignored `workspace/` and in the local database, dealer sites need Full network access and may refuse the cloud's addresses, and CLAUDE.md requires `workspace/dealer_logs/<id>/` for every dealer touched, which a reclaimed VM loses | Run scans on the Mac. A cloud session can change scanner code and run its tests |
| Railway deploys and `railway` commands | No Railway CLI or token (do not add one), Railway hosts are not allowed, tags cannot be pushed | `docs/RELEASING.md`, on the Mac |
| The real databases, the scan databases, the production database | Unreachable, refused by `pg_test_up.sh` and the pg harness by name, and they must stay that way | A session has its own empty throwaway cluster |
| Local data: `.env`, `workspace/`, the dev inventory, brochure PDFs, the dictionary catalog build | Gitignored, so not in the clone | Asset-gated tests skip, as in CI |
| Your memory and user-level setup (user skills such as dealer-discovery, plugins, user MCP servers, `~/.claude/CLAUDE.md`) | User-level files do not carry into the VM. Only the repo's own `.claude/` does (`.claude/workflows/dealer-discovery.js` is tracked; whether it loads in the cloud is not confirmed) | Put what a session needs in a tracked file, without private details |
| Untracked or unpushed work (untracked docs, other local branches) | Not on GitHub | Push or commit it first, if it may be public |
| `gh pr`, `gh issue` | GraphQL is blocked | The REST form in section 6 |
| macOS-only pieces: the local model runner, the OCR helper, launchd, the iOS app | Linux VM | The Mac |

## 8. Hand work back to the Mac

1. Make sure the session pushed (the Changes card, or ask it). Note the branch name.
2. On the Mac: `git fetch origin` and look at it with `git log --oneline origin/<branch>`. To work on
   it, either `git worktree add .claude/worktrees/<name> origin/<branch>` or
   `claude --teleport <session id>`. Teleport needs a clean tree, a checkout of this repository, the
   branch pushed and the same claude.ai account. It copies the session into the terminal; later work
   there does not appear in the cloud session.
3. Merge it into the release line the way a unit is (docs/RELEASING.md, branching model). Drop the
   VERSION bumps the hook forced, keep the CHANGELOG lines under `[Unreleased]`, and renumber at
   release.
4. Anything the session produced outside git (logs, reports) is gone when its VM is reclaimed. Ask
   for the result as a commit, or as text in the final message.

## 9. History rewrite

A rewrite changes every commit id from the first rewritten commit onward, and publishing it is a
force-push. Cloud sessions interact with that in three ways:

- A session started after the force-push clones the new history and is unaffected.
- A branch a session cut before it is based on old ids. Pushing it uploads the old commits again,
  including whatever the rewrite is meant to remove from the public repository. The pre-push hook
  does not notice.
- `claude --cloud` without the Claude GitHub App on the repository uploads your whole local
  repository, every branch's history included, as a bundle (section 1). Do not use it before the
  rewrite is done on the Mac.

So, in this order: finish or park every cloud session; do the rewrite; rebuild and push
`chore/cloud-setup` on the new history; only then start sessions. If cloud work already exists,
rebuild each branch on the rewritten base with the commit map, from the old commit it was cut at:

```bash
git rebase --onto <new id of the old base> <old base> <branch>
```

Push the result under a new branch name, then delete the old branch on GitHub from the Mac. Never
let a session push a branch that still has old ids in its history.

## 10. Not confirmed yet

As of 2026-11-10 these are not documented, or not tested here. The first session settles them.
(The scripts were run end to end in a cold Ubuntu 24.04 x86_64 container with direct network
access, as root, with PostgreSQL 16 preinstalled like the cloud image: the pasted setup script, then
the hook command, then the checks of section 4. That shows the install paths work. It cannot show
what the cloud's proxy and allowed domains do to them. The pg tier passed in full on PostgreSQL 17
(82 passed, 6 xfailed, the numbers CI gets on 18), and its fast part, built on the whole migration
chain, passed on the image's PostgreSQL 16 with pgvector 0.6.0 (30 tests). The heavy scanner tests
were not run on 16.)

| Question | How to find out |
|---|---|
| What cloud sessions name the branches they push, and whether they ever push to the start branch | Section 4, last row |
| Whether the Claude GitHub App alone can push to this public repository | The push test in section 1 |
| Whether a session's push counts as you for a ruleset's bypass list | The test in section 0 |
| Whether repo git hooks (`core.hooksPath`) run in a cloud session | Section 4: push twice without a bump |
| The image's Python minor version | `check-tools`; the setup builds its own 3.12 venv either way |
| Whether the VM exports lowercase proxy variables | `env \| grep -i _proxy` |
| The clone's depth, and whether `origin/main` exists | `git rev-parse --is-shallow-repository`; the setup fetches `origin/main` when it is missing |
| Whether the setup script's working directory is the clone, and whether its `git clone` of the repository works | The first setup run's output, or `ls /opt/ds-cloud-src` in a session |
| Whether the pre-warm fits the five minute cache budget through the cloud's proxy | Watch the first session start; `time scripts/cloud/setup.sh --force` on a warm VM |
| Whether PostgreSQL 18 from the PGDG repository installs and starts cleanly on the image, and the proxy lets apt reach it | Section 4: the pg harness row |
| Whether `dockerd` starts in a session (only needed for the Docker alternative in section 5) | `docker info` after `service docker start` |
| Whether the Workflow tool, `/effort ultracode` and `.claude/workflows/` work in the cloud | Try one workflow |
| Whether auto-memory exists in the cloud (assume not) | `/context` |
| How long a session can run, and how long an idle one is kept before it is reclaimed | Watch a long run |
| What the default Auto permission mode does about pushes | Watch the first session |

## 11. When something is wrong

- **See what is missing:** `scripts/cloud/setup.sh --check`. It changes nothing and exits 1 when a
  required item is missing. `scripts/cloud/setup.sh` with no option repairs, and `--force` redoes the
  pip install. `--skip` and `--only` take the step names `git python pgeocode node pg env`.
- **Logs:** `/tmp/ds-cloud-setup/<step>.log`, one per step.
- **`which python` is not `/opt/ds-venv/bin/python`:** the hook's `PATH` line did not reach the
  session's commands (it relies on `CLAUDE_ENV_FILE`). Use `/opt/ds-venv/bin/python` directly, or the
  `.venv/bin/python` link in the repository root, and note it in [section 10](#10-not-confirmed-yet).
- **"cpu torch" fails:** `download.pytorch.org` and `download-r2.pytorch.org` are not in the
  environment's allowed domains. The setup stops there on purpose, because the requirements install
  would otherwise pull the multi-GB CUDA torch and blow the time budget.
- **"pgeocode table" fails:** allow `download.geonames.org`, or check that the raw GitHub content host
  is reachable. Without the table every test errors at collection, because `backend/db/geo.py`
  loads it at import.
- **"postgres 18" unavailable:** allow `apt.postgresql.org`, then re-run the setup (or start a new
  session after the cache rebuilds). Until then the "postgres fallback" line says whether the
  image's PostgreSQL 16 stands in for it. Everything else keeps working without Postgres.
- **A changed setup script** reaches a session only when its VM is rebuilt. Start a new session, or
  run `scripts/cloud/setup.sh` by hand in the old one.
- **Requirements changed on a branch:** the hook notices (the digest of `requirements*.txt`
  differs) and re-syncs; it takes a minute or two.
- **Keep this file true.** `backend/tests/test_cloud_setup.py` pins the Python minor, the torch index,
  the ruff pin and the Postgres major against `.github/workflows/ci.yml`, the hook's remote-only
  guard, and that this guide names nothing private. When CI moves one of them, move the script and
  this guide with it.
