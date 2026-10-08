# shellcheck shell=bash
# Guarded Railway deploy (remediation P2B.2, owner decision D-REL10).
#
# Sourced by deploy_web.sh and deploy_scanner_nightly.sh, never run on its own,
# so both services refuse for exactly the same reasons. Written for bash 3.2
# (macOS /bin/bash) as well as bash 5 (CI).
#
# 1. Guard. The deploy is refused unless all of these hold:
#      - the work tree has no tracked changes (staged, unstaged, deleted or
#        unmerged). Untracked files are reported but do not refuse: the stage
#        is built by `git archive`, so they can never reach the image;
#      - HEAD is the tip of `main` on origin, read live with
#        `git ls-remote origin refs/heads/main` (a stale local origin/main
#        cannot pass a commit origin no longer has at its tip);
#      - HEAD carries the annotated tag v<VERSION at HEAD>, and origin has that
#        same tag object.
#    ALLOW_UNRELEASED_DEPLOY=1 deploys anyway, with a loud warning on stderr.
#    It is reserved for a mid-fleet scanner hotfix (D-REL10). Even then only
#    committed content ships: the stage is `git archive HEAD`, never the work
#    tree, and BUILD_TAG reads "unreleased" unless HEAD carries the tag.
# 2. Stage. `git archive refs/tags/v<VERSION>` into an empty directory, plus
#    MUST_SHIP_ALLOWLIST (below), plus BUILD_COMMIT (the full SHA) and
#    BUILD_TAG at the stage root, next to VERSION. /health and /api/health
#    report BUILD_COMMIT as "commit" (backend/routes/site_misc.py).
# 3. Upload. `railway up <stage> --path-as-root --service <service> --detach`,
#    run from the repo directory, where the Railway project link lives.
#
# Options (both scripts):
#   --dry-run          run the guard, build the stage, print the plan and the
#                      file count; railway is never called
#   --keep-stage DIR   stage into DIR (new, or an empty directory) and leave
#                      it there, e.g. for `docker build -f Dockerfile.web DIR`
#   -h, --help         print the usage
#
# Exit codes: 0 deployed, or dry run passed; 1 refused by the guard (nothing
# staged, railway not called); 2 usage or staging error.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  echo "deploy/railway/_guarded_deploy.sh is sourced by deploy_web.sh and deploy_scanner_nightly.sh; run one of those." >&2
  exit 2
fi

# P0A.3 ground truth (docs/SCANNING_OPS_LOG.md, "Must-ship allowlist for
# P2B.2"): every non-git path in the prod web image is generated at build
# (Dockerfile.web's static .br/.gz siblings) or at runtime (__pycache__), so
# nothing untracked rides along with `git archive <tag>`. Add a repo-relative
# path here only with new ground truth. An entry must exist in the work tree
# and must NOT be tracked at the deployed commit (a tracked path already ships,
# and copying it from the work tree could ship uncommitted edits).
MUST_SHIP_ALLOWLIST=()

_GD_HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_GD_STAGE_TMP=""

_gd_cleanup() {
  if [ -n "$_GD_STAGE_TMP" ] && [ -d "$_GD_STAGE_TMP" ]; then
    rm -rf "$_GD_STAGE_TMP"
  fi
}

_gd_die() {  # _gd_die <exit code> <message...>
  local code="$1"
  shift
  printf '%s\n' "$*" >&2
  exit "$code"
}

_gd_usage() {  # _gd_usage <script> <service>
  cat <<EOF
usage: deploy/railway/$1 [--dry-run] [--keep-stage DIR]

Deploys the release tagged v\$(cat VERSION) at HEAD to the Railway service
'$2' from a clean \`git archive\` stage that also carries BUILD_COMMIT and
BUILD_TAG. Refuses unless the tracked tree is clean, HEAD == main on origin
and HEAD carries the annotated tag (also on origin).

  --dry-run          guard, stage and print the plan; never calls railway
  --keep-stage DIR   stage into DIR (new or empty) and keep it afterwards

ALLOW_UNRELEASED_DEPLOY=1 overrides a failed guard with a loud warning
(mid-fleet hotfix only, D-REL10). Exit: 0 ok, 1 refused, 2 usage/staging error.
EOF
}

# guarded_deploy <script name> <service> <stage hook or ""> <after-deploy hint> [args...]
#
# <stage hook> is a function name called as `hook <stage dir>` after the
# archive and the allowlist are in place and before BUILD_COMMIT/BUILD_TAG are
# written; it may only rearrange files that came from the archive.
guarded_deploy() {
  local script="$1" service="$2" hook="$3" hint="$4"
  shift 4

  local dry_run=0 keep_stage=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --dry-run) dry_run=1 ;;
      --keep-stage)
        if [ $# -lt 2 ] || [ -z "$2" ]; then
          _gd_usage "$script" "$service" >&2
          _gd_die 2 "$script: --keep-stage needs a directory"
        fi
        keep_stage="$2"
        shift
        ;;
      --keep-stage=*)
        keep_stage="${1#--keep-stage=}"
        if [ -z "$keep_stage" ]; then
          _gd_die 2 "$script: --keep-stage needs a directory"
        fi
        ;;
      -h | --help)
        _gd_usage "$script" "$service"
        exit 0
        ;;
      *)
        _gd_usage "$script" "$service" >&2
        _gd_die 2 "$script: unknown argument: $1"
        ;;
    esac
    shift
  done

  local repo
  repo="$(git -C "$_GD_HERE/../.." rev-parse --show-toplevel 2>/dev/null)" ||
    _gd_die 2 "$script: $_GD_HERE/../.. is not inside a git work tree"

  # Resolve --keep-stage against the caller's directory before anything cds.
  if [ -n "$keep_stage" ]; then
    case "$keep_stage" in
      /*) ;;
      *) keep_stage="$PWD/$keep_stage" ;;
    esac
  fi

  # ---- 1. guard --------------------------------------------------------------
  local head
  head="$(git -C "$repo" rev-parse --verify -q 'HEAD^{commit}')" ||
    _gd_die 2 "$script: HEAD does not name a commit"

  # Pipelines below never stop reading early (no head, no awk exit): under
  # pipefail a writer killed by SIGPIPE would fail the whole assignment.
  local problems="" version="" tag="" tag_ok=0
  if version="$(git -C "$repo" show HEAD:VERSION 2>/dev/null)"; then
    version="$(printf '%s' "$version" | tr -d '[:space:]')"
    case "$version" in
      "")
        problems="${problems}  - VERSION at HEAD is empty
"
        ;;
      *[!0-9A-Za-z.+-]*)
        problems="${problems}  - VERSION at HEAD is not a version string: '$version'
"
        version=""
        ;;
    esac
  else
    problems="${problems}  - HEAD has no VERSION file
"
    version=""
  fi

  local dirty
  dirty="$(git -C "$repo" status --porcelain --untracked-files=no)"
  if [ -n "$dirty" ]; then
    problems="${problems}  - the work tree has tracked changes (commit or stash them):
$(printf '%s\n' "$dirty" | sed -n '1,20s/^/      /p')
"
  fi
  local untracked
  untracked="$(git -C "$repo" status --porcelain --untracked-files=normal | awk '/^\?\?/ { n++ } END { print n + 0 }')"

  local remote_main=""
  if remote_main="$(git -C "$repo" ls-remote origin refs/heads/main 2>/dev/null)"; then
    remote_main="$(printf '%s\n' "$remote_main" | awk '$2 == "refs/heads/main" && !seen { print $1; seen = 1 }')"
    if [ -z "$remote_main" ]; then
      problems="${problems}  - origin has no main branch
"
    elif [ "$remote_main" != "$head" ]; then
      problems="${problems}  - HEAD $head is not main on origin ($remote_main)
"
    fi
  else
    problems="${problems}  - could not read main from origin (git ls-remote origin refs/heads/main failed)
"
  fi

  if [ -n "$version" ]; then
    tag="v$version"
    local tag_obj="" tag_type="" tag_commit="" remote_tag=""
    if ! tag_obj="$(git -C "$repo" rev-parse --verify -q "refs/tags/$tag")"; then
      problems="${problems}  - tag $tag is missing (VERSION at HEAD is $version)
"
    else
      tag_type="$(git -C "$repo" cat-file -t "$tag_obj")"
      tag_commit="$(git -C "$repo" rev-parse --verify -q "$tag_obj^{commit}" || true)"
      if [ "$tag_type" != "tag" ]; then
        problems="${problems}  - tag $tag is lightweight; a release tag must be annotated (git tag -a)
"
      elif [ "$tag_commit" != "$head" ]; then
        problems="${problems}  - tag $tag points at $tag_commit, not HEAD $head
"
      else
        tag_ok=1
        if remote_tag="$(git -C "$repo" ls-remote origin "refs/tags/$tag" 2>/dev/null)"; then
          remote_tag="$(printf '%s\n' "$remote_tag" | awk -v r="refs/tags/$tag" '$2 == r && !seen { print $1; seen = 1 }')"
          if [ -z "$remote_tag" ]; then
            problems="${problems}  - tag $tag is not on origin (git push origin $tag)
"
          elif [ "$remote_tag" != "$tag_obj" ]; then
            problems="${problems}  - origin's tag $tag ($remote_tag) is not the local tag object ($tag_obj)
"
          fi
        else
          problems="${problems}  - could not read tag $tag from origin
"
        fi
      fi
    fi
  fi

  local ref build_tag released
  if [ -z "$problems" ]; then
    ref="refs/tags/$tag"
    build_tag="$tag"
    released=1
  elif [ "${ALLOW_UNRELEASED_DEPLOY:-}" = "1" ]; then
    ref="$head"
    if [ "$tag_ok" = 1 ]; then build_tag="$tag"; else build_tag="unreleased"; fi
    released=0
    {
      echo "########################################################################"
      echo "##  ALLOW_UNRELEASED_DEPLOY=1: DEPLOYING UNRELEASED CODE TO '$service'"
      echo "##  The release guard failed:"
      printf '%s' "$problems" | sed 's/^/##  /'
      echo "##  Shipping the COMMITTED tree of HEAD $head"
      echo "##  (BUILD_TAG=$build_tag). Uncommitted changes are NOT shipped."
      echo "##  Reserved for a mid-fleet scanner hotfix (D-REL10): cut a release"
      echo "##  to main and redeploy from its tag as soon as the fleet allows."
      echo "########################################################################"
    } >&2
  else
    {
      echo "$script: REFUSED. Deploys come only from a tagged release on main (D-REL10):"
      printf '%s' "$problems"
      if [ -n "${ALLOW_UNRELEASED_DEPLOY:-}" ]; then
        echo "  (ALLOW_UNRELEASED_DEPLOY='${ALLOW_UNRELEASED_DEPLOY}' is ignored; only the exact value 1 overrides)"
      fi
      echo "Nothing was staged and railway was not called."
      echo "Mid-fleet hotfix only: ALLOW_UNRELEASED_DEPLOY=1 deploy/railway/$script"
    } >&2
    exit 1
  fi

  if [ "$dry_run" = 0 ] && ! command -v railway >/dev/null 2>&1; then
    _gd_die 2 "$script: the railway CLI is not on PATH"
  fi

  # ---- 2. stage --------------------------------------------------------------
  local stage stage_fate="removed on exit"
  if [ -n "$keep_stage" ]; then
    if [ -e "$keep_stage" ] && [ ! -d "$keep_stage" ]; then
      _gd_die 2 "$script: --keep-stage $keep_stage exists and is not a directory"
    fi
    if [ -d "$keep_stage" ] && [ -n "$(ls -A "$keep_stage")" ]; then
      _gd_die 2 "$script: --keep-stage $keep_stage is not empty"
    fi
    mkdir -p "$keep_stage"
    stage="$(cd "$keep_stage" && pwd)"
    stage_fate="kept"
  else
    local tmp_root="${TMPDIR:-/tmp}"
    stage="$(mktemp -d "${tmp_root%/}/${service}-stage.XXXXXX")"
    _GD_STAGE_TMP="$stage"
    trap _gd_cleanup EXIT
  fi

  git -C "$repo" archive --format=tar "$ref" | tar -x -C "$stage"
  local archived
  archived="$(git -C "$repo" ls-tree -r --name-only "$ref" | wc -l | tr -d ' ')"

  local rel allowlisted=0
  for rel in ${MUST_SHIP_ALLOWLIST[@]+"${MUST_SHIP_ALLOWLIST[@]}"}; do
    case "$rel" in
      "" | /* | .. | ../* | */.. | */../*)
        _gd_die 2 "$script: allowlist entries must be repo-relative paths: '$rel'"
        ;;
    esac
    if [ ! -e "$repo/$rel" ]; then
      _gd_die 2 "$script: allowlisted path is missing from the work tree: $rel"
    fi
    if git -C "$repo" cat-file -e "$ref:$rel" 2>/dev/null; then
      _gd_die 2 "$script: allowlisted path is tracked at $ref and already ships: $rel"
    fi
    mkdir -p "$stage/$(dirname "$rel")"
    cp -Rp "$repo/$rel" "$stage/$rel"
    allowlisted=$((allowlisted + 1))
  done

  if [ -n "$hook" ]; then
    "$hook" "$stage"
  fi

  printf '%s\n' "$head" >"$stage/BUILD_COMMIT"
  printf '%s\n' "$build_tag" >"$stage/BUILD_TAG"

  local total
  total="$(find "$stage" \( -type f -o -type l \) | wc -l | tr -d ' ')"

  # ---- 3. plan, then upload --------------------------------------------------
  echo "guarded deploy: $script"
  echo "  service   $service"
  echo "  commit    $head"
  if [ "$released" = 1 ]; then
    echo "  release   $tag (annotated, on origin; HEAD == origin main)"
  else
    echo "  release   GUARD OVERRIDDEN by ALLOW_UNRELEASED_DEPLOY=1 (BUILD_TAG=$build_tag)"
  fi
  echo "  version   ${version:-unknown}"
  echo "  source    git archive $ref ($archived tracked files)"
  echo "  allowlist $allowlisted path(s)"
  echo "  stage     $stage ($total files, incl. BUILD_COMMIT and BUILD_TAG; $stage_fate)"
  if [ "$untracked" != 0 ]; then
    echo "  note      $untracked untracked path(s) in the work tree; the stage cannot include them"
  fi
  echo "  command   (cd $repo && railway up $stage --path-as-root --service $service --detach)"

  if [ "$dry_run" = 1 ]; then
    echo "dry run: railway was not called."
    if [ -n "$keep_stage" ]; then
      echo "stage kept at $stage"
    fi
    return 0
  fi

  # The Railway project link lives on the repo directory.
  cd "$repo" || _gd_die 2 "$script: cannot cd to $repo"
  railway up "$stage" --path-as-root --service "$service" --detach
  echo "deployed $head (${build_tag}) to $service"
  if [ -n "$hint" ]; then
    echo "$hint"
  fi
  if [ -n "$keep_stage" ]; then
    echo "stage kept at $stage"
  fi
}
