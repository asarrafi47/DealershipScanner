#!/bin/sh
# Bump VERSION (patch|minor|major or an explicit x.y.z), stamp CHANGELOG.md, stage both.
# Every push must carry a new VERSION (scripts/git-hooks/pre-push enforces it).
set -eu
cd "$(dirname "$0")/.."

cur=$(tr -d '[:space:]' < VERSION)
kind="${1:-patch}"
IFS=. read -r maj min pat <<EOF
$cur
EOF
case "$kind" in
  major) maj=$((maj + 1)); min=0; pat=0 ;;
  minor) min=$((min + 1)); pat=0 ;;
  patch) pat=$((pat + 1)) ;;
  [0-9]*.[0-9]*.[0-9]*) maj=${kind%%.*}; rest=${kind#*.}; min=${rest%%.*}; pat=${rest#*.} ;;
  *) echo "usage: $0 [patch|minor|major|x.y.z]" >&2; exit 2 ;;
esac
new="$maj.$min.$pat"
[ "$new" = "$cur" ] && { echo "VERSION already $cur" >&2; exit 1; }

printf '%s\n' "$new" > VERSION

today=$(date +%Y-%m-%d)
if grep -q '^## \[Unreleased\]' CHANGELOG.md; then
  # Turn the Unreleased block into this release and open a fresh Unreleased above it.
  awk -v v="$new" -v d="$today" '
    /^## \[Unreleased\]/ && !done { print "## [Unreleased]"; print ""; print "## [" v "] - " d; done=1; next }
    { print }' CHANGELOG.md > CHANGELOG.md.tmp && mv CHANGELOG.md.tmp CHANGELOG.md
else
  printf '## [%s] - %s\n\n' "$new" "$today" | cat - CHANGELOG.md > CHANGELOG.md.tmp && mv CHANGELOG.md.tmp CHANGELOG.md
fi

git add VERSION CHANGELOG.md
echo "VERSION $cur -> $new (staged with CHANGELOG.md)"
