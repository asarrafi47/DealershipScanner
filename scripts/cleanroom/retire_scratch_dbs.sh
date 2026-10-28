#!/usr/bin/env bash
# retire_scratch_dbs.sh: list, dump and (owner-gated) drop local scratch DBs (N28.5a).
#
#   scripts/cleanroom/retire_scratch_dbs.sh list
#   scripts/cleanroom/retire_scratch_dbs.sh dump --db cars_cr_x [--db ...] [--execute]
#   scripts/cleanroom/retire_scratch_dbs.sh drop --db cars_cr_x [--db ...] [--execute]
#
# list: every cars_cr_<name>, cars_rehearsal and cars_cleanroom with its size and whether a
#   verified dump and an owner OK exist. Read only.
# dump: pg_dump -Fc into workspace/backups/scratch_<db>_<date>.dump, restore it into a
#   throwaway cars_draft_retire_chk DB, compare exact per-table row counts with the source,
#   then write <dump>.verified (sha256 + counts). Without --execute it only prints the plan.
# drop: dropdb, and only when ALL hold: --execute; the name passes the scratch guard; a
#   <db> dump's .verified marker (db= line is this DB) exists, its sha256 still matches the
#   file and the live row counts equal the marker's; the owner
#   created the per-DB flag workspace/backups/retire_ok/<db>. Without --execute it prints
#   the plan and the unmet gates and drops nothing. This draft is never run with --execute
#   by the workflow; the owner runs it.
#
# Refuses: cars (the live DB) always; cars_cleanroom (the D-N1 frozen baseline) unless
# docs/remediation/OWNER_DECISIONS_LOG.md has a line "cars_cleanroom retire: approved"
# (the logged decision, N28.5b). The scratch-name regex is the one in build_db.sh. Only
# talks to the local Homebrew PG at /tmp:5432. RETIRE_BACKUP_DIR / RETIRE_DECISIONS_LOG
# override the two paths for tests.
set -euo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
BACKUPS=${RETIRE_BACKUP_DIR:-$REPO/workspace/backups}
DECISIONS=${RETIRE_DECISIONS_LOG:-$REPO/docs/remediation/OWNER_DECISIONS_LOG.md}
CHECK_DB=cars_draft_retire_chk
CMD=${1:-list}; [ $# -gt 0 ] && shift
DBS=() EXECUTE=0
usage() { sed -n '2,24p' "$0" >&2; exit 2; }
while [ $# -gt 0 ]; do
  case "$1" in
    --db) DBS+=("${2:-}"); shift 2;;
    --execute) EXECUTE=1; shift;;
    -h|--help) usage;;
    *) echo "unknown option $1" >&2; usage;;
  esac
done
refuse() { echo "REFUSED: $*" >&2; exit 2; }
PSQL=(psql -X -h /tmp -p 5432)
unset PGOPTIONS PGHOST PGPORT PGDATABASE PGSERVICE
STAMP=$(date +%Y%m%d)

guard() {  # name guard; cars_cleanroom needs the logged decision
  local db=$1
  case "$db" in
    ''|cars) refuse "'${db}' is the live DB";;
    cars_cleanroom)
      grep -qs 'cars_cleanroom retire: approved' "$DECISIONS" \
        || refuse "cars_cleanroom is the frozen baseline (D-N1); log 'cars_cleanroom retire: approved' in $DECISIONS first";;
    *) [[ "$db" =~ ^(cars_cr_[a-z0-9_]+|cars_rehearsal)$ ]] || refuse "'$db' is not a scratch name (cars_cr_<name> or cars_rehearsal)";;
  esac
}
latest_dump() {  # exact date suffix, so cars_cr_a never picks up cars_cr_a_b's dumps
  ls -1 "$BACKUPS"/scratch_"$1"_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].dump 2>/dev/null | sort | tail -1 || true
}
sha_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
verified() {  # 0 when the latest dump has a marker for this DB whose sha matches the file
  local d; d=$(latest_dump "$1"); [ -n "$d" ] && [ -f "$d.verified" ] || return 1
  [ "$(sed -n 's/^db=//p' "$d.verified")" = "$1" ] || return 1
  [ "$(sed -n 's/^sha256=//p' "$d.verified")" = "$(sha_of "$d")" ]
}
counts() {  # exact row count per public table of DB $1
  "${PSQL[@]}" -d "$1" -Atc "SELECT relname||'='||(xpath('/row/c/text()', query_to_xml(format('select count(*) as c from %I.%I', schemaname, relname), false, true, '')))[1]::text FROM pg_tables WHERE schemaname='public' ORDER BY 1"
}
all_dbs() {
  "${PSQL[@]}" -d postgres -Atc "SELECT datname||'|'||pg_database_size(datname) FROM pg_database WHERE datname ~ '^(cars_cr_[a-z0-9_]+|cars_rehearsal|cars_cleanroom)\$' ORDER BY 1"
}

cmd_list() {
  printf '%-28s %12s  %-9s %s\n' DB BYTES DUMP OWNER_OK
  local line db size dv ok
  while IFS='|' read -r db size; do
    [ -n "$db" ] || continue
    dv=none; verified "$db" && dv=verified || { [ -n "$(latest_dump "$db")" ] && dv=unverified; }
    ok=no; [ -f "$BACKUPS/retire_ok/$db" ] && ok=yes
    printf '%-28s %12s  %-9s %s\n' "$db" "$size" "$dv" "$ok"
  done < <(all_dbs)
}

dump_one() {
  local db=$1 out="$BACKUPS/scratch_${1}_${STAMP}.dump"
  guard "$db"
  echo "plan: pg_dump -Fc $db -> $out; restore into $CHECK_DB; compare row counts; write $out.verified"
  [ "$EXECUTE" = 1 ] || { echo "dry run (add --execute to dump $db)"; return 0; }
  mkdir -p "$BACKUPS"
  [ "$("${PSQL[@]}" -d postgres -Atc "SELECT count(*) FROM pg_database WHERE datname='$CHECK_DB'")" = 0 ] \
    || refuse "$CHECK_DB already exists; drop it deliberately first"
  pg_dump -h /tmp -p 5432 -Fc -d "$db" -f "$out"
  createdb -h /tmp -p 5432 "$CHECK_DB"
  local rc=0
  pg_restore -h /tmp -p 5432 --no-owner -d "$CHECK_DB" "$out" || rc=$?
  local want have; want=$(counts "$db"); have=$(counts "$CHECK_DB")
  dropdb -h /tmp -p 5432 "$CHECK_DB"   # the throwaway check DB only; the guard above pinned its name
  if [ "$want" != "$have" ]; then
    echo "$db: restore check FAILED (row counts differ); no marker written" >&2; return 1
  fi
  { echo "db=$db"; echo "sha256=$(sha_of "$out")"; echo "checked_at=$(date -u +%FT%TZ)"; echo "tables=$(echo "$want" | wc -l | tr -d ' ')"; echo "$want"; } > "$out.verified"
  echo "$db: dump verified -> $out"
}

drop_one() {
  local db=$1 missing=()
  guard "$db"
  verified "$db" || missing+=("verified dump (run dump --execute)")
  if verified "$db"; then  # rows written after the dump would be lost
    local d; d=$(latest_dump "$db")
    [ "$(sed -n '/^tables=/,$p' "$d.verified" | tail -n +2)" = "$(counts "$db")" ] \
      || missing+=("live row counts differ from the dump marker (re-run dump --execute)")
  fi
  [ -f "$BACKUPS/retire_ok/$db" ] || missing+=("owner OK flag $BACKUPS/retire_ok/$db")
  [ "$EXECUTE" = 1 ] || missing+=("--execute")
  if [ ${#missing[@]} -gt 0 ]; then
    echo "plan: dropdb $db (NOT dropping); unmet: $(IFS=';'; echo "${missing[*]}")"
    return 0
  fi
  echo "dropping $db (verified dump $(latest_dump "$db"))"
  dropdb -h /tmp -p 5432 "$db"
}

case "$CMD" in
  list) cmd_list;;
  dump|drop)
    [ ${#DBS[@]} -gt 0 ] || refuse "$CMD needs at least one --db"
    for d in "${DBS[@]}"; do guard "$d"; done   # every name checked before any work
    for d in "${DBS[@]}"; do if [ "$CMD" = dump ]; then dump_one "$d"; else drop_one "$d"; fi; done;;
  *) usage;;
esac
