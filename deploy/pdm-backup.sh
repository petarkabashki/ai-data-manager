#!/usr/bin/env bash
# Pull-based, hard-linked, dated snapshots of the PDM host's price-data.
# Config (environment, else the repo .env): PDM_SERVER, PDM_BACKUP_DIR
# (default ~/backups/pdm-price-data), PDM_BACKUP_KEEP (unset = never prune).
# PDM_BACKUP_SOURCE (environment or .env) overrides the rsync source: on the
# hub it is `pdm-backup:`, an ssh alias for the host's read-only pdm-ro account,
# whose forced `rrsync -ro` starts in price-data. A local dir works for testing.
# Without it the source is the older `$PDM_SERVER:work/price-data/`.
#
#   pdm-backup.sh           today's snapshot, <YYYY-MM-DD>; a no-op if it exists
#   pdm-backup.sh --again   one more snapshot today, <YYYY-MM-DD>T<HHMM>, for a
#                           restore point before a risky change. Names sort by
#                           time, so `latest`, --link-dest and pruning just work.
set -euo pipefail
case "${1:-}" in
  "") stamp=%F ;;
  --again) stamp=%FT%H%M ;;
  *) echo "usage: $0 [--again]" >&2; exit 2 ;;
esac

repo="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")/.." && pwd)"
# one layer of surrounding quotes is stripped, as python-dotenv does
envval() { grep -oP "^$1=\K.*" "$repo/.env" 2>/dev/null | tail -1 | sed -E "s/^\"(.*)\"\$/\\1/; s/^'(.*)'\$/\\1/" || true; }

server="${PDM_SERVER:-$(envval PDM_SERVER)}"
dest="${PDM_BACKUP_DIR:-$(envval PDM_BACKUP_DIR)}"
dest="${dest:-$HOME/backups/pdm-price-data}"
dest="${dest/#\~/$HOME}"
keep="${PDM_BACKUP_KEEP:-$(envval PDM_BACKUP_KEEP)}"
src="${PDM_BACKUP_SOURCE:-$(envval PDM_BACKUP_SOURCE)}"
if [ -z "$src" ]; then
  [ -n "$server" ] || { echo "PDM_SERVER not set" >&2; exit 2; }
  src="$server:work/price-data/"
fi
src="${src%/}/"

mkdir -p "$dest"
exec 9>"$dest/.lock"
flock -n 9 || { echo "another backup is running" >&2; exit 0; }

date_s="$(date +"$stamp")"
final="$dest/$date_s"
part="$dest/$date_s.partial"
if [ -d "$final" ]; then
  echo "snapshot $date_s already exists; nothing to do"
  exit 0
fi

# complete snapshots, oldest first: YYYY-MM-DD, or YYYY-MM-DDTHHMM from --again
snapshots() {
  find "$dest" -maxdepth 1 -mindepth 1 -type d \
    \( -name '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' \
       -o -name '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]T[0-9][0-9][0-9][0-9]' \) \
    -printf '%f\n' | sort
}
prev="$(snapshots | tail -1)"
linkopt=()
[ -n "$prev" ] && linkopt=(--link-dest="$dest/$prev")

# An interrupted run leaves <name>.partial. Adopt the newest other one as
# this run's, so rsync resumes into it instead of starting over.
if [ ! -e "$part" ]; then
  old="$(find "$dest" -maxdepth 1 -mindepth 1 -type d -name '*.partial' -printf '%f\n' | sort | tail -1)"
  if [ -n "$old" ]; then
    echo "resuming from $old"
    mv "$dest/$old" "$part"
  fi
fi
for stale in "$dest"/*.partial; do
  [ -e "$stale" ] && [ "$stale" != "$part" ] && echo "warning: stale $stale left in place" >&2
done
mkdir -p "$part"

info=stats1
[ -t 1 ] && info=stats1,progress2
ssh_cmd="ssh -o BatchMode=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=6"
pull() {
  local rc=0
  rsync -a --delete --exclude='*.tmp' --partial "$@" -e "$ssh_cmd" \
    "${linkopt[@]}" "$src" "$part/" || rc=$?
  # 24 = source files vanished mid-transfer (live host); acceptable
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 24 ]; then
    echo "rsync failed (rc=$rc); leaving $part for resume" >&2
    exit "$rc"
  fi
}
# Data first, the catalogs (top-level *.yaml) last. A catalog copied after
# its files can only be newer than them: dates that check_catalog --fix
# refreshes, never files that no catalog lists.
pull --info="$info" --exclude='/*.yaml'
pull --info=stats0 --include='/*.yaml' --exclude='*'

# A source that came back empty or truncated (path moved, host wiped) still
# rsyncs cleanly. Refuse to finalize a snapshot with under 80% of the
# previous one's files, so it can never become `latest` or rotate good
# snapshots out.
if [ -n "$prev" ]; then
  n_new="$(find "$part" -type f | wc -l)"
  n_prev="$(find "$dest/$prev" -type f | wc -l)"
  if [ "$((n_new * 5))" -lt "$((n_prev * 4))" ]; then
    echo "snapshot has $n_new files vs $n_prev in $prev; not finalizing (kept as $part)" >&2
    exit 3
  fi
fi

mv "$part" "$final"
ln -sfn "$date_s" "$dest/latest"
echo "snapshot $date_s complete"

if [ -n "$keep" ]; then
  case "$keep" in *[!0-9]*|0) echo "bad PDM_BACKUP_KEEP=$keep; not pruning" >&2; exit 0;; esac
  snapshots | head -n "-$keep" | while read -r old; do
      echo "pruning $old"; rm -rf -- "${dest:?}/$old"
    done
fi
