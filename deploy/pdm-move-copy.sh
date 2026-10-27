#!/bin/bash
# Copy PDM's code and data from one home to /home/pdm with pdm's ownership and
# modes. Used by pdm-move-prepare.sh (first copy) and pdm-move-switch.sh (the
# delta while PDM is stopped). Usage: pdm-move-copy.sh <src home> <dst home>
set -euo pipefail
src=$1 dst=$2
# Never copy once pdm runs PDM: the old tree is stale by then, and --delete
# would wipe every bar pdm downloaded since. pdm-move-switch.sh leaves the
# marker; its `back` removes it.
if [ -e "$dst/.pdm-switched" ] || systemctl --user -M pdm@ is-enabled pdm.service >/dev/null 2>&1 \
        || systemctl --user -M pdm@ is-active pdm.service >/dev/null 2>&1; then
    echo "already switched to pdm: refusing to copy over pdm's data" >&2
    exit 1
fi
# A running PDM writes `<file>.tmp` and renames it, so a file can vanish
# mid-copy: rsync's exit 24, harmless here. The switch's delta, made with PDM
# stopped, picks up whatever the first copy missed. `hide` skips .tmp files
# on the sender only, so --delete still clears any an earlier pass copied.
copy() {
    rsync -aH --delete --filter='hide *.tmp' "$@" || {
        rc=$?
        [ "$rc" = 24 ] || return "$rc"
        echo "rsync: some files vanished during the copy (fine)"
    }
}
# .venv is pdm's own (uv sync); excluded paths survive --delete on the receiver
copy --chown=pdm:pdm --chmod=o-rwx --exclude=.venv/ --exclude=__pycache__/ \
    "$src/work/ai-data-manager/" "$dst/work/ai-data-manager/"
copy --chown=pdm:pdm-data --chmod=D2750,F640 \
    "$src/work/price-data/" "$dst/work/price-data/"
chown pdm:pdm-data "$dst/work/price-data" && chmod 2750 "$dst/work/price-data"
sed -i "s|^FREQTRADE_BIN=$src/|FREQTRADE_BIN=$dst/|" "$dst/work/ai-data-manager/.env"
