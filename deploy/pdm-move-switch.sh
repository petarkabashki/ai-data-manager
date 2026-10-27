#!/bin/bash
# Move PDM to its own account, step 2 of 2, after pdm-move-prepare.sh: stop the
# current user's pdm.service, copy what changed since the first copy, start
# pdm.service as pdm. The old copy stays as it was; delete it by hand after a
# day of clean runs. Runs once: after the switch it refuses, since the old copy
# is stale (use `back` first to redo it).
#
# Pick a slot clear of the crypto lane (00:15-05:00 UTC), the hub's backup
# (04:30) and the yf lane (Mon-Fri 22:00-23:45), with no backfill running: a
# stop re-queues the running job, which resumes from disk.
#
# Right after the switch, point the hub's mount and backup at pdm-ro (the
# pdm-mount: / pdm-backup: ssh aliases). Until then they read the old copy,
# which no longer updates.
#
#   sudo bash ~/work/ai-data-manager/deploy/pdm-move-switch.sh        the switch
#   sudo bash ~/work/ai-data-manager/deploy/pdm-move-switch.sh back   undo: stop pdm's, start the old one
# `back` does not copy anything back: bars pdm downloaded meanwhile are fetched
# again by the next runs.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }
id pdm >/dev/null 2>&1 || { echo "no pdm account: run pdm-move-prepare.sh first"; exit 1; }
src_user="${SRC_USER:-${SUDO_USER:-}}"
[ -n "$src_user" ] && [ "$src_user" != root ] || { echo "set SRC_USER to the account that ran PDM"; exit 1; }
src=$(getent passwd "$src_user" | cut -d: -f6)
dst=/home/pdm
here=$(dirname "$(readlink -f "$0")")
as_src() { systemctl --user -M "$src_user@" "$@"; }
as_pdm() { systemctl --user -M pdm@ "$@"; }

if [ "${1:-}" = back ]; then
    # once the old copy is deleted there is nothing to go back to: stopping
    # pdm's unit would just leave PDM down
    [ -d "$src/work/price-data" ] && [ -f "$src/.config/systemd/user/pdm.service" ] ||
        { echo "no old copy under $src: nothing to go back to, pdm left running"; exit 1; }
    as_pdm disable --now pdm.service || true
    rm -f $dst/.pdm-switched          # a later switch may copy again
    as_src enable --now pdm.service
    sleep 5
    echo "$src_user's pdm.service: $(as_src is-active pdm.service)"
    exit 0
fi

[ ! -e $dst/.pdm-switched ] || { echo "already switched to pdm (\`back\` first to redo it)"; exit 1; }
as_src disable --now pdm.service
"$here/pdm-move-copy.sh" "$src" "$dst"
# from here on the old tree is stale: mark it before anything else can fail
touch $dst/.pdm-switched && chown root:root $dst/.pdm-switched
sudo -u pdm -H env PATH=$dst/.local/bin:/usr/local/bin:/usr/bin:/bin \
    bash -c 'cd ~/work/ai-data-manager && uv sync --frozen -q'
as_pdm daemon-reload
as_pdm enable --now pdm.service
sleep 5
echo "--- switched"
echo "$src_user's pdm.service: $(as_src is-active pdm.service || true)"
echo "pdm's pdm.service:      $(as_pdm is-active pdm.service)"
journalctl _SYSTEMD_USER_UNIT=pdm.service _UID="$(id -u pdm)" -n 5 --no-pager || true
