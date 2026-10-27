#!/bin/bash
# Move PDM to its own account, step 1 of 2: create the accounts and make a first
# copy while PDM keeps running as its current user. Nothing of that user's is
# changed or stopped. Safe to run twice; refuses to copy once the switch is done.
#
#   pdm      owns PDM: code, data, uv, freqtrade, the user unit. No sudo, lingering.
#   pdm-ro   reads price-data only, through forced commands (sftp-server -R for
#            the hub's sshfs mount, rrsync -ro for its backup). It is in group
#            pdm-data, which owns nothing but price-data, so it cannot read the
#            code, .env or the token.
#
# Run as root on the data host, from the user that runs PDM today, in the
# directory holding the hub's three public keys (id_pdm_{mount,backup,admin}.pub):
#   sudo env HUB_ADDR=<hub tailnet address> bash ~/work/ai-data-manager/deploy/pdm-move-prepare.sh
# HUB_ADDR may list several addresses, comma-separated (from= syntax).
# Step 2 is pdm-move-switch.sh.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo"; exit 1; }
: "${HUB_ADDR:?set HUB_ADDR to the hub address(es) for from=}"
src_user="${SRC_USER:-${SUDO_USER:-}}"
[ -n "$src_user" ] && [ "$src_user" != root ] || { echo "set SRC_USER to the account that runs PDM today"; exit 1; }
src=$(getent passwd "$src_user" | cut -d: -f6)
[ -d "$src/work/ai-data-manager" ] && [ -d "$src/work/price-data" ] || { echo "no PDM under $src/work"; exit 1; }
here=$(dirname "$(readlink -f "$0")")
keys="${KEYS_DIR:-$PWD}"
for f in id_pdm_mount.pub id_pdm_backup.pub id_pdm_admin.pub; do
    [ -s "$keys/$f" ] || { echo "missing $keys/$f (set KEYS_DIR)"; exit 1; }
done
dst=/home/pdm

# accounts; pdm-ro's shell must be real: sshd runs forced commands through it
getent group pdm-data >/dev/null || groupadd pdm-data
id pdm >/dev/null 2>&1 || useradd -m -s /bin/bash pdm
id pdm-ro >/dev/null 2>&1 || useradd -m -s /bin/sh pdm-ro
usermod -aG pdm-data pdm
usermod -G pdm-data pdm-ro          # exactly pdm-data: never pdm's own group
passwd -l pdm >/dev/null
passwd -l pdm-ro >/dev/null
chown pdm:pdm-data $dst && chmod 710 $dst      # pdm-data may pass through, not list
chmod 750 /home/pdm-ro
loginctl enable-linger pdm

# the hub's keys, in files only root can change
install -d -o root -g root -m 755 $dst/.ssh /home/pdm-ro/.ssh
printf 'from="%s",no-agent-forwarding,no-port-forwarding,no-X11-forwarding,no-user-rc %s\n' \
    "$HUB_ADDR" "$(cat "$keys/id_pdm_admin.pub")" > $dst/.ssh/authorized_keys
{
    printf 'from="%s",restrict,command="/usr/lib/openssh/sftp-server -R -d %s/work/price-data" %s\n' \
        "$HUB_ADDR" "$dst" "$(cat "$keys/id_pdm_mount.pub")"
    printf 'from="%s",restrict,command="/usr/bin/rrsync -ro %s/work/price-data/" %s\n' \
        "$HUB_ADDR" "$dst" "$(cat "$keys/id_pdm_backup.pub")"
} > /home/pdm-ro/.ssh/authorized_keys
chown root:root $dst/.ssh/authorized_keys /home/pdm-ro/.ssh/authorized_keys
chmod 644 $dst/.ssh/authorized_keys /home/pdm-ro/.ssh/authorized_keys

# the first copy. Code: pdm only (o-rwx; .env is 600 already). Data: group
# pdm-data, setgid dirs, nothing for others. `price-data` in the checkout is a
# relative symlink (../price-data), so it follows the copy.
install -d -o pdm -g pdm-data -m 710 $dst/work
install -d -o pdm -g pdm -m 700 $dst/.cache $dst/.local $dst/.local/bin $dst/.config $dst/.config/systemd $dst/.config/systemd/user
"$here/pdm-move-copy.sh" "$src" "$dst"
install -o pdm -g pdm -m 755 "$src/.local/bin/uv" "$src/.local/bin/uvx" $dst/.local/bin/
install -o pdm -g pdm -m 644 "$src/.config/systemd/user/pdm.service" $dst/.config/systemd/user/pdm.service
grep -q '^UMask=' $dst/.config/systemd/user/pdm.service ||
    sed -i 's/^\[Service\]$/[Service]\nUMask=0027/' $dst/.config/systemd/user/pdm.service

# pdm's own freqtrade and venv; the unit is not enabled here
as_pdm() { sudo -u pdm -H env PATH=$dst/.local/bin:/usr/local/bin:/usr/bin:/bin "$@"; }
ft=$(sudo -u "$src_user" -H "$src/.local/bin/uv" tool list 2>/dev/null | grep -oP '^freqtrade v\K\S+' || true)
as_pdm bash -c "cd && uv tool list 2>/dev/null | grep -q '^freqtrade v${ft:-}' || uv tool install freqtrade${ft:+==$ft}"
as_pdm bash -c 'cd ~/work/ai-data-manager && uv sync --frozen -q'

echo "--- prepared"
id pdm; id pdm-ro
du -sh $dst/work/price-data $dst/work/ai-data-manager
as_pdm bash -c 'freqtrade --version | tail -1; grep "^FREQTRADE_BIN=" ~/work/ai-data-manager/.env || true'
sudo -u pdm-ro test -r $dst/work/price-data/assets.yaml && echo "pdm-ro reads price-data: yes" || echo "pdm-ro reads price-data: NO"
sudo -u pdm-ro test -r $dst/work/ai-data-manager/pdm.yaml && echo "pdm-ro reads the code: YES (wrong)" || echo "pdm-ro reads the code: no"
