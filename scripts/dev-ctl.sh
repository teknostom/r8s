#!/usr/bin/env bash
# Privileged cluster control for local development, so unprivileged tooling
# (an agent, a test script) can restart the daemon without a sudo prompt.
#
# Listener, run once in a spare terminal and leave it (Ctrl-C to stop):
#   sudo scripts/dev-ctl.sh
# Client, from anywhere, as your own user:
#   scripts/dev-ctl.sh restart      # down + up with target/release, waits for /readyz
#   scripts/dev-ctl.sh up|down|status|ping
#
# Protocol: the client writes "<verb> <id>" to /tmp/r8s-ctl/cmd (a FIFO owned
# by the invoking user, mode 600) and waits for "<id> <verb> ok|fail <secs>s"
# to appear in /tmp/r8s-ctl/log. The listener matches verbs literally; the
# FIFO's contents are never passed to a shell.
set -uo pipefail

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
R8S="$REPO/target/release/r8s"
CLUSTER="${R8S_CLUSTER:-default}"
DIR=/tmp/r8s-ctl

# ---- client -----------------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    verb="${1:-ping}"
    id="$$-$(date +%s)"
    [[ -p $DIR/cmd ]] || { echo "listener not running: sudo $0" >&2; exit 2; }
    echo "$verb $id" > "$DIR/cmd"
    for _ in $(seq 1 360); do
        if line=$(grep -m1 "^$id " "$DIR/log" 2>/dev/null); then
            echo "$line"
            [[ $line == *" ok "* ]]
            exit
        fi
        sleep 0.5
    done
    echo "timed out waiting for '$verb'" >&2
    exit 1
fi

# ---- listener ---------------------------------------------------------------
[[ -x $R8S ]] || { echo "no release binary at $R8S (cargo build --release)" >&2; exit 1; }
OWNER="${SUDO_USER:-root}"
GROUP=$(id -gn "$OWNER")
install -d -m 700 -o "$OWNER" -g "$GROUP" "$DIR"
[[ -p $DIR/cmd ]] || mkfifo -m 600 "$DIR/cmd"
chown "$OWNER:$GROUP" "$DIR/cmd"
touch "$DIR/log"
chown "$OWNER:$GROUP" "$DIR/log"
chmod 600 "$DIR/log"
echo "listening on $DIR/cmd  cluster=$CLUSTER  r8s=$R8S"

run() {
    "$R8S" "$@" 2>&1 | sed 's/^/    /'
    return "${PIPESTATUS[0]}"
}

wait_ready() {
    for _ in $(seq 1 60); do
        curl -sk https://127.0.0.1:6443/readyz >/dev/null 2>&1 && return 0
        sleep 0.5
    done
    echo "    daemon did not become ready within 30s"
    return 1
}

while read -r verb id < "$DIR/cmd"; do
    id="${id:-0}"
    start=$(date +%s)
    echo "[$(date +%T)] $verb"
    case "$verb" in
        ping)    status=ok ;;
        up)      run up "$CLUSTER" && wait_ready && status=ok || status=fail ;;
        down)    run down "$CLUSTER" && status=ok || status=fail ;;
        restart)
            run down "$CLUSTER"
            # Belt and braces for the store lock: no r8sd may be alive before up.
            for _ in $(seq 1 50); do pgrep -x r8sd >/dev/null || break; sleep 0.1; done
            # `up` truncates the daemon log; keep the shutdown's tail for debugging.
            cp "/var/lib/r8s/clusters/$CLUSTER/r8sd.log" "$DIR/last-shutdown.log" 2>/dev/null
            chown "$OWNER:$GROUP" "$DIR/last-shutdown.log" 2>/dev/null
            run up "$CLUSTER" && wait_ready && status=ok || status=fail ;;
        status)  run status "$CLUSTER" && status=ok || status=fail ;;
        *)       echo "    ignoring unknown verb"; status=unknown ;;
    esac
    echo "$id $verb $status $(( $(date +%s) - start ))s" | tee -a "$DIR/log"
done
