#!/bin/bash
# Install the daily-refresh launchd job into ~/Library/LaunchAgents.
#
# Usage:
#   bash amaltash_sentiment/jobs/install_launchd.sh        # install + load
#   bash amaltash_sentiment/jobs/install_launchd.sh status # show next run
#   bash amaltash_sentiment/jobs/install_launchd.sh stop   # unload
#   bash amaltash_sentiment/jobs/install_launchd.sh run    # run once now
#   bash amaltash_sentiment/jobs/install_launchd.sh tail   # tail the log
#
# After install, the job runs daily at 06:30 local time.

set -euo pipefail

LABEL="com.amaltash.refresh"
SRC_PLIST="$(cd "$(dirname "$0")" && pwd)/com.amaltash.refresh.plist"
DEST_PLIST="$HOME/Library/LaunchAgents/${LABEL}.plist"
LOG="/Users/saieshagupta/amaltash_strats/amaltash_sentiment/jobs/refresh.log"
ERR="/Users/saieshagupta/amaltash_strats/amaltash_sentiment/jobs/refresh.err"

cmd="${1:-install}"

case "$cmd" in
    install)
        mkdir -p "$HOME/Library/LaunchAgents"
        cp "$SRC_PLIST" "$DEST_PLIST"
        # Unload first if already loaded (silent if not)
        launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null || true
        launchctl bootstrap "gui/$(id -u)" "$DEST_PLIST"
        echo "Installed. Next run: 06:30 daily local time."
        echo "Logs: $LOG (stdout), $ERR (stderr)"
        ;;
    status)
        launchctl print "gui/$(id -u)/${LABEL}" 2>/dev/null | \
            grep -E "state|last exit|next run|program" || \
            echo "Not loaded. Run: bash $0 install"
        ;;
    stop|unload)
        launchctl bootout "gui/$(id -u)/${LABEL}" 2>/dev/null && \
            echo "Unloaded." || echo "Was not loaded."
        rm -f "$DEST_PLIST"
        ;;
    run|trigger)
        launchctl kickstart -k "gui/$(id -u)/${LABEL}" && \
            echo "Triggered. Tail with: bash $0 tail"
        ;;
    tail)
        echo "==== STDOUT ($LOG) ===="
        tail -n 50 "$LOG" 2>/dev/null || echo "(no log yet)"
        echo
        echo "==== STDERR ($ERR) ===="
        tail -n 20 "$ERR" 2>/dev/null || echo "(no errors)"
        ;;
    *)
        echo "Usage: $0 {install|status|stop|run|tail}"
        exit 2
        ;;
esac
