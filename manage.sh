#!/usr/bin/env bash

set -euo pipefail

APP_NAME="message-parser"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$APP_DIR/venv"
PID_FILE="$APP_DIR/run/$APP_NAME.pid"
LOG_FILE="$APP_DIR/logs/$APP_NAME.log"
KRENEW_PID_FILE="$APP_DIR/run/$APP_NAME-krenew.pid"
PYTHON="$VENV_DIR/bin/python"

# Load .env to read keytab config
ENV_FILE="$APP_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -o allexport
    source "$ENV_FILE"
    set +o allexport
fi

KEYTAB_FILE="${KEYTAB_FILE:-}"
KERBEROS_PRINCIPAL="${KAFKA_SASL_KERBEROS_PRINCIPAL:-}"

mkdir -p "$APP_DIR/run" "$APP_DIR/logs"

is_running() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi
    return 1
}

kinit_with_keytab() {
    if [[ -z "$KEYTAB_FILE" || -z "$KERBEROS_PRINCIPAL" ]]; then
        echo "[$APP_NAME] No keytab configured — assuming active kinit session"
        return 0
    fi

    if [[ ! -f "$KEYTAB_FILE" ]]; then
        echo "[$APP_NAME] ERROR: Keytab file not found: $KEYTAB_FILE"
        exit 1
    fi

    echo "[$APP_NAME] Obtaining Kerberos ticket for $KERBEROS_PRINCIPAL..."
    kinit -kt "$KEYTAB_FILE" "$KERBEROS_PRINCIPAL"
    echo "[$APP_NAME] Kerberos ticket obtained"
}

start_krenew() {
    # Keep the Kerberos ticket alive in the background using k5start or krenew
    if [[ -z "$KEYTAB_FILE" || -z "$KERBEROS_PRINCIPAL" ]]; then
        return 0
    fi

    if command -v k5start &>/dev/null; then
        nohup k5start -f "$KEYTAB_FILE" -U -K 60 >> "$LOG_FILE" 2>&1 &
        echo $! > "$KRENEW_PID_FILE"
        echo "[$APP_NAME] k5start renewal daemon started (PID $!)"
    elif command -v krenew &>/dev/null; then
        nohup krenew -K 60 >> "$LOG_FILE" 2>&1 &
        echo $! > "$KRENEW_PID_FILE"
        echo "[$APP_NAME] krenew renewal daemon started (PID $!)"
    else
        echo "[$APP_NAME] WARNING: Neither k5start nor krenew found — ticket will NOT auto-renew."
        echo "[$APP_NAME]          Install k5start (recommended) or set up a cron for kinit."
    fi
}

stop_krenew() {
    if [[ -f "$KRENEW_PID_FILE" ]]; then
        local pid
        pid=$(cat "$KRENEW_PID_FILE")
        kill -TERM "$pid" 2>/dev/null || true
        rm -f "$KRENEW_PID_FILE"
        echo "[$APP_NAME] Kerberos renewal daemon stopped"
    fi
}

start() {
    if is_running; then
        echo "[$APP_NAME] Already running (PID $(cat "$PID_FILE"))"
        exit 1
    fi

    if [[ ! -x "$PYTHON" ]]; then
        echo "[$APP_NAME] Virtual env not found. Run: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
        exit 1
    fi

    kinit_with_keytab
    start_krenew

    echo "[$APP_NAME] Starting..."
    cd "$APP_DIR"
    nohup "$PYTHON" main.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "[$APP_NAME] Started (PID $!). Logs: $LOG_FILE"
}

stop() {
    stop_krenew

    if ! is_running; then
        echo "[$APP_NAME] Not running"
        rm -f "$PID_FILE"
        exit 0
    fi

    local pid
    pid=$(cat "$PID_FILE")
    echo "[$APP_NAME] Stopping (PID $pid)..."
    kill -TERM "$pid"

    local waited=0
    while kill -0 "$pid" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [[ $waited -ge 15 ]]; then
            echo "[$APP_NAME] Force killing (PID $pid)..."
            kill -KILL "$pid"
            break
        fi
    done

    rm -f "$PID_FILE"
    echo "[$APP_NAME] Stopped"
}

status() {
    if is_running; then
        echo "[$APP_NAME] Running (PID $(cat "$PID_FILE"))"
        if command -v klist &>/dev/null; then
            echo ""
            klist 2>/dev/null || echo "No active Kerberos ticket"
        fi
    else
        echo "[$APP_NAME] Not running"
        rm -f "$PID_FILE"
    fi
}

restart() {
    stop || true
    sleep 1
    start
}

logs() {
    tail -f "$LOG_FILE"
}

case "${1:-}" in
    start)   start   ;;
    stop)    stop    ;;
    restart) restart ;;
    status)  status  ;;
    logs)    logs    ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs}"
        exit 1
        ;;
esac
