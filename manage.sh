#!/usr/bin/env bash

set -euo pipefail

APP_NAME="message-parser"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$APP_DIR/venv"
PID_FILE="$APP_DIR/run/$APP_NAME.pid"
LOG_FILE="$APP_DIR/logs/$APP_NAME.log"
PYTHON="$VENV_DIR/bin/python"

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

start() {
    if is_running; then
        echo "[$APP_NAME] Already running (PID $(cat "$PID_FILE"))"
        exit 1
    fi

    if [[ ! -x "$PYTHON" ]]; then
        echo "[$APP_NAME] Virtual env not found. Run: python3 -m venv venv && venv/bin/pip install -r requirements.txt"
        exit 1
    fi

    echo "[$APP_NAME] Starting..."
    cd "$APP_DIR"
    nohup "$PYTHON" main.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "[$APP_NAME] Started (PID $!). Logs: $LOG_FILE"
}

stop() {
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
