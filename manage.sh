#!/usr/bin/env bash

set -euo pipefail

APP_NAME="message-parser"
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$APP_DIR/venv"
PID_FILE="$APP_DIR/run/$APP_NAME.pid"
LOG_FILE="$APP_DIR/logs/$APP_NAME.log"
KRENEW_PID_FILE="$APP_DIR/run/$APP_NAME-krenew.pid"
PYTHON="$VENV_DIR/bin/python"

# Load .env
ENV_FILE="$APP_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
    echo "[$APP_NAME] ERROR: .env file not found at $ENV_FILE"
    exit 1
fi
set -o allexport
source "$ENV_FILE"
set +o allexport

APP_ENV="${APP_ENV:-dev}"
KERBEROS_PRINCIPAL="${KAFKA_SASL_KERBEROS_PRINCIPAL:-}"
KAFKA_RESOURCES="$APP_DIR/resources/kafka/$APP_ENV"

# All three Kerberos/SSL files live under resources/kafka/<env>/
KEYTAB_FILE="$KAFKA_RESOURCES/DKFKpocepa.keytab"
KRB5_CONF="$KAFKA_RESOURCES/krb5.conf"
SSL_PEM="$KAFKA_RESOURCES/common.pem"

# Export KRB5_CONFIG so the OS Kerberos library and confluent-kafka both pick it up
export KRB5_CONFIG="$KRB5_CONF"

mkdir -p "$APP_DIR/run" "$APP_DIR/logs"

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

validate() {
    local errors=0

    if [[ ! -f "$KEYTAB_FILE" ]]; then
        echo "[$APP_NAME] ERROR: Keytab not found: $KEYTAB_FILE"
        errors=$((errors + 1))
    fi

    if [[ ! -f "$KRB5_CONF" ]]; then
        echo "[$APP_NAME] ERROR: krb5.conf not found: $KRB5_CONF"
        errors=$((errors + 1))
    fi

    if [[ ! -f "$SSL_PEM" ]]; then
        echo "[$APP_NAME] ERROR: common.pem not found: $SSL_PEM"
        errors=$((errors + 1))
    fi

    if [[ -z "$KERBEROS_PRINCIPAL" ]]; then
        echo "[$APP_NAME] ERROR: KAFKA_SASL_KERBEROS_PRINCIPAL is not set in .env"
        errors=$((errors + 1))
    fi

    if [[ ! -x "$PYTHON" ]]; then
        echo "[$APP_NAME] ERROR: Virtual env not found. Run:"
        echo "             python3 -m venv venv && venv/bin/pip install -r requirements.txt"
        errors=$((errors + 1))
    fi

    if [[ $errors -gt 0 ]]; then
        exit 1
    fi
}

# ---------------------------------------------------------------------------
# Kerberos
# ---------------------------------------------------------------------------

kinit_keytab() {
    echo "[$APP_NAME] Obtaining Kerberos ticket for $KERBEROS_PRINCIPAL..."
    kinit -kt "$KEYTAB_FILE" "$KERBEROS_PRINCIPAL"
    echo "[$APP_NAME] Kerberos ticket obtained:"
    klist
}

start_renewal() {
    if command -v k5start &>/dev/null; then
        # k5start: authenticates from keytab and keeps ticket alive (-K = renew interval in minutes)
        nohup k5start -f "$KEYTAB_FILE" -U -K 60 -q >> "$LOG_FILE" 2>&1 &
        echo $! > "$KRENEW_PID_FILE"
        echo "[$APP_NAME] Ticket renewal started via k5start (PID $!, every 60 min)"
    elif command -v krenew &>/dev/null; then
        # krenew: renews an existing ticket without needing the keytab again
        nohup krenew -K 60 -k >> "$LOG_FILE" 2>&1 &
        echo $! > "$KRENEW_PID_FILE"
        echo "[$APP_NAME] Ticket renewal started via krenew (PID $!, every 60 min)"
    else
        echo "[$APP_NAME] WARNING: k5start and krenew not found — ticket will expire without renewal."
        echo "[$APP_NAME]          Install k5start: yum install kstart  OR  apt install kstart"
    fi
}

stop_renewal() {
    if [[ -f "$KRENEW_PID_FILE" ]]; then
        local pid
        pid=$(cat "$KRENEW_PID_FILE")
        kill -TERM "$pid" 2>/dev/null || true
        rm -f "$KRENEW_PID_FILE"
        echo "[$APP_NAME] Ticket renewal daemon stopped"
    fi
}

# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

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
    validate

    if is_running; then
        echo "[$APP_NAME] Already running (PID $(cat "$PID_FILE"))"
        exit 1
    fi

    echo "[$APP_NAME] Environment : $APP_ENV"
    echo "[$APP_NAME] Keytab      : $KEYTAB_FILE"
    echo "[$APP_NAME] krb5.conf   : $KRB5_CONF"
    echo "[$APP_NAME] SSL PEM     : $SSL_PEM"
    echo "[$APP_NAME] Principal   : $KERBEROS_PRINCIPAL"

    kinit_keytab
    start_renewal

    echo "[$APP_NAME] Starting..."
    cd "$APP_DIR"
    nohup "$PYTHON" main.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "[$APP_NAME] Started (PID $!). Logs: $LOG_FILE"
}

stop() {
    stop_renewal

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
            echo "[$APP_NAME] Force killing..."
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

    echo ""
    echo "Kerberos ticket:"
    klist 2>/dev/null || echo "  No active ticket"
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
