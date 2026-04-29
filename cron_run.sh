#!/usr/bin/env bash

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$APP_DIR/.env"
PYTHON="$APP_DIR/venv/bin/python"
LOG_FILE="$APP_DIR/logs/message-parser.log"
LOCK_FILE="$APP_DIR/run/message-parser.lock"

mkdir -p "$APP_DIR/logs" "$APP_DIR/run"

# Load .env
if [[ ! -f "$ENV_FILE" ]]; then
    echo "[cron_run] ERROR: .env file not found" >> "$LOG_FILE"
    exit 1
fi
set -o allexport
source "$ENV_FILE"
set +o allexport

APP_ENV="${APP_ENV:-dev}"
KAFKA_RESOURCES="$APP_DIR/resources/kafka/$APP_ENV"
KEYTAB_FILE="$KAFKA_RESOURCES/your-service-account.keytab"
KRB5_CONF="$KAFKA_RESOURCES/krb5.conf"
KERBEROS_PRINCIPAL="${KAFKA_SASL_KERBEROS_PRINCIPAL:-}"
export KRB5_CONFIG="$KRB5_CONF"

# Prevent overlapping runs — skip if previous run is still active
if [[ -f "$LOCK_FILE" ]]; then
    echo "[cron_run] Previous run still active — skipping" >> "$LOG_FILE"
    exit 0
fi

touch "$LOCK_FILE"

cleanup() {
    rm -f "$LOCK_FILE"
}
trap cleanup EXIT

{
    echo "------------------------------------------------------------"
    echo "[cron_run] $(date '+%Y-%m-%d %H:%M:%S') Starting batch run (ENV=$APP_ENV)"

    # Refresh Kerberos ticket before each run
    kinit -kt "$KEYTAB_FILE" "$KERBEROS_PRINCIPAL"
    echo "[cron_run] Kerberos ticket obtained for $KERBEROS_PRINCIPAL"

    # Run the batch
    "$PYTHON" "$APP_DIR/main.py"

    echo "[cron_run] $(date '+%Y-%m-%d %H:%M:%S') Batch run finished"

} >> "$LOG_FILE" 2>&1
