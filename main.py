import logging
import os
import subprocess
import sys
from consumer import KafkaMessageConsumer
from db import OracleHandler
from processor import process_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

APP_ENV              = os.getenv("APP_ENV", "dev")
KEYTAB_FILE          = os.getenv("KEYTAB_FILE", f"resources/kafka/{APP_ENV}/your-service-account.keytab")
KRB5_CONF            = os.getenv("KRB5_CONFIG", f"resources/kafka/{APP_ENV}/krb5.conf")
KERBEROS_PRINCIPAL   = os.getenv("KAFKA_SASL_KERBEROS_PRINCIPAL", "")
MAX_MESSAGES_PER_RUN = int(os.getenv("MAX_MESSAGES_PER_RUN", "10000"))
POLL_TIMEOUT         = float(os.getenv("POLL_TIMEOUT_SECONDS", "5.0"))
EMPTY_POLLS_TO_STOP  = int(os.getenv("EMPTY_POLLS_BEFORE_EXIT", "3"))


def kinit():
    """Obtain a Kerberos ticket using the keytab before connecting to Kafka."""
    if not KERBEROS_PRINCIPAL:
        raise RuntimeError("KAFKA_SASL_KERBEROS_PRINCIPAL is not set in .env")
    if not os.path.isfile(KEYTAB_FILE):
        raise FileNotFoundError(f"Keytab not found: {KEYTAB_FILE}")
    if not os.path.isfile(KRB5_CONF):
        raise FileNotFoundError(f"krb5.conf not found: {KRB5_CONF}")

    os.environ["KRB5_CONFIG"] = KRB5_CONF

    result = subprocess.run(
        ["kinit", "-kt", KEYTAB_FILE, KERBEROS_PRINCIPAL],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"kinit failed: {result.stderr.strip()}")

    logger.info("Kerberos ticket obtained for %s", KERBEROS_PRINCIPAL)


def main():
    kinit()

    kafka = KafkaMessageConsumer()
    db = OracleHandler()

    processed = 0
    failed = 0
    empty_polls = 0

    try:
        db.connect()
        kafka.start()
        logger.info("Batch started — max messages per run: %d", MAX_MESSAGES_PER_RUN)

        while processed + failed < MAX_MESSAGES_PER_RUN:
            xml_message = kafka.poll(timeout=POLL_TIMEOUT)

            if xml_message is None:
                empty_polls += 1
                if empty_polls >= EMPTY_POLLS_TO_STOP:
                    logger.info("No more messages in topic — ending batch")
                    break
                continue

            empty_polls = 0

            try:
                process_message(xml_message, db)
                kafka.commit()
                processed += 1
            except KeyError as e:
                logger.error("Missing field in message: %s", e)
                failed += 1
            except Exception as e:
                logger.exception("Unexpected error processing message: %s", e)
                failed += 1

    finally:
        kafka.stop()
        db.disconnect()
        logger.info("Batch complete — processed: %d  failed: %d", processed, failed)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
