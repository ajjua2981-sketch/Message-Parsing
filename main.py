import logging
import os
import sys
from consumer import KafkaMessageConsumer
from db import OracleHandler
from processor import process_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Stop polling after this many consecutive empty responses
EMPTY_POLLS_BEFORE_EXIT = int(os.getenv("EMPTY_POLLS_BEFORE_EXIT", "3"))

# Max messages to process per run (safety cap)
MAX_MESSAGES_PER_RUN = int(os.getenv("MAX_MESSAGES_PER_RUN", "10000"))

# How long to wait for each message (seconds)
POLL_TIMEOUT = float(os.getenv("POLL_TIMEOUT_SECONDS", "5.0"))


def main():
    kafka = KafkaMessageConsumer()
    db = OracleHandler()

    processed = 0
    failed = 0
    empty_polls = 0

    try:
        db.connect()
        kafka.start()
        logger.info("Batch run started — max messages: %d", MAX_MESSAGES_PER_RUN)

        while processed + failed < MAX_MESSAGES_PER_RUN:
            xml_message = kafka.poll(timeout=POLL_TIMEOUT)

            if xml_message is None:
                empty_polls += 1
                if empty_polls >= EMPTY_POLLS_BEFORE_EXIT:
                    logger.info("No more messages — ending batch run")
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
        logger.info("Batch run complete — processed: %d  failed: %d", processed, failed)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
