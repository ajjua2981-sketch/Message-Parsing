import logging
import signal
import sys
from consumer import KafkaMessageConsumer
from db import OracleHandler
from processor import process_message

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    kafka = KafkaMessageConsumer()
    db = OracleHandler()

    def shutdown(sig, frame):
        logger.info("Shutdown signal received")
        kafka.stop()
        db.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    db.connect()
    kafka.start()

    logger.info("Message parser running — waiting for messages...")

    while kafka._running:
        xml_message = kafka.poll(timeout=1.0)
        if xml_message is None:
            continue

        try:
            process_message(xml_message, db)
            kafka.commit()
        except KeyError as e:
            logger.error("Missing field in message: %s", e)
        except Exception as e:
            logger.exception("Unexpected error processing message: %s", e)


if __name__ == "__main__":
    main()
