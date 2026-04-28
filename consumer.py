import logging
from confluent_kafka import Consumer, KafkaError, KafkaException
from config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    def __init__(self):
        self._consumer = Consumer(KafkaConfig.to_dict())
        self._topic = KafkaConfig.TOPIC
        self._running = False

    def start(self):
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("Subscribed to Kafka topic: %s", self._topic)

    def poll(self, timeout: float = 1.0):
        """Poll for a single message. Returns the message string or None."""
        msg = self._consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.debug("Reached end of partition")
            else:
                raise KafkaException(msg.error())
            return None

        raw = msg.value().decode("utf-8")
        logger.info("Message received from topic=%s partition=%s offset=%s",
                    msg.topic(), msg.partition(), msg.offset())
        return raw

    def commit(self):
        self._consumer.commit()

    def stop(self):
        self._running = False
        self._consumer.close()
        logger.info("Kafka consumer closed")
