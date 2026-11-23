import json
import logging
import os
from confluent_kafka import Producer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_COMMANDS = os.getenv("KAFKA_TOPIC_COMMANDS", "router-commands")


class KafkaCommandProducer:
    def __init__(self):
        self.producer = None
        self._init_producer()

    def _init_producer(self):
        conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'router-backend-producer',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 200,
        }
        try:
            self.producer = Producer(conf)
            logger.info(f"Kafka Producer initialized for broker(s): {KAFKA_BOOTSTRAP_SERVERS}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            self.producer = None

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed to topic {msg.topic()} [{msg.partition()}]: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def produce_command(self, job_data: dict) -> bool:
        if not self.producer:
            self._init_producer()
            if not self.producer:
                raise RuntimeError("Kafka Producer is not available")

        router_id = job_data.get("router_id")
        if not router_id:
            raise ValueError("router_id must be provided as message key")

        payload = json.dumps(job_data).encode("utf-8")
        key = router_id.encode("utf-8")

        # Produce asynchronously and poll once for event queue handling
        self.producer.produce(
            topic=KAFKA_TOPIC_COMMANDS,
            key=key,
            value=payload,
            callback=self.delivery_report
        )
        self.producer.poll(0)
        return True

    def flush(self, timeout=5.0):
        if self.producer:
            self.producer.flush(timeout)


producer_instance = KafkaCommandProducer()
