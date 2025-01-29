"""Utility functions"""
import logging
from kafka.errors import NoBrokersAvailable

from noastacingest.messaging.kafka_producer import KafkaProducer
from noastacingest.messaging.message import Message

logger = logging.getLogger(__name__)


def send_kafka_message(bootstrap_servers, topic, succeeded, failed):
    schema_def = Message.schema_response()

    try:
        producer = KafkaProducer(bootstrap_servers=bootstrap_servers, schema=schema_def)
        kafka_message = {"succeeded": succeeded, "failed": failed}
        producer.send(topic=topic, value=kafka_message)
    except NoBrokersAvailable as e:
        logger.warning("No brokers available. Continuing without Kafka. Error: %s", e)
        producer = None
