"""Utility functions"""
import os

from noastacingest.messaging.kafka_producer import KafkaProducer
from noastacingest.messaging.message import Message


def send_kafka_message(topic, succeeded, failed):
    schema_def = Message.schema_response()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, schema=schema_def)
    kafka_message = {"succeeded": succeeded, "failed": failed}
    producer.send(topic=topic, value=kafka_message)
