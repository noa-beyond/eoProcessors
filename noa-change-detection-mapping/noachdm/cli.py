"""Cli for NOA-Beyond Change Detection Mapping (ChDM) processor.

This interface and processor are used to build ChDM products from EO data
"""

from __future__ import annotations
import os
import sys
from datetime import datetime
import json
from time import sleep
import logging
from pathlib import Path

import click
from click import Argument, Option
from kafka import KafkaConsumer as k_KafkaConsumer
from kafka.errors import (
    NoBrokersAvailable,
    TopicAuthorizationFailedError,
    InvalidTopicError,
    UnknownTopicOrPartitionError,
    UnsupportedForMessageFormatError,
    InvalidMessageError
)
# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from noachdm import chdm  # noqa:402 pylint:disable=wrong-import-position
from noachdm import utils  # noqa:402 pylint:disable=wrong-import-position
from noachdm.messaging.message import Message  # noqa:402 pylint:disable=wrong-import-position
from noachdm.messaging import AbstractConsumer  # noqa:402 pylint:disable=wrong-import-position
from noachdm.messaging.kafka_consumer import KafkaConsumer  # noqa:402 pylint:disable=wrong-import-position

logger = logging.getLogger(__name__)


PROCESSOR = "[NOA-ChDM]"


@click.group(
    help=(
        """
        Change Detection Mapping EO Processor.
        Calculates changes from RGB (2,3,4) Sentinel 2 bands
        """
    )
)
@click.option(
    "--log",
    default="warning",
    help="Log level (optional, e.g. DEBUG. Default is WARNING)",
)
def cli(log):
    """Click cli group for cli commands"""
    numeric_level = getattr(logging, log.upper(), "WARNING")
    logging.basicConfig(
        format=f"[%(asctime)s.%(msecs)03d] [%(levelname)s] {PROCESSOR} %(message)s",
        level=numeric_level,
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


@cli.command(
    help=(
        """
        Produces Change Detection Mapping product. Needs a
        'from_path' argument, pointing to the directory where
        the rgb Sentinel 2 band rasters before the change reside,
        and a 'to_path' for the respective files after the
        change has occurred.
        """
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the verbose",
)
@click.argument("from_path", required=True)
@click.argument("to_path", required=True)
@click.option("--output_path", default="./data", help="Output path")
def produce(
    from_path: Argument | str,
    to_path: Argument | str,
    output_path: Option | str,
    verbose: Option | bool,
) -> None:
    """
    Instantiate ChDM class and calls produce function.

    Parameters:
        from_path (click.Argument | str): path of rasters before change
        to_path (click.Argument | str): path of rasters after change
        verbose (click.Option | bool): to show verbose
    """

    click.echo("Producing...\n")
    click.echo(output_path)
    chdm_producer = chdm.ChDM(
        output_path=output_path,
        verbose=verbose
    )
    chdm_producer.produce(from_path, to_path)


@cli.command(
    help=(
        """
        Microservice - Product Generation as a Service.
        Implemented by using a kafka producer/consumer pattern
        """
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the verbose",
)
@click.option(
    "--test",
    "-t",
    is_flag=True,
    help="Testing kafka receiving requests. No other functionality",
)
@click.argument("config_file", required=True)
@click.option("--output_path", default="./data", help="Output path")
def noa_pgaas_chdm(
    config_file: str,
    output_path: str,
    test: bool,
    verbose: bool
) -> None:
    """
    Instantiate ChDM class and activate service, listening to kafka topic.
    When triggered, generates ChDM product from all paths, based on the
    list argument from kafka message.

    Parameters:
        output_path (click.Option | str): where to produce to
        verbose (click.Option | bool): verbose
    """
    # if config_file:
    logger.debug("Starting NOA-ChDM service...")
    logger.info("Testing: %s", test)

    chdm_producer = chdm.ChDM(
        config_file=config_file,
        output_path=output_path,
        verbose=verbose,
        is_service=True
    )

    # Consumer
    consumer: AbstractConsumer | k_KafkaConsumer = None
    # Warning: topics is a list, even if there is only one topic
    # So it should be set as a list in the config file
    consumer_topics = chdm_producer.config.get(
        "topics_consumer", os.environ.get(
            "KAFKA_INPUT_TOPICS", ["noa.chdm.request"]
        )
    )
    schema_def = Message.schema_request()
    consumer_bootstrap_servers = chdm_producer.config.get(
        "kafka_bootstrap_servers",
        (os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    )
    kafka_group_id = chdm_producer.config.get(
        "kafka_request_group_id",
        (os.getenv("KAFKA_REQUEST_GROUP_ID", "chdm-group-request"))
    )

    # Producer
    producer_bootstrap_servers = chdm_producer.config.get(
        "kafka_bootstrap_servers",
        os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
    )

    producer_topic = chdm_producer.config.get(
        "topic_producer", os.environ.get(
            "KAFKA_OUTPUT_TOPIC", "noa.chdm.response")
    )

    retries = 0
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=consumer_bootstrap_servers,
                group_id=kafka_group_id,
                topics=consumer_topics,
                schema=schema_def
            )
            consumer.subscribe_to_topics(consumer_topics)
        except NoBrokersAvailable as e:
            logger.error(
                "Kafka configuration error, no brokers available for (%s) : %s ",
                consumer_bootstrap_servers,
                e
            )
            raise
        except (
            UnknownTopicOrPartitionError,
            TopicAuthorizationFailedError,
            InvalidTopicError
        ) as e:
            if retries < 5:
                logger.warning("Could not subscribe to Topic(s): %s", consumer_topics)
                if consumer is None:
                    sleep(5)
                    retries += 1
                    continue
            else:
                logger.error(
                    "Kafka Error on Topic subscription after %i retries: %s",
                    retries,
                    e
                )

    logger.info("Service started, subscribed to topics...")
    click.echo(f"Service started. Output path: {output_path}\n")

    while True:
        try:
            for message in consumer.read():
                item = message.value
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"Digesting Item from Topic {message.topic} ({now_time})..."
                msg += "\n> Item: " + json.dumps(item)
                logger.info(msg)
                click.echo("Received lists use:")
                click.echo(item)
                order_id = item["orderId"]
                items_from = item["initialSelectionProductPaths"]
                items_to = item["finalSelectionProductPaths"]
                bbox = item["bbox"]
                new_product_path = chdm_producer.produce_from_items_lists(
                    items_from, items_to, bbox
                )
                logger.info(
                    "Order ID: %s. New change detection mapping product at: %s",
                    order_id,
                    new_product_path
                )
                click.echo(
                    f"Consumed ChDM message for orderId {order_id}"
                )

                try:
                    utils.send_kafka_message(
                        producer_bootstrap_servers,
                        producer_topic,
                        order_id,
                        new_product_path
                    )
                    print(f"Kafka message of New ChDM Product sent to: {producer_topic}")
                except BrokenPipeError as e:
                    print(f"Error sending kafka message to: {producer_topic}")
                    logger.warning("Error sending kafka message: %s ", e)
            sleep(1)
        except (UnsupportedForMessageFormatError, InvalidMessageError) as e:
            click.echo(f"Error in reading kafka message: {item}")
            logger.warning("Error in reading kafka message: %s", e)
            continue


if __name__ == "__main__":  # pragma: no cover
    cli()
