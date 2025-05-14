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
    TopicAlreadyExistsError,
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
    click.echo("Done.\n")


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
    config_file: Argument | str,
    output_path: Option | str,
    verbose: Option | bool
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

    chdm_producer = chdm.ChDM(
        config_file=config_file,
        output_path=output_path,
        verbose=verbose,
        is_service=True
    )

    consumer: AbstractConsumer | k_KafkaConsumer = None

    # Warning: topics is a list, even if there is only one topic
    # So it should be set as a list in the config file
    topics = chdm_producer.config.get(
        "topics_consumer", os.environ.get(
            "KAFKA_INPUT_TOPICS", ["chdm.order.requested"]
        )
    )
    schema_def = Message.schema_request()
    num_partitions = int(chdm_producer.config.get(
        "num_partitions", os.environ.get(
            "KAFKA_NUM_PARTITIONS", 2
        )
    ))
    replication_factor = int(chdm_producer.config.get(
        "replication_factor", os.environ.get(
            "KAFKA_REPLICATION_FACTOR", 3
        )
    ))

    while consumer is None:
        consumer = KafkaConsumer(
            bootstrap_servers=chdm_producer.config.get(
                "kafka_bootstrap_servers",
                (os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
            ),
            group_id=chdm_producer.config.get(
                "kafka_request_group_id",
                (os.getenv("KAFKA_REQUEST_GROUP_ID", "chdm-group-request"))
            ),
            topics=topics,
            schema=schema_def
        )
        try:
            consumer.subscribe_to_topics(topics)
        except (UnknownTopicOrPartitionError, TopicAuthorizationFailedError, InvalidTopicError) as e:
            logger.warning("Kafka Error on Topic subscription: %s", e)
            logger.warning("Trying to create it:")
            try:
                consumer.create_topics(
                    topics=topics, num_partitions=num_partitions, replication_factor=replication_factor)
            except (TopicAlreadyExistsError,
                    UnknownTopicOrPartitionError,
                    TopicAuthorizationFailedError,
                    InvalidTopicError) as g:
                logger.error("Kafka: Could not subscribe or create producer topic: %s", g)
                return
        if consumer is None:
            sleep(5)

    click.echo(f"Service started. Output path: {output_path}\n")

    while True:
        try:
            for message in consumer.read():
                item = message.value
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"Digesting Item from Topic {message.topic} ({now_time})..."
                msg += "\n> Item: " + json.dumps(item)
                logger.debug(msg)
                click.echo("Received lists use:")
                click.echo(item)
                items_from = item["ids_date_from"]
                items_to = item["ids_date_to"]
                # will get bbox as [minX, minY, maxX, maxY] or minLon-minLat/maxLon-maxLat
                bbox = item["bbox"]
                new_product_path = chdm_producer.produce_from_items_lists(
                    items_from, items_to, bbox
                )
                logger.debug(
                    "New change detection mapping product at: %s",
                    new_product_path
                )
                click.echo(
                    f"Consumed ChDM message and used {items_from} and {items_to} items"
                )

                kafka_topic = chdm_producer.config.get(
                    "topic_producer", os.environ.get(
                        "KAFKA_OUTPUT_TOPIC", "chdm.order.completed")
                )
                logger.info("New ChDM Product . Sending kafka message")
                try:
                    bootstrap_servers = chdm_producer.config.get(
                        "kafka_bootstrap_servers", os.getenv(
                            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
                        )
                    )
                    utils.send_kafka_message(
                        bootstrap_servers,
                        kafka_topic,
                        new_product_path
                    )
                    print(f"Kafka message of New ChDM Product sent to: {kafka_topic}")
                except BrokenPipeError as e:
                    print(f"Error sending kafka message to: {kafka_topic}")
                    logger.warning("Error sending kafka message: %s ", e)
            sleep(1)
        except (UnsupportedForMessageFormatError, InvalidMessageError) as e:
            click.echo(f"Error in reading kafka message: {item}")
            logger.warning("Error in reading kafka message: %s", e)
            continue


if __name__ == "__main__":  # pragma: no cover
    cli()
