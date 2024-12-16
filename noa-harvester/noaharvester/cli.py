"""Cli for NOA-Beyond Harvester processor.

This interface and processor are used to query and download EO data
from various data providers: Copernicus and Earthdata.
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

from noaharvester import harvester  # noqa:402 pylint:disable=wrong-import-position
from noaharvester.messaging.message import Message  # noqa:402 pylint:disable=wrong-import-position
from noaharvester.messaging import AbstractConsumer  # noqa:402 pylint:disable=wrong-import-position
from noaharvester.messaging.kafka_consumer import KafkaConsumer  # noqa:402 pylint:disable=wrong-import-position

logger = logging.getLogger(__name__)


@click.group(
    help=(
        "Queries and/or Downloads data from Copernicus and EarthData services "
        "according to parameters as defined in the [CONFIG_FILE]."
        "It can also receive a [SHAPE_FILE] path as an argument, in order "
        "for the bounding box to be defined there instead of the [CONFIG_FILE]."
    )
)
@click.option(
    "--log",
    default="warning",
    help="Log level (optional, e.g. DEBUG. Default is WARNING)",
)
def cli(log):
    """Click cli group for query, download, describe cli commands"""
    numeric_level = getattr(logging, log.upper(), "WARNING")
    logging.basicConfig(level=numeric_level, format="%(asctime)s %(message)s")


@cli.command(help="Describe collection query fields (Copernicus only)")
@click.argument("config_file", required=True)
def describe(config_file: Argument | str) -> None:
    """
    Instantiate Harvester Class and call "describe" for available query terms
    of the selected collections (only available for Copernicus)

    Parameters:
        config_file (click.Argument | str): config json file listing
            providers, collections and search terms
    """
    if config_file:
        logger.debug("Cli describing for config file: %s", config_file)

        harvest = harvester.Harvester(config_file=config_file)
        click.echo("Available parameters for selected collections:\n")
        harvest.describe()


@cli.command(
    help=(
        "Downloads data from the selected providers and query terms. "
        "You can also provide (optional) a [SHAPE_FILE] path in order to define "
        "the bounding box there instead of the config file."
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for Copernicus only)",
)
@click.option(
    "--bbox_only",
    "-bb",
    is_flag=True,
    help="Only use multipolygon total bbox, not individual",
)
@click.argument("config_file", required=True)
@click.argument("shape_file", required=False)
@click.option("--output_path", default="./data", help="Output path")
def download(
    config_file: Argument | str,
    shape_file: Argument | str,
    output_path: Option | str,
    bbox_only: Option | bool,
    verbose: Option | bool,
) -> None:
    """
    Instantiate Harvester class and call download function.
    Downloads all relevant data as defined in the config file.

    Parameters:
        config_file (click.Argument | str): config json file listing
            providers, collections and search terms
        verbose (click.Option | bool): to show download progress indicator or not.
    """
    if config_file:
        logger.debug("Cli download for config file: %s", config_file)

        click.echo("Downloading...\n")
        click.echo(output_path)
        harvest = harvester.Harvester(
            config_file=config_file,
            output_path=output_path,
            shape_file=shape_file,
            verbose=verbose,
            bbox_only=bbox_only,
        )
        harvest.download_data()
        click.echo("Done.\n")


@cli.command(
    help=(
        """
        Microservice facilitating EO data downloads. Implemented by
        using a kafka producer/consumer pattern.
        """
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for Copernicus only)",
)
@click.option(
    "--test",
    "-t",
    is_flag=True,
    help="Testing kafka receiving requests. No other functionality",
)
@click.argument("config_file", required=True)
@click.option("--output_path", default="./data", help="Output path")
def noa_harvester_service(
    config_file: Argument | str,
    output_path: Option | str,
    verbose: Option | bool,
    test: Option | bool
) -> None:
    """
    Instantiate Harvester class activate service, listening to kafka topic.
    When triggered, downloads all ids from Products table, based on the 
    list argument from kafka message.

    Parameters:
        output_path (click.Option | str): where to download to
        verbose (click.Option | bool): to show download progress indicator or not
    """
    # if config_file:
    logger.debug("Starting NOA-Harvester service...")

    harvest = harvester.Harvester(
        config_file=config_file,
        output_path=output_path,
        verbose=verbose,
        is_service=True
    )

    consumer: AbstractConsumer | k_KafkaConsumer = None

    # Warning: topics is a list, even if there is only one topic
    # So it should be set as a list in the config file
    topics = harvest.config.get(
        "topics_consumer", os.environ.get(
            "KAFKA_INPUT_TOPICS", ["harvester.order.requested"]
        )
    )
    schema_def = Message.schema_request()
    num_partitions = int(harvest.config.get(
        "num_partitions", os.environ.get(
            "KAFKA_NUM_PARTITIONS", 2
        )
    ))
    replication_factor = int(harvest.config.get(
        "replication_factor", os.environ.get(
            "KAFKA_REPLICATION_FACTOR", 3
        )
    ))

    while consumer is None:
        consumer = KafkaConsumer(
            bootstrap_servers=harvest.config.get(
                "kafka_bootstrap_servers",
                (
                    os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                              "localhost:9092"
                            )
                )
            ),
            group_id=harvest.config.get(
                "kafka_request_group_id",
                (
                        os.getenv("KAFKA_REQUEST_GROUP_ID",
                                "harvester-group-request"
                                )
                )
            ),
            topics=topics,
            schema=schema_def
        )
        try:
            consumer.subscribe_to_topics(topics)
        except (UnknownTopicOrPartitionError, TopicAuthorizationFailedError, InvalidTopicError) as e:
            logger.warning("[NOA-Harvester] Kafka Error on Topic subscription: %s", e)
            logger.warning("[NOA-Harvester] Trying to create it:")
            try:
                consumer.create_topics(
                    topics=topics, num_partitions=num_partitions, replication_factor=replication_factor)
            except (TopicAlreadyExistsError,
                    UnknownTopicOrPartitionError,
                    TopicAuthorizationFailedError,
                    InvalidTopicError) as g:
                logger.error("[NOA-Harvester] Kafka: Could not subscribe or create producer topic: %s", g)
                return
        if consumer is None:
            sleep(5)

    click.echo(f"[NOA-Harvester] NOA-Harvester service started. Output path: {output_path}\n")

    while True:
        try:
            for message in consumer.read():
                item = message.value
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"[NOA-Harvester] Digesting Item from Topic {message.topic} ({now_time})..."
                msg += "\n> Item: " + json.dumps(item)
                logger.debug(msg)
                click.echo("[NOA-Harvester] Received list to download:")
                click.echo(item)
                if test:
                    downloaded_uuids = "Some downloaded ids"
                    failed_uuids = "Some failed ids"
                else:
                    uuid_list = item["Ids"]
                    downloaded_uuids, failed_uuids = harvest.download_from_uuid_list(uuid_list)
                logger.debug("[NOA-Harvester] Downloaded items: %s", downloaded_uuids)
                if failed_uuids:
                    click.echo(f"[NOA-Harvester] Failed items: {failed_uuids}")
                    logger.warning("[NOA-Harvester] Failed uuids: %s", failed_uuids)
                click.echo(f"[NOA-Harvester] Consumed download message and downloaded {downloaded_uuids}")
            sleep(1)
        except (UnsupportedForMessageFormatError, InvalidMessageError) as e:
            logger.warning("[NOA-Harvester] Error in reading kafka message: %s", e)
            continue


# TODO v2: integrate functionality in download command
@cli.command(
    help=(
        "Download data from the provided provider and URI list. "
        "Command also expects the provider credentials but can also "
        "get them from the optional config file."
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for Copernicus only)",
)
@click.argument("config_file", required=True)
@click.option("--output_path", default="./data", help="Output path")
@click.option(
    "--uuid",
    "-u",
    multiple=True,
    help="Id field of products table. Can be set multiple times"
)
def from_uuid_list(
    config_file: Argument | str,
    output_path: Option | str,
    uuid: Option | tuple[str],
    verbose: Option | bool,
) -> tuple:
    """
    Instantiate Harvester class and call download function.
    Downloads all ids from Products table, based on the --uuid multiple option.

    Parameters:
        config_file (click.Argument | str): config json file listing
            providers, collections and search terms
        output_path (click.Option | str): where to download to
        uuid (click.Option | tuple[str]): A tuple of uuids to download
        verbose (click.Option | bool): to show download progress indicator or not
    """

    logger.debug("Cli download for config file: %s", config_file)

    click.echo(f"Downloading at: {output_path}\n")
    harvest = harvester.Harvester(
        config_file=config_file,
        output_path=output_path,
        verbose=verbose,
        # TODO is not a service, but needs refactoring of Harvester and logic
        # since we have changed the functionality
        is_service=True
    )
    downloaded_uuids, failed_uuids = harvest.download_from_uuid_list(uuid)
    if failed_uuids:
        logger.error("Failed uuids: %s", failed_uuids)
    # TODO The following is a dev test: to be converted to unit tests
    # harvest.test_db_connection()
    print(downloaded_uuids)
    click.echo("Done.\n")
    return downloaded_uuids, failed_uuids


@cli.command(
    help=(
        "Queries for available products according to the config file."
        "You can also provide (optional) a [SHAPE_FILE] path in order to define "
        "the bounding box there instead of the config file."
    )
)
@click.argument("config_file", required=True)
@click.argument("shape_file", required=False)
@click.option(
    "--bbox_only",
    "-bb",
    is_flag=True,
    help="Only use multipolygon total bbox, not individual",
)
def query(
    config_file: Argument | str, shape_file: Argument | str, bbox_only: Option | bool
) -> None:
    """
    Instantiate Harvester class and call query function in order to search for
    available products for the selected collections.

    Parameters:
        config_file (click.Argument | str): config json file listing
            providers, collections and search terms
    """
    if config_file:
        logger.debug("Cli query for config file: %s", config_file)

        click.echo("Querying providers for products:\n")
        harvest = harvester.Harvester(
            config_file, shape_file=shape_file, bbox_only=bbox_only
        )
        harvest.query_data()


if __name__ == "__main__":  # pragma: no cover
    cli()
