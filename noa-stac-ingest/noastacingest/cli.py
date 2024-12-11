"""Cli for NOA-Beyond STAC Ingest processor.

This interface and processor are used to create STAC Items
from various data sources.
"""

from __future__ import annotations
import os
import sys
from time import sleep
from datetime import datetime
import json
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

from noastacingest import ingest  # noqa:402 pylint:disable=wrong-import-position
from noastacingest.messaging.message import Message # noqa:402 pylint:disable=wrong-import-position
from noastacingest.messaging import AbstractConsumer # noqa:402 pylint:disable=wrong-import-position
from noastacingest.messaging.kafka_consumer import KafkaConsumer # noqa:402 pylint:disable=wrong-import-position

logger = logging.getLogger(__name__)


@click.group(
    help=(
        "Creates STAC Items from filename, "
        "according to parameters as defined in the [CONFIG_FILE]."
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


@cli.command(help="Create STAC Item from SAFE path")
@click.argument("input_path", required=True)
@click.argument("config", required=True)
@click.option("--collection", "-c", help="Collection for item(s) to be child of")
@click.option("--recursive", "-r", is_flag=True, help="Ingest all (SAFE) directories under path")
@click.option("--update_db", "-udb", is_flag=True, help="Update STAC db, ingesting(upsert) new items")
def create_item_from_path(
    input_path: Argument | str,
    config: Argument | str,
    collection: Option | str | None,
    recursive: Option | bool,
    update_db: Option | bool
) -> None:
    """
    Instantiate Ingest Class and call "create_item"

    Parameters:
        input (click.Argument | str): Input filename path
        config_file (click.Argument | str) - optional: config json file
            selecting file types etc
        collection (click.Option | str | None): Collection id of which the new Item will be an Item of
        recursive (click.Option | bool): To ingest all (SAFE) directories under input (for multiple item creation)
        update_db (click.Option | bool): Update pgstac for new items, using upsert. It also updates the collections
    """
    # TODO needs refactor. Updating and creating items can be done in batches, especially in db
    if config:
        logger.debug("Cli STAC creation using config file: %s", config)

    ingestor = ingest.Ingest(config=config)

    if recursive:
        click.echo(f"Ingesting items in path {input_path}\n")
        for single_item in os.listdir(input_path):
            item = Path(input_path, single_item)
            if item.is_dir():
                ingestor.single_item(item, collection, update_db)
    else:
        click.echo("Ingesting single item from path\n")
        ingestor.single_item(Path(input_path), collection, update_db)


@cli.command(
    help=(
        """
        Microservice facilitating EO data STAC db ingestion.
        Implemented using a kafka producer/consumer pattern.
        """
    )
)
@click.option(
    "--test",
    "-t",
    is_flag=True,
    help="Testing kafka receiving requests. No other functionality",
)
@click.option(
    "--db_ingest",
    "-db",
    is_flag=True,
    help="Ingesting to pgSTAC, not only creating STAC Items. Should be enabled",
)
@click.option(
    "--collection",
    "-col",
    help="""
        Explicit collection name to ingest to.
        If not set, ingestor will try to derive it from path name
    """,
)
@click.argument("config_file", required=True)
def noa_stac_ingest_service(
    config_file: Argument | str,
    collection: Option | str,
    test: Option | bool,
    db_ingest: Option | bool
) -> None:
    """
    Instantiate Ingest class, activating service, listening to kafka topic.
    When triggered, it creates, stores and populates pgstac with Items,
    looking for input paths from ids from Products table,
    based on the list argument from kafka message.

    Parameters:
        config_file (click.Argument | str): config file with necessary parameters
        collection (click.Option | str): Ingest to specific collection
        test (click.Option | bool): for kafka testing purposes
        db_ingest (click.Option | bool): Ingest Item to db. Should be set to true for production
    """
    # if config_file:
    logger.debug("Starting NOA-STAC-Ingest service...")

    ingestor = ingest.Ingest(config=config_file)

    consumer: AbstractConsumer | k_KafkaConsumer = None

    # Warning: topics is a list, even if there is only one topic
    # So it should be defined as a list in the config json file
    topics = ingestor.config.get(
        "topics_consumer", os.environ.get(
            "KAFKA_INPUT_TOPICS", ["stacingest.order.requested"]
        )
    )
    schema_def = Message.schema_request()
    num_partitions = int(ingestor.config.get(
        "num_partitions", os.environ.get(
            "KAFKA_NUM_PARTITIONS", 2
        )
    ))
    replication_factor = int(ingestor.config.get(
        "replication_factor", os.environ.get(
            "KAFKA_REPLICATION_FACTOR", 3
        )
    ))

    while consumer is None:
        consumer = KafkaConsumer(
            bootstrap_servers=ingestor.config.get(
                "kafka_bootstrap_servers",
                (
                    os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                              "localhost:9092"
                            )
                )
            ),
            group_id=ingestor.config.get(
                "kafka_request_group_id",
                (
                        os.getenv("KAFKA_REQUEST_GROUP_ID",
                                "stacingest-group-request"
                                )
                )
            ),
            topics=topics,
            schema=schema_def
        )
        try:
            consumer.subscribe_to_topics(topics)
        except (UnknownTopicOrPartitionError, TopicAuthorizationFailedError, InvalidTopicError) as e:
            logger.warning("[NOA-STACIngest] Kafka Error on Topic subscription: %s", e)
            logger.warning("[NOA-STACIngest] Trying to create it:")
            try:
                consumer.create_topics(
                    topics=topics, num_partitions=num_partitions, replication_factor=replication_factor)
            except (TopicAlreadyExistsError,
                    UnknownTopicOrPartitionError,
                    TopicAuthorizationFailedError,
                    InvalidTopicError) as g:
                logger.error("[NOA-STACIngest] Kafka: Could not subscribe or create producer topic: %s", g)
                return
        if consumer is None:
            sleep(5)

    click.echo("NOA-STACIngest service started.\n")
    if not db_ingest:
        logger.warning("Items will not be ingested to pgSTAC. Did you enable the -db flag?")
    while True:
        try:
            for message in consumer.read():
                item = message.value
                now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                msg = f"[NOA-STACIngest] Digesting Item from Topic {message.topic} ({now_time})..."
                msg += "\n> Item: " + json.dumps(item)
                logger.debug(msg)
                click.echo("[NOA-STACIngest] Received list to ingest")
                if test:
                    ingested = "Some ingested ids"
                    failed = "Some failed to ingest ids"
                else:
                    uuid_list = json.loads(item)
                    ingested, failed = ingestor.from_uuid_db_list(
                        uuid_list["Ids"],
                        collection,
                        db_ingest
                    )
                logger.debug("[NOA-STACIngest] Ingested items: %s", ingested)
                if failed:
                    logger.error("[NOA-STACIngest] Failed uuids: %s", failed)
            sleep(1)
        except (UnsupportedForMessageFormatError, InvalidMessageError) as e:
            logger.error("[NOA-Harvester] Error in reading kafka message: %s", e)
            continue


if __name__ == "__main__":  # pragma: no cover
    cli()
