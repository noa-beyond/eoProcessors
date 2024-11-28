"""Cli for NOA-Beyond STAC Ingest processor.

This interface and processor are used to create STAC Items
from various data sources.
"""

from __future__ import annotations
import os
import sys
import logging
from pathlib import Path

import click
from click import Argument, Option

# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from noastacingest import ingest  # noqa:402 pylint:disable=wrong-import-position

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
        update_db (click.Option | bool): Update STAC db (pgstac) for new items, using upsert. It also updates the collections
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


if __name__ == "__main__":  # pragma: no cover
    cli()
