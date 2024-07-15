"""Cli for NOA-Beyond Postprocess:Aggregate processor.

This interface and processor are used for aggregating EO data.
"""

from __future__ import annotations
import sys
import logging
from pathlib import Path

import click
from click import Argument, Option

# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from noapostaggregate import postaggregate  # noqa:402 pylint:disable=wrong-import-position

logger = logging.getLogger(__name__)


@click.group(
    help=(
        "Processes EO data from [PATH] according to parameters defined "
        "in the [CONFIG_FILE]."
    )
)
@click.option(
    "--log",
    default="warning",
    help="Log level (optional, e.g. DEBUG. Default is WARNING)",
)
def cli(log):
    """Click cli group for processing cli commands"""
    numeric_level = getattr(logging, log.upper(), "WARNING")
    logging.basicConfig(level=numeric_level, format="%(asctime)s %(message)s")


@cli.command(
    help=(
        "Aggregate according to [agg_function] argument for files in [path]."
    )
)
@click.argument("agg_function", required=True)
@click.argument("data_path", required=True)
@click.option("--output_path", default="./output", help="Output path")
def aggregate(agg_function: Argument | str, data_path: Argument | str, output_path: Option | str) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        agg_function (click.Argument | str): Aggregate function [median, mean, min, max]
        data_path (click.Argument | str): Path to look for files
        output_path (click.Argument | str): Path to store output files
        config_file (click.Argument | str): config json file
    """

    click.echo(f"Processing files in path {data_path}:\n")
    process = postaggregate.Aggregate(data_path, output_path)
    process.from_path(agg_function)


if __name__ == "__main__":  # pragma: no cover
    cli()
