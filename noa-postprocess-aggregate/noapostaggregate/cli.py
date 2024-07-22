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

from noapostaggregate import ( # noqa:402 pylint:disable=wrong-import-position
    postaggregate
)

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
    help=("Aggregate according to [agg_function] argument for files in [path].")
)
@click.argument("agg_function", required=True)
@click.argument("data_path", required=True)
@click.option("--output_path", default="./output", help="Output path")
def aggregate(
    agg_function: Argument | str, data_path: Argument | str, output_path: Option | str
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        agg_function (click.Argument | str): Aggregate function [median, mean, min, max]
        data_path (click.Argument | str): Path to look for files
        output_path (click.Option | str): Path to store output files
    """

    click.echo(f"Processing files in path {data_path}:\n")
    process = postaggregate.Aggregate(data_path, output_path)
    process.from_path(agg_function)


@cli.command(
    help=("Histogram matching of files in [input_path] based on a reference raster")
)
@click.option("--output_path", default="./output", help="Output path")
@click.argument("input_path", required=True)
@click.argument("input_reference_filename", required=False)
def histogram_matching(
    input_path: Argument | str,
    output_path: Option | str,
    input_reference_filename: Argument | str | None,
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        input_reference_filename (click.Argument | str): Filename to match against to.
        input_path (click.Argument | str): Path of files to be processed
        output_path (click.Option | str): Path to store output files
    """

    if input_reference_filename is None:
        click.echo("No reference file is given. Will process full path\n")

    click.echo(f"Processing files in path {input_path}:\n")
    process = postaggregate.Aggregate(input_path, output_path)
    process.histogram_matching(input_reference_filename)


@cli.command(
    help=("Total difference vector of files in [input_path] against a reference raster")
)
@click.option("--output_path", default="./output", help="Output path")
@click.argument("input_path", required=True)
@click.argument("input_reference_filename", required=False)
def difference_vector(
    input_path: Argument | str,
    output_path: Option | str,
    input_reference_filename: Argument | str | None,
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        input_reference_filename (click.Argument | str): Filename to calculate difference from
        input_path (click.Argument | str): Path of files to be processed
        output_path (click.Option | str): Path to store output files
    """

    if input_reference_filename is None:
        click.echo("No reference file is given. Will process full path\n")

    click.echo(f"Processing files in path {input_path}:\n")
    process = postaggregate.Aggregate(input_path, output_path)
    process.difference_vector(input_reference_filename)


if __name__ == "__main__":  # pragma: no cover
    cli()
