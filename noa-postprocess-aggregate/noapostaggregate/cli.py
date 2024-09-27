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

from noapostaggregate import (  # noqa:402 pylint:disable=wrong-import-position
    postaggregate,
)

logger = logging.getLogger(__name__)


@click.group(
    help=(
        "Processes EO data from [input_path] according to parameters defined "
        "in [config_file]."
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
    help=("Aggregate according to [agg_function] argument for files in [input_path].")
)
@click.argument("agg_function", required=True)
@click.argument("input_path", required=True)
@click.option("--output_path", default="", help="Output path")
def aggregate(
    agg_function: Argument | str, input_path: Argument | str, output_path: Option | str
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        agg_function (click.Argument | str): Aggregate function [median, mean, min, max]
        input_path (click.Argument | str): Path to look for files
        output_path (click.Option | str): Path to store output files
    """

    click.echo(f"Processing files in path {input_path}:\n")

    input_path = Path(input_path).resolve()

    if output_path == "":
        output_path = Path(input_path, "agg_function").resolve()
    else:
        output_path = Path(output_path).resolve()

    process = postaggregate.Aggregate(input_path, output_path)
    process.from_path(agg_function)


# TODO TODO TODO: check gdal info version when installing requirements somehow
@cli.command(
    help=(
        "Histogram matching of files in [input_path] based on an optional reference raster. "
        "If no reference raster is provided, then a random (usually the first grammatically) "
        "raster from the folder is selected as reference."
    )
)
@click.option("--output_path", default="", help="Output path")
@click.argument("input_path", required=True)
@click.argument("reference_file", required=False)
def histogram_matching(
    input_path: Argument | str,
    output_path: Option | str,
    reference_file: Argument | str | None,
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        reference_file (click.Argument | str): Filename to match against to.
        input_path (click.Argument | str): Path of files to be processed
        output_path (click.Option | str): Path to store output files
    """

    if reference_file is None:
        click.echo("No reference file is given. Will process full path\n")

    click.echo(f"Processing files in path {input_path}:\n")

    input_path = Path(input_path).resolve()

    if output_path == "":
        output_path = Path(input_path, "histogram_matched").resolve()
    else:
        output_path = Path(output_path).resolve()

    process = postaggregate.Aggregate(input_path, output_path)
    process.histogram_matching(reference_file)


@cli.command(
    help=(
        "Per raster and total difference vector of rasters in [input_path] "
        "against a reference raster. If no reference is provided, "
        "then 'all against all' difference strategy is performed. "
        "If '-pm' flag is used, then differences are calculated in a "
        "per month basis. e.g. all August rasters are compared with "
        "all September rasters, and a Total difference vector is produced "
        "for every reference raster of August against all rasters "
        "of September. Please note that it only calculates adjacent months."
    )
)
@click.option("--output_path", default="./output", help="Output path")
@click.option(
    "--per_month",
    "-pm",
    is_flag=True,
    show_default=True,
    default=False,
    help="If true, then a per month dif is produced under the same folder",
)
@click.argument("input_path", required=True)
@click.argument("reference_file", required=False)
def difference_vector(
    input_path: Argument | str,
    output_path: Option | str,
    reference_file: Argument | str | None,
    per_month: Option | bool,
) -> None:
    """
    Instantiate Postprocess class and process path contents.

    Parameters:
        reference_file (click.Argument | str): Filename to calculate difference from
        input_path (click.Argument | str): Path of files to be processed
        output_path (click.Option | str): Path to store output files
    """

    if reference_file is None and not per_month:
        click.echo(
            "No reference file is given. Will process full path, searching for 'reference' in filename\n"
        )

    click.echo(f"Processing files in path {input_path}:\n")

    input_path = Path(input_path).resolve()

    if output_path == "":
        output_path = Path(input_path, "dif_vector").resolve()
    else:
        output_path = Path(output_path).resolve()

    process = postaggregate.Aggregate(input_path, output_path)
    if per_month:
        process.difference_vector_per_month()
    else:
        # TODO this does not work as intended: it looks for 'reference' files
        process.difference_vector(reference_file)


if __name__ == "__main__":  # pragma: no cover
    cli()
