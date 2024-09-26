"""Cli for NOA-Beyond Preprocess processor.

This interface and processor are used to preprocess downloaded EO data.
"""

from __future__ import annotations
import sys
import logging
from pathlib import Path

import click
from click import Argument, Option

# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from noapreprocess import preprocess  # noqa:402 pylint:disable=wrong-import-position

logger = logging.getLogger(__name__)


@click.group(
    help=(
        "Processes downloaded EO data in [input_path] according to parameters defined "
        "in the [config_file], and store them in [output_path]."
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


@cli.command(
    help=(
        "Generic extract and COG transforming of rasters in [input_path], using [config_file] options. "
        "If output_path option is not defined, extraction is performed where input file(s) are. "
        "If -t flag is used, the (TILE_)YEAR_MONTH_DAY folder structure is used when extracting. "
        "If -s flag is used, a simple extract all is performed (bypassing config in a way). "
        "The extraction process works for Sentinel 1 and Sentinel 2 SAFE files, while for Sentinel"
        " 3, a simple extraction is performed without Tile format."
    )
)
@click.option("--output_path", "-o", default="", help="Output path")
@click.option("--simple", "-s", is_flag=True, help="Just extract. No config used.")
@click.option("--tile_tree", "-t", is_flag=True, help="Extract in TILE/YYYY/MM/DD tree structure")
@click.argument("input_path", required=True)
@click.argument("config_file", required=True)
def extract(
    input_path: Argument | str,
    config_file: Argument | str,
    output_path: Option | str,
    simple: Option | bool,
    tile_tree: Option | bool
) -> None:
    """
    Instantiate Preprocess class and process path contents.

    Parameters:
        input_path (click.Argument | str): Path to look for files
        config_file (click.Argument | str): config json file
        output_path (click.Option | str): Path to store output
        simple (click.Option | bool): Just extract, no options
        tile_tree (click.Option | bool): Create Tile tree folder structure
        # TODO: raster resolutions config filter is dumb. Only checks if "in" filename.
          If "all" is set, it also downloads quality masks. So either search by filename field,
          or set config option of "quality_files": True
            "raster_resolutions": ["10m", "60m"]: all, 10m, 20m 60m, qa?
    """
    if config_file:
        logger.debug("Cli preprocessing using config file: %s", config_file)

    input_path = Path(input_path).resolve()

    if output_path == "":
        output_path = input_path
    else:
        output_path = Path(output_path).resolve()

    click.echo(
        f"Processing files in path {str(input_path)}, storing in {str(output_path)}\n"
    )
    process = preprocess.Preprocess(input_path, output_path, config_file)
    process.update_config({
        "simple": simple,
        "tile_tree": tile_tree
    })
    process.extract()


@cli.command(
    help=(
        "Clip files in [input_path] against a [shapefile] path. "
        "Both path options are recursive, meaning that all "
        "files found under input path are going to be clipped "
        "against all shapefiles under shapefile path."
        "Config file argument serves for custom nodata value "
        "in new clipped raster results, along with input and output "
        "raster extensions."
        )
    )
@click.argument("input_path", required=True)
@click.argument("shapefile_path", required=True)
@click.argument("config_file", required=True)
@click.option("--output_path", default="./output", help="Output path")
def clip(
    input_path: Argument | str,
    shapefile_path: Argument | str,
    output_path: Option | str,
    config_file: Argument | str,
) -> None:
    """
    Instantiate Preprocess class and clip input path contents against a shapefile path.

    Parameters:
        input_path (click.Argument | str): Path to look for rasters (no walk in tree please)
        shapefile_path (click.Argument | str): Path where 1 or more shapefile paths exist. The program does
        not expect from you to know the shapefile name. It expects to give a path where one or more shapefile
        folders exist. This is because some users have divided their multipolygon shapefiles to individual polygons.
        An ad-hoc scenario uses the name of each individual shapefile for the output folder structure.
        output_path (click.Option | str): Path to store output
        config_file (click.Argument | str): config json file
    """
    logger.debug("Cli preprocessing/clipping using config file: %s", config_file)

    click.echo(f"Processing files in path {input_path}, storing in {output_path}\n")
    process = preprocess.Preprocess(input_path, output_path, config_file)
    process.clip(shapefile_path)


if __name__ == "__main__":  # pragma: no cover
    cli()
