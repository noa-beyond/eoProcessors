import sys
from pathlib import Path
import click

# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))


from cdsetool_cli.utils import (  # noqa:402
    download_data_copernicus,
    available_parameters,
    query_items,
    query_data_modis
 )


@click.group()
def cli():
    pass


@cli.command(
    help=(
        "Downloads Copernicus data according to parameters as defined in the [CONFIG_FILE],"
        " or shows available query parameters for Satellites present in the [CONFIG_FILE]"
    )
)
@click.option(
    "--download",
    "-d",
    is_flag=True,
    help="Downloads available Copernicus data according to CONFIG_FILE",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for download option only)",
)
@click.option(
    "--parameters",
    "-p",
    is_flag=True,
    help="Show available query Parameters for Satellites present in CONFIG_FILE",
)
@click.option(
    "--query",
    "-q",
    is_flag=True,
    help="Show multitude of available items for query in CONFIG_FILE",
)
@click.argument("config_file", required=True)
def copernicus(download, parameters, query, config_file, verbose):
    if parameters:
        click.echo("Available parameters per data source list:\n")
        available_parameters(config_file)
    elif download:
        click.echo("Downloading:\n")
        download_data_copernicus(config_file, verbose)
    elif query:
        click.echo("Querying:\n")
        query_items(config_file)
    else:
        click.echo("Please select at least one option (-d, -p)")


@cli.command()
@click.option(
    "--query",
    "-q",
    is_flag=True,
    help="Show multitude of available items for query in CONFIG_FILE",
)
@click.argument("config_file", required=True)
def modis(query, config_file):
    query_data_modis(config_file)


if __name__ == "__main__":  # pragma: no cover
    cli()
