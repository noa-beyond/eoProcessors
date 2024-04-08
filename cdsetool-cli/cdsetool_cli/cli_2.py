import sys
from pathlib import Path
import click

# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))


from cdsetool_cli.utils import (  # noqa:402
    available_parameters,
    query_data_copernicus,
    query_data_modis,
    download_data_copernicus,
    download_data_modis,
    query_items
 )


@click.group(
    help=(
        "Queries and Downloads Copernicus and MODIS data according to parameters as defined in"
        " the [CONFIG_FILE]. It also shows available query parameters for Satellites for Copernicus"
    )
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for download option only)",
)
@click.pass_context
def cli():
    pass


@cli.command()
@click.option(
    "--modis",
    "-m",
    is_flag=True,
    help="Query MODIS data",
)
@click.option(
    "--copernicus",
    "-c",
    is_flag=True,
    help="Query Copernicus data",
)
@click.option(
    "--parameters",
    "-p",
    is_flag=True,
    help="Show available query Parameters for Satellites present in CONFIG_FILE",
)
@click.argument("config_file", required=True)
def query(copernicus, modis, parameters, config_file):
    if parameters:
        click.echo("Available parameters per data source list:\n")
        available_parameters(config_file)
    if copernicus:
        query_data_copernicus(config_file)
    elif modis:
        query_data_modis(config_file)
    else:
        click.echo("Please choose data source with the respective cli option")


@cli.command()
@click.option(
    "--modis",
    "-m",
    is_flag=True,
    help="Downloads MODIS data",
)
@click.option(
    "--copernicus",
    "-c",
    is_flag=True,
    help="Downloads Copernicus data",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for download option only)",
)
@click.argument("config_file", required=True)
def download(copernicus, modis, verbose, config_file):
    if copernicus:
        download_data_copernicus(config_file, verbose)
    elif modis:
        download_data_modis(config_file)
    else:
        click.echo("Please choose data source with the respective cli option")


if __name__ == "__main__":  # pragma: no cover
    cli()
