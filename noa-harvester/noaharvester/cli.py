import sys
from pathlib import Path
import click


# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from noaharvester import harvester  # noqa:402


@click.group(
    help=(
        "Queries and/or Downloads data from Copernicus and EarthData services "
        "according to parameters as defined in the [CONFIG_FILE]."
    )
)
def cli():
    pass


@cli.command(help=("Queries for available products according to the config file"))
@click.argument("config_file", required=True)
def query(config_file) -> None:
    """
    Instantiates the Harvester class and calls query function
    in order to search for available products for the selected collections.
    """
    if config_file:
        click.echo("Querying providers for products:\n")
        harvest = harvester.Harvester(config_file)
        harvest.query_data()


@cli.command(help=("Downloads data from the selected providers and query terms"))
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for Copernicus only)",
)
@click.argument("config_file", required=True)
def download(config_file, verbose) -> None:
    """
    Instantiates the Harvester class and calls download function.
    Downloads all relevant data as defined in the config file.
    """
    if config_file:
        click.echo("Downloading...\n")
        harvest = harvester.Harvester(config_file, verbose)
        harvest.download_data()
        click.echo("Done.\n")


@cli.command(help=("Describe collection query fields (Copernicus only)"))
@click.argument("config_file", required=True)
def describe(config_file) -> None:
    """
    Instantiates the Harvester class and calls describe for
    available query terms of the selected collections.
    This function is currently only available for Copernicus providers
    """
    if config_file:
        harvest = harvester.Harvester(config_file)
        click.echo("Available parameters for selected collections:\n")
        harvest.describe()


if __name__ == "__main__":  # pragma: no cover
    cli()
