import sys
from pathlib import Path
import click


# Appending the module path in order to have a kind of cli "dry execution"
sys.path.append(str(Path(__file__).parent / ".."))

from cdsetool_cli import harvester # noqa:402


@click.group(
    help=(
        "Queries and/or Downloads data according to parameters as defined in the [CONFIG_FILE]."
    )
)
def cli():
    pass


@cli.command()
@click.argument("config_file", required=True)
def query(config_file):
    if config_file:
        harvest = harvester.Harvester(config_file)
        harvest.query_data()
    else:
        click.echo("Please provide the [config file] argument")


@cli.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Shows the progress indicator (for download command only)",
)
@click.argument("config_file", required=True)
def download(config_file, verbose):
    if config_file:
        harvest = harvester.Harvester(config_file, verbose)
        harvest.download_data()
        click.echo("done")
    else:
        click.echo("Please provide the [config file] argument")


@cli.command()
@click.argument("config_file", required=True)
def describe(config_file):
    if config_file:
        harvest = harvester.Harvester(config_file)
        harvest.describe()
    else:
        click.echo("Please provide the [config file] argument")


if __name__ == "__main__":  # pragma: no cover
    cli()
