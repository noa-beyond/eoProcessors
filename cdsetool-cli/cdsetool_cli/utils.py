import os
import json
import click
import pathlib

from cdsetool.query import query_features
from cdsetool.query import describe_collection
from cdsetool.download import download_features
from cdsetool.credentials import Credentials
from cdsetool.monitor import StatusMonitor


def available_parameters(config_file) -> None:
    """
        Checks the input file for top level keys, which have the satellite collection names.
        For each of these keys the Copernicus service is queried for available query terms.
    """

    with open(f"config/{config_file}") as f:
        config = json.load(f)
    for satellite, _ in config.items():
        search_terms = describe_collection(satellite).keys()
        click.echo(f"{satellite} available search terms: \n {search_terms}\n")


# TODO Logging
# TODO typing
def download_data(config_file, verbose):
    """
        Downloads using the query terms of config_file. Verbose is used for detailed
        progress bar indicators. Download is using a concurrency setting of 4 threads.
    """

    # TODO: check check .netrc in different function or generic credentials check
    credentials = Credentials(
        os.environ.get("login", None),
        os.environ.get("password", None)
    )
    monitor = StatusMonitor() if verbose else False

    with open(f"config/{config_file}") as f:
        config = json.load(f)

    for satellite, search_terms in config.items():

        download_path = pathlib.Path("./data").absolute()
        download_path.mkdir(parents=True, exist_ok=True)

        features = list(query_features(satellite, search_terms))
        click.echo(f"Available items for {satellite}: {len(features)} \n")

        list(
            download_features(
                features,
                download_path,
                {"concurrency": 4, "monitor": monitor, "credentials": credentials},
            )
        )
