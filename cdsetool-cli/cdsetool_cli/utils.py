import os
import sys
import json
# import netrc

import click
import pathlib

from cdsetool.query import query_features
from cdsetool.query import describe_collection
from cdsetool.download import download_features
from cdsetool.credentials import Credentials
from cdsetool.monitor import StatusMonitor

import earthaccess


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


def query_data_copernicus(config_file):

    with open(f"config/{config_file}") as f:
        config = json.load(f)

    for satellite, search_terms in config.items():
        features = list(query_features(satellite, search_terms))
        click.echo(f"Available items for {satellite}: {len(features)} \n")


def query_items(config_file):

    with open(f"config/{config_file}") as f:
        config = json.load(f)

    for satellite, search_terms in config.items():
        features = list(query_features(satellite, search_terms))
        click.echo(f"Available items for {satellite}: {len(features)} \n")


# TODO introduce checking of netrc for borh copernicus and earth data
# netrc.netrc().authenticators("urs.earthdata.nasa.gov")
def query_data_modis(config_file):
    earthaccess.login()
    download_path = pathlib.Path("./data").absolute()
    download_path.mkdir(parents=True, exist_ok=True)

    search_terms = ""
    config = None
    download_path = pathlib.Path("./data").absolute()
    download_path.mkdir(parents=True, exist_ok=True)

    with open(f"config/{config_file}") as f:
        config = json.load(f)

    for collection, terms in config.items():
        search_terms = terms

        bbox = tuple(float(i) for i in search_terms["box"].split(","))
        print(bbox)
        start_date = search_terms["startDate"]
        end_date = search_terms["completionDate"]

        results = earthaccess.search_data(
            short_name=terms["short_name"],
            cloud_hosted=True,
            bounding_box=bbox,
            temporal=(start_date, end_date)
        )
        print(len(results))
        # files = earthaccess.download(results, download_path)
    # print(len(files))


# TODO Logging
# TODO typing
def download_data_copernicus(config_file, verbose):
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

        sys.stdout.flush()

        list(
            download_features(
                features,
                download_path,
                {"concurrency": 4, "monitor": monitor, "credentials": credentials},
            )
        )


# TODO introduce checking of netrc for borh copernicus and earth data
# netrc.netrc().authenticators("urs.earthdata.nasa.gov")
def download_data_modis(config_file):
    earthaccess.login()
    download_path = pathlib.Path("./data").absolute()
    download_path.mkdir(parents=True, exist_ok=True)

    search_terms = ""
    config = None
    download_path = pathlib.Path("./data").absolute()
    download_path.mkdir(parents=True, exist_ok=True)

    with open(f"config/{config_file}") as f:
        config = json.load(f)

    for collection, terms in config.items():
        search_terms = terms

        bbox = tuple(float(i) for i in search_terms["box"].split(","))
        start_date = search_terms["startDate"]
        end_date = search_terms["completionDate"]

        results = earthaccess.search_data(
            short_name=terms["short_name"],
            cloud_hosted=True,
            bounding_box=bbox,
            temporal=(start_date, end_date)
        )
        print(len(results))
        files = earthaccess.download(results, download_path)
        print(len(files))
