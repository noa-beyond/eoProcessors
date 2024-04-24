from __future__ import annotations

import sys
import click

from cdsetool.credentials import Credentials
from cdsetool.monitor import StatusMonitor
from cdsetool.query import query_features, describe_collection
from cdsetool.download import download_features

from noaharvester.providers import DataProvider


class Copernicus(DataProvider):
    """
    The Copernicus service provider, implementing query, download and describe collection
    functions. It uses (wraps) the cdsetool Python package.

    Properties:
        credentials: returns the Credentials (cdsetool) instance

    Methods:
        query (item): Query a collection based on search terms in [item].
                    Item also includes the collection name.
        download (item): Download a collection [item].
        describe (collection): Output available search terms of [collection].
    """

    def __init__(self, verbose: bool = False) -> Copernicus:
        """
        Copernicus provider. Constructor also perfoms the login operation based
        on credentials present in the .netrc file.

        Parameters:
            verbose (bool): If True, download progress indicator will be visible
        """
        super().__init__()

        # From .netrc (or _netrc for Windows)
        # TODO introduce checking of netrc for borh copernicus and earth data
        # netrc.netrc().authenticators("urs.earthdata.nasa.gov")
        self._credentials = Credentials()

        self._monitor = StatusMonitor() if verbose else False

    @property
    def credentials(self) -> Credentials:
        """Return the credentials instance."""
        return self._credentials

    def query(self, item: dict) -> tuple[str, int]:
        """
        Query Copernicus for item["collection"], item["search_terms"]] items.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (str, int):  Collection name, sum of available items.
        """
        features = list(query_features(item["collection"], item["search_terms"]))
        click.echo(f"Available items for {item['collection']}: {len(features)} \n")
        return item["collection"], len(features)

    def describe(self, collection: str) -> tuple[str, list]:
        """
        Ask Copernicus for describe the collection.

        Parameters:
            collection (string): The collection to be queried.

        Returns:
            tuple (string, list):  Collection name, list of available search terms.
        """
        search_terms = describe_collection(collection).keys()
        click.echo(f"{collection} available search terms: \n {search_terms}\n")

        return collection, list(search_terms)

    def download(self, item: dict) -> tuple[str, int]:
        """
        Download from Copernicus from item["collection"] the item["search_terms"]] items.
        Download is using a concurrency setting of 4 threads and stored in local execution
        folder, under /data.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (string, int):  Collection name, sum of downloaded files.
        """

        self._download_path.mkdir(parents=True, exist_ok=True)

        features = list(query_features(item["collection"], item["search_terms"]))
        click.echo(f"Available items for {item['collection']}: {len(features)} \n")

        sys.stdout.flush()

        downloaded_files = list(
            download_features(
                features,
                self._download_path,
                {
                    "concurrency": 4,
                    "monitor": self._monitor,
                    "credentials": self.credentials,
                },
            )
        )

        return item["collection"], len(downloaded_files)
