import sys
import click

from cdsetool.credentials import Credentials
from cdsetool.monitor import StatusMonitor
from cdsetool.query import query_features, describe_collection
from cdsetool.download import download_features

from noaharvester.providers import DataProvider


class Copernicus(DataProvider):
    def __init__(self, verbose=False):
        super().__init__()

        # From .netrc (or _netrc for Windows)
        self._credentials = Credentials()

        self._monitor = StatusMonitor() if verbose else False

    @property
    def credentials(self):
        return self._credentials

    def query(self, item):

        features = list(query_features(item["collection"], item["search_terms"]))
        click.echo(f"Available items for {item['collection']}: {len(features)} \n")

    def describe(self, collection):
        search_terms = describe_collection(collection).keys()
        click.echo(f"{collection} available search terms: \n {search_terms}\n")

    def download(self, item):
        """
            Downloads using the query terms of config_file. Verbose is used for detailed
            progress bar indicators. Download is using a concurrency setting of 4 threads.
        """

        self._download_path.mkdir(parents=True, exist_ok=True)

        features = list(query_features(item["collection"], item["search_terms"]))
        click.echo(f"Available items for {item['collection']}: {len(features)} \n")

        sys.stdout.flush()

        list(
            download_features(
                features,
                self._download_path,
                {"concurrency": 4, "monitor": self._monitor, "credentials": self.credentials},
            )
        )
