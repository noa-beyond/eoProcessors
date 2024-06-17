"""Earthdata data provider class. Implements Provider ABC."""

from __future__ import annotations

import logging
import requests
import shutil
from pqdm.threads import pqdm

import click
import pystac_client

from noaharvester.providers import DataProvider


logger = logging.getLogger(__name__)


class Earthsearch(DataProvider):
    """
    The Earthsearch cloud provider, implementing STAC search and download
    functions. It uses PySTAC client to perform the search operations.
    TODO: for now, it only downloads S2 L2A COGs.

    Methods:
        query (item): Query the S2 L2A COGs collection based on search terms in [item].
        download (item): Download an [item].
    """

    def __init__(self) -> Earthsearch:
        """
        Earthsearch cloud provider.
        """
        super().__init__()

        logger.debug("Earthsearch provider. No credentials needed.")
        self._STAC_URL = "https://earth-search.aws.element84.com/"

    def query(self, item: dict) -> tuple[str, int]:
        """
        Query S2 L2A COGs collection for item["search_terms"] items.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (str, int):  Collection name, sum of available items.
        """
        logger.debug("Search terms: %s", item["search_terms"])

        results = self._search_stac_collection(item)

        click.echo(f"Available items for {item['collection']}: {len(results.items)}")
        return item["collection"], len(results.items)

    def download(self, item: dict) -> tuple[str, int]:
        """
        Download is using a concurrent http connections setting of 8 threads and stored in local execution
        folder, under /data.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (string, int):  Collection name, sum of downloaded files.
        """
        logger.debug("Download search terms: %s", item["search_terms"])

        file_items = []

        results = self._search_stac_collection(item)

        for result in results.items:
            click.echo(f"Data: {(result.assets['visual'].href)}")
            file_items.append([result.id, result.assets["visual"].href])

        click.echo(
            f"Total items to be downloaded for {item['collection']}: {len(results.items)} \n"
        )
        self._download_path.mkdir(parents=True, exist_ok=True)

        arguments = [(url, self._download_path) for url in file_items]
        results = pqdm(
            arguments,
            self._download_file,
            n_jobs=8,
            argument_type="args",
        )

        return item["collection"], len(results)

    def describe(self, collection):
        """Not implemented for Earthsearch. Service is not provided."""
        logger.error("Describe is not available for Earthdata.")
        raise NotImplementedError(
            "Earthdata (earthaccess) does not have a describe collection function"
        )

    def _search_stac_collection(self, item: dict) -> tuple[str, int]:
        """
        Search from Earthsearch item[collection] and item[version] for item["search_terms"]] items.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (string, int):  Collection name, sum of downloaded files.
        """
        logger.debug("Searching. Arguments: %s", item["search_terms"])

        search_terms = item["search_terms"]

        # INFO: adding the version number of this provider (v0 or v1). It seems
        # that v1 has more products but lets TODO check that in the
        # near future
        catalog = pystac_client.Client.open(self._STAC_URL + item["version"])
        catalog.add_conforms_to("ITEM_SEARCH")

        results = catalog.search(
            collections=[item["collection"]],
            bbox=str(search_terms["box"]),
            datetime=[search_terms["startDate"], search_terms["completionDate"]],
            query={"eo:cloud_cover": {"lt": search_terms["cloud_cover_lt"]}},
        ).item_collection()

        return results

    # TODO put this in utils
    # TODO check if file exists
    def _download_file(self, file_item, download_path):

        # self._download_path.mkdir(parents=True, exist_ok=True)
        filename = str(download_path) + "/" + file_item[0] + ".tif"
        response = requests.get(file_item[1], stream=True)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                # This is to cap memory usage for large files at 1MB per write to disk per thread
                # https://docs.python-requests.org/en/latest/user/quickstart/#raw-response-content
                # TODO check if this is response.raw or response.content
                shutil.copyfileobj(response.raw, f, length=1024 * 1024)

                logger.info("File %s downloaded successfully!", filename)
        else:
            print("Failed to download the file.")
