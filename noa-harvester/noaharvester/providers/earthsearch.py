"""Earthdata data provider class. Implements Provider ABC."""

from __future__ import annotations

import logging
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        self._STAC_URL = "https://earth-search.aws.element84.com/v0"

    def query(self, item: dict) -> tuple[str, int]:
        """
        Query S2 L2A COGs collection for item["search_terms"] items.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (str, int):  Collection name, sum of available items.
        """
        logger.debug("Search terms: %s", item["search_terms"])

        search_terms = item["search_terms"]

        catalog = pystac_client.Client.open(self._STAC_URL)
        catalog.add_conforms_to("ITEM_SEARCH")

        results = catalog.search(
            collections=[item["collection"]],
            bbox=str(search_terms["box"]),
            datetime=[search_terms["startDate"], search_terms["completionDate"]],
            query={"eo:cloud_cover": {"lt": search_terms["cloud_cover_lt"]}},
        ).item_collection()

        click.echo(f"Available items for {item['collection']}: {len(results.items)}")
        return item["collection"], len(results.items)

    def download(self, item: dict) -> tuple[str, int]:
        """
        Download from Earthsearch from S2 L2A COGs collection the item["search_terms"]] items.
        Download is using a concurrent http connections setting of 8 threads and stored in local execution
        folder, under /data.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (string, int):  Collection name, sum of downloaded files.
        """
        logger.debug("Download search terms: %s", item["search_terms"])

        file_items = []
        search_terms = item["search_terms"]

        catalog = pystac_client.Client.open(self._STAC_URL)
        catalog.add_conforms_to("ITEM_SEARCH")

        results = catalog.search(
            collections=[item["collection"]],
            bbox=str(search_terms["box"]),
            datetime=[search_terms["startDate"], search_terms["completionDate"]],
            query={"eo:cloud_cover": {"lt": search_terms["cloud_cover_lt"]}},
        ).item_collection()

        for result in results.items:
            click.echo(f"Data: {(result.assets['visual'].href)}")
            file_items.append([result.id, result.assets["visual"].href])

        click.echo(
            f"Total items to be downloaded for {item['collection']}: {len(results.items)} \n"
        )

        executor = ThreadPoolExecutor(max_workers=24)

        # Use a list to store the download tasks
        download_tasks = []

        # Submit the download tasks
        for file_item in file_items:
            download_task = executor.submit(self._download_file, file_item)
            download_tasks.append(download_task)

        # Process the completed tasks
        for completed_task in as_completed(download_tasks):
            result = completed_task.result()

        return item["collection"], len(results)

    def describe(self, collection):
        """Not implemented for Earthsearch. Service is not provided."""
        logger.error("Describe is not available for Earthdata.")
        raise NotImplementedError(
            "Earthdata (earthaccess) does not have a describe collection function"
        )

    def _download_file(self, file_item):

        self._download_path.mkdir(parents=True, exist_ok=True)
        filename = str(self._download_path) + "/" + file_item[0] + ".tif"
        response = requests.get(file_item[1])
        if response.status_code == 200:
            with open(filename, "wb") as file:
                file.write(response.content)
                print(f"File {filename} downloaded successfully!")
        else:
            print("Failed to download the file.")
