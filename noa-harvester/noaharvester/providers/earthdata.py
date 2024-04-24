from __future__ import annotations
import click

from noaharvester.providers import DataProvider

import earthaccess


class Earthdata(DataProvider):
    """
    The Earthdata service provider, implementing query and download
    functions. It uses (wraps) the earthaccess Python package.

    Methods:
        query (item): Query a collection based on search terms in [item].
                Item also includes the collection name.
        download (item): Download a collection [item].
        describe (collection): Output available search terms of [collection].
    """

    def __init__(self) -> Earthdata:
        """
        Earthdata provider. Constructor also perfoms the login operation based
        on credentials present in the .netrc file.
        """
        super().__init__()

        # TODO introduce checking of netrc for borh copernicus and earth data
        # netrc.netrc().authenticators("urs.earthdata.nasa.gov")
        earthaccess.login()

    def query(self, item: dict) -> tuple[str, int]:
        """
        Query Earthdata for item["collection"], item["search_terms"]] items.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (str, int):  Collection name, sum of available items.
        """
        search_terms = item["search_terms"]
        bbox = tuple(float(i) for i in search_terms["box"].split(","))
        start_date = search_terms["startDate"]
        end_date = search_terms["completionDate"]

        results = earthaccess.search_data(
            short_name=search_terms["short_name"],
            cloud_hosted=True,
            bounding_box=bbox,
            temporal=(start_date, end_date),
        )
        click.echo(f"Available items for {item['collection']}: {len(results)}")
        return item["collection"], len(results)

    def download(self, item: dict) -> tuple[str, int]:
        """
        Download from Earthdata from item["collection"] the item["search_terms"]] items.
        Download is using a concurrency setting of 8 threads and stored in local execution
        folder, under /data.

        Parameters:
            item (dict): Dictionary as per config file structure.

        Returns:
            tuple (string, int):  Collection name, sum of downloaded files.
        """
        self._download_path.mkdir(parents=True, exist_ok=True)
        search_terms = item["search_terms"]

        bbox = tuple(float(i) for i in search_terms["box"].split(","))
        start_date = search_terms["startDate"]
        end_date = search_terms["completionDate"]

        results = earthaccess.search_data(
            short_name=search_terms["short_name"],
            cloud_hosted=True,
            bounding_box=bbox,
            temporal=(start_date, end_date),
        )

        earthaccess.download(results, self._download_path)
        return item["collection"], len(results)

    def describe(self):
        """Not implemented for Earthdata. Service is not provided."""
        raise NotImplementedError(
            "Earthdata (earthaccess) does not have a describe collection function"
        )
