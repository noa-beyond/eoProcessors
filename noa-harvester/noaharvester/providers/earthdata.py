# import sys
import click

from noaharvester.providers import DataProvider

import earthaccess


class Earthdata(DataProvider):
    def __init__(self):
        super().__init__()
        # TODO introduce checking of netrc for borh copernicus and earth data
        # netrc.netrc().authenticators("urs.earthdata.nasa.gov")
        earthaccess.login()

    def query(self, item):

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

    def download(self, item):
        # TODO Logging
        # TODO typing

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
        raise NotImplementedError(
            "Earthdata (earthaccess) does not have a describe collection function"
        )
