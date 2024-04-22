import json

from noaharvester.providers import DataProvider, copernicus, earthdata


class Harvester():

    def __init__(self, config_file, verbose=False) -> None:

        self._config_filename = config_file
        self._verbose = verbose

        self._search_items: list = []
        self._providers = {}
        with open(f"config/{config_file}") as f:
            self._config = json.load(f)

        for item in self._config:
            self._search_items.append(item)

    def query_data(self):

        for item in self._search_items:
            provider = self._resolve_provider_instance(item.get("provider"))
            provider.query(item)

    def download_data(self):

        for item in self._search_items:
            provider = self._resolve_provider_instance(item.get("provider"))
            provider.download(item)

    def describe(self):

        for item in self._search_items:
            provider = self._resolve_provider_instance(item.get("provider"))
            if provider.__class__.__name__ == "Copernicus":
                provider.describe(item.get("collection"))

    def _resolve_provider_instance(self, provider) -> DataProvider:
        if provider not in self._providers:
            if provider == "copernicus":
                self._providers[provider] = copernicus.Copernicus(self._verbose)
            elif provider == "earthdata":
                self._providers[provider] = earthdata.Earthdata()
        return self._providers[provider]
