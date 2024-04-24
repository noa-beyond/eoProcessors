from __future__ import annotations

import json

from noaharvester.providers import DataProvider, copernicus, earthdata


class Harvester:
    """
    Harvester main class and module. Loads search items and calls providers instances.

    Methods:
        query_data: Search for items (products) in collections.
        download_data: Download items from providers and collections.
        describe: Describe available search terms of collections (Copernicus only)
    """

    def __init__(self, config_file: str, verbose: bool = False) -> Harvester:
        """
        Harvester class. Constructor reads and loads the search items json file.

        Parameters:
            config_file (str): Config filename (json) which includes all search items.
            verbose (bool): Indicate if Copernicus download progress is verbose.
        """
        self._config_filename = config_file
        self._verbose = verbose

        self._search_items: list = []
        self._providers = {}
        with open(config_file) as f:
            self._config = json.load(f)

        for item in self._config:
            self._search_items.append(item)

    def query_data(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the provider instance and query for available collection items.
        """
        for item in self._search_items:
            provider = self._resolve_provider_instance(item.get("provider"))
            provider.query(item)

    def download_data(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the provider instance and download all available collection items according to
        search terms for that item.
        """
        for item in self._search_items:
            provider = self._resolve_provider_instance(item.get("provider"))
            provider.download(item)

    def describe(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the Copernicus provider instance and describe the collection (available search
        terms).
        """
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
