"""Main Harvester module, calling abstract provider functions."""

from __future__ import annotations
from copy import deepcopy

import logging
import json
import zipfile

from noaharvester.providers import DataProvider, copernicus, earthdata, earthsearch
from noaharvester import utils
from noaharvester.db import utils as db_utils

logger = logging.getLogger(__name__)


class Harvester:
    """
    Harvester main class and module. Loads search items and calls providers instances.

    Methods:
        query_data: Search for items (products) in collections.
        download_data: Download items from providers and collections.
        describe: Describe available search terms of collections (Copernicus only)
    """

    def __init__(
        self,
        config_file: str,
        output_path: str = None,
        shape_file: str = None,
        verbose: bool = False,
        bbox_only: bool = False,
        from_uri: bool = False
    ) -> Harvester:
        """
        Harvester class. Constructor reads and loads the search items json file.

        Parameters:
            config_file (str): Config filename (json) which includes all search items.
            shape_file (str - Optional): Read and use shapefile instead of config coordinates.
            verbose (bool - Optional): Indicate if Copernicus download progress is verbose.
        """
        self._config_filename = config_file
        self._output_path = output_path
        self._verbose = verbose

        self._search_items: list = []
        self._providers = {}
        self._shape_file_bbox_list = None

        if shape_file:
            logger.debug("Using shapefile from path: %s", shape_file)
            self._shape_file_bbox_list = utils.get_bbox_from_shp(shape_file, bbox_only)

        with open(config_file, encoding="utf8") as f:
            self._config = json.load(f)

        if from_uri:
            pass
        else:
            for item in self._config:
                if self._shape_file_bbox_list:
                    for bbox in self._shape_file_bbox_list:
                        sub_item = deepcopy(item)
                        sub_item["search_terms"]["box"] = str(bbox).strip()[1:-1]
                        self._search_items.append(sub_item)
                        logger.debug("Appending search item: %s", sub_item)
                else:
                    self._search_items.append(item)
                    logger.debug("Appending search item: %s", item)
        logger.debug("Total search items: %s", len(self._search_items))

    def download_from_uuid_list(self, uuid_list) -> tuple[list, list]:
        """
        Utilize the minimum provider interface for downloading single items
        """
        # TODO make db connection optional. One could want to download per uuid without the db interface
        print(uuid_list)
        downloaded_items = []
        failed_items = []
        # TODO: this looks at S2 table config
        db_config = db_utils.get_env_config()
        if not db_config:
            db_config = db_utils.get_local_config()
        else:
            logger.error("Not db configuration found, in env vars nor local database.ini file.")

        for single_uuid in uuid_list:
            uuid_db_entry = db_utils.query_all_from_table_column_value(
                db_config, "products", "uuid", single_uuid)
            provider = db_utils.query_all_from_table_column_value(
                db_config, "providers", "id", uuid_db_entry.get("provider_id", None)
                ).get("name")
            if provider == "cdse":
                provider = "copernicus"
            print(provider)
            download_provider = self._resolve_provider_instance(provider)
            # Check for uuid as passed from request. It should replace uri
            # uuid = None # Test uuid: "83c19de3-e045-40bd-9277-836325b4b64e"
            if uuid_db_entry:
                logger.debug("Found db entry with uuid: %s", single_uuid)
                uuid_title = (single_uuid, uuid_db_entry.get("name"))
                print(uuid_title)
                downloaded_item_path = download_provider.single_download(*uuid_title)
                # Unfortunately, need to distinguish cases:
                # Up to now, Copernicus products are .SAFE zip files, and as such
                # need to be indexed (after decompressed) to the db
                # So the following unzip function (instead of calling a "preprocessor"
                # to perform such a task), needs to be in place.
                if downloaded_item_path.suffix == ".zip":
                    with zipfile.ZipFile(str(downloaded_item_path), "r") as archive:
                        archive.extractall(path=downloaded_item_path.parent)
                        downloaded_item_path = str(downloaded_item_path).removesuffix(".zip")
                update_status = db_utils.update_uuid(
                    db_config, "products", single_uuid, "downloaded", "true")

                update_item_path = db_utils.update_uuid(
                    db_config, "products", single_uuid, "path", str(downloaded_item_path))

                if update_status & update_item_path:
                    downloaded_items.append(single_uuid)
                else:
                    failed_items.append(single_uuid)
                    logger.error("Could not update uuid: %s", single_uuid)
            return (downloaded_items, failed_items)

    def test_db_connection(self):
        db_config = db_utils.get_env_config()
        if not db_config:
            db_config = db_utils.get_local_config()
        else:
            logger.error("Not db configuration found, in env vars nor local database.ini file.")
        db_utils.describe_table(db_config, "products")
        # db_utils.query_all_items(db_config)
        # uuid = "83c19de3-e045-40bd-9277-836325b4b64e"
        uuid = "ef7c4935-4969-43e3-83e4-f9fb15acd81f"
        uuid = "caf8620d-974d-5841-b315-7489ffdd853b"
        # db_utils.update_uuid(db_config, "products", uuid, "geo_served", "false")
        result = db_utils.query_all_from_table_column_value(db_config, "products", "uuid", uuid)
        if result:
            print(result.get("uuid"))
            print(result)
        else:
            print("missing")

    def query_data(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the provider instance and query for available collection items.
        """
        for item in self._search_items:
            logger.debug(
                "Querying %s collection %s",
                item.get("provider"),
                item.get("collection"),
            )

            provider = self._resolve_provider_instance(item.get("provider"))
            provider.query(item)

    def download_data(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the provider instance and download all available collection items according to
        search terms for that item.
        """
        for item in self._search_items:
            logger.debug(
                "Download from %s and collection: %s",
                item.get("provider"),
                item.get("collection"),
            )

            provider = self._resolve_provider_instance(item.get("provider"))
            provider.download(item)

    def describe(self) -> None:
        """
        For every item in config file loaded by Harvester instance, create or retrieve
        the Copernicus provider instance and describe the collection (available search
        terms).
        """
        for item in self._search_items:
            if item.get("provider") == "copernicus":
                logger.debug(
                    "Describing from: %s, collection: %s",
                    item.get("provider"),
                    item.get("collection"),
                )
                provider = self._resolve_provider_instance(item.get("provider"))
                provider.describe(item.get("collection"))

    def _resolve_provider_instance(self, provider: str) -> DataProvider:
        if provider not in self._providers:
            logger.debug(
                "Provider: %s DataProvider instance not found. Creating new.", provider
            )
            if provider.lower() == "copernicus":
                self._providers[provider] = copernicus.Copernicus(self._output_path, self._verbose)
            elif provider.lower() == "earthdata":
                self._providers[provider] = earthdata.Earthdata(self._output_path)
            elif provider.lower() == "earthsearch":
                self._providers[provider] = earthsearch.Earthsearch(self._output_path)
        else:
            logger.info("Provider: %s DataProvider instance found.", provider)
        return self._providers[provider]
