"""Main Ingest module."""
from __future__ import annotations
import os

from pathlib import Path
import json
import logging

from stactools.sentinel1.grd.stac import create_item as create_item_s1_grd
from stactools.sentinel1.rtc.stac import create_item as create_item_s1_rtc
from stactools.sentinel1.slc.stac import create_item as create_item_s1_slc
from stactools.sentinel2.commands import create_item as create_item_s2
from stactools.sentinel3.commands import create_item as create_item_s3

from pystac import Catalog

from noastacingest import utils
from noastacingest.db import utils as db_utils


FILETYPES = ("SAFE", "SEN3")
logger = logging.getLogger(__name__)


class Ingest:
    """
    A class
    """

    def __init__(
        self,
        config: str | None
    ) -> Ingest:
        """
        Ingest main class implementing single and batch item creation.
        """
        self._config = {}
        with open(config, encoding="utf8") as f:
            self._config = json.load(f)
        print(self._config)

        self._catalog = Catalog.from_file(
            Path(
                self._config["catalog_path"],
                self._config["catalog_filename"]
            )
        )

    @property
    def config(self):
        """Get config"""
        return self._config

    def single_item(self, path: Path, collection: str | None, update_db: bool):
        """
        Just an item
        """
        if path.name.endswith(FILETYPES):
            platform = str(path.name).split("_", maxsplit=1)[0]
            satellite = platform[:2]
            item = {}
            match satellite:
                case "S1":
                    sensor = str(path.name).split("_")[:3]
                    match sensor:
                        case "GRD":
                            item = create_item_s1_grd(str(path))
                            if not collection:
                                collection = "sentinel1-grd"
                        case "RTC":
                            item = create_item_s1_rtc(str(path))
                            if not collection:
                                collection = "sentinel1-rtc"
                        case "SLC":
                            item = create_item_s1_slc(str(path))
                            if not collection:
                                collection = "sentinel1-slc"
                case "S2":
                    item = create_item_s2(str(path))
                    if not collection:
                        collection = "sentinel2-l2a"
                case "S3":
                    item = create_item_s3(str(path))
                    if not collection:
                        collection = "sentinel3"
            item_path = self._config.get("collection_path") + collection + "/items/" + item.id
            json_file_path = str(Path(item_path, item.id + ".json"))
            print(json_file_path)

            # TODO add to item:
            # feature_collection = {
            #     "type": "FeatureCollection",
            #     "features": [item.to_dict() for item in collection_instance.get_all_items()]
            # }
            if collection:
                item.set_root(self._catalog)
                collection_instance = self._catalog.get_child(collection)
                item.set_collection(collection_instance)
                # TODO most providers do not have a direct collection/items relation
                # Rather, they provide an items link, where all items are present
                # e.g. https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a/items
                # Like so, I do not know if an "extent" property is needed. If it is, update it:
                # collection_instance.update_extent_from_items()
                collection_instance.normalize_and_save(
                    self._config.get("collection_path") + collection + "/"
                )
                if update_db:
                    db_utils.load_stac_items_to_pgstac(
                        [collection_instance.to_dict()], collection=True
                    )

            item.set_self_href(json_file_path)
            item.save_object(include_self_link=True)
            if update_db:
                db_utils.load_stac_items_to_pgstac([item.to_dict()])

    def from_uuid_db_list(self, uuid_list, collection, db_ingest):
        """ Get from products table the paths to ingest"""
        ingested_items = []
        failed_items = []
        # TODO: this looks at S2 table config
        db_config = db_utils.get_env_config()
        if not db_config:
            db_config = db_utils.get_local_config()
        else:
            logger.error("Not db configuration found, in env vars nor local database.ini file.")
            failed_items.append(uuid_list)
            return ingested_items, failed_items

        for single_uuid in uuid_list:
            item_path = db_utils.query_all_from_table_column_value(
                db_config, "products", "id", single_uuid
            ).get("path")
            # For production the two options should be "None, True"
            try:
                self.single_item(Path(item_path), collection, db_ingest)
                ingested_items.append(single_uuid)
            except RuntimeWarning:
                logger.warning("Item could not be ingested to pgSTAC: %s", single_uuid)
                failed_items.append(single_uuid)
                continue

        kafka_topic = os.environ.get("KAFKA_INPUT_TOPIC", "stacingest.order.completed")
        try:
            utils.send_kafka_message(kafka_topic, ingested_items, failed_items)
            logger.info("Kafka message sent")
        except BrokenPipeError as e:
            logger.error("Error sending kafka message: %s", e)

        return ingested_items, failed_items