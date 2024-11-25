"""Main Ingest module."""
from __future__ import annotations
from pathlib import Path
import json

from stactools.sentinel1.grd.stac import create_item as create_item_s1_grd
from stactools.sentinel1.rtc.stac import create_item as create_item_s1_rtc
from stactools.sentinel1.slc.stac import create_item as create_item_s1_slc
from stactools.sentinel2.commands import create_item as create_item_s2
from stactools.sentinel3.commands import create_item as create_item_s3

from pystac import Catalog, Collection
from pystac.layout import AsIsLayoutStrategy

FILETYPES = ("SAFE", "SEN3")


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
        self._config = None
        with open(config, encoding="utf8") as f:
            self._config = json.load(f)
        print(self._config)

        self._catalog = Catalog.from_file(Path(self._config["catalog_path"], self._config["catalog_filename"]))

    def single_item(self, path: Path, collection: str | None):
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
                        case "RTC":
                            item = create_item_s1_rtc(str(path))
                        case "SLC":
                            item = create_item_s1_slc(str(path))
                case "S2":
                    item = create_item_s2(str(path))
                    collection = "sentinel2-l2a"
                case "S3":
                    item = create_item_s3(str(path))
            # TODO: item should be under collection path not in generic
            item_path = self._config.get("collection_path") + collection + "/items/" + item.id
            json_file_path = str(Path(item_path, item.id + ".json"))
            # json_file_path = str(Path(self._output_path, str(path.name) + ".STAC.json"))
            print(json_file_path)

            # Save the item as a JSON file
            # TODO add to item:
            # collection: text (e.g. SENTINEL-2 - either local, or from source: SAFE S2 files do not have one (obviously), so maybe local)
            # links:
            # root: NOA Catalog (has all collections)
            # self: self ref (file. it seems that there is no .json extension in other collections. ask how it is served)
            # collection: specific collection

            # item.save_object(include_self_link=True, dest_href=json_file_path)
            item.make_asset_hrefs_absolute()
            item.save_object(include_self_link=True, dest_href=json_file_path)

            #feature_collection = {
            #    "type": "FeatureCollection",
            #    "features": [item.to_dict() for item in collection_instance.get_all_items()]
            #}
            if collection:
                collection_instance = self._catalog.get_child(collection)
                collection_instance.add_item(item)
                collection_instance.update_extent_from_items()
                collection_instance.normalize_and_save(self._config.get("collection_path") + collection + "/")
                # collection_instance.normalize_hrefs(self._config.get("collection_path") + collection + "/")
                # collection_instance.make_all_asset_hrefs_absolute()
                # collection_instance.save()

            # TODO see where this is needed
            # self._catalog.normalize_hrefs()

            # self._catalog.make_all_asset_hrefs_relative()
            # self._catalog.make_all_asset_hrefs_absolute()
            # self._catalog.save()
