"""Main Ingest module."""
from __future__ import annotations
from pathlib import Path
import json

from stactools.sentinel1.grd.stac import create_item as create_item_s1_grd
from stactools.sentinel1.rtc.stac import create_item as create_item_s1_rtc
from stactools.sentinel1.slc.stac import create_item as create_item_s1_slc
from stactools.sentinel2.commands import create_item as create_item_s2
from stactools.sentinel3.commands import create_item as create_item_s3

from pystac import Catalog


# The STATIC, noa INTERNAL catalog id:
NOAINTERNAL = "noa-internal"
"""
To create new Catalog (or collection, since it is a subclass of Catalog):

from pystac import (
    Catalog,
    CatalogType,
    Collection,
    Extent,
    SpatialExtent,
    TemporalExtent
)

catalog_path = "/tmp/data/dev/STAC/catalog/catalog.json"
catalog = Catalog(id="test", description="test1")
catalog.normalize_hrefs("THE PATH OF CATALOG [root?]")
catalog.make_all_asset_hrefs_absolute()
catalog.save(CatalogType.ABSOLUTE_PUBLISHED, catalog_path)

Then, to load:
catalog = Catalog.from_file("/tmp/data/dev/STAC/catalog/catalog.json")


For Collection:
DEFAULT_EXTENT = Extent(
    SpatialExtent([[-180, -90, 180, 90]]),
    TemporalExtent([[datetime.datetime.now(datetime.timezone.utc), None]]),
)

l2a_collection = Collection(
    id="sentinel2-l2a",
    description="Internal downloaded Sentinel2 L2A data",
    extent=DEFAULT_EXTENT,
)

# catalog.add_children([l2a_collection])
"""

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
        self._output_path = self._config["output_path"]
        Path(self._output_path).mkdir(parents=True, exist_ok=True)

        self._catalog = Catalog.from_file(Path(self._config["catalog_path"], "catalog.json"))

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
                case "S3":
                    item = create_item_s3(str(path))
            # TODO: item should be under collection path not in generic
            json_file_path = str(Path(self._output_path, str(path.name) + ".STAC.json"))
            print(json_file_path)

            # Save the item as a JSON file
            # TODO add to item:
            # collection: text (e.g. SENTINEL-2 - either local, or from source: SAFE S2 files do not have one (obviously), so maybe local)
            # links:
            # root: NOA Catalog (has all collections)
            # self: self ref (file. it seems that there is no .json extension in other collections. ask how it is served)
            # collection: specific collection

            if collection:
                collection_instance = self._catalog.get_child(collection)
                collection_instance.add_item(item)
                collection_instance.update_extent_from_items()
                collection_instance.save()

            item.save_object(include_self_link=True, dest_href=json_file_path)

            # TODO see where this is needed
            self._catalog.normalize_hrefs()

            self._catalog.make_all_asset_hrefs_relative()
            self._catalog.make_all_asset_hrefs_absolute
            self._catalog.save()
