"""
Warning! This might be a destructive action, if a Catalog/Collection
with the same name exists
"""
import sys
import json
import datetime

from pystac import (
    Catalog,
    CatalogType
)

def main(config_file):

    config = {}

    with open(config_file, encoding="utf8") as f:
        config = json.load(f)

    catalog = Catalog(id=config["catalog_id"], description=config["catalog_description"])
    catalog.normalize_hrefs(config["catalog_path"])
    catalog.make_all_asset_hrefs_absolute()
    catalog.save(CatalogType.ABSOLUTE_PUBLISHED, config["catalog_path"])

    # Then, to load:
    # catalog = Catalog.from_file(config["catalog_path"] + config["catalog_filename"])

if __name__ == "__main__":  # pragma: no cover
    if len(sys.argv) != 2:
        print("Usage: python _create_catalog.py <config_file_path>")
    else:
        config_file = sys.argv[1]
        main(config_file)