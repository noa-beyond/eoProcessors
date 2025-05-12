"""
Change detection mapping class
"""
from __future__ import annotations

import json

from noachdm.utils import crop_and_make_mosaic


class ChDM:
    """
    Change Detection Mapping main class and module.
    Creates ChDM products.

    Methods:
    """

    def __init__(
        self,
        config_file: str = None,
        output_path: str = None,
        verbose: bool = False,
        is_service: bool = False
    ) -> ChDM:
        """
        ChDM class. Constructor reads and loads the config json file

        Parameters:
            config_file (str): Config filename (json) which includes all search items
            verbose (bool - Optional): Verbose
        """
        self._config = {}
        self._output_path = output_path
        self._verbose = verbose

        with open(config_file, encoding="utf8") as f:
            self._config = json.load(f)

    @property
    def config(self):
        """Get config"""
        return self._config

    def produce(self, from_path, to_path):
        """
        Could accept path full of tifs
        """
        print("Done already")
        return "the_product_path"

    def produce_from_items_lists(self, items_from, items_to, bbox):
        """
        Must accept list of s3 uris probably
        """
        print("Must accept list of EO Data")
        print(items_from, items_to)

        from_path = crop_and_make_mosaic(items_from, bbox)
        to_path = crop_and_make_mosaic(items_to, bbox)

        new_product_path = ""
        new_product_path = self.produce(from_path=from_path, to_path=to_path)

        from_path.cleanup()
        to_path.cleanup()
        return new_product_path
