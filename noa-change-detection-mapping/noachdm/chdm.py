"""
Change detection mapping class
"""
from __future__ import annotations

import json


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

    def produce(self):
        """
        Could accept path full of tifs
        """
        print("Done already")

    def produce_from_path_list(self, uri_list):
        """
        Must accept list of s3 uris probably
        """
        print("Must accept list of EO Data")
        print(uri_list)
        return (True, True)
