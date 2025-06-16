"""
Change detection mapping class
"""

from __future__ import annotations

import os
import json
import pathlib

import torch

import logging

from noachdm import utils as chdm_utils


class ChDM:
    """
    Change Detection Mapping main class and module.
    Creates ChDM products.

    Methods:
    """

    def __init__(
        self,
        config_file: str = None,
        output_path: str = "./output",
        verbose: bool = False,
        is_service: bool = False,
        logger: logging.Logger = logging.getLogger(__name__),
    ) -> ChDM:
        """
        ChDM class. Constructor reads and loads the config if any

        Parameters:
            config_file (str): Config filename (json)
            verbose (bool - Optional): Verbose
        """
        self._is_service = is_service
        self.logger = logger

        self._config = {}
        self._output_path = pathlib.Path(output_path)
        self._verbose = verbose

        if config_file:
            with open(config_file, encoding="utf8") as f:
                self._config = json.load(f)

        # Creating both the output and the intermediate temp path
        self._temp_path = pathlib.Path(self._output_path, "temp")
        self._temp_path.mkdir(parents=True, exist_ok=True)

    @property
    def config(self):
        """Get config"""
        return self._config

    def produce(self, from_path, to_path):
        """
        Could accept path full of tifs
        """
        dataset = chdm_utils.SentinelChangeDataset(pre_dir=from_path, post_dir=to_path)
        # getting the trained local model
        trained_model_path = os.path.join(
            os.path.dirname(__file__), "models_checkpoints", "BIT_final_refined.pth"
        )
        self.logger.info("Starting prediction")
        product_path = chdm_utils.predict_all_scenes_to_mosaic(
            model_weights_path=trained_model_path,
            dataset=dataset,
            output_dir=self._output_path,
            device="cuda" if torch.cuda.is_available() else "cpu",
            service=self._is_service,
        )
        self.logger.info("Products saved at: %s", product_path)
        return product_path

    def produce_from_items_lists(self, items_from, items_to, bbox):
        """
        Must accept list of s3 uris probably
        """
        self.logger.info("Processing incoming items lists")
        self.logger.debug("Items from: %s", items_from)
        self.logger.debug("Items to: %s", items_to)

        try:
            from_path = pathlib.Path(self._temp_path, "from_date")
            from_mosaic_filename = chdm_utils.crop_and_make_mosaic(
                items_from, bbox, output_path=from_path, service=self._is_service
            )
            to_path = pathlib.Path(self._temp_path, "to_date")
            to_mosaic_filename = chdm_utils.crop_and_make_mosaic(
                items_to, bbox, output_path=to_path, service=self._is_service
            )
        except RuntimeError as e:
            self.logger.error("Could not create or parse input items: %s", e)

        self.logger.debug(
            "Creating product from mosaics: %s, %s",
            from_mosaic_filename,
            to_mosaic_filename,
        )
        new_product_path = ""
        new_product_path = self.produce(from_path=from_path, to_path=to_path)

        for file in from_path.iterdir():
            if file.is_file():
                file.unlink()
        for file in to_path.iterdir():
            if file.is_file():
                file.unlink()

        return new_product_path
