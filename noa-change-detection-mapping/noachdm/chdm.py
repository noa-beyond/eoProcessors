"""
Change detection mapping class
"""
from __future__ import annotations

import os
import json
import tempfile

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
        output_path: str = None,
        verbose: bool = False,
        is_service: bool = False,
        logger=logging.getLogger(__name__)
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
        self._output_path = output_path
        self._verbose = verbose

        if config_file:
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
        output_dir = "data/"

        dataset = chdm_utils.SentinelChangeDataset(pre_dir=from_path, post_dir=to_path)
        # getting the trained local model
        trained_model_path = os.path.join(
            os.path.dirname(__file__),
            "models_checkpoints",
            "BIT_final_refined.pth"
        )
        if self._is_service:
            # If to be saved in s3, we use a temp dir to save the product
            output_dir = tempfile.TemporaryDirectory()
        self.logger.info("Starting prediction")
        product_path = chdm_utils.predict_all_scenes_to_mosaic(
            model_weights_path=trained_model_path,
            dataset=dataset,
            output_dir=output_dir,
            device="cuda" if torch.cuda.is_available() else "cpu",
            service=self._is_service
        )
        self.logger.info("Products saved at s3 path: %s", product_path)
        return product_path

    def produce_from_items_lists(
        self,
        items_from,
        items_to,
        bbox
    ):
        """
        Must accept list of s3 uris probably
        """
        self.logger.info("Processing incoming items lists")
        self.logger.debug("Items from: %s", items_from)
        self.logger.debug("Items to: %s", items_to)

        try:
            from_path = chdm_utils.crop_and_make_mosaic(
                items_from,
                bbox,
                self._is_service
            )
            to_path = chdm_utils.crop_and_make_mosaic(
                items_to,
                bbox,
                self._is_service
            )
        except RuntimeError as e:
            self.logger.error("Could not create or parse input items: %s", e)
            raise

        new_product_path = ""
        new_product_path = self.produce(from_path=from_path, to_path=to_path)

        from_path.cleanup()
        to_path.cleanup()
        return new_product_path
