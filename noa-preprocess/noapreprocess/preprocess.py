"""Main Preprocess module"""

from __future__ import annotations

import os
import logging
import json
from pathlib import Path
import zipfile
import click

logger = logging.getLogger(__name__)


class Preprocess:
    """
    Preprocess main class and module.

    Methods:
        from_path: Unzips and extracts according to config file parameters.
    """

    def __init__(
        self, input_path: str, output_path: str, config_file: str
    ) -> Preprocess:
        """
        Preprocess class. Constructor reads and loads the search items json file.

        Parameters:
            input_path (str): Where to find files
            output_path (str): Where to store results
            config_file (str): Config filename (json)
        """
        self._input_path = Path(input_path).absolute()
        self._output_path = Path(output_path).absolute()
        os.makedirs(str(self._output_path), exist_ok=True)

        with open(config_file, encoding="utf8") as f:
            self._config = json.load(f)

    def from_path(self):
        for filename in os.listdir(str(self._input_path)):
            if filename.endswith(self._config["input_file_type"]):
                zip_path = os.path.join(str(self._input_path), filename)
                with zipfile.ZipFile(zip_path, "r") as archive:
                    # TODO write the following spaghetti better
                    for file in archive.namelist():
                        for resolution in self._config["raster_resolutions"]:
                            for band in self._config["bands"]:
                                if (
                                    file.endswith(self._config["raster_suffix_input"])
                                    and (resolution in file or resolution == "all")
                                    and (band in file or band == "all")
                                ):
                                    # TODO separate private function
                                    # Do not retain directory structure, just extract
                                    data = archive.read(file, self._input_path)
                                    output_file_path = (
                                        self._output_path / Path(file).name
                                    )
                                    output_file_path.write_bytes(data)

                                    # NOTE: If you want to retain directory structure:
                                    #       comment above, uncomment below
                                    # archive.extract(file, self._output_path)
                                    click.echo(
                                        f"Extracted {Path(file).name} from {filename} to {self._output_path}"
                                    )

    def clip(self):
        pass
