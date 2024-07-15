import os
import glob
from pathlib import Path
import logging
import click
import xarray as xr
import rasterio as rio
import rioxarray
import numpy as np

logger = logging.getLogger(__name__)


class Aggregate:

    def __init__(self, data_path, output_path) -> None:
        self._path = data_path
        self._output_path = Path(output_path).absolute()
        pass

    def from_path(self, agg_function):

        for tile in self._get_tiles_list():
            click.echo(f"Processing tile: {tile}")
            images, profile = self._get_images_and_profile(tile)
            logger.debug("Calculating aggregation of tile: %s", tile)
            aggregated_img = self._get_aggregated_img(agg_function, images, 3).astype(rio.uint8)

            with rio.Env():
                # TODO check output folder (create it first...)
                profile.update(driver='GTiff')
                self._output_path.mkdir(parents=True, exist_ok=True)
                with rio.open(f"{str(self._output_path)}/{tile}_{agg_function}.tif", "w", **profile) as dst:
                    dst.write(aggregated_img)
            images = []

    def _get_images_and_profile(self, tile):
        tci_images = []
        profile = None
        for root, dirs, files in os.walk(self._path, topdown=True):
            for fname in files:
                if fname.endswith(".tif") and fname.split("_")[-5] == tile:
                    if profile is None:
                        temp_image = rio.open(os.path.join(root, fname))
                        profile = temp_image.profile
                    img = rioxarray.open_rasterio(os.path.join(root, fname), chunks={"x": 1024, "y": 1024})
                    img = img.where(img != 0.0, np.nan)
                    tci_images.append(img)
        return (tci_images, profile)

    def _get_aggregated_img(self, agg_function, imgs, no_of_bands):

        bands_aggregation = []
        for b in range(no_of_bands):
            bands = [img.sel(band=b+1) for img in imgs]
            bands_comp = xr.concat(bands, dim="band")
            bands_aggregation.append(getattr(bands_comp, agg_function)(dim="band", skipna=True))
        return xr.concat(bands_aggregation, dim="band")

    def _get_tiles_list(self):

        tiles_list = []

        for filename in glob.glob(f"{self._path}/*.tif"):
            tile = filename.split("_")[-5]
            if tile not in tiles_list:
                tiles_list.append(tile)
        return tiles_list
