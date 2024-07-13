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

    def __init__(self, data_path, output_path, config_file) -> None:
        self._path = data_path
        self._output_path = Path(output_path).absolute()
        pass

    def from_path(self, agg_function):

        for tile in self._get_tiles_list():
            click.echo(f"Processing tile: {tile}\n")
            images, profile = self._get_images_and_profile(tile)
            logger.debug("Calculating aggregation of tile: %s", tile)
            median_img = self._get_median_img(images, 3).astype(rio.uint8)

        # profile.update(
        #         driver="GTiff",
        #         width=median_img.shape[2],
        #         height=median_img.shape[1]
        #     )
            with rio.Env():
                # TODO check output folder (create it first...)
                profile.update(driver='GTiff')
                self._output_path.mkdir(parents=True, exist_ok=True)
                with rio.open(f"{str(self._output_path)}/{tile}_{agg_function}.tif", "w", **profile) as dst:
                    dst.write(median_img)
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

    def _get_median_img(self, imgs, no_of_bands):

        bands_medians = []
        for b in range(no_of_bands):
            bands = [img.sel(band=b+1) for img in imgs]
            bands_comp = xr.concat(bands, dim="band")
            bands_medians.append(bands_comp.median(dim="band", skipna=True))
        return xr.concat(bands_medians, dim="band")

    def _get_tiles_list(self):

        tiles_list = []

        for filename in glob.glob(f"{self._path}/*.tif"):
            tile = filename.split("_")[-5]
            if tile not in tiles_list:
                tiles_list.append(tile)
        return tiles_list
