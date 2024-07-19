import os
import glob
from pathlib import Path
import logging
import click
import xarray as xr
import rasterio as rio
import rioxarray
from osgeo import gdal
import numpy as np

from skimage.exposure import match_histograms


logger = logging.getLogger(__name__)


class Aggregate:

    def __init__(self, input_path, output_path) -> None:
        self._input_path = input_path
        self._output_path = Path(output_path).absolute()
        self._output_path.mkdir(parents=True, exist_ok=True)

    def from_path(self, agg_function):

        for tile in self._get_tiles_list():
            click.echo(f"Processing tile: {tile}")
            images, profile = self._get_images_and_profile(tile)
            logger.debug("Calculating aggregation of tile: %s", tile)
            aggregated_img = self._get_aggregated_img(agg_function, images, 3).astype(
                rio.uint8
            )

            with rio.Env():
                # TODO check output folder (create it first...)
                profile.update(driver="GTiff")
                self._output_path.mkdir(parents=True, exist_ok=True)
                with rio.open(
                    f"{str(self._output_path)}/{tile}_{agg_function}.tif",
                    "w",
                    **profile,
                ) as dst:
                    dst.write(aggregated_img)
            images = []

    def histogram_matching(self, reference_image: str | None):

        for root, dirs, files in os.walk(self._input_path, topdown=True):
            for file in files:
                if "reference" in file and str(file).endswith(".tif"):
                    if reference_image is None:
                        ref_image = str(Path(root, file))
                    else:
                        ref_image = reference_image
                else:
                    continue
                reference_parts = ref_image.split("_")
                reference = gdal.Open(ref_image, gdal.GA_Update)
                reference_array = np.array(reference.GetRasterBand(1).ReadAsArray())
                for filename in glob.glob(f"{root}/*.tif"):
                    filename_parts = filename.split("_")
                    if (
                        reference_parts[-1] == filename_parts[-1]
                        and reference_parts[-2] == filename_parts[-2]
                        and reference_parts[-4] == filename_parts[-4]
                        and "reference" not in filename
                    ):
                        self._match_and_save(root, filename, reference_array)
                del reference_array

    def _get_images_and_profile(self, tile):
        tci_images = []
        profile = None
        for root, _, files in os.walk(self._input_path, topdown=True):
            for fname in files:
                if fname.endswith(".tif") and fname.split("_")[-5] == tile:
                    if profile is None:
                        temp_image = rio.open(os.path.join(root, fname))
                        profile = temp_image.profile
                    img = rioxarray.open_rasterio(
                        os.path.join(root, fname), chunks={"x": 1024, "y": 1024}
                    )
                    img = img.where(img != 0.0, np.nan)
                    tci_images.append(img)
        return (tci_images, profile)

    def _get_aggregated_img(self, agg_function, imgs, no_of_bands):

        bands_aggregation = []
        for b in range(no_of_bands):
            bands = [img.sel(band=b + 1) for img in imgs]
            bands_comp = xr.concat(bands, dim="band")
            bands_aggregation.append(
                getattr(bands_comp, agg_function)(dim="band", skipna=True)
            )
        return xr.concat(bands_aggregation, dim="band")

    def _get_tiles_list(self):

        tiles_list = []

        for filename in glob.glob(f"{self._input_path}/*.tif"):
            tile = filename.split("_")[-5]
            if tile not in tiles_list:
                tiles_list.append(tile)
        return tiles_list

    def _match_and_save(self, parent_folder, source, reference_array):
        image_to_match = gdal.Open(source, gdal.GA_Update)
        image_array = np.array(image_to_match.GetRasterBand(1).ReadAsArray())

        matched = match_histograms(
            image_array, reference_array, channel_axis=None
        )

        geotransform = image_to_match.GetGeoTransform()
        projection = image_to_match.GetProjection()

        creation_options = ["COMPRESS=LZW", "TILED=YES"]

        output_image = (
            parent_folder
            + "/"
            + "histomatch_"
            + source.split("/")[-1]
        )
        print(output_image)

        driver = gdal.GetDriverByName("GTiff")
        dst_dataset = driver.Create(
            output_image,
            image_to_match.RasterXSize,
            image_to_match.RasterYSize,
            1,
            gdal.GDT_UInt16,
            options=creation_options,
        )
        dst_dataset.SetGeoTransform(geotransform)
        dst_dataset.SetProjection(projection)

        dst_band = dst_dataset.GetRasterBand(1)
        dst_band.WriteArray(matched)
        dst_band.FlushCache()

        dst_band.SetNoDataValue(0)
        # Clean up
        del image_to_match
        del dst_dataset
