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

logger = logging.getLogger(__name__)


class Aggregate:

    def __init__(self, input_path, output_path) -> None:
        self._path = input_path
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

    def histogram_stretch(self, reference_image: str):

        reference_parts = reference_image.split("_")
        reference_stats = self._compute_statistics(reference_image)
        print(reference_parts)
        print("Reference Image Statistics:", reference_stats)
        print("---------------------------")

        for filename in glob.glob(f"{self._path}/*.tif"):
            filename_parts = filename.split("_")
            if (reference_parts[1] == filename_parts[1] and
                    reference_parts[3] == filename_parts[3] and
                    "REF" not in filename):
                print(filename_parts)
                source_stats = self._compute_statistics(filename)
                print("Source Image Statistics:", source_stats)
                source_min, source_max, source_mean, source_stddev = source_stats
                reference_min, reference_max, reference_mean, reference_stddev = reference_stats

                # Calculate the scaling factor and offset
                scale = reference_stddev / source_stddev
                offset = reference_mean - (scale * source_mean)
                print(f"Scale: {scale}")
                print(f"Offset: {offset}")
                print(self._output_path)
                self._output_path.mkdir(parents=True, exist_ok=True)
                output_image = str(self._output_path) + "/" + filename.split("/")[-1] + "_stretched.tif"
                print(output_image)
                self._apply_transformation(filename, output_image, scale, offset, reference_min, reference_max)

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

    def _compute_statistics(self, image_path):
        dataset = gdal.Open(image_path)
        band = dataset.GetRasterBand(1)
        stats = band.GetStatistics(True, True)  # min, max, mean, stddev
        return stats

    def _apply_transformation(self, input_path, output_path, scale, offset, reference_min, reference_max):
        src_dataset = gdal.Open(input_path, gdal.GA_Update)
        src_band = src_dataset.GetRasterBand(1)
        src_data = src_band.ReadAsArray().astype(np.uint8)

        # Apply the scaling and offset
        transformed_data = (src_data * scale) + offset

        # Optionally, clip the data to the valid range (e.g., 0-255 for 8-bit images)
        transformed_data = np.clip(transformed_data, 0, 255)

        # Get the geotransform and projection from the source image
        geotransform = src_dataset.GetGeoTransform()
        projection = src_dataset.GetProjection()

        creation_options = [
            'COMPRESS=LZW',
            'TILED=YES'
        ]

        # Create the output image
        driver = gdal.GetDriverByName('GTiff')
        dst_dataset = driver.Create(output_path, src_dataset.RasterXSize, src_dataset.RasterYSize, 1, gdal.GDT_UInt16, options=creation_options)
        dst_dataset.SetGeoTransform(geotransform)
        dst_dataset.SetProjection(projection)

        # Write the transformed data to the output image
        dst_band = dst_dataset.GetRasterBand(1)
        dst_band.WriteArray(transformed_data)
        dst_band.FlushCache()

        dst_band.SetNoDataValue(0)
        # Clean up
        del src_dataset
        del dst_dataset
