import os
import glob
import re
import shutil
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

    def __init__(self, input_path: Path, output_path: Path) -> None:
        self._input_path = input_path
        self._output_path = output_path
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
                    str(Path(self._output_path, f"{tile}_{agg_function}.tif")),
                    "w",
                    **profile,
                ) as dst:
                    dst.write(aggregated_img)
            images = []

    def histogram_matching(self, reference_image: str | None):
        for root, dirs, _ in os.walk(str(self._input_path), topdown=True):
            for directory in dirs:
                bands = []
                # TODO: explain the pattern in log or generalize the prefix
                pattern = re.compile(r"(T\d\d.*)_(\d+)T(\d+)_(.*)_(.*)\.tif")
                for file in os.listdir(Path(root, directory)):
                    if pattern.match(str(file)):
                        resolution = str(file).rsplit("_", maxsplit=1)[-1]
                        band = str(file).split("_")[-2]
                        tile = str(file).split("_")[-4]

                        if band not in bands:
                            bands.append(band)
                        else:
                            continue

                        if reference_image is None:
                            ref_image = str(Path(root, directory, file))
                        else:
                            ref_image = reference_image
                        reference = gdal.Open(ref_image, gdal.GA_Update)
                        reference_array = np.array(reference.GetRasterBand(1).ReadAsArray())
                        # A raster (especially after clipping) might be empty (zero values)
                        # Do not use it as reference
                        if not np.any(reference_array):
                            del reference_array
                            continue

                        for file_to_match in os.listdir(Path(root, directory)):
                            if pattern.match(str(file_to_match)):
                                resolution_to_match = str(file_to_match).rsplit("_", maxsplit=1)[-1]
                                band_to_match = str(file_to_match).split("_")[-2]
                                tile_to_match = str(file_to_match).split("_")[-4]
                                filename_to_match = str(Path(root, directory, file_to_match))
                                if (
                                    tile == tile_to_match
                                    and band == band_to_match
                                    and resolution == resolution_to_match
                                    and filename_to_match != ref_image
                                ):
                                    match_folder = Path(self._output_path)
                                    current_dir = str(Path(root, directory))
                                    p = [str(self._input_path), current_dir]
                                    commonprefix = os.path.commonprefix(p)
                                    path_to_save = Path(match_folder, current_dir.replace(commonprefix, ""))
                                    path_to_save.mkdir(parents=True, exist_ok=True)
                                    self._match_and_save(path_to_save, filename_to_match, reference_array)
                                    if not os.path.isfile(Path(path_to_save, file)):
                                        shutil.copy2(ref_image, str(Path(path_to_save, file)))

                        del reference_array

    def difference_vector_per_month(self):

        yearmonth_set = set()

        for root, dirs, files in os.walk(str(self._input_path), topdown=True):
            for directory in dirs:
                for file in os.listdir(Path(root, directory)):
                    if str(file).endswith(".tif") and "dif" not in str(file):
                        yearmonth = str(file).split("_")[-3]
                        if not re.search("\\d{8}T\\d{6}", yearmonth):
                            click.echo(
                                f"Wrong input filename(s): {file}. The third from the "
                                f"end '_' delimiter part, should be the timedate, "
                                f"following the 'YyyyMmDdTHhMmSs' pattern. This "
                                f"follows the Copernicus naming convention. If "
                                f"you need another type of naming for date "
                                f"retrieval, please open a ticket."
                            )
                            return False
                        yearmonth = yearmonth.split("T")[0][:6]
                        yearmonth_set.add(int(yearmonth))
                yearmonth_list = list(sorted(yearmonth_set, key=int))
                for month in yearmonth_list[:-1]:
                    if abs(month) % 100 == 12:
                        next_month = month + 89
                    else:
                        next_month = month + 1
                    for ref_file in os.listdir(Path(root, directory)):
                        if (
                            str(ref_file).endswith(".tif")
                            and os.path.isfile(Path(root, directory, ref_file))
                            and "TOTAL" not in str(ref_file)
                        ):
                            ref_date = str(ref_file).split("_")[-3].split("T")[0]
                            ref_month = int(ref_date[:6])
                            if month == ref_month:
                                difference_vector = []
                                ref_band = str(ref_file).split("_")[-2]
                                ref_tile = str(ref_file).split("_")[-4]
                                da_ref = rioxarray.open_rasterio(
                                    str(Path(root, directory, ref_file))
                                )
                                # Get the metadata from the original image
                                with rio.open(
                                    str(Path(root, directory, ref_file))
                                ) as src:
                                    meta = src.meta
                                # Update the metadata for the output image
                                meta.update(dtype=rio.uint8)
                                for dif_file in os.listdir(Path(root, directory)):
                                    if os.path.isfile(Path(root, directory, dif_file)):
                                        dif_date = (
                                            str(dif_file).split("_")[-3].split("T")[0]
                                        )
                                        dif_month = int(dif_date[:6])
                                        dif_band = str(dif_file).split("_")[-2]
                                        dif_tile = str(dif_file).split("_")[-4]
                                        if (
                                            next_month == dif_month
                                            and ref_band == dif_band
                                            and ref_tile == dif_tile
                                        ):
                                            output_path = Path(
                                                root,
                                                directory,
                                                (str(month) + "_" + str(next_month)),
                                            )
                                            output_path.mkdir(
                                                parents=True, exist_ok=True
                                            )
                                            single_difference_vector = (
                                                self._difference_vector(
                                                    da_ref,
                                                    Path(root, directory, dif_file),
                                                )
                                            )
                                            difference_vector.append(
                                                single_difference_vector
                                            )
                                            output_image = Path(
                                                root,
                                                directory,
                                                output_path,
                                                str(
                                                    ref_tile
                                                    + "_"
                                                    + ref_date
                                                    + "_dif_"
                                                    + dif_date
                                                    + "_"
                                                    + ref_band
                                                    + ".tif"
                                                ),
                                            )
                                            # Save the resulting output image as a GeoTIFF
                                            with rio.open(
                                                output_image, "w", **meta
                                            ) as dst:
                                                dst.write(single_difference_vector)
                                if difference_vector:
                                    # Sum all the elements in the array to produce the final output image
                                    sum_image = np.sum(
                                        difference_vector, axis=0
                                    ).astype(np.uint8)
                                    self._save_difference_vector(
                                        str(Path(root, directory, output_path)), str(Path(root, directory, ref_file)),
                                        sum_image
                                    )
                yearmonth_set.clear()

    # TODO: need to remove here the look for "reference" in file and histomatch in result. Work with folders
    def difference_vector(self, reference_image: str | None):

        for root, dirs, files in os.walk(self._input_path, topdown=True):
            for file in files:
                if (
                    "reference" in file
                    and str(file).endswith(".tif")
                    and "dif_vector" not in file
                ):
                    if reference_image is None:
                        ref_image = str(Path(root, file))
                    else:
                        ref_image = reference_image
                else:
                    continue
                reference_parts = ref_image.split("_")
                da_ref = rioxarray.open_rasterio(ref_image)
                difference_vector = []
                for _, _, filenames in os.walk(root):
                    for filename in filenames:
                        if filename.endswith(".tif"):
                            filename_parts = filename.split("_")
                            if (
                                reference_parts[-1] == filename_parts[-1]
                                and reference_parts[-2] == filename_parts[-2]
                                and reference_parts[-4] == filename_parts[-4]
                                and "reference" not in filename
                                and "dif_vector" not in filename
                                and "histomatch" in filename
                            ):
                                # Get the normalized difference vector
                                # Get its power of 2 and store it in the collection
                                single_difference_vector = self._difference_vector(
                                    da_ref, str(Path(root, filename))
                                )
                                self._save_difference_vector(
                                    root,
                                    ref_image,
                                    single_difference_vector,
                                    filename_parts[-3],
                                )
                                difference_vector.append(single_difference_vector)
                if difference_vector:
                    # Sum all the elements in the array to produce the final output image
                    sum_image = np.sum(difference_vector, axis=0).astype(np.uint8)
                    self._save_difference_vector(root, ref_image, sum_image)

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

    def _match_and_save(self, path_to_save, source, reference_array):

        image_to_match = gdal.Open(source, gdal.GA_Update)
        image_array = np.array(image_to_match.GetRasterBand(1).ReadAsArray())
        if not np.any(image_array):
            del image_to_match
            return
        matched = match_histograms(image_array, reference_array, channel_axis=None)

        geotransform = image_to_match.GetGeoTransform()
        projection = image_to_match.GetProjection()

        creation_options = ["COMPRESS=LZW", "TILED=YES"]

        output_image = str(path_to_save) + "/" + source.split("/")[-1]
        print(f"Matched: {output_image}")

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

    def _difference_vector(self, reference_data_array, image_filename):
        da = rioxarray.open_rasterio(image_filename)
        difference = reference_data_array - da
        difference = difference * difference
        # Normalize the difference to the 0-255 range
        difference_min = difference.min().item()
        difference_max = difference.max().item()
        # Prevent division by zero in case of a flat image
        if difference_min == difference_max:
            normalized_difference = np.zeros_like(difference, dtype=np.uint8)
        else:
            normalized_difference = (
                (difference - difference_min) / (difference_max - difference_min) * 255
            ).astype(np.uint8)

        # return difference
        return normalized_difference

    def _save_difference_vector(
        self, parent_folder, reference_image, dif_image, source_filename_part=None
    ):

        source_text = "TOTAL"
        if source_filename_part:
            source_text = source_filename_part
        ref_file = reference_image.split("/")[-1].split("_")
        file_string = (
            ref_file[-4]
            + "_"
            + ref_file[-3]
            + "_dif_"
            + source_text
            + "_"
            + ref_file[-2]
            + "_"
            + ref_file[-1]
        )
        output_image = str(Path(parent_folder, file_string))
        print(output_image)

        # Get the metadata from the original image
        with rio.open(reference_image) as src:
            meta = src.meta
        # Update the metadata for the output image
        meta.update(dtype=rio.uint8)

        # Save the resulting output image as a GeoTIFF
        with rio.open(output_image, "w", **meta) as dst:
            dst.write(dif_image)  # Write the first band
