"""Main Preprocess module"""

from __future__ import annotations

import os
import logging
import json
import glob
import zipfile
from pathlib import Path, PurePath

import rasterio as rio
from rasterio.io import MemoryFile
from rasterio.mask import mask
from rio_cogeo.profiles import cog_profiles
from rio_cogeo.cogeo import cog_translate
from shapely.geometry import box, shape, mapping
from shapely.ops import transform
import shapefile
import pyproj

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

        self._input_path = input_path
        self._output_path = output_path
        os.makedirs(str(self._output_path), exist_ok=True)

        with open(config_file, encoding="utf8") as f:
            self._config = json.load(f)

    def extract(self):
        for filename in os.listdir(str(self._input_path)):
            if filename.endswith(self._config["input_file_type"]):
                zip_path = Path(self._input_path, filename)
                platform = zip_path.name.split("_")[0]
                with zipfile.ZipFile(str(zip_path), "r") as archive:
                    if "S2" in platform:
                        self.extract_s2(zip_path, archive)
                    elif "S1" in platform:
                        self.extract_s1(zip_path, archive)
                    else:
                        self.generic_extract(zip_path, archive)

    def clip(self, shapefile_path):
        for root, dirs, files in os.walk(shapefile_path):
            for file in files:
                if file.endswith(".shp"):
                    shapefile_path = os.path.join(root, file)
                    for tif_root, _, tif_files in os.walk(self._input_path):
                        for raster_file in tif_files:
                            if raster_file.endswith(self._config['raster_suffix_input']):
                                raster_path = os.path.join(tif_root, raster_file)
                                raster_bbox = self._get_raster_bbox(raster_path)

                                raster_crs = self._get_raster_crs(raster_path)
                                transformed_shapefile_geom = self._transform_shapefile_geometry(
                                    shapefile_path, raster_crs
                                )
                                transformed_shapefile_bbox = transformed_shapefile_geom.bounds

                                shapefile_bbox_polygon = box(*transformed_shapefile_bbox)

                                if raster_bbox.intersects(shapefile_bbox_polygon):
                                    shp_output_path = Path(
                                        self._output_path, PurePath(shapefile_path).parent.name
                                    )
                                    os.makedirs(shp_output_path, exist_ok=True)
                                    output_raster_path = os.path.join(
                                        shp_output_path,
                                        f"clipped_{Path(raster_path).stem}{self._config['raster_suffix_output']}",
                                    )
                                    self._clip_raster_with_rasterio(
                                        raster_path, shapefile_path, output_raster_path
                                    )

    # TODO: with "self", you pass the whole object, where you don't need it. It needs
    # triage, to separate functions to utils and important ones as private.
    def extract_s2(self, zip_path, archive):
        for file in archive.namelist():
            for resolution in self._config["raster_resolutions"]:
                for band in self._config["bands"]:
                    if (
                        file.endswith(self._config["raster_suffix_input"])
                        and (resolution in file or resolution == "all")
                        and (band in file or band == "all")
                    ):
                        filename_parts = Path(file).name.split("_")
                        tile = filename_parts[-4]
                        year = filename_parts[-3].split("T")[0][:4]
                        month = filename_parts[-3].split("T")[0][4:6]
                        day = filename_parts[-3].split("T")[0][6:8]
                        data = archive.read(file, self._input_path)
                        output_file_path = Path(
                            self._output_path, tile, year, month, day, Path(file).name
                        )
                        os.makedirs(
                            Path(self._output_path, tile, year, month, day),
                            exist_ok=True,
                        )
                        output_file_path.write_bytes(data)

                        if self._config.get("convert_to_cog", False):
                            cog_output_path = str(output_file_path).replace(
                                self._config["raster_suffix_input"],
                                f'-cog{self._config["raster_suffix_output"]}',
                            )
                            self._convert_to_cog(output_file_path, cog_output_path)
                            os.remove(output_file_path)

                        click.echo(
                            f"Extracted {Path(file).name} from {zip_path} to {self._output_path}"
                        )

    def extract_s1(self, zip_path, archive):
        default_s1_raster_suffix = ".tiff"
        for file in archive.namelist():
            if file.endswith(default_s1_raster_suffix):
                # absoluteOrbitNumber
                filename_parts = Path(file).name.split("-")
                orbit_number = filename_parts[-3]
                year = filename_parts[-5].split("T")[0][:4]
                month = filename_parts[-5].split("T")[0][4:6]
                day = filename_parts[-5].split("T")[0][6:8]
                data = archive.read(file, self._input_path)
                output_file_path = Path(
                    self._output_path, orbit_number, year, month, day, Path(file).name
                )
                os.makedirs(
                    Path(self._output_path, orbit_number, year, month, day),
                    exist_ok=True,
                )
                output_file_path.write_bytes(data)

                if self._config.get("convert_to_cog", False):
                    # Sentinel 1 has a "new" COG translated Product. In case
                    # you have not downloaded that, then:
                    if "COG" not in str(zip_path):
                        cog_output_path = str(output_file_path).replace(
                            default_s1_raster_suffix, f"-cog{default_s1_raster_suffix}"
                        )
                        self._convert_to_cog(output_file_path, cog_output_path)
                        os.remove(output_file_path)

                click.echo(
                    f"Extracted {Path(file).name} from {zip_path} to {self._output_path}"
                )

    def generic_extract(self, zip_path, archive: zipfile.ZipFile):

        try:
            archive.extractall(path=self._output_path)
            click.echo(f"Extracted {zip_path} to {self._output_path}")
        except NotADirectoryError as e:
            click.echo(
                f"Please note that some zipped files cannot be extracted in the same folder as the source zip. "
                f"(e.g. Sentinel 3 .SEN3 files, which do not have a '.zip' suffix) "
                f"Please change output folder. Error: {e}"
            )

    def _get_shapefile_bbox(self, shapefile_path):
        with shapefile.Reader(
            shapefile_path, encoding=self._get_encoding(shapefile_path)
        ) as shp:
            shapes = shp.shapes()
            bbox = shapes[0].bbox  # xmin, ymin, xmax, ymax
            bbox_polygon = box(*bbox)
        return bbox_polygon

    def _get_raster_bbox(self, raster_path):
        with rio.open(raster_path) as src:
            bbox = src.bounds  # left, bottom, right, top
            bbox_polygon = box(*bbox)
        return bbox_polygon

    def _get_raster_crs(self, raster_path):
        with rio.open(raster_path) as src:
            return src.crs

    def _get_shapefile_crs(self, shapefile_path):
        prj_path = shapefile_path.replace(".shp", ".prj")
        with open(prj_path, "r") as prj_file:
            prj = prj_file.read()
        return pyproj.CRS(prj)

    def _get_encoding(self, shapefile_path):
        encoding = "utf-8"
        cpg_path = shapefile_path.replace(".shp", ".cpg")
        if os.path.exists(cpg_path):
            with open(cpg_path, "r") as cpg_file:
                for line in cpg_file:
                    encoding = str(line).split("_", maxsplit=1)[0]
        return encoding

    def _transform_shapefile_geometry(self, shapefile_path, target_crs):
        source_crs = self._get_shapefile_crs(shapefile_path)
        with shapefile.Reader(
            shapefile_path, encoding=self._get_encoding(shapefile_path)
        ) as shp:
            geom = shape(shp.shape(0).__geo_interface__)

        project = pyproj.Transformer.from_crs(
            source_crs, target_crs, always_xy=True
        ).transform
        transformed_geom = transform(project, geom)
        return transformed_geom

    def _clip_raster_with_rasterio(
        self, raster_path, shapefile_path, output_raster_path
    ):
        raster_crs = self._get_raster_crs(raster_path)
        geom = self._transform_shapefile_geometry(shapefile_path, raster_crs)

        with rio.open(raster_path) as src:
            out_image, out_transform = mask(
                src, [mapping(geom)], crop=True, filled=True, nodata=src.nodata
            )
            out_meta = src.meta.copy()
            out_meta.update(
                {
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform,
                    "nodata": self._config.get("nodata_custom_value", src.nodata),
                }
            )

        with rio.open(output_raster_path, "w", **out_meta) as dest:
            dest.write(out_image)

        print(
            f"Clipped {raster_path} using {shapefile_path} and saved to {output_raster_path}"
        )

    # Based on https://guide.cloudnativegeo.org/cloud-optimized-geotiffs/writing-cogs-in-python.html
    def _convert_to_cog(self, original_raster, cog_filename):

        with rio.Env(GDAL_DRIVER_NAME="JP2OpenJPEG"):
            with rio.open(original_raster, "r") as src:
                arr = src.read()
                kwargs = src.meta
                kwargs.update(driver="GTiff", predictor=2)
                config = {"GDAL_NUM_THREADS": "ALL_CPUS", "TILED": "TRUE"}

            with MemoryFile() as memfile:
                # Opening an empty MemoryFile for in memory operation - faster
                with memfile.open(**kwargs) as mem:
                    # Writing the array values to MemoryFile using the rasterio.io module
                    # https://rasterio.readthedocs.io/en/stable/api/rasterio.io.html
                    mem.write(arr)

                    dst_profile = cog_profiles.get("lerc_deflate")

                    # Creating destination COG
                    cog_translate(
                        mem,
                        cog_filename,
                        dst_profile,
                        config=config,
                        forward_band_tags=True,
                        forward_ns_tags=True,
                        use_cog_driver=True,
                        in_memory=False,
                    )
