"""Main Preprocess module"""

from __future__ import annotations

import os
import logging
import json
import glob
from pathlib import Path, PurePath

import rasterio as rio
from rasterio.mask import mask
from shapely.geometry import box, shape, mapping
from shapely.ops import transform
import shapefile
import pyproj

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

    def clip(self, shapefile_path):
        for root, dirs, files in os.walk(shapefile_path):
            for file in files:
                if file.endswith(".shp"):
                    shapefile_path = os.path.join(root, file)

                    for raster_file in glob.glob(
                        os.path.join(
                            self._input_path, f"*{self._config['raster_suffix_input']}"
                        )
                    ):
                        raster_path = raster_file
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
            with open(cpg_path) as cpg_file:
                for line in cpg_file:
                    encoding = str(line).split("_")[0]
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
