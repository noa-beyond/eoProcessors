"""
Generic helper functions for creating Beyond specific STAC items
"""

from typing import Final

from pathlib import Path
from datetime import datetime, timezone
import logging

import antimeridian

from shapely import Geometry
from shapely.geometry import mapping as shapely_mapping
from shapely.geometry import shape as shapely_shape
from shapely.validation import make_valid

import pystac
from pystac import Asset, Item, Provider
from pystac.utils import now_to_rfc3339_str

from pystac.extensions.eo import EOExtension
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension, RasterBand

from stactools.sentinel2.stac import band_from_band_id
from stactools.sentinel2.constants import (
    BANDS_TO_ASSET_NAME,
    UNSUFFIXED_BAND_RESOLUTION,
    ASSET_TO_TITLE,
)

import rasterio

logger = logging.getLogger(__name__)

COORD_ROUNDING: Final[int] = 6


def create_wrf_item(path: Path, additional_providers: list[Provider]):
    """
    Weather Research and Forecasting Beyond data.
    STAC Item creation based on Sentinel2 stactools item creation
    """

    # ensure that we have a valid geometry, fixing any antimeridian issues
    # get the Geometry and create a shapely Geometry
    geometry = Geometry()
    shapely_geometry = shapely_shape(antimeridian.fix_shape(geometry))
    geometry = make_valid(shapely_geometry)
    if (ga := geometry.area) > 100:
        raise Exception(f"Area of geometry is {ga}, which is too large to be correct.")
    bbox = [round(v, COORD_ROUNDING) for v in antimeridian.bbox(geometry)]

    # id: filename or something. Should first read the file
    # datetime whatever you want for that item
    item = Item(
        id=metadata.scene_id,
        geometry=shapely_mapping(geometry),
        bbox=bbox,
        datetime=metadata.datetime,
        properties={"created": now_to_rfc3339_str()},
    )
    item.common_metadata.providers = additional_providers

    return item


def create_sentinel_2_monthly_median_items(
    path: Path, additional_providers: list[Provider]
) -> set[Item]:
    """
    Create a STAC Item from S2 monthly median composites.
    Code is based on how Sentinel 2 stactools creates Items.
    Gets a path and has to:
    1) For every raster (.tif) in path / (or for every month range?)
    1a) Which has a pattern of "name_datefrom_dateto_band.tif"
    1b) or, if item (name, date) exists, skip that file
    2) Reads a custom metadata file inside that folder
    (we need a way to include name, dates etc like an area string (e.g. North_East_Greece)
    3) Get all bands and add them to new Item, so that
    5) A single item is per area and month but has separate assets
    per band.
    """
    # TODO choose:
    #  each beyond median product must have a metadata file from which
    #  the STAC Item will be created
    #    area="North_West_Greece",
    #    month=2,
    #    year=2022,
    # OR
    #  derive this information from filenames without extra metadata files
    processed = set()
    created_items = set()

    for image in path.glob("*.tif"):
        parts = image.stem.split("_")

        if len(parts) <= 3:
            logger.error(
                "Invalid filename pattern: %s (expected name_date_date_band.tif)",
                image.name,
            )
            continue

        area_dates = "_".join(parts[:-1])
        if area_dates not in processed:
            processed.add(area_dates)

            area = "_".join(parts[:-3])
            scene_id = "_".join(["S2", "MM", parts[-3], parts[-2], area])

            with rasterio.open(image) as src:
                bounds = src.bounds
                bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
                geometry = {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [bounds.left, bounds.bottom],
                            [bounds.left, bounds.top],
                            [bounds.right, bounds.top],
                            [bounds.right, bounds.bottom],
                            [bounds.left, bounds.bottom],
                        ]
                    ],
                }
                crs = src.crs.to_epsg()
                transform = src.transform
                shape = [src.height, src.width]

            start_datetime = datetime.strptime(parts[-3], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            end_datetime = datetime.strptime(parts[-2], "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )

            item = Item(
                id=scene_id,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                geometry=geometry,
                bbox=bbox,
                datetime=None,
                properties={"created": now_to_rfc3339_str()},
                collection="s2_monthly_median",
            )
            item.common_metadata.providers = additional_providers
            # TODO NOA-Product id as in service is external to this item creation

            projection = ProjectionExtension.ext(item, add_if_missing=True)
            projection.epsg = crs
            centroid = antimeridian.centroid(item.geometry)
            projection.centroid = {
                "lat": round(centroid.y, 5),
                "lon": round(centroid.x, 5),
            }

            # Add eo for bands in assets and reserve for future cloud coverage
            # eo = EOExtension.ext(item, add_if_missing=True)
            # eo.cloud_cover = metadata.cloudiness_percentage
            EOExtension.add_to(item)
            RasterExtension.add_to(item)
            # Add each band as an asset
            band_paths = {}
            for band_file in path.glob(area_dates + "*"):
                band = band_file.name.rsplit("_", maxsplit=1)[-1].split(".")[0]
                band_paths[band] = band_file

            for band_name, band_path in band_paths.items():
                with rasterio.open(band_path) as src:
                    dtype = src.dtypes[0]
                    nodata = src.nodata
                    resolution = (src.res[0], src.res[1])
                    shape = [src.height, src.width]
                    transform = src.transform
                    crs = src.crs.to_epsg()

                resolution = highest_asset_res(band)
                asset_id = BANDS_TO_ASSET_NAME[band]

                asset = Asset(
                    href=str(band_path.resolve()),
                    media_type=pystac.MediaType.GEOTIFF,
                    roles=["data", "aggregation"],
                    title=f"{ASSET_TO_TITLE[asset_id.split('_')[0]]} - {resolution}m",
                )

                eo_band = band_from_band_id(band)
                EOExtension.ext(asset).bands = [eo_band]

                # TODO check if needed: (from stactools, sentinel 2 stac.py)
                # (it's how it creates the projection extension)
                # set_asset_properties(asset, band_gsd)
                proj_asset = ProjectionExtension.ext(asset)
                proj_asset.epsg = crs
                proj_asset.transform = list(transform)[:6]
                proj_asset.shape = shape

                raster_asset = RasterExtension.ext(asset)
                raster_asset.bands = [
                    RasterBand.create(
                        data_type=dtype,
                        nodata=nodata,
                        spatial_resolution=resolution,  # TODO check best practices for this one
                    )
                ]

                item.add_asset(band_name, asset)
            created_items.add(item)
    return created_items


def highest_asset_res(band_id: str) -> int:
    return UNSUFFIXED_BAND_RESOLUTION[BANDS_TO_ASSET_NAME[band_id]]
