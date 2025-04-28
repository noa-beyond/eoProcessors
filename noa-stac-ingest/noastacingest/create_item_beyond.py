"""
Generic helper functions for creating Beyond specific STAC items
"""
from typing import Final

from pathlib import Path
import logging

import antimeridian

from shapely import Geometry
from shapely.geometry import mapping as shapely_mapping
from shapely.geometry import shape as shapely_shape
from shapely.geometry import box as shapely_box
from shapely.validation import make_valid

from pystac import Item, Provider
from pystac.utils import now_to_rfc3339_str

from noastacingest.utils import get_raster_bbox


logger = logging.getLogger(__name__)


COORD_ROUNDING: Final[int] = 6


def create_wrf_item(
    path: Path,
    additional_providers: list[Provider]
):
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


def create_sentinel_2_monthly_median_item(
    path: Path,
    additional_providers: list[Provider]
):
    """
    Create a STAC Item from monthly median composites.
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

    processed = []
    for image in path.glob("*.tif"):
        # WARNING: pattern: a_complex_area_name_startingDate_endingDate_band.tif"
        try:
            assert len(str(image).split("_")) > 3
        except AssertionError:
            logger.error("Filename does not follow name_date_date_band.tif pattern")
            continue
        parts = str(image).split("_")

        # get the "an_area_startDate_endDate" part, exclude "..._band.tif"
        area = "_".join(parts[0:-3])
        area_dates = "_".join(parts[0:-1])
        if area_dates not in processed:
            processed.append(area_dates)
            scene_id = "_".join(["S2", "MM", parts[-3], parts[-2], area])

            # TODO that's not a proper way to construct a datetime
            # However, I do not think we will be able to have a datetime which makes
            # sense in a monthly aggregation
            start_datetime = parts[-3] + "T00:00:00.000000Z"
            end_datetime = parts[-2] + "T00:00:00.000000Z"

            # ensure that we have a valid geometry, fixing any antimeridian issues
            geometry = shapely_box(get_raster_bbox(image))
            shapely_geometry = shapely_shape(antimeridian.fix_shape(geometry))
            geometry = make_valid(shapely_geometry)
            if (ga := geometry.area) > 100:
                raise Exception(f"Area of geometry is {ga}, which is too large to be correct.")
            bbox = [round(v, COORD_ROUNDING) for v in antimeridian.bbox(geometry)]

            # ID: S2_MM_DATEFROM_DATETO_AREANAME
            item = Item(
                id=scene_id,
                geometry=shapely_mapping(geometry),
                bbox=bbox,
                datetime=None,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                properties={"created": now_to_rfc3339_str()},
            )
            item.common_metadata.providers = additional_providers

            return item
