"""
Generic helper functions for creating Beyond specific STAC items
"""
from typing import Final, Optional, Pattern

import re
from pathlib import Path
import logging

import antimeridian

from shapely import Geometry
from shapely.geometry import mapping as shapely_mapping
from shapely.geometry import shape as shapely_shape
from shapely.geometry import box as shapely_box
from shapely.validation import make_valid

import pystac
from pystac import Asset, Item, Provider
from pystac.utils import now_to_rfc3339_str
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.eo import EOExtension

from stactools.core.projection import transform_from_bbox
from stactools.sentinel2.stac import band_from_band_id
from stactools.sentinel2.constants import (
    SENTINEL_INSTRUMENTS,
    SENTINEL_CONSTELLATION,
    BANDS_TO_ASSET_NAME,
    UNSUFFIXED_BAND_RESOLUTION,
    ASSET_TO_TITLE
)
from noastacingest.utils import get_raster_bbox, get_raster_size_shape


logger = logging.getLogger(__name__)


# -- Constants --
COORD_ROUNDING: Final[int] = 6
BAND_ID_PATTERN: Final[Pattern[str]] = re.compile(r"[_/](B\d[A\d])")
# ------------------


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

    processed = []
    for image in path.glob("*.tif"):
        # WARNING: pattern: a_complex_area_name_startingDate_endingDate_band.tif"
        try:
            assert len(str(image.name).split("_")) > 3
        except AssertionError:
            logger.error("Filename does not follow name_date_date_band.tif pattern")
            continue
        parts = str(image.name).split("_")

        # get the "an_area_startDate_endDate" part, exclude "..._band.tif"
        area = "_".join(parts[0:-3])
        area_dates = "_".join(parts[0:-1])
        if area_dates not in processed:
            processed.append(area_dates)
            scene_id = "_".join(["S2", "MM", parts[-3], parts[-2], area])

            bands = []
            for band_suffix in path.glob(area_dates + "*"):
                bands.append(parts[-1].split(".")[0])
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


            # Platform cannot be filled, since composites come from different platforms
            # item.common_metadata.platform = metadata.platform.lower()
            item.common_metadata.providers = additional_providers
            item.common_metadata.constellation = SENTINEL_CONSTELLATION
            item.common_metadata.instruments = SENTINEL_INSTRUMENTS
            # TODO NOA-Product id as in service is external to this item creation

            # -- Extensions --

            # Projection Extension
            projection = ProjectionExtension.ext(item, add_if_missing=True)
            projection.epsg = metadata.epsg
            if projection.epsg is None:
                raise ValueError(
                    f"Could not determine EPSG code for {granule_href}; which is required."
                )

            centroid = antimeridian.centroid(item.geometry)
            projection.centroid = {"lat": round(centroid.y, 5), "lon": round(centroid.x, 5)}
            # --Assets--

            image_assets = dict(
                [
                    image_asset_from_similar_path(
                        source_asset_href=image,
                        band=band,
                        proj_bbox=metadata.proj_bbox,
                        media_type=pystac.MediaType.GEOTIFF,
                        boa_add_offsets=metadata.boa_add_offsets,
                    )
                    for band in bands
                ]
            )

            for key, asset in chain(image_assets.items(), metadata.extra_assets.items()):
                assert key not in item.assets
                item.add_asset(key, asset)

            return item


def image_asset_from_similar_path(
    source_asset_href: Path,
    band: str,
    # TODO this proj box
    proj_bbox: list[float],
    media_type: Optional[str],
    # TODO these offsets
    boa_add_offsets: Optional[dict[str, int]] = None,
) -> tuple[str, Asset]:
    band_asset_href = ''.join(
        str(source_asset_href).split("_")[:-1].append("_" + band + ".tif")
    )
    logger.debug(f"Creating asset for image {band_asset_href}")

    asset_media_type = media_type
    # Extract gsd and proj info
    resolution = highest_asset_res(band)
    asset_res = resolution

    shape = list(get_raster_size_shape(band_asset_href))
    # should be something like list(10980, 10980)  # shape of 10m Sentinel 2 raster
    transform = transform_from_bbox(proj_bbox, shape)

    def set_asset_properties(_asset: Asset, _band_gsd: Optional[int] = None):
        if _band_gsd:
            pystac.CommonMetadata(_asset).gsd = _band_gsd
        asset_projection = ProjectionExtension.ext(_asset)
        asset_projection.shape = shape
        asset_projection.bbox = proj_bbox
        asset_projection.transform = transform

    # Handle band image
    # Get the asset resolution from the file name.
    # If the asset resolution is the band GSD, then
    # include the gsd information for that asset. Otherwise,
    # do not include the GSD information in the asset
    # as this may be confusing for users given that the
    # raster spatial resolution and gsd will differ.
    # See https://github.com/radiantearth/stac-spec/issues/1096
    band_gsd: Optional[int] = None
    if asset_res == highest_asset_res(band):
        asset_id = BANDS_TO_ASSET_NAME[band]
        band_gsd = asset_res
    else:
        # If this isn't the default resolution, use the raster
        # resolution in the asset key.
        # TODO: Use the raster extension and spatial_resolution
        # property to encode the spatial resolution of all assets.
        asset_id = f"{BANDS_TO_ASSET_NAME[band]}_{int(asset_res)}m"

    asset = Asset(
        href=band_asset_href,
        media_type=asset_media_type,
        title=f"{ASSET_TO_TITLE[asset_id.split('_')[0]]} - {asset_res}m",
        roles=["data", "aggregation"],
    )

    asset_eo = EOExtension.ext(asset)
    asset_eo.bands = [band_from_band_id(band)]
    set_asset_properties(asset, band_gsd)

    RasterExtension.ext(asset).bands = raster_bands(
        boa_add_offsets, processing_baseline, band, resolution
    )
    maybe_res = extract_gsd(band_asset_href)

    return asset_id, asset


def highest_asset_res(band_id: str) -> int:
    return UNSUFFIXED_BAND_RESOLUTION[BANDS_TO_ASSET_NAME[band_id]]
