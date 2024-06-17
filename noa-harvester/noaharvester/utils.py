"""Utility functions"""
import logging

from pyproj import Transformer, CRS
import shapefile

logger = logging.getLogger(__name__)


def get_bbox_from_shp(shp_path: str) -> list:
    """
    Get bbox from shape file path. The path should have two files: .shp and .prj.
    Function transforms from source CRS (through prj file) to default EPSG:4326
    projection, which is used by the providers.

    Parameters:
        shp_path (str): The shapefile path. It should contain .shp and .prj files.

    Returns:
        [west, south, east, north] (list(float)): Bounding box coordinates.
    """

    shp_path_shape = shp_path + ".shp"
    shp_path_projection = shp_path + ".prj"
    target_crs = "EPSG:4326"  # default CRS

    sf = shapefile.Reader(shp_path_shape)
    minx, miny, maxx, maxy = sf.bbox

    logger.debug("Shapefile (.shp) bounging box coordinates: %s", sf.bbox)

    with open(shp_path_projection, "r", encoding="utf-8") as f:
        wkt = f.read()
        prj_crs = CRS.from_wkt(wkt)

    logger.debug("Source CRS: %s", prj_crs)

    transformer = Transformer.from_crs(prj_crs, target_crs)
    minx, miny = transformer.transform(minx, miny)  # pylint:disable=unpacking-non-sequence
    maxx, maxy = transformer.transform(maxx, maxy)  # pylint:disable=unpacking-non-sequence

    west = miny
    south = minx
    east = maxy
    north = maxx

    logger.debug(
        "Transformed coordinates: \n west: %s, south: %s, east: %s, north: %s",
        west, south, east, north
    )

    return [west, south, east, north]
