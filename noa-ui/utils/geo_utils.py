def bbox_to_polygon(bbox):
    """
    Converts a bounding box [xmin, ymin, xmax, ymax] into a GeoJSON Polygon.

    Parameters:
    - bbox: List of floats [xmin, ymin, xmax, ymax].

    Returns:
    - dict: GeoJSON Polygon.
    """
    if len(bbox) != 4:
        raise ValueError("Bounding box must be a list of 4 coordinates: [xmin, ymin, xmax, ymax]")

    xmin, ymin, xmax, ymax = bbox

    coordinates = [
        [xmin, ymin],  
        [xmax, ymin],  
        [xmax, ymax], 
        [xmin, ymax],  
        [xmin, ymin], 
    ]

    polygon = {
        "type": "Polygon",
        "coordinates": [coordinates],
    }

    return polygon
