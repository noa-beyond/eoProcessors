import pyproj
import pystac_client
import stackstac
import xarray as xr
from shapely.geometry import box
from shapely.ops import transform
import dask.diagnostics
import matplotlib.pyplot as plt
import pandas as pd
from dask.distributed import Client
import odc.stac
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime
import numpy as np
import os


def digitize_date(month,day):    
    if month<10:
        month=f'0{month}'
    
    if day<10:
        day=f'0{day}'
    
    return month,day

# STAC connection information for Sentinel 2 COGs
STAC_URL = "https://earth-search.aws.element84.com/v0"
STAC_COLLECTION = "sentinel-s2-l2a-cogs"

# spatial projection information
CRS_STRING = "epsg:4326"
EPSG = pyproj.CRS.from_string(CRS_STRING).to_epsg()

# Area of Interest (Marseille) in WGS84
AOI = box(2.0805106853903363,42.3815567903073216,7.6901801340472709,45.2725885681995308)
bounds = (2.0805106853903363,42.3815567903073216,7.6901801340472709,45.2725885681995308)

# BANDS = ["B04", "B03", "B02"]
START_DATE = "2017-01-01"
END_DATE = "2024-04-29"

# STAC items store bounding box info in epsg:4326
transformer_4326 = pyproj.Transformer.from_crs(
    crs_from=CRS_STRING,
    crs_to="epsg:4326",
    always_xy=True,
)
bbox_4326 = transform(transformer_4326.transform, AOI).bounds

catalog = pystac_client.Client.open(STAC_URL)
catalog.add_conforms_to("ITEM_SEARCH")

stac_items = catalog.search(
    collections=[STAC_COLLECTION],
    bbox=bbox_4326,
    datetime=[START_DATE, END_DATE],
    query={"eo:cloud_cover": {"lt": 10}}
)#.item_collection()

# using Open Data Cube stac component
from odc.geo.geobox import GeoBox
dx = 3/3600  # ~90m resolution

epsg = 4326
geobox = GeoBox.from_bbox(bounds, crs=f"epsg:{epsg}", resolution=dx)

ds_odc = odc.stac.load(
    stac_items.items(),
    assets=["red", "green", "blue"],
    chunks={},
    geobox=geobox,
    resampling="bilinear",
    groupby="solar_day" # delete duplicates due to satellite overlap
)

ds_odc_res = ds_odc.resample(time='QS-DEC').median("time")

data = ds_odc_res

numb_days= data[["B04"]].time.size

existing_rgb = []
for rgb in os.listdir('rgb'):
    existing_rgb.append(rgb)

for t in range(0,numb_days):
    try:
      rgb_image = data[["B04","B03","B02"]].isel(time=t).to_array()
      dt = pd.to_datetime(rgb_image.time.values)
      
      year,month,day = dt.year, dt.month, dt.day

      month,day = digitize_date(month,day)
    
      date_string= f"{year}_{month}_{day}"
      date_string_title= f"{month}/{day}/{year}"
      
      full_path = "rgb/rgb_image_" + date_string + ".png"
     
      print("Checking img:",full_path)
      
      if os.path.exists(full_path) or full_path in existing_rgb:
          print("Already exists")
          continue
    
      print("Plotting image...")
      count1 = np.count_nonzero(rgb_image[1,:,:])
      count2 = np.size(rgb_image[1,:,:])
      ratio= count1/count2
    
      # Print Date and Coverage 
      print(f"Date:{date_string}, Coverage:{format(ratio*100,'.2f')}%")
    
      if ratio>0.7:
        # Plot the RGB image
        fig, ax = plt.subplots()
        rgb_image.plot.imshow(robust=True)
        plt.title(f"Marseille {date_string_title} by Sentinel-2")
        plt.savefig(f"rgb/rgb_image_{date_string}.png")
        plt.close(fig)
    except Exception as e:
        print(e)