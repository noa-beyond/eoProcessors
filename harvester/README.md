# Using the harvester processor (Under construction)

## Prerequisites 
### Common for all platforms

* Required hub registrations:
  * ### For Copernicus, Sentinel data (Sentinel 2 ...)
    Users must be registered with the Copernicus Dataspace Eco System to access and download the data. Registration is free and can be completed on the Copernicus Dataspace Eco System (https://dataspace.copernicus.eu/).
  * ### For MODIS/VIISS  
  * (please note Citation guidelines: https://modis.ornl.gov/citation.html)

### Windows
* Docker: Docker must be installed on your machine. (include links)
* Git (and a shell environment): https://git-scm.com/download/win

### Linux
* Docker:
* Git. Usually alredy present, if not:
   
## Next steps

Open a terminal window and navigate to your prefered working directory.
Then download the source code:

```
git clone https://github.com/Agri-Hub/eoProcessors.git
```

Navigate to the folder:

```
cd eoProcessors/harvester
```

and build the harvester image.
Please note that under Linux most probably you will need administrative rights to build and execute the images.

```
docker build -t noaeoharvester .
```

Then either:
* Edit `.env.list` file, including your configuration values (please check the [Parameters](https://github.com/Agri-Hub/eoProcessors/harvester/README.md#parameters) section below for more information):
    
```username, password, start_date, end_date, bbox, cloud_cover, tile, level```

and then execute:
(don't forget to replace **/path/to/local/data** with the path on your local machine where downloaded data will be stored):

```
docker run -rm -v [/path/to/local/data]:/app/data --env-file .env.list noaeoharvester
```

*OR*

* Introduce the enviromental variables in the "run" command:
  
```bash
docker run --rm -v [/path/to/local/data]:/app/data noaeoharvester \
-e username='YOUR_EMAIL' \
-e password='YOUR_PASSWORD' \
-e start_date='YYYY-MM-DD' \
-e end_date='YYYY-MM-DD' \
-e bbox='MIN_LON,MIN_LAT,MAX_LON,MAX_LAT' \
-e cloud_cover=MAX_CLOUD_COVER \
-e level=PROCESSING_LEVEL \
noaeoharvester
```

## Parameters
* /path/to/local/data: Path on your local machine where downloaded data will be stored.
* username: Your username for accessing Copernicus Dataspace Eco System.
* password: Your password for accessing Copernicus Dataspace Eco System.
* start_date / end_date: Date range for the imagery search (format: YYYY-MM-DD).
* bbox **or** tile: Bounding Box for the search area (format: MIN_LON,MIN_LAT,MAX_LON,MAX_LAT) and tile for the tile name of the product e.g. 34uFG
* cloud_cover: Maximum acceptable cloud cover percentage for the images (0-100).
* level: Processing level of the Sentinel-2 data (1 for Level1C, 2 for Level2A, or 12 for both).

## Copernicus collections (eodata)
As described here

the following collections can be retrieved:

    Sentinel1 
    Sentinel2 
    Sentinel3 
    Sentinel5P
    Sentinel6
    Sentinel1RTC (Sentinel-1 Radiometric Terrain Corrected)


## Tests

If something goes wrong, first run the tests:

```
docker run --env-file .env.list noaeoharvester sh -c "python -m unittest -v ./tests/test*"
```
