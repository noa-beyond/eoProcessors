Using the harvester processor (Under construction)

Prerequisites

For Copernicus, Sentinel data (Sentinel 2 ...)
. Copernicus Registration: Users must be registered with the Copernicus Dataspace Eco System to access and download the data. Registration is free and can be completed on the Copernicus Dataspace Eco System (https://dataspace.copernicus.eu/).
. Docker Installation: Docker must be installed on your machine. (include links)

Windows
install git

Linux
If not present, install git

Common commands, after prerequisites and platform specific commands/installation

. git clone https://github.com/Agri-Hub/eoProcessors.git
cd harvester
. docker build -t noaeoharvester .

Either:
    . edit file .env.list, including your configuration values:
            username, password, start_date, end_date, bbox, cloud_cover, tile, level
         and then execute:
            docker run -rm -v ./data:/app/data --env-file .env.list noaeoharvester

    . introduce the enviromental variables in the "run" command:
      docker run --rm -v /path/to/local/data:/app/data noaeoharvester \
        -e username='YOUR_EMAIL' \
        -e password='YOUR_PASSWORD' \
        -e start_date='YYYY-MM-DD' \
        -e end_date='YYYY-MM-DD' \
        -e bbox='MIN_LON,MIN_LAT,MAX_LON,MAX_LAT' \
        -e cloud_cover=MAX_CLOUD_COVER \
        -e level=PROCESSING_LEVEL \
            noaeoharvester
            
Tests

If something goes wrong, first run the tests:
docker run -v ./data:/app/data --env-file .env.list noaeoharvester sh -c "python -m unittest -v ./tests/test*"
