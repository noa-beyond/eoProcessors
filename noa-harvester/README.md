# NOA Harvester processor

This processor facilitates the query and download functions from EO data providers like Copernicus (ESA) and Earthdata (NASA).

It is a simple cli wrapper of the following (to be expanded) libraries:
- CDSETool (https://github.com/SDFIdk/CDSETool)
- earthaccess (https://github.com/nsidc/earthaccess)

and common STAC API functionality, for accessing public catalogs:
- Element84 COG STAC collection (earthsearch - https://github.com/Element84/earth-search)

It can be used as a standalone cli application or be built in a Docker container.

# Using the processor

The noaharvester processor can be executed as:
- Standalone [**Cli application**](#standalone-cli-execution) or
- Inside a [**Container**](#docker-execution)
- As a container, inside a Kubernetes environment with kafka, with a postgres database. This is a Beyond specific setup, where a user can instantiate Harvester and request the download of Products, based on an id from the postgres Products table. This table includes a uuid and a title fields, necessary to construct the request to CDSE (for now). Then, it updates the products table for the downloaded path, while it posts to a kafka topic the result for each of these ids.
- As a microservice inside a Kubernetes environment with kafka, with a postgres database. Same as above, but now it can be deployed as a service: always listening to the appointed kafka topic for uuid lists, inside a specific message.

In either case, a user must have credentials for accessing one or more data hubs:
- [Copernicus]
- [EarthData] ([EarthData Register])

## Common prerequisites

- Install Git:  https://git-scm.com/book/en/v2/Getting-Started-Installing-Git

- Introduce hub credentials as environmental variables:
```
COPERNICUS_LOGIN=
COPERNICUS_PASSWORD=
EARTHDATA_LOGIN=
EARTHDATA_PASSWORD=
```

- For Windows: execute **git bash** and execute the rest of the operations on the git bash terminal.

Open a terminal window (or git bash...), navigate to your preferred working directory and download the source code:

```
git clone https://github.com/Agri-Hub/eoProcessors.git
```

You can now either execute the processor as a [standalone cli application](#standalone-cli-execution) or [build a container and run it](#docker-execution).

## Standalone CLI execution

1. You must have a Python version greater than 3.**12**.
2. Navigate to where you cloned the repo.
3. You should perform the following operations in a virtual environment:
    - Windows:
        - Install miniconda (https://docs.anaconda.com/free/miniconda/miniconda-install/)
    - Linux:
        - Install conda (https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)
    - **Common**:
        - Create the environment:
            - Execute `conda create -n noaharvester_env python==3.12`
            - Execute `conda activate noaharvester_env`
4. Then:

```
    cd eoProcessors/noa-harvester
    generate_netrc_local.sh
```
  in order for the `.netrc` file to be created in your local environment. This file will take care of credential information based on the environmental variables you have set up before.

5. Finally, install necessary requirements inside your virtual environment:
```
pip install -r requirements.txt
```

6. You are ready to execute the cli scripts:

```
python noaharvester/cli.py [command] config/config.json [shape_file]
```

Available commands are:
- query - Queries the collection for available data
- download - Downloads data
- describe (Copernicus only) - Describe collection's available query fields

#### Config file
The config file *should* be placed inside `eoProcessors/noa-harvester/config`, but of course you could use any path.

#### Shape files
The optional shapefile argument can be used in the end, to draw the bounding box from shapefile information (`.shp` for the shape and `.prj` for the projection information). Please note that you should use the common file base name for both files as the argument. E.g. for `weird_area.shp` and `weird_area.prj`, use `weird_area` as the argument

Please check the [Config](#Config-file-parameters) section regarding config file specification.

Moreover, a `-v` parameter is available to print verbose download progress bar for Copernicus.

## Docker execution

1. Install Docker: https://docs.docker.com/get-docker/

2. Navigate to the folder (remember, from git bash in Windows - use *winpty* in case git bash complains before every command):

```
    cd eoProcessors/noa-harvester
```

3. Then:

```
docker build -t noaharvester \
--secret id=COPERNICUS_LOGIN \
--secret id=COPERNICUS_PASSWORD \
--secret id=EARTHDATA_LOGIN \
--secret id=EARTHDATA_PASSWORD .
```

Please note that you **should not** replace the above command with your already set environmental variables.

You now have a local container named noaharvester.


4. Edit `config/config.json` (or create a new one)

5.1 Execute either:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
--entrypoint /bin/bash \
noaharvester
```

to enter into the container and execute the cli application from there:
`python noaharvester/cli.py download -v --output_path /app/data/ config/config.json`

5.2 Or execute the command leaving the container when the command is completed:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
noaharvester download -v config/config.json
```

Please note that in the aforementioned commands you can replace:
    * `[./data]` with the folder where the downloaded data will be stored. The default location is "./data"
    * `[./config/config.json]` with the local location of your configuration file. In that way you will use the local edited file instead of the container one. If you have edited the already present config file before building the container, leave it as is is.


## Config file parameters

Take a look at the sample config.json. 
```
[
    {
        "provider": "copernicus",
        "collection": "Sentinel1",
        "search_terms":        
        {
            "maxRecords": "100",
            "startDate": "2024-01-06",
            .....
        }
    },
    {
        "provider": "earthdata",
        "collection": "MODIS",
        "search_terms":
        {
            "maxRecords": "100",
            "startDate": "2019-01-01",
            .....
        }
    },
    {
        "provider": "earthsearch",
        "version": "v1",
        "collection": "sentinel-s2-l2a-cogs",
        "assets": ["visual"],
        "search_terms":
        {
            "maxRecords": "100",
            "startDate": "2023-05-01",
            "cloud_cover_lt": 90,
            .....
        }
    }
]
```
As the top level keys, you have to introduce the provider and collection names. You can include as many sources as needed.

Please refer to
- Copernicus [OData API] for available collections and query keys
- Earthdata and [earthaccess] python library for available EarthData collections, product type short names and query terms.
- Element84 COG STAC collection (earthsearch - https://github.com/Element84/earth-search)

Please note that in the STAC collections search, we do not search/download by bands, but by **assets**.
This is because many providers provide interesting composites like snow/cloud probabilities under the item assets, along with the bands.

## Cli options

Cli can be executed with the following:

- Commands
    * `download` - The main option. Downloads according with the config file parameters.
    * `from-uuid-list` - Download from uuid db list. Needs to be combined with -u option. Necessary a db connection (TODO: optional)
    * `noa_harvester_service` - Deploy a service always listening to a specific kafka topic (can also be defined in config file - look at config/config_service.json).
    * `query` - Queries the collection(s) for products according to the parameters present in the config file.
    * `describe` (Copernicus only) - Describes the available query parameters for the collections as defined in the config file.
- Options
    * `--output_path` (download only) Custom download location. Default is `.data`
    * `-u, --uuid` [**multiple**] (from-uuid-list only). Multiple option of uuids.
    * `-t, --test` **Service only**: testing kafka consumer functionality.
    * `-bb, --bbox_only` Draw total bbox, not individual polygons in multipolygon shapefile.
    * `-v`, `--verbose` Shows the progress indicator when downloading (Copernicus - only for download command)
    * `--log LEVEL (INFO, DEBUG, WARNING, ERROR)` Shows the logs depending on the selected `LEVEL`
- Arguments
    * `config_file` - Necessary argument for the commands, indicating which config file will be used.
    * `shape_file` - Optional. Create the query/download bounding box from a shapefile instead of the config file. Please note that this argument receives the base name of `.shp` and `.prj` files (e.g. argument should be `Thessalia` for `Thessalia.shp` and `Thessalia.prj` files)

## DB Considerations for uuid list download

Please note that for uuid list download, for now, a postgres db is required.
You can provide credentials either by having set up env vars or by filling up the `database.ini` file under db folder.
The necessary env vars are:
`DB_USER`
`DB_PASSWORD`
`DB_HOST`
`DB_PORT`
`DB_NAME`

Moreover, Harvester will query the db to get the UUID of the Product to be downloaded, and Title (it does not query CDSE for metadata - it only downloads).
So make sure that a postgres with a table named "Products", includes at least a `uuid` field and a `name` field.

## Examples

* Show available query parameters for Copernicus Collections as defined in the config file:

```
docker run -it \
-v ./config/config_test_copernicus.json:/app/config/config.json \
noaharvester describe config/config.json
```

* Download (with download indicator) from Copernicus providing an id list (which corresponds to an entry in Products db table) and store in mnt point:

```
docker run -it \
-v ./config/config.json:/app/config/config_from_id.json \
-v /mnt/data:/app/data \
noaharvester from-uuid-list -v -u caf8620d-974d-5841-b315-7489ffdd853b config/config_from_id.json
```

* Deploying Harvester as a service (for kafka testing - if you do not want to test, omit flag -t):

```
docker run -it \
-v ./config/config.json:/app/config/config_service.json \
-v /mnt/data:/app/data \
noaharvester noa-harvester-service -v -t config/config_service.json
```

* Download (with download indicator) from Copernicus and Earthdata as defined in the config file, for an area provided by the shapefile files (`area.shp` and `area.prj`) located in folder `/home/user/project/strange_area`:

```
docker run -it \
-v ./config/config.json:/app/config/config.json \
-v /home/user/project/strange_area:/app/shapes/strange_area/ \
-v /home/user/project/data:/app/data \
noaharvester download -v config/config.json shapes/strange_area/area
```

## Tests

Execute 
```
pytest .
```
on  `eoProcessors/noa-harvester`  folder

or

```
docker run -it --entrypoint pytest noaharvester
```

for the container

[Copernicus]: https://dataspace.copernicus.eu/
[OData API]: https://documentation.dataspace.copernicus.eu/APIs/OData.html
[EarthData]: https://www.earthdata.nasa.gov/
[earthaccess]: https://earthaccess.readthedocs.io/en/stable/
[EarthData Register]: https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/earthdata-login