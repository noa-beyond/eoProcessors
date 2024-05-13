# NOA Harvester processor

This processor facilitates the query and download functions from EO data providers like Copernicus (ESA) and Earthdata (NASA).

It is a simple cli wrapper of the following (to be expanded) libraries:
- CDSETool (https://github.com/SDFIdk/CDSETool)
- earthaccess (https://github.com/nsidc/earthaccess)

It can be used as a standalone cli application or be built in a Docker container.

# Using the processor

The noaharvester processor can be executed as a standalone cli application or inside a container. 
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

Open a terminal window (or git bash...), navigate to your prefered working directory and download the source code:

```
git clone https://github.com/Agri-Hub/eoProcessors.git
```

You can also download the repo directly from github.

You can now either execute the processor as a [standalone cli application](#standalone-cli-execution) or [build a container and run it](#docker-execution).

## Standalone CLI execution

- You must have a Python version greater than 3.11.
- Navigate to where you cloned the repo.
- You should perform the following operations in a virtual environment:
    - Windows:
        - Install miniconda (https://docs.anaconda.com/free/miniconda/miniconda-install/)
    - Linux:
        - Install conda (https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html)
    - Common:
        - Create the environment:
            - Execute `conda create -n noaharvester_env python==3.11`
            - Execute `conda activate noaharvester_env`
- Then:

```
    cd eoProcessors/noa-harvester
    generate_netrc_local.sh
```
  in order for the `.netrc` file to be created in your local environment. This file will take care of credential information based on the environmental variables you have set up before.

- Finally, install necesarry requirements inside your virtual environment:
```
pip install -r requirements.txt
```

You are ready to execute the cli scripts:

```
python noaharvester/cli.py [command] config/config.json [shape_file]
```

Available commands are:
- query - Queries the collection for available data
- download - Downloads data
- describe (Copernicus only) - Describe collection's available query fields

The config file *should* be placed inside `eoProcessors/noa-harvester/config`, but of course you could use any path.
The optional shapefile argument can be used in the end, to draw the bounding box from shapefile information (`.shp` for the shape and `.prj` for the projection information). Please note that you should use the common file root for both files as the argument. E.g. for `weird_area.shp` and `weird_area.prj`, use `weird_area` as the argument)

Please check the [Config](#Config-file-parameters) section regarding config file specification.

Moreover, a `-v` parameter is available to print verbose download progress bar for Copernicus.

## Docker execution

* Install Docker: https://docs.docker.com/get-docker/

* Navigate to the folder (remeber, from git bash in Windows - use *winpty* in case git bash complains before every command):

```
    cd eoProcessors/noa-harvester
```

- Then:

```
docker build -t noaharvester \
--secret id=COPERNICUS_LOGIN \
--secret id=COPERNICUS_PASSWORD \
--secret id=EARTHDATA_LOGIN \
--secret id=EARTHDATA_PASSWORD .
```

Please note that you **should not** replace the above command with your already set environmental variables.

You now have a local container named noaharvester.


* Edit `config/config.json` (or create a new one)

* Execute either:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
--entrypoint /bin/bash \
noaharvester
```

to enter into the container and execute the cli application from there:
`python noaharvester/cli.py download -v config/config.json`

* Or execute the command leaving the container when the command is completed:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
noaharvester download -v config/config.json
```

Please note that in the aforementioned commands you can replace:
    * `[./data]` with the folder where the downloaded data will be stored
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
    }
    {
        "provider": "earthdata",
        "collection": "MODIS",
        "search_terms":
        {
            "maxRecords": "100",
            "startDate": "2019-01-01",
        .....
        }
    }        
]
```
As the top level keys, you have to introduce the provider and collection names. You can include as many sources as needed.

Please refer to
- Copernicus [OData API] for available collections and query keys
- Earthdata and [earthaccess] python library for available EarthData collections, product type short names and query terms.

## Cli options

Cli can be executed with the following:

- Commands
    * `download` - The main option. Downloads according with the config file parameters.
    * `query` - Queries the collection(s) for products according to the parameters present in the config file.
    * `describe` (Copernicus only) - Describes the available query parameters for the collections as defined in the config file.
- Options
    * `-v`, `--verbose` Shows the progress indicator when downloading (Copernicus - only for download command)
    * `--log LEVEL (INFO, DEBUG, WARNING, ERROR)` Shows the logs depending on the selected `LEVEL`
- Arguments
    * `config_file` - Necessary argument for the commands, indicating which config file will be used.
    * `shape_file` - Optional. Create the query/donwload bounding box from a shapefile instead of the config file. Please note that this argument receives the root name of `.shp` and `.prj` files (e.g. argument should be `Thessalia` for `Thessalia.shp` and `Thessalia.prj` files)

## Examples

* Show available query parameters for Copernicus Collections as defined in the config file:

```
docker run -it \
-v ./config/config_test_copernicus.json:/app/config/config.json \
noaharvester describe config/config.json
```

* Download (with download indicator) from Copernicus and Earthdata as defined in the config file, for an area provided by the shapefile files (`area.shp` and `area.prj`) located in folder `/home/user/project/strange_area`:

```
docker run -it \
-v ./config/config.json:/app/config/config.json \
-v /home/user/project/strange_area:/app/shapes/strange_area/ \
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