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

- The respective data hub credentials must be introduced as environmental variables:
```
COPERNICUS_LOGIN=
COPERNICUS_PASSWORD=
EARTHDATA_LOGIN=
EARTHDATA_PASSWORD=
```

Open a terminal window, navigate to your prefered working directory and download the source code:

```
git clone https://github.com/Agri-Hub/eoProcessors.git
```

You can also download the repo directly from github.

## Standalone CLI execution

- You must have a Python version greater than 3.11
- Navigate to the folder and execute:

```
    cd eoProcessors/noaharvester
    generate_netrc_local.sh
```
  in order for the `netrc` file to be created in your local environment. This file will take care of credential information based on the environmental variables you have set up before.

-  Execute
```
python noaharvester/cli.py [command] config.json
```

Available commands are:
- query - Queries the collection for available data
- download - Downloads data
- describe (Copernicus only) - Describe collection's available query fields

The config file must be placed inside `eoProcessors/noaharvester/config`.
Please check the [Config](#Config-file-parameters) section regarding config file specification.

Moreover, a `-v` parameter is available to print verbose download progress bar for Copernicus.

## Docker execution

* Install Docker: https://docs.docker.com/get-docker/

* Navigate to the folder:

```
    cd eoProcessors/noaharvester
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


* Edit `config.json` found in the config folder (or create a new one)

* Execute either:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
--entrypoint /bin/bash \
noaharvester
```

to enter into the container and execute the cli application from there:
`python noaharvester/cli.py download -v config.json`

* Or execute the command leaving the container when the command is completed:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
noaharvester download -v config.json
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
    * `-v`, `--verbose` Shows the progress indicator when downloading (Copernicus)
- Arguments
    * `config_file` - Necessary argument for the commands, indicating which config file will be used.

Examples:
* Show available query parameters for Copernicus Collections as defined in the config file:

```
docker run -it \
-v ./config/config_test_copernicus.json:/app/config/config.json \
noaharvester describe config.json
```


## Tests **TODO:**


```
docker run noaharvester sh -c "python -m unittest -v ./tests/test*"
```

[Copernicus]: https://dataspace.copernicus.eu/
[OData API]: https://documentation.dataspace.copernicus.eu/APIs/OData.html
[EarthData]: https://www.earthdata.nasa.gov/
[earthaccess]: https://earthaccess.readthedocs.io/en/stable/
[EarthData Register]: https://www.earthdata.nasa.gov/eosdis/science-system-description/eosdis-components/earthdata-login