# NOA Change Detection Mapping (NOAChDM) processor

This processor lorem ipsum

## Using the processor

The NOAChDM processor can be executed as:
- Standalone [**Cli application**](#standalone-cli-execution) or
- Inside a [**Container**](#docker-execution)
- As a container, inside a Kubernetes environment with kafka, with a postgres database. This is a Beyond specific setup, where a user can instantiate NOAChDM and request the production of a single product.
- As a microservice inside a Kubernetes environment with kafka, with a postgres database. Same as above, but now it can be deployed as a service: Product Generation as a Service (PGaaS).

## Standalone CLI execution

1. Use your favorite flavor of virtual environment, e.g. conda:
    - Create/activate the environment:
        - Execute `conda create -n noa-chdm python==3.12.10`
        - Execute `conda activate noa-chdm`
2. Then:

```
    cd eoProcessors/noa-change-detection-mapping
```
and install necessary requirements inside your virtual environment:
```
pip install -r requirements.txt
```

3. You are ready to execute the cli script:

```
python noachdm/cli.py [command] config/config.json
```

Available commands are:

### Config file
The config file *should* be placed inside `eoProcessors/noa-change-detection-mapping/config`, but of course you could use any path.
Please check the [Config](#Config-file-parameters) section regarding config file specification.

## Docker execution

1. Install Docker: https://docs.docker.com/get-docker/
2. Navigate to the folder 
```
    cd eoProcessors/noa-change-detection-mapping
```
3. Then:

```
docker build -t noa-chdm .
```

4. Edit `config/config.json` (or create a new one)

5. Execute either:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
--entrypoint /bin/bash \
noa-chdm
```

to enter into the container and execute the cli application from there:
`python noachdm/cli.py produce -v config/config.json`

5.2 Or execute the command leaving the container when the command is completed:

```
docker run -it \
-v [./data]:/app/data \
-v [./config/config.json]:/app/config/config.json \
noa-chdm produce -v config/config.json
```

Please note that in the aforementioned commands you can replace:
    * `[./data]` with the folder where the downloaded data will be stored. The default location is "./data"
    * `[./config/config.json]` with the local location of your configuration file. In that way you will use the local edited file instead of the container one. If you have edited the already present config file before building the container, leave it as is is.


## Config file parameters

Take a look at the sample config.json. 
```
[
    {
        ...
    }
]
```

## Cli options

Cli can be executed with the following:

- Commands
    * `produce` - The main option. lorem ipsum.
- Options
    * `--output_path` Custom download location. Default is `.data`
    * `-v`, `--verbose` Shows the verbose output
    * `--log LEVEL (INFO, DEBUG, WARNING, ERROR)` Shows the logs depending on the selected `LEVEL`
- Arguments
    * `config_file` - Necessary argument for the commands, indicating which config file will be used.

## Examples

## Tests

Execute 
```
pytest .
```
on  `eoProcessors/noa-change-detection-mapping`  folder

or

```
docker run -it --entrypoint pytest noa-chdm
```

for the container
