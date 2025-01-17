# NOA STAC Ingest processor

Ingest geospatial data, by creating STAC Items according to the STAC 1.0.0 Specification
https://github.com/radiantearth/stac-spec


## Initial steps

Please make sure that you have a working pgSTAC instance and a valid Catalog STAC path:
This processor initially creates a STAC json for every Item and then populates the
pgSTAC instance. In order to do so, the Items are related to a parent Collection
which is also a child of a Catalog.

In case you are starting fresh, there are two helper files in order to create an initial
Catalog and an initial Collection under that Catalog:

```
noa-stac-ingest/noastacingest/_create_catalog.py
noa-stac-ingest/noastacingest/_create_collection.py
```

## How to use as a service, consuming/producing from/to kafka, and ingesting to pgSTAC
- Build docker image (Dockerfile or compose)  
- Execute (please mind the config file. If no changes are needed, do not mount the following config)
```
docker run -it \
-v ./config/config.json:/app/config/default_config.json \
-v /mnt/data/dev/:/mnt/data/dev/ \
noastacingest noa-stac-ingest-service -db config/default_config.json
```

Also mind any port forwarding needed for kafka or pgSTAC.
Please note that you can use the -t flag instead of -db in order to check kafka communication,
and have in mind that by using -db, pgSTAC will be populated (production mode)

## DB Considerations for uuid list download

Please note that for uuid path extraction, for now, a postgres db is required.
You can provide credentials either by having set up env vars or by filling up the `database.ini` file under db folder.
The necessary env vars are:
`DB_USER`
`DB_PASSWORD`
`DB_HOST`
`DB_PORT`
`DB_NAME`