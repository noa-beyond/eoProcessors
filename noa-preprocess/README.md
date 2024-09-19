# Pre-process

A pre-process processor which:
1. Extracts Copernicus rasters based on a config file (check config folder for examples) and optionally converts them to COGs
2. Clips against an input shapefile

## Requirements

1. `pip install requirements.txt` on a virtual environment

## Usage

`python noapreprocess/cli.py --help`:
1. `python noapreprocess/cli.py extract config/default_unzip_all.json /data/path`
2. `python noapreprocess/cli.py extract --output_path /home/user/data/ config/default_unzip_all.json /data/path`
3. `python noapreprocess/cli.py clip config/default_unzip_all.json /shape/file/path /input/data/path/`