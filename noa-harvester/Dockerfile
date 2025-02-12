# Use the official Python 3.12 image as the base image
FROM python:3.12.0-slim

RUN pip install --upgrade pip setuptools
RUN apt-get update

# Install GDAL dependencies
RUN apt-get install -y libgdal-dev g++

# Update C env vars so compiler can find gdal
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY generate_netrc.sh .
RUN --mount=type=secret,id=COPERNICUS_LOGIN \
    --mount=type=secret,id=COPERNICUS_PASSWORD \
    --mount=type=secret,id=EARTHDATA_LOGIN \
    --mount=type=secret,id=EARTHDATA_PASSWORD \
    /bin/bash generate_netrc.sh

# Set the working directory in the container to /app
WORKDIR /app

# Copy the Python script into the container at /app
COPY requirements.txt .
COPY . .

# Create a default Products download dir
RUN mkdir -p /mnt/data/poc/products

# Install the required Python packages specified in a requirements.txt file
# If you don't have a requirements.txt, you'll need to create one with the necessary packages
RUN pip install -r requirements.txt --no-cache-dir

# Command to run the script when the container starts
ENTRYPOINT ["python", "noaharvester/cli.py"]

LABEL org.opencontainers.image.source=https://github.com/noa-beyond/eoProcessors/
LABEL org.opencontainers.image.description="National Observatory of Athens - EO Data Harvester"
LABEL org.opencontainers.image.licenses="GNU Affero General Public License v3.0"