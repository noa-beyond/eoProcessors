# Use the official Python 3.10 slim image as base
FROM python:3.10-slim

# Set environment variables to prevent Python from writing pyc files and buffer outputs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies including GDAL
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    postgresql-client \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the entire project
COPY . /app/
RUN mkdir -p /app/staticfiles

# Set environment variables for database connection using build arguments
ARG POSTGRES_HOST
ARG POSTGRES_DB
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG HOST

# Set environment variables from build arguments
ENV POSTGRES_HOST=$POSTGRES_HOST
ENV POSTGRES_DB=$POSTGRES_DB
ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
ENV HOST=$HOST
# Remove GDAL verification step since GDAL is now installed
# RUN gdalinfo --version

# Copy the entrypoint script
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Define the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Remove the CMD since the entrypoint script handles starting Gunicorn
# CMD ["gunicorn", "sentinel_browser.wsgi:application", "--workers", "3", "--bind", "0.0.0.0:8000"]

LABEL org.opencontainers.image.source=https://github.com/noa-beyond/eoProcessors/
LABEL org.opencontainers.image.description="National Observatory of Athens - EO Product Catalog UI"
LABEL org.opencontainers.image.licenses="GNU Affero General Public License v3.0"
