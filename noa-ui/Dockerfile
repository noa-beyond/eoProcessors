# Use the official Python 3.10 slim image as base
FROM python:3.10-slim

# Set environment variables to prevent Python from writing pyc files and buffer outputs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (without GDAL)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the entire project
COPY . /app/

# Remove GDAL verification step since GDAL is not used
# RUN gdalinfo --version

# Copy the entrypoint script
COPY entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh

# Define the entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

# Remove the CMD since the entrypoint script handles starting Gunicorn
# CMD ["gunicorn", "sentinel_browser.wsgi:application", "--workers", "3", "--bind", "0.0.0.0:8000"]
