# Use the official Python 3.10 image as the base image
FROM python:3.10.11

# Set the working directory in the container to /app
WORKDIR /app

# Copy the Python script into the container at /app
COPY requirements.txt .
COPY . .

# Install the required Python packages specified in a requirements.txt file
# If you don't have a requirements.txt, you'll need to create one with the necessary packages
RUN pip install -r requirements.txt --no-cache-dir

# Command to run the script when the container starts
CMD ["python", "./download.py"]
