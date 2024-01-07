# Use an official Python runtime as a parent image
FROM python:3.8-slim-bullseye

# Set the working directory in the container to /app
WORKDIR /app

# Install the necessary packages for building PostgreSQL extensions and libraries
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    libgeos-dev \
    libproj-dev \
    libeccodes-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

RUN mkdir /dataStore

# Upgrade pip and install any needed packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r packages.txt

# Run your script when the container launches
# CMD ["python3", "hrrrScheduler.py"]
CMD ["python3", "hrrrSuperSampling.py"]

