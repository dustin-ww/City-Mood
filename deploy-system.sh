#!/usr/bin/env bash
set -e

echo "Starting kafka, spark instances, postgres and grafana..."
#docker compose up -d 

echo "Starting to build api fetching services in dedicated containers..."

# Config
BASE_IMAGE="city-mood-python-base"
BASE_TAG="latest"

echo "Building base image..."
docker build \
  -t ${BASE_IMAGE}:${BASE_TAG} \
  -f ./app/Dockerfile .

SERVICES=(
  weather_fetcher
)

for SERVICE in "${SERVICES[@]}"; do
  SERVICE_TAG=${SERVICE//_/-}

  echo "🔨 Building ${SERVICE_TAG}..."
  docker build --no-cache \
    -t citymoodmap/city-mood-${SERVICE_TAG}:latest \
    -f ./app/services/${SERVICE}/Dockerfile . 2>&1 | tee build-output.txt
done

echo "All images built!"
