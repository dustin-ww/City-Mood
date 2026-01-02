#!/usr/bin/env bash
set -e

echo "Starting kafka, spark instances, postgres and grafana..."
docker-compose up -d 

echo "Starting to build api fetching services in dedicated containers..."

# Config
BASE_IMAGE="city-mood-python-base"
BASE_TAG="1.0.0"

echo "Building base image..."
docker build \
  -t ${BASE_IMAGE}:${BASE_TAG} \
  -f /app/services/Dockerfile .

SERVICES=(
  scheduler
  weather-fetcher
)

for SERVICE in "${SERVICES[@]}"; do
  echo "🔨 Building ${SERVICE}..."
  docker build \
    -t ${SERVICE}:latest \
    -f /app/services/${SERVICE}/Dockerfile .
done

echo "All images built!"
