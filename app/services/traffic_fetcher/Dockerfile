FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Traffic Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Traffic Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/traffic_fetcher ./services/traffic_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.traffic_fetcher.traffic_fetcher"]
