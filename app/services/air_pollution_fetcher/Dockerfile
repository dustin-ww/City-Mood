FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Air Pollution Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Air Pollution Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/air_pollution_fetcher ./services/air_pollution_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.air_pollution_fetcher.air_pollution_fetcher"]
