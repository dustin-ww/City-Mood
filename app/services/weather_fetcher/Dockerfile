FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Weather Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Weather Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/weather_fetcher ./services/weather_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.weather_fetcher.weather_fetcher"]
