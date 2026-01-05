FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Map Water Level Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Water Level Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/water_level_fetcher ./services/water_level_fetcher

ENV PYTHONPATH=/app

CMD ["python", "-m", "services.water_level_fetcher.water_level_fetcher"]
