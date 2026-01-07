FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Street Construction Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Street Construction Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/street_construction_fetcher ./services/street_construction_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.street_construction_fetcher.street_construction_fetcher"]
