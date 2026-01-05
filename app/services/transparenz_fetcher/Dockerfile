FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Transparenz Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Transparenz Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/transparenz_fetcher ./services/transparenz_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.transparenz_fetcher.transparenz_fetcher"]
