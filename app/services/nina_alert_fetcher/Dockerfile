FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Nina Alert Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Nina Alert Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/nina_alert_fetcher ./services/nina_alert_fetcher

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.nina_alert_fetcher.nina_alert_fetcher"]
