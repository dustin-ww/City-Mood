FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Weather Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Weather Fetcher Service"

COPY /app/services/weather-fetcher/main.py .
CMD ["python", "main.py"]