FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood New York Times News Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="New York Times News Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/nyt_news_fetcher ./services/nyt_news_fetcher

RUN pip3 install --no-cache-dir -r ./services/nyt_news_fetcher/requirements.txt

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.nyt_news_fetcher.nyt_news_fetcher"]
