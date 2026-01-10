FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood BBC News Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="BBC News Fetcher Service"

WORKDIR /app

COPY app/common ./common
COPY app/services/bbc_news_fetcher ./services/bbc_news_fetcher

RUN pip3 install -r ./services/bbc_news_fetcher/requirements.txt

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.bbc_news_fetcher.bbc_news_fetcher"]
