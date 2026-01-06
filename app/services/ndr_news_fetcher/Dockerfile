FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood NDR News Fetcher"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="NDR News Fetcher Service"

WORKDIR /app

COPY 

COPY app/common ./common
COPY app/services/ndr_news_fetcher ./services/ndr_news_fetcher

RUN pip3 install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app


CMD ["python", "-m", "services.ndr_news_fetcher.ndr_news_fetcher"]
