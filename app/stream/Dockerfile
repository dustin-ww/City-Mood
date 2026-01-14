FROM ubuntu:26.04

FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Spark Pipeline"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="City Mood Spark Pipeline Container with pyspark and Dependencies"

WORKDIR /app

# Install Java (OpenJDK 21)
RUN apt-get update && \
    apt-get install -y openjdk-21-jdk python3 python3-pip


# Copy python application files
COPY app/stream/city_mood_pipeline.py ./city_mood_pipeline.py
COPY app/stream/requirements.txt ./requirements.txt

RUN pip3 install -r ./requirements.txt

ENV PYTHONPATH=/app


CMD ["python3", "./city_mood_pipeline.py"]
