# Base image for all City Mood Python services
FROM python:3.11-slim

WORKDIR /app

COPY ./app/services/requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt