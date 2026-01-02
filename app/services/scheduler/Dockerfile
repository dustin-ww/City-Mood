FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Scheduler"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Scheduler Service"

COPY /app/services/scheduler/scheduler.py .
CMD ["python", "scheduler.py"]