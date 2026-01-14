FROM city-mood-python-base:latest

LABEL org.opencontainers.image.title="City Mood Scheduler"
LABEL org.opencontainers.image.version="1.0"
LABEL org.opencontainers.image.description="Scheduler Service"

# Set working directory
WORKDIR /app

# Copy scheduler script
COPY app/services/scheduler/scheduler.py ./scheduler.py

# Install requirements
# COPY app/services/scheduler/requirements.txt ./requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt 

# Start script directly
CMD ["python", "scheduler.py"]
