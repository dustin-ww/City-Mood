FROM python:3.10-slim

# Create a non-root user to run the application
ARG NEW_UID=1000
ARG NEW_GID=1000
ARG REMOTE_USER=citymooddev

# Install necessary packages and create user
RUN groupadd -g ${NEW_GID} ${REMOTE_USER} \
    && useradd -m -u ${NEW_UID} -g ${NEW_GID} ${REMOTE_USER} \
    && apt-get update && apt-get install -y default-jre wget curl git bash

USER ${REMOTE_USER}
WORKDIR /workspace/app