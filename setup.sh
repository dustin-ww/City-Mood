#!/bin/bash
# Run 'source ./setup.sh' to set up the virtual environment and install dependencies

VENV_NAME="city-mood-venv"

# Create Virtual Environment if it doesn't exist
if [ ! -d "$VENV_NAME" ]; then
    echo "Creating virtual env..."
    python3 -m venv $VENV_NAME

    source $VENV_NAME/bin/activate
    
    # Install dependencies
    echo "Install pip dependencies..."
    pip install schedule kafka-python pyspark

else
    echo "Virtual env already exists. Activating..."
    source $VENV_NAME/bin/activate
fi

