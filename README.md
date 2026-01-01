# City Mood Project

This big data project aims to analyze and visualize the mood of a city based on various data sources including traffic patterns, social media activity, and environmental factors.

## Installation

Use the setup.sh script to set up the virtual environment and install dependencies:

```bash
source ./setup.sh
```

## Usage

```sh
docker compose up -d
```
This command will start all necessary services including Kafka, Apache Spark and Postgres. 

All data fetcher scripts will run automatically based on their defined schedules. They can also be run manually if needed and found in /app/api-fetcher/.

The Kafka ui will be available at: http://localhost:8090. 

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

