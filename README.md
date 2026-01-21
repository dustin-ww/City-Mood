# City Mood Map Project

This big data project aims to analyze and visualize the mood of a city based on various data sources including traffic patterns, news feeds, and environmental factors.

## Installation

You can easily set up the system using the build_container.sh script. This will automatically build the necessary services as Docker containers. These containers are saved in your local Docker registry. Please note that the building process may take some time due to some large dependencies. 

```bash
./build_containers.sh
```

After that you can simply use the docker compose file from the main directory 
## Usage

```sh
docker compose up -d
```
This command will start all the necessary services, including Kafka, Apache Spark, PostgreSQL, and Grafana. 

All data fetcher scripts run automatically based on schedules defined by the central scheduler service, which coordinates fetches via dedicated Kafka topics (fetch-*). These scripts can also be run manually if needed and are located in the /app/api-fetcher/ directory.

The Kafka UI will be available at http://localhost:8090. 

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

