
STREET_CONSTRUCTION_API_URL = (
    "https://api.hamburg.de/datasets/v1/baustellen/collections/baustelle/items?limit=1000&offset=0&f=json"
)



CURRENT_WEATHER_TOPIC = "hh-weather-current"
DAILY_WEATHER_TOPIC = "hh-weather-daily"

REDIS_DAILY_KEY = "weather:daily:last_sent"

class StreetConstructionFetcher(BaseFetcher):

   
if __name__ == "__main__":
    fetcher = WeatherFetcher(wakeup_topic="fetch-weather", group_id="weather-fetcher")
    fetcher.run()
