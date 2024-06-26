""" Module providing functions that fetch weather data
    and store it in postgres database. """

from datetime import datetime
import psycopg2
import requests
from dotenv import load_dotenv
from pathlib import Path
from ratelimit import limits, sleep_and_retry
from retrying import retry

from .create_table import load_config, env_config_loading, create_weather_table




CITIES = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]
ONE_MINUTE = 60

def insert_data(conn, city_name, response_dict):
    """ Inset data in the weather_data table """
    command = """INSERT INTO weather_data (city_name, temperature, pressure, humidity, date_time)
                    VALUES (%s, %s, %s, %s, %s);"""

    if 'current' not in response_dict:
        raise ValueError(f"""Error fetching data for {city_name}:
                             'current' key not found in response """)

    temperature = response_dict['current']['temperature']
    pressure = response_dict['current']['pressure']
    humidity = response_dict['current']['humidity']

    try:
        with conn.cursor() as cur:
            now = datetime.now()
            date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
            val = (city_name, temperature, pressure, humidity, date_time)
            cur.execute(command, val)
            print(cur.rowcount, "record inserted.")
            return cur.rowcount

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        return




@sleep_and_retry
@limits(calls=60, period=ONE_MINUTE)
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_weather_data(city, api_key, api_base_url):
    """ Fetch weather data from the API  """
    url = f"{api_base_url}?access_key={api_key}&query={city}"

    try:
        response = requests.get(url, timeout=10)
        return response.json()

    except (requests.ConnectionError , Exception) as error:
        print(error)
        return




if __name__=='__main__':
    config = load_config()
    connection = create_weather_table(config)
    api_key, api_base_url = env_config_loading()

    for city_nm in CITIES:
        response_data = fetch_weather_data(city_nm, api_key, api_base_url)
        insert_data(connection, city_nm, response_data)
