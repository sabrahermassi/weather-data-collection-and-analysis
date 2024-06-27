""" Module providing functions that load a config file
    and create a weather_table in postgres database. """

from configparser import ConfigParser
from dotenv import load_dotenv
from pathlib import Path
import os
import requests
from ratelimit import limits, sleep_and_retry
from retrying import retry




ONE_MINUTE = 60




def load_config(filename='database.ini', section='postgresql'):
    """ Load weatherInfoDb database configuration from config file """
    parser = ConfigParser()
    parser.read(filename)

    config = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        return config

    else:
        raise ValueError(f"Section {section} not found in the {filename} file")




def env_config_loading():
    """ Fetch configuration from .env file """
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    api_key = os.getenv('API_KEY')
    api_base_url = os.getenv('API_BASE_URL')
    return api_key, api_base_url




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
