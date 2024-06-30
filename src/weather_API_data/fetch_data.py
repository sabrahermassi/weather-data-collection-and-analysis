""" Module providing functions that load a config file
    and create a weather_table in postgres database. """

from configparser import ConfigParser
import os
from pathlib import Path
from dotenv import load_dotenv
import requests
from ratelimit import limits, sleep_and_retry
from retrying import retry




ONE_MINUTE = 60




def load_config(filename, section):
    """ Load weatherInfoDb database configuration from config file """
    parser = ConfigParser()
    parser.read(filename,  encoding='locale')

    config = {}

    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
        return config

    raise ValueError(f"Section {section} not found in the {filename} file")




def env_config_loading(env_path):
    """ Fetch configuration from .env file """
    try:
        load_dotenv(dotenv_path=env_path)
        if not env_path.exists():
            raise FileNotFoundError(f".env file not found")

        api_key = os.getenv('API_KEY')
        if api_key is None or api_key == '':
            raise ValueError(f"api_key is not found in the .env file")

        api_base_url = os.getenv('API_BASE_URL')
        if api_key is None or api_key == '':
            raise ValueError(f"api_base_url is not found in the .env file")
        
        return api_key, api_base_url

    except AttributeError as error:
        print(f".env file not found: {error}")
        raise

    except Exception as error:
        print(f"Unexpected error: {error}")
        raise




@sleep_and_retry
@limits(calls=60, period=ONE_MINUTE)
@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_weather_data(city, api_key, api_base_url):
    """ Fetch weather data from the API  """
    url = f"{api_base_url}?access_key={api_key}&query={city}"

    try:
        response = requests.get(url, timeout=10)
        return response.json()

    except (requests.RequestException , Exception) as error:
        print(f"Failed request to fetch weather data : {error}")
        return None
