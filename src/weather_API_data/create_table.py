""" Module providing functions that load a config file
    and create a weather_table in postgres database. """

from configparser import ConfigParser
import psycopg2
from dotenv import load_dotenv
from pathlib import Path
import os




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




def create_weather_table(config):
    """ Create weather_data table in the PostgreSQL database"""
    command = """CREATE TABLE weather_data (
                    id SERIAL PRIMARY KEY,
                    city_name VARCHAR(255),
                    temp FLOAT,
                    pressure INT,
                    humidity INT,
                    date_time TIMESTAMP
               )"""
    
    try:
        with psycopg2.connect(**config) as conn:
            print("Successfully connected to the postgres server")
            # Create a table in the database
            with conn.cursor() as cur:
                cur.execute("DROP TABLE  IF EXISTS weather_data")
                cur.execute(command)
            return conn

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        return
