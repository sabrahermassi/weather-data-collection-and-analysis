""" Module providing functions that fetch weather data
    and store it in postgres database. """

from datetime import datetime
import psycopg2
import sys
sys.path.append('./')
from src.weather_api_data.create_table import load_config, env_config_loading, fetch_weather_data




CITIES = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]




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




if __name__=='__main__':
    config = load_config()
    connection = create_weather_table(config)
    api_key, api_base_url = env_config_loading()

    for city_nm in CITIES:
        response_data = fetch_weather_data(city_nm, api_key, api_base_url)
        insert_data(connection, city_nm, response_data)
