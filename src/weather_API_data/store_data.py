""" Module providing functions that fetch weather data
    and store it in postgres database. """

from datetime import datetime
import psycopg2
import sys
sys.path.append('./')
from src.weather_api_data.create_table import load_config, env_config_loading, fetch_weather_data




CITIES = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]
create_table_command = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city_name VARCHAR(255),
        temperature FLOAT,
        pressure INT,
        humidity INT,
        date_time TIMESTAMP
    )
"""
command_create_db = """CREATE DATABASE IF NOT EXISTS weather_info_db"""




def create_weather_database(config_main, config_new, command):
    """ Create weather database"""

    try:
        conn = psycopg2.connect(**config_main)
        cur = conn.cursor()

        # Check if the database exists
        database_name = config_new['database']

        cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s", (database_name,))
        exists = cur.fetchone()
        if exists is None:
            cur.execute(command)
            print(f"Database {config_new['database']} created successfully.")

        return psycopg2.connect(**config_new)

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        return




def create_weather_table(config, command):
    """ Create weather_data table in the PostgreSQL database"""

    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        cur.execute(command)
        print("Table created successfully.")
        return conn

    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error creating table: {error}")




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
    config_main_db = load_config('database.ini', 'main_database')
    config_new_db = load_config('database.ini', 'weather_info_database')
    print(config_main_db, config_new_db)

    db_connection  = create_weather_database(config_main_db, config_new_db, command_create_db)
    print('db_connection', db_connection)
    if db_connection is not None:
        conn = create_weather_table(config_new_db, create_table_command)

        api_key, api_base_url = env_config_loading()
        for city_nm in CITIES:
            response_data = fetch_weather_data(city_nm, api_key, api_base_url)
            insert_data(conn, city_nm, response_data)
