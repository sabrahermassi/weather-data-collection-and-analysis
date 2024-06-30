""" Module providing functions that fetch weather data
    and store it in postgres database. """

import sys
from datetime import datetime
import psycopg2
from pathlib import Path
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
sys.path.append('./')
from src.weather_API_data.create_table import load_config, env_config_loading, fetch_weather_data




ENV_PATH = Path('.') / '.env'
CITIES = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]
CREATE_TABLE_COMMAND = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city_name VARCHAR(255),
        temperature FLOAT,
        pressure INT,
        humidity INT,
        date_time TIMESTAMP
    )
"""
CREATE_DATABASE_COMMAND = """CREATE DATABASE weather_info_db"""
INSERT_DATA_COMMAND = """INSERT INTO weather_data (city_name, temperature, pressure, humidity, date_time)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id;"""




def create_weather_database(config_main, config_new, command):
    """ Create weather database"""

    try:
        conn = psycopg2.connect(**config_main)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if the database exists
        database_name = config_new['database']
        cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
                    (database_name,))
        exists = cur.fetchone()
        if exists is None:
            cur.execute(command)
            print(f"Database {config_new['database']} created successfully.")
        
        return psycopg2.connect(**config_new)

    except psycopg2.DatabaseError as error:
        print(f"Failed to connect to weather database : {error}")
        return None

    except Exception as error:
        print(f"Failed to create to weather database : {error}")
        return None




def create_weather_table(config, command):
    """ Create weather_data table in the PostgreSQL database"""

    try:
        conn = psycopg2.connect(**config)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        cur.execute(command)
        print("Table created successfully.")
        return conn

    except psycopg2.DatabaseError as error:
        print(f"Failed to create weather table in weather database : {error}")
        return None

    except Exception as error:
        print(f"Failed to create weather table in weather database : {error}")
        return None




def insert_data(conn, city_name, response_dict, command):
    """ Inset data in the weather_data table """

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

            rows = cur.fetchone()
            if rows:
                inserted_id = rows[0]
            return inserted_id

    except psycopg2.DatabaseError as error:
        print(f"Failed to insert data into weather database : {error}")
        return None

    except Exception as error:
        print(f"Failed to insert data into weather database : {error}")
        return None




if __name__=='__main__':
    config_main_db = load_config('database.ini', 'main_database')
    config_new_db = load_config('database.ini', 'weather_info_database')
    db_connection  = create_weather_database(config_main_db, config_new_db, CREATE_DATABASE_COMMAND)

    if db_connection is not None:
        conn_tbl = create_weather_table(config_new_db, CREATE_TABLE_COMMAND)

        api_key, api_base_url = env_config_loading(ENV_PATH)
        for city_nm in CITIES:
            response_data = fetch_weather_data(city_nm, api_key, api_base_url)
            insert_data(conn_tbl, city_nm, response_data, INSERT_DATA_COMMAND)
