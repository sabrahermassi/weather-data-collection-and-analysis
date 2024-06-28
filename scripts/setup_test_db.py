""" A script that creates a testing database. """

import psycopg2
import sys
sys.path.append('./')
from src.weather_api_data.create_table import load_config
from src.weather_api_data.store_data import create_weather_database, create_weather_table




create_table_command = """
    CREATE TABLE IF NOT EXISTS test_weather_data (
        id SERIAL PRIMARY KEY,
        city_name VARCHAR(255),
        temperature FLOAT,
        pressure INT,
        humidity INT,
        date_time TIMESTAMP
    )
"""

command_create_test_db = """CREATE DATABASE test_weather_db"""




def create_test_database(config_file):
    """ Create  test database test_weather_db and tests table test_weather_data """

    db_connection = None
    connection = None

    try:
        config_main_db = load_config(config_file, 'main_database')
        config_new_db = load_config(config_file, 'test_weather_database')

        db_connection  = create_weather_database(config_main_db, config_new_db, command_create_test_db)
        if db_connection is not None:
            create_weather_table(config_new_db, create_table_command)


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)




if __name__ == '__main__':
    create_test_database('test_database.ini')
