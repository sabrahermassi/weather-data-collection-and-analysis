""" Module providing functions that load a config file
    and create a weather_table in postgres database. """

from configparser import ConfigParser
import psycopg2




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
        raise ValueError(f'Section {0} not found in the {1} file'.format(section, filename))




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
