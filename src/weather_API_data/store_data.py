""" Module providing functions that fetch weather data
    and store it in postgres database. """

from datetime import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT




def create_weather_database(config_main_db, config_new_db, command):
    """ Create weather database """

    try:
        conn = psycopg2.connect(**config_main_db)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if the database exists
        database_name = config_new_db['database']
        cur.execute("SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
                    (database_name,))
        exists = cur.fetchone()
        if exists is None:
            cur.execute(command)
            print(f"Database {config_new_db['database']} created successfully.")

        return psycopg2.connect(**config_new_db)

    except psycopg2.DatabaseError as error:
        print(f"Failed to connect to weather database : {error}")
        return None

    except Exception as error:
        print(f"Failed to create to weather database : {error}")
        return None




def create_weather_table(config_new_db, command):
    """ Create weather_data table in the PostgreSQL database"""

    try:
        conn = psycopg2.connect(**config_new_db)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Create the table if it does not exist
        cur.execute(command)

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
        raise ValueError(
            f"""Error fetching data for {
                city_name
                }: 'current' key not found in response""")

    try:
        temperature = response_dict['current']['temperature']
        pressure = response_dict['current']['pressure']
        humidity = response_dict['current']['humidity']

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
        print(f"""Database error : {error}""")
        raise

    except KeyError as error:
        print(f"""Error fetching data for {city_name}: {error} key not found in response""")
        raise

    except Exception as error:
        print(f"Failed to save data into weather database : {error}")
        raise
