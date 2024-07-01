import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import sys
sys.path.append('./')
from src.weather_API_data.fetch_data import load_config, env_config_loading, fetch_weather_data




COMMAND = "SELECT * FROM weather_data"




def read_data(db_conf, command):

    try:
        with psycopg2.connect(**db_conf) as conn:
                data = pd.read_sql_query(command, conn)
                if not data.empty:
                    print("Weather Data:")
                    return data
    
    except psycopg2.DatabaseError as error:
        print(f"Error connecting to database {error}")

    except Exception as error:
        print(f"Exception occured {error}")

if __name__ == '__main__':
    config_new_db = load_config('database.ini', 'weather_info_database')
    result = read_data(config_new_db, COMMAND)
    print(result) 
