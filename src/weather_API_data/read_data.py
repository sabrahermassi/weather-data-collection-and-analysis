import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
import sys
sys.path.append('./')
from src.weather_API_data.fetch_data import load_config, env_config_loading, fetch_weather_data




COMMAND = "SELECT * FROM weather_data"




def read_data(db_conf, command, filters=None):

    try:
        with psycopg2.connect(**db_conf) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            if filters:
                filter_clauses = [f"{col} = %s" for col in filters.keys()]
                command += " WHERE " + " AND ".join(filter_clauses)
            
            data = pd.read_sql_query(command, conn, params=list(filters.values()) if filters else None)
            if not data.empty:
                print("Weather Data:")  
                return data
    
    except psycopg2.DatabaseError as error:
        print(f"Error connecting to database {error}")

    except Exception as error:
        print(f"Exception occured {error}")

if __name__ == '__main__':
    config_new_db = load_config('database.ini', 'weather_info_database')
    filters = {
        'city_name' : 'pusan'
        }
    result = read_data(config_new_db, COMMAND, filters)
    print(result) 
