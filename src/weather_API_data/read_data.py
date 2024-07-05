""" Module providing functions that reads weather data from the postgres database. """

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT




def get_weather_data(db_conf, command, filters=None):
    """ Retrieve data from the weather_data table """

    try:
        with psycopg2.connect(**db_conf) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:  
                params = []
                if filters:
                    filter_clauses = [
                        f"{key} IN ({','.join(['%s'] * len(value))})" if isinstance(value, list)
                        else f"{key} = %s"
                        for key, value in filters.items()
                    ]

                    if filter_clauses:
                        command += " WHERE " + " AND ".join(filter_clauses)

                    params = [
                        item
                        for sublist in(
                            [value] if not isinstance(value, list) else value
                            for value in filters.values()
                            )
                            for item in sublist
                            ]

                else:
                    params = []

                new_var = command
                rows = cur.execute(new_var, params)
                print("The number of cities: ", cur.rowcount)

                rows = cur.fetchall()
        
                return rows

    except psycopg2.DatabaseError as error:
        print(f"Error connecting to database {error}")
        raise

    except Exception as error:
        print(f"Exception occured {error}")
        raise
