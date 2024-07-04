""" Module providing functions that reads weather data from the postgres database. """

import psycopg2




def get_weather_data(db_conf, filters=None):
    """ Retrieve data from the weather_data table """

    try:
        with psycopg2.connect(**db_conf) as conn:
            with conn.cursor() as cur:
                command = "SELECT * FROM weather_data"

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


                rows = cur.execute(command, params)
                print("The number of cities: ", cur.rowcount)
                rows = cur.fetchall()

                return rows

    except psycopg2.DatabaseError as error:
        print(f"Error connecting to database {error}")
        return None

    except Exception as error:
        print(f"Exception occured {error}")
        return None
