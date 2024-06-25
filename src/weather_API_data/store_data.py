""" Module providing functions that fetch weather data
    and store it in postgres database. """

from datetime import datetime
import sys
import psycopg2
import requests
sys.path.append('../')
from weather_api_data.create_table import load_config, create_weather_table




# Set up the API parameters
API_KEY = "325de9e270755c796bfe89791f69f365"
API_BASE_URL = "http://api.weatherstack.com/current"
CITIES = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]




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

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)




def fetch_weather_data(city):
    """ Fetch weather data from the API  """
    url = f"{API_BASE_URL}?access_key={API_KEY}&query={city}"

    try:
        response = requests.get(url, timeout=10)
        response_dict = response.json()
        return response_dict

    except (requests.ConnectionError , Exception) as error:
        print(error)
        return




if __name__=='__main__':
    config = load_config()
    connection = create_weather_table(config)

    for city_nm in CITIES:
        response_data = fetch_weather_data(city_nm)
        insert_data(connection, city_nm, response_data)
