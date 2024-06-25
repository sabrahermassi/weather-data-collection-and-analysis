import psycopg2
import requests
from datetime import datetime
import sys
sys.path.append('../')
from src.weather_API_data.createTable import load_config, create_weather_table




# Set up the API parameters
api_key = "325de9e270755c796bfe89791f69f365"
api_base_url = "http://api.weatherstack.com/current"
cities = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]




def insert_data(conn, city_name, response_dict):
    """ Inset data in the weather_data table """
    command = """INSERT INTO weather_data (city_name, temperature, pressure, humidity, date_time)
                    VALUES (%s, %s, %s, %s, %s);"""
    
    if 'current' not in response_dict:
        raise Exception(f"Error fetching data for {city}: 'current' key not found in response")
    else:
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
    url = f"{api_base_url}?access_key={api_key}&query={city}"

    try: 
        response = requests.get(url)
        response_dict = response.json()
        return response_dict

    except (requests.ConnectionError , Exception) as error:
        print(error)




if __name__=='__main__':
    config = load_config()
    conn = create_weather_table(config)

    for city in cities:
        response_dict = fetch_weather_data(city)
        insert_data(conn, city, response_dict)
