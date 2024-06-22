import psycopg2
import requests
from datetime import datetime
from createTable import load_config, create_weather_table




# Set up the API parameters
api_key = "325de9e270755c796bfe89791f69f365"
api_base_url = "http://api.weatherstack.com/current"
cities = ["Seoul", "pusan", "Malmö", "Stockholm", "Paris", "Taipei", "London"]




def insert_data(conn, city_name, temp, pressure, humidity):
    """ Inset data in the weather_data table """
    command = """INSERT INTO weather_data (city_name, temp, pressure, humidity, date_time)
                    VALUES (%s, %s, %s, %s, %s);"""

    try:
        with conn.cursor() as cur:
            now = datetime.now()
            date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
            val = (city_name, temp, pressure, humidity, date_time)
            cur.execute(command, val)
            print(cur.rowcount, "record inserted.")

    except (psycopg2.DatabaseError, Exception) as error:
        print(error)




def fetch_insert_weather_data(conn):
    """ Fetch weather data from the API and call insert method """
    for city in cities:
        url = f"{api_base_url}?access_key={api_key}&query={city}"
        response = requests.get(url)
        response_dict = response.json()
        if 'current' not in response_dict:
            print(f"Error fetching data for {city}: 'current' key not found in response")
            continue

        temperature = response_dict['current']['temperature']
        pressure = response_dict['current']['pressure']
        humidity = response_dict['current']['humidity']
        insert_data(conn, city, temperature, pressure, humidity)




if __name__=='__main__':
    config = load_config()
    conn = create_weather_table(config)
    fetch_insert_weather_data(conn)
