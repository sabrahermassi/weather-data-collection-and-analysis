import unittest
import psycopg2
import testing.postgresql
from sqlalchemy import create_engine
import sys
sys.path.append('../')
from src.weather_API_data.storeData import insert_data




response_dict =    {
      "request": {
         "type": "City",
         "query": "Seoul, South Korea",
         "language": "en",
         "unit": "m"
      },
      "location": {
         "name": "Seoul",
         "country": "South Korea",
         "region": "",
         "lat": "37.566",
         "lon": "127.000",
         "timezone_id": "Asia/Seoul",
         "localtime": "2024-06-22 03:43",
         "localtime_epoch": 1719027780,
         "utc_offset": "9.0"
      },
      "current": {
         "observation_time": "06:43 PM",
         "temperature": 24,
         "weather_code": 113,
         "weather_icons": [
            "https://cdn.worldweatheronline.com/images/wsymbols01_png_64/wsymbol_0008_clear_sky_night.png"
         ],
         "weather_descriptions": [
            "Clear"
         ],
         "wind_speed": 7,
         "wind_degree": 160,
         "wind_dir": "SSE",
         "pressure": 1001,
         "precip": 0,
         "humidity": 74,
         "cloudcover": 0,
         "feelslike": 25,
         "uv_index": 1,
         "visibility": 10,
         "is_day": "no"
      }
   }


class MyTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.postgresql = testing.postgresql.Postgresql()
        connection_str = self.postgresql.url()
        engine = create_engine(connection_str)
        self.db = psycopg2.connect(**self.postgresql.dsn())
        try:
            with engine.connect() as connection_str:
                print('Successfully connected to the PostgreSQL database')
        except Exception as ex:
            print(f'Sorry failed to connect: {ex}')
        
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
           self.db.cursor.execute(command)
        except (psycopg2.DatabaseError, Exception) as error:
            print(error)

    def tearDown(self):
        self.postgresql.stop()

    def test_store_data_into_Db(self):
        insert_data(self.db, "Seoul", response_dict)



if __name__ == "__main__":
    unittest.main()