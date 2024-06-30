""" Module providing integration tests for the weather data application. """

import os
import unittest
import subprocess
import psycopg2
import sys
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.weather_API_data.create_table import load_config, env_config_loading, fetch_weather_data
from src.weather_API_data.store_data import insert_data, create_weather_database, create_weather_table




# TODO: Add more tests for fetching
class TestIntegrationFetchingWeatherData(unittest.TestCase):
    """List of integartion tests for fetching seather sata ."""
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        cls.env_path = Path('.') / '.env'

    def test_fetch_weather_data_success(self):
        """Integaration Tests for fetching weather data."""

        api_key, api_base_url = env_config_loading(self.env_path)
        resp_data = fetch_weather_data("Seoul", api_key, api_base_url)
#        self.assertIn('current', resp_data)
#        self.assertIn('temperature', resp_data)
#        self.assertIn('pressure', resp_data)
#        self.assertIn('humidity', resp_data)

    @classmethod
    def tearDownClass(self):
        pass




class TestIntegrationStoreWeatherData(unittest.TestCase):
    """List of integartion tests for storing weather data in the database."""

    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '../../scripts/setup_test_db.py'))

        subprocess.run(["python", script_path])
        cls.main_db_conf = load_config('test_database.ini', 'main_database')
        cls.test_db_conf = load_config('test_database.ini', 'test_weather_database')
        cls.conn = psycopg2.connect(**cls.test_db_conf)

    def setUp(self):
        """Set up before each test."""
        self.create_test_table_cmd = """
                CREATE TABLE IF NOT EXISTS test_weather_data (
                    id SERIAL PRIMARY KEY,
                    city_name VARCHAR(255),
                    temperature FLOAT,
                    pressure INT,
                    humidity INT,
                    date_time TIMESTAMP
                )
                """
        self.insert_data_cmd = """INSERT INTO test_weather_data (
                        city_name, temperature, pressure, humidity, date_time)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id;"""

        if self.conn is not None:
            self.tbl_conn = create_weather_table(self.test_db_conf, self.create_test_table_cmd)

    def test_insert_data_success(self):
        """Integration test for insert_data method."""

        city = "Paris"
        weather_data = {
            "current": {
                "temperature": 24,
                "pressure": 1001,
                "humidity": 74,
            }}

        for index in range(11):
            row_id = insert_data(self.tbl_conn, city, weather_data, self.insert_data_cmd)
            self.assertEqual(row_id, index + 1)

    @classmethod
    def tearDownClass(self):
        db_name = 'test_weather_db'
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()

        conn = psycopg2.connect(**self.test_db_conf)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_weather_data")
        if hasattr(self, 'conn') and self.conn is not None:
            self.conn.close()

        # Drop test_weather_db
        conn = psycopg2.connect(**self.main_db_conf)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                            SELECT datname from pg_catalog.pg_database
                            WHERE datname ='{db_name}'
                            """)
                if cur.fetchone() is not None:
                    cur.execute("DROP DATABASE test_weather_db")
                    print(f"Database {db_name} dropped successfully")

        except(psycopg2.DatabaseError, Exception) as error:
            print(f"Error dropping database {db_name} : {error}")

        finally:
            conn.close()




if __name__ == '__main__':
    unittest.main()
