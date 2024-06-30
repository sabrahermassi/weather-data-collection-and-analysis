""" Module providing integration tests for the weather data application. """

import os
import unittest
import subprocess
import psycopg2
import sys
from pathlib import Path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from weather_API_data.fetch_data import load_config, env_config_loading, fetch_weather_data
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

        self.api_key, self.api_base_url = env_config_loading(self.env_path)
        self.assertIsNotNone(self.api_key)
        self.assertIsNotNone(self.api_base_url)
        
        self.resp_data = fetch_weather_data("Seoul", self.api_key, self.api_base_url)
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

        db_create_cmd = """CREATE DATABASE test_weather_db"""

        cls.main_db_conf = load_config('test_database.ini', 'main_database')
        assert 'host' in cls.main_db_conf, "Main database config missing 'host'"
        assert 'port' in cls.main_db_conf, "Main database config missing 'port'"
        assert 'user' in cls.main_db_conf, "Main database config missing 'user'"
        assert 'password' in cls.main_db_conf, "Main database config missing 'password'"

        cls.test_db_conf = load_config('test_database.ini', 'test_weather_database')
        assert 'host' in cls.test_db_conf, "Test database config missing 'host'"
        assert 'user' in cls.test_db_conf, "Tesy database config missing 'user'"
        assert 'password' in cls.test_db_conf, "Test database config missing 'password'"

        cls.db_conn  = create_weather_database(
            cls.main_db_conf,
            cls.test_db_conf,
            db_create_cmd
            )
        assert cls.db_conn is not None

        cls.conn = psycopg2.connect(**cls.test_db_conf)
        assert cls.conn is not None

    def setUp(self):
        """Set up before each test."""
        self.city = "Paris"
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
            assert self.tbl_conn is not None

    def test_insert_data_success(self):
        """Integration test for insert_data method success case."""

        weather_data = {
            "current": {
                "temperature": 24,
                "pressure": 1001,
                "humidity": 74,
            }}

        for index in range(11):
            row_id = insert_data(self.tbl_conn, self.city, weather_data, self.insert_data_cmd)
            self.assertEqual(row_id, index + 1)

    def test_insert_data_wrong_data_format(self):
        """Integration test for insert_data method failure : Wrong data format."""

        wrong_weather_data = {
            "wrong_current": {
                "temperature": 24,
                "pressure": 1001,
                "humidity": 74,
            }}

        with self.assertRaises(ValueError) as context:
            insert_data(self.tbl_conn, self.city, wrong_weather_data, self.insert_data_cmd)
        
        expected_message = "Error fetching data for Paris: 'current' key not found in response"
        actual_message = str(context.exception)
        self.assertTrue(expected_message in actual_message, f"Expected error message not found. Actual message: {actual_message}")

    def test_insert_data_missing_data_in_current(self):
        """Integration test for insert_data method failure : Missing data in current."""

        wrong_weather_data = {
            "current": {
                "pressure": 1001,
                "humidity": 74,
            }}

        with self.assertRaises(KeyError) as context:
            insert_data(self.tbl_conn, self.city, wrong_weather_data, self.insert_data_cmd)
        
        expected_message = "'temperature'"
        actual_message = str(context.exception)
        self.assertTrue(expected_message in actual_message, f"Expected error message not found. Actual message: {actual_message}")

    def test_insert_data_conn_error(self):
        """Integration test for insert_data method failure : Connection problem."""

        self.tbl_conn = None
        weather_data = {
            "current": {
                "temperature": 24,
                "pressure": 1001,
                "humidity": 74,
            }}

        with self.assertRaises(AttributeError) as context:
            insert_data(self.tbl_conn, self.city, weather_data, self.insert_data_cmd)

        expected_message = "'NoneType' object has no attribute 'cursor'"
        actual_message = str(context.exception)
        self.assertTrue(expected_message in actual_message, f"Expected error message not found. Actual message: {actual_message}")

    def test_insert_data_wrong_command(self):
        """Integration test for insert_data method failure : Wrong command."""

        self.insert_data_wrong_cmd = """INSERT INTO test_wrong_weather_data (
                        city_name, temperature, pressure, humidity, date_time)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id;"""
        weather_data = {
            "current": {
                "temperature": 24,
                "pressure": 1001,
                "humidity": 74,
            }}

        with self.assertRaises(psycopg2.DatabaseError) as context:
            insert_data(self.tbl_conn, self.city, weather_data, self.insert_data_wrong_cmd)
        
        expected_message = """relation "test_wrong_weather_data" does not exist"""
        actual_message = str(context.exception)
        self.assertTrue(expected_message in actual_message, f"Expected error message not found. Actual message: {actual_message}")

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
