""" Module providing integration tests for the weather data application. """

import os
import unittest
import subprocess
import psycopg2
from src.weather_api_data.create_table import load_config, env_config_loading, fetch_weather_data
from src.weather_api_data.store_data import insert_data



class TestIntegration(unittest.TestCase):
    """List of integartion tests for the weather data application."""

    @classmethod
    def setUpClass(self):
        script_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), '../../scripts/setup_test_db.py'))

        subprocess.run(["python", script_path])
        self.main_db_conf = load_config('test_database.ini', 'main_database')
        self.test_db_conf = load_config('test_database.ini', 'test_weather_database')
        self.conn = psycopg2.connect(**self.test_db_conf)   

    def test_env_config_loading(self):
        """Test for env_config_loading."""

        api_key, api_base_url = env_config_loading()
        self.assertIsNotNone(api_key)
        self.assertIsNotNone(api_base_url)

    def test_create_weather_table(self):
        """Test for create_weather_table."""

        cur = self.conn.cursor()
        cur.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'test_weather_data')")
        res = cur.fetchone()
        self.assertIsNotNone(res)
        res1 = res[0]
        self.assertTrue(res1)

    def test_fetch_weather_data(self):
        """Test for fetch_weather_data."""

        api_key, api_base_url = env_config_loading()
        resp_data = fetch_weather_data("Seoul", api_key, api_base_url)
#        self.assertIn('current', resp_data)
#        self.assertIn('temperature', resp_data)
#        self.assertIn('pressure', resp_data)
#        self.assertIn('humidity', resp_data)

    def test_insert_data(self):
        """Test for insert_data."""

        city = "Paris"
        api_key, api_base_url = env_config_loading()
        resp_data = fetch_weather_data(city, api_key, api_base_url)
#        row_count = insert_data(self.conn, city, resp_data)
#        self.assertEqual(row_count, 1)

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
