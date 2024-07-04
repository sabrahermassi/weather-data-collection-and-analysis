""" Module providing Unit Tests for create_weather_table 
    and insert_data methods in store_data.py file. """

import unittest
import unittest.mock

from datetime import datetime
import sys
from unittest.mock import MagicMock, patch
import psycopg2
from psycopg2 import extensions



sys.path.append('./')

from src.weather_API_data.store_data import (
    create_weather_database,
    create_weather_table,
    insert_data
)




class TestCreateDatabase(unittest.TestCase):
    """Tests for creating the weather_data database."""

    @classmethod
    def setUpClass(cls):
        cls.config_main = {
            'host': 'localhost',
            'database': 'test_db_main',
            'user': 'test_user_main',
            'password': 'test_password_main',
        }

        cls.config_new = {
            'host': 'localhost',
            'database': 'test_db_new',
            'user': 'test_user_new',
            'password': 'test_password_new',
        }

    @patch('psycopg2.connect')
    def test_create_weather_database_success(self, mock_connect):
        """Test for success case of create_weather_database."""

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur
        mock_conn.set_isolation_level.return_value = extensions.ISOLATION_LEVEL_AUTOCOMMIT

        mock_cur.fetchone.return_value = None
        command = """CREATE DATABASE test_db_new"""

        result = create_weather_database(self.config_main, self.config_new,command)
        mock_connect.assert_called_with(**self.config_new)
        mock_conn.cursor.assert_called_once()
        database_name = self.config_new['database']
        mock_cur.execute.assert_any_call(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = %s",
            (database_name,)
            )
        mock_cur.execute.assert_any_call(command)
        self.assertEqual(result, mock_conn)

    @patch('psycopg2.connect')
    def test_create_weather_database_error(self, mock_connect):
        """Test for failure case of create_weather_database."""

        mock_connect.side_effect = psycopg2.DatabaseError("Connection error")

        command = """CREATE DATABASE test_db_new"""

        result = create_weather_database(self.config_main, self.config_new, command)
        mock_connect.assert_called_once_with(**self.config_main)
        self.assertEqual(result, None)




class TestCreateTable(unittest.TestCase):
    """Tests for creating the weather_data table in the database."""

    @classmethod
    def setUpClass(cls):
        cls.config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
        }

        cls.command = """CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(255),
                temp FLOAT,
                pressure INT,
                humidity INT,
                date_time TIMESTAMP
            )"""

    @patch('psycopg2.connect')
    def test_create_weather_table_success(self, mock_connect):
        """Test for success case of create_weather_table."""

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        result = create_weather_table(self.config, self.command)

        mock_connect.assert_called_once_with(**self.config)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call(unittest.mock.ANY)
        self.assertEqual(result, mock_conn)

    @patch('psycopg2.connect')
    def test_create_weather_table_error(self, mock_connect):
        """Test for failure case of create_weather_table."""

        mock_connect.side_effect = psycopg2.DatabaseError("Connection error")

        result = create_weather_table(self.config, self.command)

        mock_connect.assert_called_once_with(**self.config)
        self.assertEqual(result, None)




class TestStoreWeatherData(unittest.TestCase):
    """ Tests for storing weather data in the database """

    @classmethod
    def setUpClass(cls):
        cls.city = "Seoul"
        cls.command = """INSERT INTO weather_data (
                        city_name, temperature, pressure, humidity, date_time)
                        VALUES (%s, %s, %s, %s, %s) RETURNING id;"""
        cls.weather_data = { "current":
                                {
                                "temperature": 24,
                                "pressure": 1001,
                                "humidity": 74,
                                }
                            }
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        cls.weather_value = ("Seoul", 24, 1001, 74, date_time)

    def setUp(self):
        self.mock_conn = MagicMock()

    def test_insert_data_success(self):
        """Test for success case of insert_data."""

        mock_cur = MagicMock()
        self.mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.execute.return_value = None
        mock_cur.fetchone.return_value = (1,)

        result = insert_data(self.mock_conn, self.city, self.weather_data, self.command)
        self.mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call(unittest.mock.ANY, self.weather_value)
        self.assertEqual(result, 1)

    def test_insert_data_failure(self):
        """Test for failure case of insert_data."""

        self.mock_conn.cursor.side_effect = psycopg2.DatabaseError("Connection error")

        with self.assertRaises(psycopg2.DatabaseError):
            insert_data(self.mock_conn, self.city, self.weather_data, self.command)




if __name__ == '__main__':
    unittest.main()
