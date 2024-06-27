""" Module providing Unit Tests for create_weather_table 
    and insert_data methods in store_data.py file. """

from datetime import datetime
import psycopg2
import sys
import psycopg2
import unittest
from unittest.mock import MagicMock, patch
import unittest.mock
sys.path.append('../')
from src.weather_api_data.store_data import create_weather_table, insert_data 




class TestCreateTable(unittest.TestCase):
    """Tests for creating the weather_data table in the database."""

    @patch('psycopg2.connect')
    def test_create_weather_table_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cur

        config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
        }

        command = """CREATE IF NOT EXISTS TABLE weather_data (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(255),
                temp FLOAT,
                pressure INT,
                humidity INT,
                date_time TIMESTAMP
            )"""

        result = create_weather_table(config, command)

        mock_connect.assert_called_once_with(**config)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call(unittest.mock.ANY)
        self.assertEqual(result, mock_conn)
    
    @patch('psycopg2.connect')
    def test_create_weather_table_error(self, mock_connect):
        mock_connect.side_effect = psycopg2.DatabaseError("Connection error")

        config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
        }

        command = """CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city_name VARCHAR(255),
                temp FLOAT,
                pressure INT,
                humidity INT,
                date_time TIMESTAMP
            )"""

        result = create_weather_table(config, command)

        mock_connect.assert_called_once_with(**config)
        self.assertEqual(result, None)




class TestStoreWeatherData(unittest.TestCase):
    """ Tests for storing weather data in the database """

    @classmethod
    def setUpClass(self):
        self.city = "Seoul"
        self.weather_data = { "current":
                                {
                                "temperature": 24,
                                "pressure": 1001,
                                "humidity": 74,
                                }
                            }
        now = datetime.now()
        date_time = now.strftime("%m/%d/%Y, %H:%M:%S")
        self.weather_value = ("Seoul", 24, 1001, 74, date_time)

    def test_insert_data_success(self):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.execute.return_value = None
        mock_cur.rowcount = 1  # Simulate the rowcount

        result = insert_data(mock_conn, self.city, self.weather_data)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call(unittest.mock.ANY, self.weather_value)
        self.assertEqual(result, 1)
    
    def test_insert_data_failure(self):
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = psycopg2.DatabaseError("Connection error")

        result = insert_data(mock_conn, self.city, self.weather_data)
        self.assertEqual(result, None)





if __name__ == '__main__':
    unittest.main()
