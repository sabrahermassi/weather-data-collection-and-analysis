""" Module providing Unit Tests for get_weather_data. """

import unittest
from unittest.mock import MagicMock, patch
import sys
import psycopg2
from psycopg2 import extensions
sys.path.append('./')
from src.weather_API_data.read_data import get_weather_data




class TestReadData(unittest.TestCase):
    """Tests for creating the weather_data database."""

    def setUp(self):
        self.config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
        }

        self.city_filter = {'city_name' : ['Seoul']}

        self.weather_data = [
            [5, "Seoul", 24.0, 1001, 74, "Thu, 04 Jul 2024 22:42:12 GMT"]
            ]

    @patch('psycopg2.connect')
    def test_get_weather_data_success(self, mock_connect):
        """Test for success case of get_weather_data."""

        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.set_isolation_level.return_value = None
        mock_cur.execute.return_value = None
        mock_cur.fetchall.return_value = self.weather_data

        weather_data = get_weather_data(self.config, self.city_filter)

        mock_connect.assert_called_once_with(**self.config)
        mock_conn.set_isolation_level.assert_called_with(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call(
            "SELECT * FROM weather_data WHERE city_name IN (%s)", ['Seoul']
        )
        self.assertEqual(weather_data, self.weather_data)

    @patch('psycopg2.connect')
    def test_get_weather_data_filters_none(self, mock_connect):
        """Test for passing no filters case for get_weather_data."""
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_conn.set_isolation_level.return_value = None
        mock_cur.execute.return_value = None
        mock_cur.fetchall.return_value = None

        weather_data = get_weather_data(self.config)

        mock_connect.assert_called_once_with(**self.config)
        mock_conn.set_isolation_level.assert_called_with(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call("SELECT * FROM weather_data", [])
        self.assertIsNone(weather_data)

    @patch('psycopg2.connect')
    def test_get_weather_data_failure(self, mock_connect):
        """Test for failure case of get_weather_data."""

        mock_connect.side_effect = psycopg2.DatabaseError("Connection error")

        with self.assertRaises(psycopg2.DatabaseError):
            weather_data = get_weather_data(self.config, self.city_filter)
            mock_connect.assert_called_once_with(**self.config)
            self.assertIsNone(weather_data)




if __name__ == '__main__':
    unittest.main()
