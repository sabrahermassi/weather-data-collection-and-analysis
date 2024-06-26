""" Module providing Unit Tests for load_config, env_config_loading
    and create_weather_table methods in create_table.py file. """

import sys
import unittest
import psycopg2
from unittest.mock import MagicMock, patch
sys.path.append('../')
from src.weather_api_data.create_table import load_config, env_config_loading, create_weather_table




class TestLoadConfig(unittest.TestCase):
    """Tests for loading configuration."""

    def test_load_config_suceess(self):
        # Create a sample database.ini file
        with open('database.ini', 'w') as f:
            f.write("[postgresql]\nhost=localhost\ndatabase=random_database\nuser=random_user\npassword=random_password")

        config_infos = load_config('database.ini', 'postgresql')

        self.assertEqual(config_infos['host'], 'localhost')
        self.assertEqual(config_infos['database'], 'random_database')
        self.assertEqual(config_infos['user'], 'random_user')
        self.assertEqual(config_infos['password'], 'random_password')
    
    def test_load_config_failure(self):
        # Load a file that does not exist
        with self.assertRaises(ValueError):
            load_config('db_random.ini', 'postgresql')




class TestCreateTable(unittest.TestCase):
    """Tests for creating the weather_data table in the database."""

    @patch('psycopg2.connect')
    def test_create_weather_table_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur = MagicMock()

        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
        }
        result = create_weather_table(config)

        mock_connect.assert_called_once_with(**config)
        mock_conn.cursor.assert_called()
        mock_cur.execute.assert_any_call("DROP TABLE  IF EXISTS weather_data")
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
        result = create_weather_table(config)

        mock_connect.assert_called_once_with(**config)
        self.assertEqual(result, None)




if __name__ == '__main__':
    unittest.main()
