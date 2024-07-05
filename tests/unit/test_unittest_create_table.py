""" Module providing Unit Tests for load_config, and
    fetch_weather_data methods in create_table.py file. """

import os
from pathlib import Path
from http import HTTPStatus
import json
import unittest
import sys
sys.path.append('./')
from unittest.mock import MagicMock, patch, mock_open
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.weather_API_data.fetch_data import load_config, env_config_loading, fetch_weather_data




class TestLoadConfig(unittest.TestCase):
    """Test Suite for load_config method."""

    @patch('builtins.open', new_callable=mock_open, read_data="""[postgresql]
            host=localhost
            database=random_database
            user=random_user
            password=random_password""")
    def test_load_config_suceess(self, mock_file):
        """Test Success for load_config."""
        config_infos = load_config('temp_database.ini', 'postgresql')

        self.assertEqual(config_infos['host'], 'localhost')
        self.assertEqual(config_infos['database'], 'random_database')
        self.assertEqual(config_infos['user'], 'random_user')
        self.assertEqual(config_infos['password'], 'random_password')

        mock_file.assert_called_with('temp_database.ini', encoding='locale')

    @patch('builtins.open', new_callable=mock_open, read_data="")
    def test_load_config_empty_file(self, mock_file):
        """Test failure for load_config: Empty file"""
        with self.assertRaises(ValueError):
            load_config('empty_database.ini', 'postgresql')

        mock_file.assert_called_with('empty_database.ini', encoding='locale')

    @patch('builtins.open', new_callable=mock_open, read_data="""[WrongSection]
            host=localhost
            database=random_database
            user=random_user
            password=random_password""")
    def test_load_config_missing_section_file(self, mock_file):
        """Test failure for load_config: Missing Section"""
        with self.assertRaises(ValueError):
            load_config('wrong_section.ini', 'postgresql')

        mock_file.assert_called_with('wrong_section.ini', encoding='locale')


    def test_load_config_file_does_not_exist(self):
        """Test failure for load_config: File does not exist."""
        # Load a file that does not exist
        with self.assertRaises(ValueError):
            load_config('db_random.ini', 'postgresql')



class TestLoadEnvConfig(unittest.TestCase):
    """Test Suite for env_config_loading method."""

    @patch('builtins.open', new_callable=mock_open, read_data="""
           API_KEY=test_key\nAPI_BASE_URL=https://api.example.com""")
    @patch('os.getenv')
    def test_env_config_loading_success(self, mock_getenv, mock_file):
        """Test Success for env_config_loading."""
        mock_getenv.side_effect = lambda key: {
            'API_KEY': 'test_key',
            'API_BASE_URL': 'https://api.example.com'
            }.get(key)

        env_path = Path('.') / '.env'
        api_key, api_base_url = env_config_loading(env_path)

        self.assertEqual(api_key, 'test_key')
        self.assertEqual(api_base_url, 'https://api.example.com')

        mock_file.assert_called_with(env_path, encoding='utf-8')

    @patch('builtins.open', new_callable=mock_open, read_data="""API_KEY=\nAPI_BASE_URL=""")
    @patch('os.getenv')
    def test_env_config_loading_empty_keys_env(self, mock_getenv, mock_file):
        """Test Failure for env_config_loading: values for keys in .env file are empty"""
        mock_getenv.side_effect = lambda key: {'API_KEY': '', 'API_BASE_URL': ''}.get(key)

        env_path = Path('.') / '.env'
        with self.assertRaises(ValueError):
            env_config_loading(env_path)

        mock_file.assert_called_with(env_path, encoding='utf-8')

    @patch('builtins.open', new_callable=mock_open, read_data="")
    @patch('os.getenv')
    def test_env_config_loading_empty_env(self, mock_getenv, mock_file):
        """Test Failure for env_config_loading: .env file is empty"""
        mock_getenv.side_effect = lambda key: {'API_KEY': '', 'API_BASE_URL': ''}.get(key)

        env_path = Path('.') / '.env'
        with self.assertRaises(ValueError):
            env_config_loading(env_path)

        mock_file.assert_called_with(env_path, encoding='utf-8')

    def test_env_config_loading_inexistent_file(self):
        """Test Failure for env_config_loading: .env file is inexitent"""
        env_path = Path('.') / '.nonexistentenv'
        with self.assertRaises(FileNotFoundError):
            env_config_loading(env_path)




class TestFetchWeatherData(unittest.TestCase):
    """Test Suite for fetch_weather_data method."""
    @classmethod
    def setUpClass(cls):
        file_path = os.path.join(os.path.dirname(__file__), "resources", "weather.json")
        with open(file_path, encoding='locale') as f:
            file_content = json.load(f)
            cls.json_object_success = file_content[0]
            cls.json_object_104 = file_content[1]
            cls.fake_api_key = "1234567890qwertyuiop"
            cls.fake_api_base_url = "http://api.fakeweatherapi.com"
            cls.city = "Seoul"

    @patch('requests.get')
    def test_fetch_weather_data_success(self, mocker_get):
        """Given a city name, test that a HTML report about the weather is generated
        correctly."""
        # Creates a fake requests response object
        fake_resp = MagicMock()
        fake_resp.json.return_value = self.json_object_success
        fake_resp.status_code = HTTPStatus.OK
        mocker_get.return_value = fake_resp

        weather_info = fetch_weather_data(self.city, self.fake_api_key, self.fake_api_base_url)
        mocker_get.assert_called()
        self.assertEqual(weather_info, self.json_object_success)

    @patch('requests.get')
    def test_fetch_weather_data_failure(self, mocker_get):
        """Test that your monthly usage limit has been reached."""
        # Creates a fake requests response object
        fake_resp = MagicMock()
        fake_resp.json.return_value = self.json_object_104
        fake_resp.status_code = 104
        mocker_get.return_value = fake_resp

        weather_info = fetch_weather_data(self.city, self.fake_api_key, self.fake_api_base_url)
        mocker_get.assert_called()
        self.assertEqual(weather_info, self.json_object_104)




if __name__ == '__main__':
    unittest.main()
