""" Module providing Unit Tests for load_config, and
    fetch_weather_data methods in create_table.py file. """

import unittest
import json
from http import HTTPStatus
import os
import sys
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from src.weather_API_data.create_table import load_config, fetch_weather_data




class TestLoadConfig(unittest.TestCase):
    """Tests for loading configuration."""

    def test_load_config_suceess(self):
        """Test for test_load_config_suceess."""
        with open('temp_database.ini', 'w', encoding='utf-8') as f:
            f.write(""" [postgresql]\nhost=localhost\ndatabase=random_database
                    \nuser=random_user\npassword=random_password """)

        config_infos = load_config('temp_database.ini', 'postgresql')

        self.assertEqual(config_infos['host'], 'localhost')
        self.assertEqual(config_infos['database'], 'random_database')
        self.assertEqual(config_infos['user'], 'random_user')
        self.assertEqual(config_infos['password'], 'random_password')

    def test_load_config_failure(self):
        """Test for test_load_config_failure."""
        # Load a file that does not exist
        with self.assertRaises(ValueError):
            load_config('db_random.ini', 'postgresql')




class TestFetchWeatherData(unittest.TestCase):
    """ Tests for fetching weather data from the weather API """
    @classmethod
    def setUpClass(self):
        file_path = os.path.join(os.path.dirname(__file__), "resources", "weather.json")
        with open(file_path, encoding='utf-8') as f:
            file_content = json.load(f)
            self.json_object_success = file_content[0]
            self.json_object_104 = file_content[1]
            self.fake_api_key = "1234567890qwertyuiop"
            self.fake_api_base_url = "http://api.fakeweatherapi.com"
            self.city = "Seoul"

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
