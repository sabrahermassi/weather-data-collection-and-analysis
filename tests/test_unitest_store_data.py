""" Module providing Unit Tests for weather data fetching. """

import sys
import json
from http import HTTPStatus
import os
import unittest
from unittest.mock import MagicMock, patch
sys.path.append('../')
from src.weather_api_data.store_data import fetch_weather_data




class TestFetchWeatherData(unittest.TestCase):
    """ Tests for fetching weather data from the weather API """
    @classmethod
    def setUpClass(self):
        file_path = os.path.join(os.path.dirname(__file__), "resources", "weather.json")
        with open(file_path) as f:
            file_content = json.load(f)
            self.json_object_success = file_content[0]
            self.json_object_104 = file_content[1]
            self.fake_api_key = "1234567890qwertyuiop"
            self.fake_api_base_url = "http://api.fakeweatherapi.com"
            self.city = "Seoul"
    
    @patch('requests.get')
    def test_retrieve_weather_success(self, mocker_get):
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
    def test_retrieve_weather_failure(self, mocker_get):
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
