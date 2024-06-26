""" Module providing Unit Tests for weather data fetching. """

from datetime import datetime
import psycopg2
import sys
import json
from http import HTTPStatus
import os
import unittest
from unittest.mock import MagicMock, patch
import unittest.mock
sys.path.append('../')
from src.weather_api_data.store_data import fetch_weather_data, insert_data




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
