""" Module providing Unit Tests for weather data fetching. """

import sys
import json
from http import HTTPStatus
import pytest
sys.path.append('../')
from weather_api_data.store_data import fetch_weather_data




@pytest.fixture()
def fake_weather_info():
    """Fixture that returns a static weather data."""
    with open("resources/weather.json") as f:
        file_content = json.load(f)
        json_object_success = file_content[0]
        json_object_104 = file_content[1]
        return json_object_success, json_object_104

def test_retrieve_weather_success(mocker, fake_weather_info):
    """Given a city name, test that a HTML report about the weather is generated
    correctly."""
    # Creates a fake requests response object
    fake_resp = mocker.Mock()
    fake_resp.json = mocker.Mock(return_value=fake_weather_info[0])
    fake_resp.status_code = HTTPStatus.OK
    mocker.patch("requests.get", return_value=fake_resp)
    weather_info = fetch_weather_data(city="Seoul")
    assert weather_info == fake_weather_info[0]

def test_retrieve_weather_104_error(mocker, fake_weather_info):
    """Test that your monthly usage limit has been reached."""
    # Creates a fake requests response object
    fake_resp = mocker.Mock()
    fake_resp.json = mocker.Mock(return_value=fake_weather_info[1])
    fake_resp.status_code = 104
    mocker.patch("requests.get", return_value=fake_resp)
    weather_info = fetch_weather_data(city="Seoul")
    assert weather_info == fake_weather_info[1]
