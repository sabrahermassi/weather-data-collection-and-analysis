""" Module providing API endpoints. """

import sys
sys.path.append('./')
from flask import Flask, jsonify
from pathlib import Path
from src.weather_API_data.read_data import get_weather_data
from src.weather_API_data.fetch_data import load_config




app = Flask(__name__)

@app.route('/api/weather/seoul', methods=['GET']) #TODO this is just for testing, use <city>
def get_city_weather(city={'city_name' : 'pusan'}): #TODO this is just for testing, use city
    config_path = Path('database.ini')
    weather_db_conf = load_config(config_path, 'weather_info_database')

    weather_data = get_weather_data(weather_db_conf, city)
    print(weather_data)

    if weather_data:
        return jsonify(weather_data), 200
    else:
        return jsonify({"error": "weather data not found"}), 404




if __name__ == '__main__':
    app.run(debug=True)
