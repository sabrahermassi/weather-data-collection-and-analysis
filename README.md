# Weather Data Collection, Storing and Fetching

![Weather Data Collection, Storing and Fetching](https://img.shields.io/badge/Weather%20Data%20Collection%20and%20Analysis-Python%20%7C%20PostgreSQL%20%7C%20Kafka%20%7C%20Pandas-blue)

## Overview

This project collects weather data using Python, Flask and PostgreSQL. It periodically fetches real-time data from a Weather API, stores it in PostgreSQL, and returns the weather data for a specific city.

## Prerequisites

Before you begin, ensure you have the following installed and set up on your development environment:
- Python: Version 3.12.4 or higher.
- PostgreSQL: Ensure you have PostgreSQL installed and running locally. You can download it from PostgreSQL Downloads.
- `(https://www.postgresql.org/download/)` is the URL to the PostgreSQL Downloads page.

## Installation

1 - Clone the Repository:
- git clone https://github.com/sabrahermassi/weather-data-collection-and-fetching.git
- cd weather-data-collection-and-fetching

2 - Install Python Dependencies:
Install project dependencies using pip, this will install all required Python packages listed in requirements.txt.
- python -m pip install --upgrade pip
- pip install -r requirements.txt

## Configuration

1 - Set Up Database Configuration:
Create a test_database.ini file in the root directory for testing purposes:
- `main_database`: First section for the postgres main database running on your computer
- `host`: The hostname of the postgres database
- `port`: The port number of the postgres database
- `database`: The name of your main postgres database
- `user`: The user name of the postgres database
- `password`: The password created for the postgres database
- `test_weather_database`: Second section for the test database you created
- `host`: The hostname of the test database
- `port`: The port number of the test database
- `database`: The name of your test database
- `user`: The user name of the test database
- `password`: The password created for the test database

2 - Set Up Environment Variables:
Create a .env file in the root directory with the following content (Update API_KEY with your actual API key for weather data access.):
- `API_KEY` : your api key from the weather API of your choice
- `API_BASE_URL` : URL to the weather API of your choise

## Usage

To start the application, run:
- python api/app.py
To run unit tests, use:
- pytest tests/unit
To run integration tests, use:
- pytest tests/integration
