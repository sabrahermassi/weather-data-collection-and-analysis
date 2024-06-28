""" A script for packaging and distributing Weather API Data project. """

import setuptools

with open("README.rst", 'r') as f:
    long_description = f.read()

setuptools.setup(
    name='WEATHER_DATA_API',
    version='1.0.0',
    description=""" An application that fetches weather data 
                    from weather API and stores it to a database """,
    long_description=long_description,
    author='Sabra Hermassi',
    author_email='sabra.herm@gmail.com',
    url="https://github.com/sabrahermassi/weather_API_data.git",
    packages=setuptools.find_packages('src'),
    package_dir={'':'src'},
    install_requires=[
        'psycopg2',
        'requests',
        'ConfigParser',
        'retry',
        'limits',
        'sleep_and_retry',
        'os',
        'datetime'
    ],
)
