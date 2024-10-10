from pymongo import MongoClient


client = MongoClient('mongodb://172.17.242.253:27017')
accidents_db = client['accidents_db']

crashes = accidents_db['crashes']
daily_crashes = accidents_db['daily_crashes']
weekly_crashes = accidents_db['weekly_crashes']
monthly_crashes = accidents_db['monthly_crashes']
area_statistics = accidents_db['area_statistics']
