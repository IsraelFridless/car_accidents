from pymongo import MongoClient


client = MongoClient('mongodb://172.17.242.253:27017')
accidents_db = client['accidents_db']

original_data = accidents_db['original_data']
monthly = accidents_db['monthly']
weekly = accidents_db['weekly']
daily = accidents_db['daily']
