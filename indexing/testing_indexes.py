import logging
from database.connect import area_statistics, daily_crashes, weekly_crashes, monthly_crashes, crashes

logging.basicConfig(level=logging.INFO)

def create_indexes():
    daily_crashes.create_index([('date', 1), ('area', 1)], unique=False)
    weekly_crashes.create_index([('week_start', 1), ('week_end', 1), ('area', 1)], unique=False)
    monthly_crashes.create_index([('year', 1), ('month', 1), ('area', 1)], unique=False)
    area_statistics.create_index([('area', 1)], unique=False)

    logging.info("Indexes created successfully.")

def drop_indexes():
    collections = [crashes, daily_crashes, weekly_crashes, monthly_crashes, area_statistics]
    for collection in collections:
        collection.drop_indexes()
        logging.info(f"Indexes dropped from collection {collection.name}")


def index_information():
    collections = [crashes, daily_crashes, weekly_crashes, monthly_crashes, area_statistics]
    for collection in collections:
        # Use explain directly on the find() method
        explain_result = collection.find({}).explain()
        execution_time = explain_result["executionStats"]["executionTimeMillis"]
        print(f"Collection: {collection.name}")
        print(f"Execution Time (ms): {execution_time}")
        print()


create_indexes()
index_information()
drop_indexes()
index_information()
