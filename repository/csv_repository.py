import csv
import os
import logging

from utils.data_handling import parse_date, get_week_range, safe_int
from database.connect import crashes, area_statistics, daily_crashes, weekly_crashes, monthly_crashes

logging.basicConfig(level=logging.INFO)

def read_csv(path: str):
    with open(path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def init_accidents():
    crashes.drop()
    daily_crashes.drop()
    weekly_crashes.drop()
    monthly_crashes.drop()
    area_statistics.drop()

    counter = 0

    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'data.csv')
    for row in read_csv(data_path):
        crash_date = parse_date(row['CRASH_DATE'])
        area = row['BEAT_OF_OCCURRENCE']

        # Create crash document with nested injuries object
        crash_doc = {
            'date': crash_date,
            'area': area,
            'injuries': {
                'total': safe_int(row['INJURIES_TOTAL']),
                'fatal': safe_int(row['INJURIES_FATAL']),
                'non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL']),
            },
            'contributing_factor': row["PRIM_CONTRIBUTORY_CAUSE"]
        }
        crash_result = crashes.insert_one(crash_doc)  # Save the crash document and get the crash ID
        crash_id = crash_result.inserted_id  # Retrieve the crash ID

        # Daily statistics update
        daily_doc = {
            'date': crash_date,
            'area': area,
            '$inc': {
                'total_accidents': 1,
                'injuries.total': safe_int(row['INJURIES_TOTAL']),
                'injuries.fatal': safe_int(row['INJURIES_FATAL']),
                'injuries.non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL']),
                f'contributing_factors.{row["PRIM_CONTRIBUTORY_CAUSE"]}': 1
            }
        }
        daily_crashes.update_one({'date': crash_date, 'area': area}, {'$inc': daily_doc['$inc']}, upsert=True)

        # Weekly statistics update
        week_start, week_end = get_week_range(crash_date)
        weekly_doc = {
            'week_start': week_start.strftime('%Y-%m-%d'),  # Format as string
            'week_end': week_end.strftime('%Y-%m-%d'),  # Format as string
            'area': area,
            '$inc': {
                'total_accidents': 1,
                'injuries.total': safe_int(row['INJURIES_TOTAL']),
                'injuries.fatal': safe_int(row['INJURIES_FATAL']),
                'injuries.non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL']),
                f'contributing_factors.{row["PRIM_CONTRIBUTORY_CAUSE"]}': 1
            }
        }
        weekly_crashes.update_one(
            {'week_start': weekly_doc['week_start'], 'week_end': weekly_doc['week_end'], 'area': area},
            {'$inc': weekly_doc['$inc']}, upsert=True)

        # Monthly statistics update
        monthly_doc = {
            'year': str(crash_date.year),
            'month': str(crash_date.month),
            'area': area,
            '$inc': {
                'total_accidents': 1,
                'injuries.total': safe_int(row['INJURIES_TOTAL']),
                'injuries.fatal': safe_int(row['INJURIES_FATAL']),
                'injuries.non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL']),
                f'contributing_factors.{row["PRIM_CONTRIBUTORY_CAUSE"]}': 1
            }
        }
        monthly_crashes.update_one({'year': str(crash_date.year), 'month': str(crash_date.month), 'area': area},
                                   {'$inc': monthly_doc['$inc']}, upsert=True)

        # Update area statistics
        update_query = {'area': area}
        update_area = {
            '$inc': {
                'total_accidents': 1,
                'injuries.total': safe_int(row['INJURIES_TOTAL']),
                'injuries.fatal': safe_int(row['INJURIES_FATAL']),
                'injuries.non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL']),
                f'contributing_factors.{row["PRIM_CONTRIBUTORY_CAUSE"]}': 1
            },
            '$push': {
                'crash_ids': crash_id  # Add the crash ID to the list
            }
        }
        area_statistics.update_one(update_query, update_area, upsert=True)
        counter += 1
        logging.info(f'Rows handled: {counter}')


