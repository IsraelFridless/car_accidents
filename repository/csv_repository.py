import csv
import os
from database.connect import daily, weekly, monthly, original_data
from utils.data_handling import parse_date, get_week_range, safe_int, safe_float
import logging

logging.basicConfig(level=logging.INFO)

def read_csv(path: str):
    with open(path, 'r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            yield row


def init_accidents():
    original_docs = []
    daily_docs = []
    weekly_docs = []
    monthly_docs = []
    data_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'data.csv')

    counter = 0
    for row in read_csv(data_path):

        crash_date = parse_date(row['CRASH_DATE'])
        area = row['BEAT_OF_OCCURRENCE']

        # Original document
        original_docs.append(convert_to_crash_record(row))  # Append the entire row

        # Daily document
        daily_doc = {
            'date': crash_date,
            'area': area,
            'total_accidents': 1,
            'injuries': {
                'total': safe_int(row['INJURIES_TOTAL']),
                'fatal': safe_int(row['INJURIES_FATAL']),
                'non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL'])
            },
            'contributing_factors': {
                row['PRIM_CONTRIBUTORY_CAUSE']: 1
            }
        }
        daily_docs.append(daily_doc)

        # Weekly document
        week_start, week_end = get_week_range(crash_date)
        weekly_doc = {
            'week_start': str(week_start),
            'week_end': str(week_end),
            'area': area,
            'total_accidents': 1,
            'injuries': {
                'total': safe_int(row['INJURIES_TOTAL']),
                'fatal': safe_int(row['INJURIES_FATAL']),
                'non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL'])
            },
            'contributing_factors': {
                row['PRIM_CONTRIBUTORY_CAUSE']: 1
            }
        }
        weekly_docs.append(weekly_doc)

        # Monthly document
        monthly_doc = {
            'year': str(crash_date.year),
            'month': str(crash_date.month),
            'area': area,
            'total_accidents': 1,
            'injuries': {
                'total': safe_int(row['INJURIES_TOTAL']),
                'fatal': safe_int(row['INJURIES_FATAL']),
                'non_fatal': safe_int(row['INJURIES_TOTAL']) - safe_int(row['INJURIES_FATAL'])
            },
            'contributing_factors': {
                row['PRIM_CONTRIBUTORY_CAUSE']: 1
            }
        }
        monthly_docs.append(monthly_doc)

        # Insert data in batches of 1000
        counter += 1
        if counter % 1000 == 0:
            original_data.insert_many(original_docs)
            daily.insert_many(daily_docs)
            weekly.insert_many(weekly_docs)
            monthly.insert_many(monthly_docs)
            original_docs = []
            daily_docs = []
            weekly_docs = []
            monthly_docs = []
            logging.info(f'Inserted {counter} records')

    # Insert any remaining documents
    if daily_docs:
        original_data.insert_many(original_docs)
        daily.insert_many(daily_docs)
        weekly.insert_many(weekly_docs)
        monthly.insert_many(monthly_docs)
        logging.info(f'Final insert for {counter} records completed')


def convert_to_crash_record(row):
    # Prepare the crash record with safe handling
    crash_record = {
        'CRASH_RECORD_ID': row.get('CRASH_RECORD_ID'),
        'CRASH_DATE': parse_date(row.get('CRASH_DATE')),
        'POSTED_SPEED_LIMIT': safe_int(row.get('POSTED_SPEED_LIMIT', 0)),
        'TRAFFIC_CONTROL_DEVICE': row.get('TRAFFIC_CONTROL_DEVICE'),
        'DEVICE_CONDITION': row.get('DEVICE_CONDITION'),
        'WEATHER_CONDITION': row.get('WEATHER_CONDITION'),
        'LIGHTING_CONDITION': row.get('LIGHTING_CONDITION'),
        'FIRST_CRASH_TYPE': row.get('FIRST_CRASH_TYPE'),
        'TRAFFICWAY_TYPE': row.get('TRAFFICWAY_TYPE'),
        'LANE_CNT': safe_int(row.get('LANE_CNT', 0)),  # Default to 0 if empty
        'ALIGNMENT': row.get('ALIGNMENT'),
        'ROADWAY_SURFACE_COND': row.get('ROADWAY_SURFACE_COND'),
        'ROAD_DEFECT': row.get('ROAD_DEFECT'),
        'REPORT_TYPE': row.get('REPORT_TYPE'),
        'CRASH_TYPE': row.get('CRASH_TYPE'),
        'INTERSECTION_RELATED_I': row.get('INTERSECTION_RELATED_I'),
        'NOT_RIGHT_OF_WAY_I': row.get('NOT_RIGHT_OF_WAY_I'),
        'HIT_AND_RUN_I': row.get('HIT_AND_RUN_I') == 'Y',  # Convert to boolean
        'DAMAGE': row.get('DAMAGE'),
        'DATE_POLICE_NOTIFIED': parse_date(row.get('DATE_POLICE_NOTIFIED')),
        'PRIM_CONTRIBUTORY_CAUSE': row.get('PRIM_CONTRIBUTORY_CAUSE'),
        'SEC_CONTRIBUTORY_CAUSE': row.get('SEC_CONTRIBUTORY_CAUSE'),
        'STREET_NO': safe_int(row.get('STREET_NO', 0)),  # Convert street number to int
        'STREET_DIRECTION': row.get('STREET_DIRECTION'),
        'STREET_NAME': row.get('STREET_NAME'),
        'BEAT_OF_OCCURRENCE': row.get('BEAT_OF_OCCURRENCE'),
        'PHOTOS_TAKEN_I': row.get('PHOTOS_TAKEN_I'),
        'STATEMENTS_TAKEN_I': row.get('STATEMENTS_TAKEN_I'),
        'DOORING_I': row.get('DOORING_I'),
        'WORK_ZONE_I': row.get('WORK_ZONE_I'),
        'WORK_ZONE_TYPE': row.get('WORK_ZONE_TYPE'),
        'WORKERS_PRESENT_I': row.get('WORKERS_PRESENT_I'),
        'NUM_UNITS': safe_int(row.get('NUM_UNITS', 0)),  # Convert to int
        'MOST_SEVERE_INJURY': row.get('MOST_SEVERE_INJURY'),
        'INJURIES_TOTAL': safe_int(row.get('INJURIES_TOTAL', 0)),
        'INJURIES_FATAL': safe_int(row.get('INJURIES_FATAL', 0)),
        'INJURIES_INCAPACITATING': safe_int(row.get('INJURIES_INCAPACITATING', 0)),
        'INJURIES_NON_INCAPACITATING': safe_int(row.get('INJURIES_NON_INCAPACITATING', 0)),
        'INJURIES_REPORTED_NOT_EVIDENT': safe_int(row.get('INJURIES_REPORTED_NOT_EVIDENT', 0)),
        'INJURIES_NO_INDICATION': safe_int(row.get('INJURIES_NO_INDICATION', 0)),
        'INJURIES_UNKNOWN': safe_int(row.get('INJURIES_UNKNOWN', 0)),
        'CRASH_HOUR': safe_int(row.get('CRASH_HOUR', 0)),
        'CRASH_DAY_OF_WEEK': safe_int(row.get('CRASH_DAY_OF_WEEK', 0)),
        'CRASH_MONTH': safe_int(row.get('CRASH_MONTH', 0)),
        'LATITUDE': safe_float(row.get('LATITUDE', '')),  # Handle empty or invalid float
        'LONGITUDE': safe_float(row.get('LONGITUDE', '')),  # Handle empty or invalid float
        'LOCATION': row.get('LOCATION')
    }
    return crash_record