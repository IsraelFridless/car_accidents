from datetime import datetime, timedelta
from typing import List, Dict
import toolz as t

from database.connect import area_statistics, daily_crashes, weekly_crashes, monthly_crashes, crashes
from utils.data_handling import convert_object_ids_to_str



def find_total_accidents_in_area(area: str) -> int:
    area_data = area_statistics.find_one({'area': area}, {'total_accidents': 1})
    return area_data.get('total_accidents', 0) if area_data else 0


def find_total_accidents(period, date, area) -> int:
    # Parse the date to a standard format
    date_obj = datetime.strptime(date, '%Y-%m-%d')

    if period == 'week':
        collection = weekly_crashes
        week_start = date_obj - timedelta(days=date_obj.weekday())  # Monday of that week
        week_end = week_start + timedelta(days=7)  # End of that week
        result = collection.find_one({
            'area': area,
            'week_start': {'$lte': week_end.strftime('%Y-%m-%d')},
            'week_end': {'$gte': week_start.strftime('%Y-%m-%d')}
        })
    elif period == 'day':
        collection = daily_crashes
        result = collection.find_one({
            'area': area,
            'date': date_obj
        })
    elif period == 'month':
        collection = monthly_crashes
        result = collection.find_one({
            'area': area,
            'year': str(date_obj.year),
            'month': str(date_obj.month)  # Ensure this is a string
        })
    else:
        raise ValueError("Invalid period provided")

    total_accidents = result['total_accidents'] if result else 0
    return total_accidents


def find_accidents_grouped_by_cause(area: str) ->List[dict]:
    area_document = area_statistics.find_one({'area': area})

    if not area_document or not area_document.get('crash_ids'):
        raise ValueError('Invalid area provided!')

    crash_ids = area_document['crash_ids']
    pipeline = [
        {"$match": {"_id": {"$in": crash_ids}}},
        {
            "$group": {
                "_id": "$contributing_factor",
                "count": {"$sum": 1},
                "crashes": {"$push": "$$ROOT"},
                "total_injuries": {"$sum": "$injuries.total"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "contributing_factor": "$_id",
                "count": 1,
                "crashes": 1,
                "total_injuries": 1
            }
        },
        {"$sort": {"count": -1}}
    ]

    return t.pipe(
        crashes.aggregate(pipeline),
        list,
        t.partial(convert_object_ids_to_str)
    )


def extract_area_statistics(area: str) -> Dict[str, List[str]]:
    area_document = area_statistics.find_one({'area': area})

    if not area_document:
        raise ValueError('Invalid area provided!')

    injuries = area_document.get('injuries', {})

    crash_ids = area_document.get('crash_ids', [])

    crash_documents = list(crashes.find({'_id': {'$in': crash_ids}}))

    contributing_factors = [
        crash['contributing_factor']
        for crash in crash_documents
        if crash['injuries']['total'] > 0
    ]

    return {
        'injuries': injuries,
        'contributing_factors': contributing_factors
    }





