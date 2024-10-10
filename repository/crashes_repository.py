from datetime import datetime, timedelta

from database.connect import area_statistics, daily_crashes, weekly_crashes, monthly_crashes


def find_total_accidents_in_area(area: str) -> int:
    area_data = area_statistics.find_one({'area': area}, {'total_accidents': 1})
    return area_data.get('total_accidents', 0) if area_data else 0


def get_total_accidents(period, date, area):
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

