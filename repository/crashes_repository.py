
from database.connect import area_statistics


def find_total_accidents_in_area(area: str) -> int:
    area_data = area_statistics.find_one({'area': area}, {'total_accidents': 1})
    return area_data.get('total_accidents', 0) if area_data else 0




