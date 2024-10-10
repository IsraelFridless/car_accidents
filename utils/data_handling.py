from datetime import datetime, timedelta


def parse_date(date_str: str):
    has_seconds = len(date_str.split(' ')) > 2
    date_format = '%m/%d/%Y %H:%M:%S %p' if has_seconds else '%m/%d/%Y %H:%M'
    return datetime.strptime(date_str, date_format)


def get_week_range(date):
    start = date - timedelta(days=date.weekday())
    end = start + timedelta(days=6)
    return start, end


def safe_int(value) -> int:
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0