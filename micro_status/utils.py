from datetime import datetime
import requests
from .settings import RESTRICT_MOVING_TIME, MOVE_TIMES


def can_be_moved():
    # time restrictions
    if not RESTRICT_MOVING_TIME:
        return True
    r = requests.get("http://worldtimeapi.org/api/timezone/America/New_York")
    r_json = r.json()
    local_time_str = r_json['datetime']
    local_time = datetime.strptime(local_time_str, '%Y-%m-%dT%H:%M:%S.%f%z')
    week_day = r_json['day_of_week']
    if week_day == 6 or week_day == 7:
        return True
    if local_time.hour >= MOVE_TIMES['start'] and local_time.hour <= MOVE_TIMES['stop']:
        return True
    return False
