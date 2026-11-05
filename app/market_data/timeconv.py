import calendar
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
SHIFT = timedelta(hours=7)
FIXED = os.getenv("SERVER_UTC_OFFSET_SECONDS")


@lru_cache(maxsize=16384)
def server_to_utc(ts: int) -> int:
   if FIXED:
      return ts - int(FIXED)
   wall = datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None) - SHIFT
   return int(wall.replace(tzinfo=NY).timestamp())


def utc_to_server(ts: int) -> int:
   if FIXED:
      return ts + int(FIXED)
   wall = datetime.fromtimestamp(ts, tz=NY).replace(tzinfo=None) + SHIFT
   return calendar.timegm(wall.timetuple())