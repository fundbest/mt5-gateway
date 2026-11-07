from datetime import datetime

from .timeconv import NY


def is_open(kind, ts):
   if kind == "crypto":
      return True
   t = datetime.fromtimestamp(ts, tz=NY)
   if (t.month == 12 and t.day == 25) or (t.month == 1 and t.day == 1):
      return False
   wd = t.weekday()
   if wd == 5 or (wd == 4 and t.hour >= 17) or (wd == 6 and t.hour < 17):
      return False
   return not (kind == "metal" and t.hour == 17)


def awake(kind, ts):
   return is_open(kind, ts + 90) or is_open(kind, ts - 300)