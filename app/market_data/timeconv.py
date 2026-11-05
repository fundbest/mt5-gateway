import json
import os
from datetime import datetime
from functools import lru_cache
from zoneinfo import ZoneInfo

NY = ZoneInfo("America/New_York")
ATHENS = ZoneInfo("Europe/Athens")
STATE_FILE = os.getenv("TZ_STATE_FILE", r"Z:\config\market_data_tz.json")

_rule = ["ny7", 0]


def offset_at(kind, seconds, utc_ts):
   if kind == "fixed":
      return seconds
   if kind == "athens":
      return int(datetime.fromtimestamp(utc_ts, tz=ATHENS).utcoffset().total_seconds())
   return int(datetime.fromtimestamp(utc_ts, tz=NY).utcoffset().total_seconds()) + 25200


def utc_to_server(ts):
   return ts + offset_at(_rule[0], _rule[1], ts)


@lru_cache(maxsize=16384)
def server_to_utc(ts):
   kind, sec = _rule
   u = ts - offset_at(kind, sec, ts)
   u = ts - offset_at(kind, sec, u)
   return ts - offset_at(kind, sec, u)


def set_rule(kind, seconds=0):
   _rule[:] = [kind, seconds]
   server_to_utc.cache_clear()


def current_rule():
   return tuple(_rule)


def measure(server_ts, now):
   raw = server_ts - now
   off = round(raw / 900) * 900
   return off if abs(raw - off) <= 120 else None


def pick_rule(off, now):
   for kind in ("ny7", "athens"):
      if offset_at(kind, 0, now) == off:
         return kind, 0
   return "fixed", off


def load_state():
   try:
      with open(STATE_FILE) as f:
         d = json.load(f)
      return d["kind"], int(d["seconds"])
   except (OSError, ValueError, KeyError):
      return None


def save_state(kind, seconds):
   try:
      with open(STATE_FILE, "w") as f:
         json.dump({"kind": kind, "seconds": seconds}, f)
   except OSError:
      pass