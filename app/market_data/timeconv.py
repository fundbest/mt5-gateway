import json
import os
from collections import Counter
from datetime import datetime, timezone
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
      return int(datetime.fromtimestamp(utc_ts, tz=ATHENS).utcoffset().total_seconds()) + seconds
   return int(datetime.fromtimestamp(utc_ts, tz=NY).utcoffset().total_seconds()) + 25200 + seconds


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


def fit_history(opens):
   obs = []
   for o in opens:
      d = datetime.fromtimestamp(o - 7200, tz=timezone.utc).astimezone(NY)
      nominal = int(datetime(d.year, d.month, d.day, 17, tzinfo=NY).timestamp())
      obs.append((nominal, o - nominal))
   if len(obs) < 8:
      return None

   best = None
   for kind in ("ny7", "athens"):
      res = Counter(round((off - offset_at(kind, 0, nominal)) / 900) * 900 for nominal, off in obs)
      shift, score = res.most_common(1)[0]
      if best is None or score > best[0]:
         best = (score, kind, shift)

   fixed, score = Counter(round(off / 900) * 900 for _, off in obs).most_common(1)[0]
   if score > best[0]:
      best = (score, "fixed", fixed)
   return best[1], best[2], best[0] / len(obs)


def load_state(server):
   try:
      with open(STATE_FILE) as f:
         d = json.load(f)[server]
      return d["kind"], int(d["seconds"])
   except (OSError, ValueError, KeyError):
      return None


def save_state(server, kind, seconds):
   try:
      try:
         with open(STATE_FILE) as f:
            data = json.load(f)
      except (OSError, ValueError):
         data = {}
      data[server] = {"kind": kind, "seconds": seconds}
      with open(STATE_FILE, "w") as f:
         json.dump(data, f)
   except OSError:
      pass