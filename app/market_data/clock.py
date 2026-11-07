import time
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np

NY = ZoneInfo("America/New_York")
UTC = timezone.utc


def week_offsets(times):
   t = np.asarray(times, dtype=np.int64)
   starts, offs = [], []
   for o in t[1:][np.diff(t) > 129600].tolist():
      p = datetime.fromtimestamp(o, tz=UTC)
      sun = p.date() - timedelta(days=(p.weekday() + 1) % 7)
      raw = o - int(datetime(sun.year, sun.month, sun.day, 17, tzinfo=NY).timestamp())
      if abs(raw) <= 50400:
         starts.append(o - o % 86400)
         offs.append(round(raw / 900) * 900)
   return starts, offs


class Clock:
   __slots__ = ("starts", "offs", "pending", "next_obs")

   def __init__(self):
      self.starts = ()
      self.offs = ()
      self.pending = None
      self.next_obs = 0.0

   @property
   def ready(self):
      return bool(self.offs)

   def fit(self, times):
      starts, offs = week_offsets(times)
      if offs:
         self.starts, self.offs = tuple(starts), tuple(offs)

   def off_at(self, ts):
      return self.offs[max(bisect_right(self.starts, ts) - 1, 0)]

   def to_utc(self, ts):
      return ts - self.off_at(ts)

   def to_server(self, utc):
      return utc + self.off_at(utc + self.off_at(utc))

   def observe(self, ms):
      wall = time.time()
      if self.pending is None and wall < self.next_obs:
         return True
      self.next_obs = wall + 1.0
      raw = ms / 1000 - wall
      off = round(raw / 900) * 900
      if abs(raw - off) > 120:
         return self.pending is None
      cur = self.offs[-1] if self.offs else None
      if off == cur:
         self.pending = None
         return True
      if cur is None or self.pending == off:
         self.adopt(off, ms // 1000)
         return True
      self.pending = off
      return False

   def adopt(self, off, at):
      at -= at % 86400
      self.pending = None
      if not self.offs:
         self.starts, self.offs = (0,), (off,)
      elif at > self.starts[-1]:
         self.starts, self.offs = self.starts + (at,), self.offs + (off,)
      else:
         self.offs = self.offs[:-1] + (off,)

   def describe(self):
      return "%d segments, offsets %s h" % (len(self.offs), sorted({o / 3600 for o in self.offs}))