import logging
import threading
import time

import MetaTrader5 as mt5  # type: ignore
import redis  # type: ignore

from . import feed

log = logging.getLogger("refprice")

PAIRS = {"TRXUSD": "TRC20", "BNBUSD": "BEP20"}
TTL = 180
STALE = 120


class RefPrices(threading.Thread):
   def __init__(self, url):
      super().__init__(daemon=True, name="refprice")
      self.rdb = redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2)
      self.seen = {}

   def run(self):
      while not feed.STOP.wait(1):
         now = time.monotonic()
         pipe = self.rdb.pipeline(transaction=False)
         dirty = False
         for name, net in PAIRS.items():
            with feed.LOCK:
               t = mt5.symbol_info_tick(name)
               if t is None:
                  mt5.symbol_select(name, True)
            if t is None or t.bid <= 0:
               continue
            stamp = int(t.time_msc)
            prev = self.seen.get(name)
            if prev is None or prev[0] != stamp:
               self.seen[name] = (stamp, now)
            elif now - prev[1] > STALE:
               continue
            pipe.set("refpx:" + net, round(t.bid * 1e8), ex=TTL)
            dirty = True
         if dirty:
            try:
               pipe.execute()
            except redis.RedisError as e:
               log.warning("redis: %s", e)