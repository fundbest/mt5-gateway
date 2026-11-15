import logging
import queue
import threading

import redis

log = logging.getLogger("publisher")


class RedisPublisher(threading.Thread):
   def __init__(self, url):
      super().__init__(daemon=True, name="redis-pub")
      self.rdb = redis.Redis.from_url(
         url, socket_timeout=2, socket_connect_timeout=2, socket_keepalive=True, health_check_interval=30
      )
      self.q = queue.Queue(maxsize=2000)
      self.dropped = 0

   def submit(self, px, live, ticks):
      try:
         self.q.put_nowait((px, live, ticks))
      except queue.Full:
         self.dropped += 1
         if self.dropped % 100 == 1:
            log.warning("redis queue full, dropped=%d", self.dropped)

   def run(self):
      while True:
         px, live, ticks = self.q.get()
         try:
            while True:
               p2, l2, t2 = self.q.get_nowait()
               px.update(p2)
               live.update(l2)
               ticks.extend(t2)
         except queue.Empty:
            pass
         try:
            p = self.rdb.pipeline(transaction=False)
            if px:
               p.hset("px", mapping=px)
            if live:
               p.hset("live", mapping=live)
            for v in ticks:
               p.publish("ticks", v)
            p.execute()
         except redis.RedisError as e:
            log.warning("redis: %s", e)