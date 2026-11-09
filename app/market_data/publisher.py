import logging
import queue
import threading

import redis
import requests
import base64

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
            p.hset("px", mapping=px)
            if live:
               p.hset("live", mapping=live)
            for v in ticks:
               p.publish("ticks", v)
            p.execute()
         except redis.RedisError as e:
            log.warning("redis: %s", e)


class CentrifugoPublisher(threading.Thread):
   def __init__(self, url, key):
      super().__init__(daemon=True, name="centrifugo-pub")
      self.url = url.rstrip("/") + "/api/batch"
      self.key = key
      self.http = requests.Session()
      self.q = queue.Queue(maxsize=50)

   def submit(self, msgs):
      try:
         self.q.put_nowait(msgs)
      except queue.Full:
         pass

   def run(self):
      while True:
         msgs = self.q.get()
         try:
            while True:
               msgs.update(self.q.get_nowait())
         except queue.Empty:
            pass
         body = {"commands": [
            {"publish": {"channel": ch, "b64data": base64.b64encode(raw).decode()}}
            for ch, raw in msgs.items()
         ]}
         try:
            r = self.http.post(self.url, json=body, headers={"X-API-Key": self.key}, timeout=1)
            if r.status_code != 200 or '"error"' in r.text:
               log.warning("centrifugo %s %s", r.status_code, r.text[:200])
            # log.info("centrifugo response: %s", r.text)
         except requests.RequestException as e:
            log.warning("centrifugo: %s", e)