import json
import logging
import queue
import threading

import redis
import requests

log = logging.getLogger("publisher")


class Publisher(threading.Thread):
   def __init__(self, cent_url, cent_key, redis_url):
      super().__init__(daemon=True, name="publisher")
      self.url = cent_url.rstrip("/") + "/api/batch"
      self.key = cent_key
      self.http = requests.Session()
      self.rdb = redis.Redis.from_url(redis_url) if redis_url else None
      self.q = queue.Queue(maxsize=20)

   def submit(self, msgs, px, live):
      try:
         self.q.put_nowait((msgs, px, live))
      except queue.Full:
         pass

   def run(self):
      while True:
         msgs, px, live = self.q.get()
         try:
            while True:
               m, p, l = self.q.get_nowait()
               msgs.update(m)
               px.update(p)
               live.update(l)
         except queue.Empty:
            pass
         self.centrifugo(msgs)
         self.cache(px, live)

   def centrifugo(self, msgs):
      body = {"commands": [{"publish": {"channel": ch, "data": d}} for ch, d in msgs.items()]}
      try:
         r = self.http.post(self.url, json=body, headers={"X-API-Key": self.key}, timeout=3)
         if r.status_code != 200 or '"error"' in r.text:
            log.warning("centrifugo %s %s", r.status_code, r.text[:200])
      except requests.RequestException as e:
         log.warning("centrifugo: %s", e)

   def cache(self, px, live):
      if self.rdb is None:
         return
      try:
         p = self.rdb.pipeline()
         p.hset("px", mapping=px)
         if live:
            p.hset("live", mapping=live)
         for v in px.values():
            p.publish("ticks", v)
         p.execute()
      except redis.RedisError as e:
         log.warning("redis: %s", e)