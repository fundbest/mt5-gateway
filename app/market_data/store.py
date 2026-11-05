import logging
import queue
import threading
import time

import psycopg

log = logging.getLogger("store")

UPSERT = """
INSERT INTO sim_candles (symbol_id, tf, ts, o, h, l, c, v)
SELECT x.s, x.f, to_timestamp(x.t), x.o::numeric, x.h::numeric, x.l::numeric, x.c::numeric, x.v::numeric
FROM unnest(%s::int2[], %s::int2[], %s::bigint[], %s::text[], %s::text[], %s::text[], %s::text[], %s::text[])
  AS x(s, f, t, o, h, l, c, v)
ON CONFLICT (symbol_id, tf, ts) DO UPDATE
SET o = EXCLUDED.o, h = EXCLUDED.h, l = EXCLUDED.l, c = EXCLUDED.c, v = EXCLUDED.v
"""


class Store:
   def __init__(self, dsn):
      self.dsn = dsn
      self.conn = None

   def db(self):
      if self.conn is None or self.conn.closed:
         self.conn = psycopg.connect(self.dsn, autocommit=True)
      return self.conn

   def reset(self):
      if self.conn is not None:
         try:
            self.conn.close()
         except Exception:
            pass
      self.conn = None

   def upsert(self, rows):
      for i in range(0, len(rows), 5000):
         cols = [list(c) for c in zip(*rows[i:i + 5000])]
         self.db().execute(UPSERT, cols)

   def symbols(self):
      return self.db().execute(
         "SELECT id, symbol, digits, spread_markup FROM sim_symbols WHERE enabled ORDER BY id"
      ).fetchall()

   def last_ts(self, sid, tf):
      row = self.db().execute(
         "SELECT extract(epoch FROM max(ts))::bigint FROM sim_candles WHERE symbol_id = %s AND tf = %s",
         (sid, tf),
      ).fetchone()
      return row[0]

   def prune(self, days):
      self.db().execute(
         "DELETE FROM sim_candles WHERE tf = 1 AND ts < now() - make_interval(days => %s)", (days,)
      )


class Sink(threading.Thread):
   def __init__(self, dsn):
      super().__init__(daemon=True, name="sink")
      self.store = Store(dsn)
      self.q = queue.Queue(maxsize=50000)

   def put(self, row):
      try:
         self.q.put_nowait(row)
      except queue.Full:
         log.warning("sink full, candle dropped")

   def run(self):
      buf = []
      while True:
         try:
            buf.append(self.q.get(timeout=1))
            while len(buf) < 500:
               buf.append(self.q.get_nowait())
         except queue.Empty:
            pass
         if not buf:
            continue
         try:
            self.store.upsert(buf)
            buf.clear()
         except Exception:
            log.exception("candle upsert failed")
            self.store.reset()
            if len(buf) > 20000:
               buf.clear()
            time.sleep(2)