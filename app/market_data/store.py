import logging
import queue
import threading

import psycopg

from .candles import TFS

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
         self.conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
      return self.conn

   def reset(self):
      if self.conn is not None:
         try:
            self.conn.close()
         except Exception:
            pass
      self.conn = None

   def upsert(self, rows):
      if not rows:
         return
      conn = self.db()
      for i in range(0, len(rows), 5000):
         conn.execute(UPSERT, [list(c) for c in zip(*rows[i:i + 5000])])

   def symbols(self):
      return self.db().execute(
         "SELECT id, symbol, kind, digits, spread_markup FROM sim_symbols WHERE enabled ORDER BY id"
      ).fetchall()

   def bounds(self, sid):
      rows = self.db().execute(
         """
         SELECT t.tf,
            (SELECT extract(epoch FROM min(ts))::bigint FROM sim_candles WHERE symbol_id = %s AND tf = t.tf),
            (SELECT extract(epoch FROM max(ts))::bigint FROM sim_candles WHERE symbol_id = %s AND tf = t.tf)
         FROM unnest(%s::int2[]) AS t(tf)
         """,
         (sid, sid, list(TFS)),
      ).fetchall()
      return {tf: (lo, hi) for tf, lo, hi in rows}

   def prune(self, days):
      self.db().execute(
         "DELETE FROM sim_candles WHERE tf = 1 AND ts < now() - make_interval(days => %s)", (days,)
      )

   def sync_symbols(self, rows, update):
      conflict = "DO NOTHING"
      if update:
         conflict = (
            "DO UPDATE SET kind = EXCLUDED.kind, base_ccy = EXCLUDED.base_ccy, quote_ccy = EXCLUDED.quote_ccy, "
            "digits = EXCLUDED.digits, contract_size = EXCLUDED.contract_size, min_lot = EXCLUDED.min_lot, "
            "max_lot = EXCLUDED.max_lot, lot_step = EXCLUDED.lot_step"
         )
      sql = (
         "INSERT INTO sim_symbols (id, symbol, kind, base_ccy, quote_ccy, digits, contract_size, min_lot, max_lot, "
         "lot_step, max_leverage, commission_per_lot, spread_markup, min_stop_distance) "
         "VALUES ((SELECT COALESCE(max(id), 0) + 1 FROM sim_symbols), %s::text, %s::text, %s::text, %s::text, "
         "%s::smallint, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::int, %s::numeric, %s::numeric, "
         "%s::numeric) ON CONFLICT (symbol) " + conflict
      )
      conn = self.db()
      with conn.transaction():
         conn.execute("SELECT pg_advisory_xact_lock(7101)")
         for r in rows:
            conn.execute(sql, r)


class Sink(threading.Thread):
   def __init__(self, dsn):
      super().__init__(daemon=True, name="sink")
      self.store = Store(dsn)
      self.q = queue.Queue(maxsize=50000)
      self.stopping = threading.Event()

   def put(self, row):
      try:
         self.q.put_nowait(row)
      except queue.Full:
         log.warning("sink full, candle dropped")

   def close(self):
      self.stopping.set()
      self.join(10)

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
            if self.stopping.is_set():
               return
            continue
         try:
            self.store.upsert(buf)
            buf.clear()
         except Exception:
            log.exception("candle upsert failed")
            self.store.reset()
            if self.stopping.is_set():
               return
            if len(buf) > 20000:
               buf.clear()
            self.stopping.wait(2)