import logging
import os
import threading
import time
from datetime import datetime, timezone

import MetaTrader5 as mt5  # type: ignore
import numpy as np

from . import symbols
from .candles import TFS
from .clock import Clock

log = logging.getLogger("feed")
LOCK = threading.RLock()
STOP = threading.Event()

BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
M1_DAYS = int(os.getenv("BACKFILL_M1_DAYS", "14"))
OVERLAP_BARS = int(os.getenv("BACKFILL_OVERLAP_BARS", "200"))
CHUNK_BARS = int(os.getenv("HISTORY_CHUNK_BARS", "5000"))
HISTORY_DELAY = float(os.getenv("HISTORY_START_DELAY", "20"))
HISTORY_PAUSE = float(os.getenv("HISTORY_PAUSE", "1.5"))
SYMBOL_PAUSE = float(os.getenv("HISTORY_SYMBOL_PAUSE", "5"))
REPAIR_BARS = int(os.getenv("REPAIR_BARS", "5"))
HEARTBEAT = os.getenv("HEARTBEAT_FILE", r"Z:\tmp\mt5-md-heartbeat")

TF = {
   1: mt5.TIMEFRAME_M1, 5: mt5.TIMEFRAME_M5, 15: mt5.TIMEFRAME_M15, 30: mt5.TIMEFRAME_M30,
   60: mt5.TIMEFRAME_H1, 240: mt5.TIMEFRAME_H4, 1440: mt5.TIMEFRAME_D1,
}
EMPTY = ((), (), ())
_beat_ready = [False]


class Sym:
   __slots__ = (
      "id", "name", "kind", "broker", "digits", "half", "clock", "cursor",
      "next_poll", "next_cent", "next_live", "last_q", "pending", "seeded",
   )

   def __init__(self, sid, name, kind, broker, digits, markup):
      self.id = sid
      self.name = name
      self.kind = kind
      self.broker = broker
      self.digits = digits
      self.half = float(markup) / 2
      self.clock = Clock()
      self.cursor = None
      self.next_poll = self.next_cent = self.next_live = 0.0
      self.last_q = None
      self.pending = None
      self.seeded = False


def dt(server_ts):
   return datetime.fromtimestamp(server_ts, tz=timezone.utc)


def connect():
   while not STOP.is_set():
      with LOCK:
         if mt5.initialize() and mt5.account_info() is not None:
            return True
         log.warning("mt5 not ready: %s", mt5.last_error())
      STOP.wait(5)
   return False


def reconnect(syms):
   with LOCK:
      mt5.shutdown()
   if not connect():
      return False
   with LOCK:
      for s in syms:
         mt5.symbol_select(s.broker, True)
         s.cursor = None
   return True


def shutdown():
   with LOCK:
      mt5.shutdown()


def fit_clock(s):
   for _ in range(5):
      with LOCK:
         rates = mt5.copy_rates_from_pos(s.broker, mt5.TIMEFRAME_H1, 0, 10000)
      if rates is not None and len(rates) > 1:
         s.clock.fit(rates["time"])
         break
      STOP.wait(1)
   log.info("%s clock: %s", s.name, s.clock.describe())


def setup(st):
   with LOCK:
      infos = mt5.symbols_get()

   for i in infos or ():
      name = i.name.upper()
      if any(x in name for x in ("XAU", "GOLD", "BTC", "XAG", "SILVER")):
         log.info(
            "MT5 CANDIDATE | name=%s base=%s profit=%s mode=%s visible=%s tradeable=%s",
            i.name,
            i.currency_base,
            i.currency_profit,
            i.trade_mode,
            i.visible,
            i.trade_allowed,
         )

   found = symbols.pick(infos or ())
   for name in symbols.WATCH:
      if name not in found:
         log.warning("symbol %s not found at broker", name)
   rows = [symbols.row(n, i) for n, i in found.items()]

   while not STOP.is_set():
      try:
         st.sync_symbols(rows)
         db = st.symbols()
         break
      except Exception:
         log.exception("db not ready")
         st.reset()
         STOP.wait(3)
   else:
      return []

   out = []
   for sid, name, kind, digits, markup in db:
      info = found.get(name)
      if info is None:
         continue
      with LOCK:
         ok = mt5.symbol_select(info.name, True)
      if not ok:
         log.warning("symbol %s (%s) not available at broker", name, info.name)
         continue
      s = Sym(sid, name, kind, info.name, digits, markup)
      fit_clock(s)
      out.append(s)
      log.info("%s -> %s", name, info.name)
   return out


def fmt_row(s, tf, ts, o, h, l, c, v):
   d = s.digits
   return (s.id, tf, ts, f"{o:.{d}f}", f"{h:.{d}f}", f"{l:.{d}f}", f"{c:.{d}f}", str(int(v)))


def rate_rows(s, tf, rates):
   clock = s.clock
   limit = clock.to_server(int(time.time())) - tf * 60
   h = s.half
   return [
      fmt_row(s, tf, clock.to_utc(t), o - h, hi - h, lo - h, c - h, v)
      for t, o, hi, lo, c, v in zip(
         rates["time"].tolist(), rates["open"].tolist(), rates["high"].tolist(),
         rates["low"].tolist(), rates["close"].tolist(), rates["tick_volume"].tolist(),
      )
      if t <= limit
   ]


def pull(st, s, tf, from_utc, to_utc):
   clock = s.clock
   span = tf * 60 * CHUNK_BARS
   t = from_utc
   while t < to_utc and not STOP.is_set():
      end = min(t + span, to_utc)
      a, b = dt(clock.to_server(t)), dt(clock.to_server(end))
      rates = None
      for attempt in range(4):
         with LOCK:
            rates = mt5.copy_rates_range(s.broker, TF[tf], a, b)
         if rates is not None:
            break
         STOP.wait(2 * 2 ** attempt)
      if rates is None:
         raise RuntimeError("copy_rates_range failed %s tf=%d: %s" % (s.name, tf, mt5.last_error()))
      if len(rates):
         st.upsert(rate_rows(s, tf, rates))
      t = end
      STOP.wait(HISTORY_PAUSE)


def backfill(st, s):
   now = int(time.time())
   bounds = st.bounds(s.id)
   for tf in TFS:
      sec = tf * 60
      floor = now - (M1_DAYS if tf == 1 else BACKFILL_DAYS) * 86400
      lo, hi = bounds.get(tf, (None, None))
      jobs = []
      if lo is None:
         jobs.append((floor, now))
      else:
         if lo > floor + 3 * 86400:
            jobs.append((floor, lo + sec * 3))
         jobs.append((max(hi - sec * OVERLAP_BARS, floor), now))
      for a, b in jobs:
         log.info("backfill %s tf=%d %s -> %s", s.name, tf, dt(a).isoformat(timespec="minutes"), dt(b).isoformat(timespec="minutes"))
         pull(st, s, tf, a, b)
   log.info("backfill done %s", s.name)


def repair_rows(s):
   rows = []
   for tf in TFS:
      with LOCK:
         rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 1, REPAIR_BARS)
      if rates is not None and len(rates):
         rows += rate_rows(s, tf, rates)
   return rows


def init_cursor(s):
   with LOCK:
      t = mt5.symbol_info_tick(s.broker)
   if t is None or not t.time_msc:
      return None
   ms = int(t.time_msc)
   return [ms, 1, (ms, t.bid, t.ask)]


def new_ticks(s, cur):
   with LOCK:
      info = mt5.symbol_info_tick(s.broker)
      if info is None:
         return None
      if (int(info.time_msc), info.bid, info.ask) == cur[2]:
         return EMPTY
      ticks = mt5.copy_ticks_from(s.broker, dt(cur[0] // 1000), 5000, mt5.COPY_TICKS_ALL)
   if ticks is None:
      return None
   if not len(ticks):
      return EMPTY

   ms, skip = cur[0], cur[1]
   tm = ticks["time_msc"]
   keep = tm > ms
   keep[np.flatnonzero(tm == ms)[skip:]] = True
   new = ticks[keep]
   if not len(new):
      return EMPTY

   new_tm = new["time_msc"]
   last_ms = int(new_tm[-1])
   n = int(np.count_nonzero(new_tm == last_ms))
   if last_ms == ms:
      n += skip
   cur[0], cur[1], cur[2] = last_ms, n, (last_ms, float(new["bid"][-1]), float(new["ask"][-1]))

   raw = new["bid"]
   good = raw > 0
   if not good.all():
      new = new[good]
      if not len(new):
         return EMPTY
      raw = new["bid"]
   ask = new["ask"]
   h = s.half
   return (raw - h).tolist(), (np.where(ask > 0, ask, raw) + h).tolist(), new["time_msc"].tolist()


def seed(builder, s):
   now = s.clock.to_server(int(time.time()))
   h = s.half
   for tf in TFS:
      with LOCK:
         rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 0, 1)
      if rates is None or not len(rates):
         continue
      r = rates[0]
      if int(r["time"]) + tf * 60 > now:
         builder.seed(
            s.name, tf, [int(r["time"]), float(r["open"]) - h, float(r["high"]) - h,
            float(r["low"]) - h, float(r["close"]) - h, int(r["tick_volume"])]
         )


def heartbeat():
   with LOCK:
      info = mt5.terminal_info()
   if info is None or not info.connected:
      return
   try:
      if not _beat_ready[0]:
         os.makedirs(os.path.dirname(HEARTBEAT) or ".", exist_ok=True)
         _beat_ready[0] = True
      with open(HEARTBEAT, "w") as f:
         f.write(str(int(time.time())))
   except OSError as e:
      log.warning("heartbeat: %s", e)