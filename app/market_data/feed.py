import json
import logging
import os
import threading
import time
from collections import namedtuple
from datetime import datetime, timezone

import MetaTrader5 as mt5  # type: ignore
import numpy as np

from . import symbols
from .candles import TFS
from .timeconv import (
   current_rule, fit_history, load_state, measure, offset_at, pick_rule, save_state,
   server_to_utc, set_rule, utc_to_server,
)

log = logging.getLogger("feed")
LOCK = threading.RLock()
STOP = threading.Event()
Sym = namedtuple("Sym", "id name kind broker digits")

PREFIX = os.getenv("MT5_SYMBOL_PREFIX", "")
SUFFIX = os.getenv("MT5_SYMBOL_SUFFIX", "")
SYMBOL_MAP = json.loads(os.getenv("MT5_SYMBOL_MAP", "{}"))
ONLY = {x.strip() for x in os.getenv("SYMBOLS", "").split(",") if x.strip()}
SYNC_MODE = os.getenv("SYMBOLS_SYNC", "insert")
PROBES = ("EURUSD", "GBPUSD", "USDJPY")

BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
M1_DAYS = int(os.getenv("BACKFILL_M1_DAYS", "14"))
OVERLAP_BARS = int(os.getenv("BACKFILL_OVERLAP_BARS", "200"))
CHUNK_BARS = int(os.getenv("HISTORY_CHUNK_BARS", "20000"))
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
HALF = {}
_beat_ready = [False]


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


def reconnect():
   with LOCK:
      mt5.shutdown()
   return connect()


def shutdown():
   with LOCK:
      mt5.shutdown()


def broker_names():
   with LOCK:
      infos = mt5.symbols_get()
   return {i.name for i in infos} if infos else set()


def resolve(name, names):
   if name in SYMBOL_MAP:
      return SYMBOL_MAP[name]
   exact = PREFIX + name + SUFFIX
   if exact in names:
      return exact
   if PREFIX or SUFFIX:
      return None
   found = sorted((n for n in names if n.upper().startswith(name.upper()) and len(n) - len(name) <= 4), key=len)
   return found[0] if found else None


# x = False

def sync_symbols(st, names):
   # if not x:
   for s in ("BTCUSD", "EURUSD"):
      t = mt5.symbol_info_tick(s)
      if not t:
         log.info(t)
         log.info(s, round((t.time - time.time()) / 3600, 2), "h")
   r = mt5.copy_rates_from_pos("EURUSD", mt5.TIMEFRAME_M1, 0, 1)[0]
   log.info(time.strftime("%a %H:%M", time.gmtime(int(r["time"]))))

   if SYNC_MODE == "off":
      return
   rows = []
   for name, kind, points in symbols.LIST:
      if ONLY and name not in ONLY:
         continue
      broker = resolve(name, names)
      if broker is None:
         log.warning("symbol %s not found at broker", name)
         continue
      with LOCK:
         info = mt5.symbol_info(broker) if mt5.symbol_select(broker, True) else None
      if info is None:
         log.warning("symbol %s (%s) not available at broker", name, broker)
         continue
      rows.append(symbols.row(name, kind, points, info))
   if rows:
      st.sync_symbols(rows, SYNC_MODE == "update")
      log.info("synced %d symbols (%s)", len(rows), SYNC_MODE)


def load_symbols(st, names):
   while not STOP.is_set():
      try:
         rows = st.symbols()
         break
      except Exception:
         log.exception("db not ready")
         st.reset()
         STOP.wait(3)
   else:
      return []

   out = []
   for sid, name, kind, digits, markup in rows:
      if ONLY and name not in ONLY:
         continue
      broker = resolve(name, names)
      if broker is None:
         log.warning("symbol %s not found at broker", name)
         continue
      with LOCK:
         ok = mt5.symbol_select(broker, True) and mt5.symbol_info(broker) is not None
      if ok:
         HALF[name] = float(markup) / 2
         out.append(Sym(sid, name, kind, broker, digits))
      else:
         log.warning("symbol %s (%s) not available at broker", name, broker)
   return out


def refresh_markup(st):
   for _, name, _, _, markup in st.symbols():
      if name in HALF:
         HALF[name] = float(markup) / 2


def fmt_row(s, tf, ts, o, h, l, c, v):
   d = s.digits
   return (s.id, tf, ts, f"{o:.{d}f}", f"{h:.{d}f}", f"{l:.{d}f}", f"{c:.{d}f}", str(int(v)))


def rate_rows(s, tf, rates):
   limit = utc_to_server(int(time.time())) - tf * 60
   h = HALF.get(s.name, 0.0)
   return [
      fmt_row(s, tf, server_to_utc(t), o - h, hi - h, lo - h, c - h, v)
      for t, o, hi, lo, c, v in zip(
         rates["time"].tolist(), rates["open"].tolist(), rates["high"].tolist(),
         rates["low"].tolist(), rates["close"].tolist(), rates["tick_volume"].tolist(),
      )
      if t <= limit
   ]


def pull(st, s, tf, from_utc, to_utc):
   span = tf * 60 * CHUNK_BARS
   t = from_utc
   while t < to_utc and not STOP.is_set():
      end = min(t + span, to_utc)
      rates = None
      for attempt in range(4):
         with LOCK:
            rates = mt5.copy_rates_range(s.broker, TF[tf], dt(utc_to_server(t)), dt(utc_to_server(end)))
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
         log.info(
            "backfill %s tf=%d %s -> %s", s.name, tf,
            datetime.fromtimestamp(a, tz=timezone.utc).isoformat(timespec="minutes"),
            datetime.fromtimestamp(b, tz=timezone.utc).isoformat(timespec="minutes"),
         )
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
   h = HALF[s.name]
   return (raw - h).tolist(), (np.where(ask > 0, ask, raw) + h).tolist(), new["time_msc"].tolist()


def seed(builder, s):
   now = utc_to_server(int(time.time()))
   h = HALF.get(s.name, 0.0)
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


def history_opens(s, days=420):
   with LOCK:
      rates = mt5.copy_rates_from_pos(s.broker, mt5.TIMEFRAME_H1, 0, days * 24)
   if rates is None or len(rates) < 2:
      return []
   t = rates["time"].astype("int64")
   return t[1:][np.diff(t) > 36 * 3600].tolist()


def sample_offset(syms, wait):
   base = {}
   for s in syms:
      with LOCK:
         t = mt5.symbol_info_tick(s.broker)
      base[s.name] = t.time_msc if t is not None else 0
   end = time.time() + wait
   while time.time() < end and not STOP.is_set():
      STOP.wait(1)
      for s in syms:
         with LOCK:
            t = mt5.symbol_info_tick(s.broker)
         if t is not None and t.time_msc and t.time_msc != base[s.name]:
            off = measure(t.time_msc / 1000, time.time())
            if off is not None:
               return off
   return None


def detect(syms, wait):
   with LOCK:
      info = mt5.account_info()
   server = info.server if info is not None else ""
   probe = next((s for s in syms if s.name in PROBES), None) or next((s for s in syms if s.kind != "crypto"), None)
   hist = fit_history(history_opens(probe)) if probe else None
   live = sample_offset(syms, wait) if wait and syms else None
   now = int(time.time())

   if hist and hist[2] >= 0.8:
      kind, sec, score = hist
      if live is None or offset_at(kind, sec, now) == live:
         set_rule(kind, sec)
         save_state(server, kind, sec)
         log.info("time rule from history: %s %ds match=%.2f", kind, sec, score)
         return
      log.warning("history rule %s %ds disagrees with live offset %ds", kind, sec, live)

   if live is not None:
      kind, sec = pick_rule(live, now)
      set_rule(kind, sec)
      save_state(server, kind, sec)
      log.info("time rule from live tick: %s %ds", kind, sec)
      return

   saved = load_state(server)
   if saved:
      set_rule(*saved)
      log.warning("no history/live data, using saved time rule %s", saved)
      return
   log.error("cannot detect broker time rule, keeping %s", current_rule())


def resolve_rule(syms, wait=20):
   mode = os.getenv("SERVER_TZ_MODE", "auto")
   fixed = os.getenv("SERVER_UTC_OFFSET_SECONDS")
   if mode == "auto" and fixed:
      mode = "fixed"
   if mode == "fixed":
      set_rule("fixed", int(fixed))
   elif mode in ("ny7", "athens"):
      set_rule(mode)
   else:
      detect(syms, wait)
   log.info("time rule: %s", current_rule())