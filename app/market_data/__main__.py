import json
import logging
import os
import threading
import time
from collections import namedtuple
from datetime import datetime, timezone

import MetaTrader5 as mt5 # type: ignore

from .candles import TFS, Builder
from .publisher import Publisher
from .store import Sink, Store
from .timeconv import server_to_utc, utc_to_server

log = logging.getLogger("market_data")
LOCK = threading.RLock()
Sym = namedtuple("Sym", "id name broker digits")

DSN = os.environ["DATABASE_URL"]
SUFFIX = os.getenv("MT5_SYMBOL_SUFFIX", "")
SYMBOL_MAP = json.loads(os.getenv("MT5_SYMBOL_MAP", "{}"))
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
M1_DAYS = int(os.getenv("BACKFILL_M1_DAYS", "14"))
POLL = int(os.getenv("POLL_INTERVAL_MS", "100")) / 1000
REPAIR_SECONDS = int(os.getenv("REPAIR_SECONDS", "60"))
CHANNEL = os.getenv("CENTRIFUGO_PX_PREFIX", "market:px.")
HEARTBEAT = os.getenv("HEARTBEAT_FILE", r"Z:\tmp\mt5-md-heartbeat")

TF = {
   1: mt5.TIMEFRAME_M1, 5: mt5.TIMEFRAME_M5, 15: mt5.TIMEFRAME_M15, 30: mt5.TIMEFRAME_M30,
   60: mt5.TIMEFRAME_H1, 240: mt5.TIMEFRAME_H4, 1440: mt5.TIMEFRAME_D1,
}


def dt(server_ts):
   return datetime.fromtimestamp(server_ts, tz=timezone.utc)


def connect():
   while True:
      with LOCK:
         if mt5.initialize() and mt5.account_info() is not None:
            return
         log.warning("mt5 not ready: %s", mt5.last_error())
      time.sleep(5)


def fmt_row(s, tf, ts, o, h, l, c, v):
   d = s.digits
   return (s.id, tf, ts, f"{o:.{d}f}", f"{h:.{d}f}", f"{l:.{d}f}", f"{c:.{d}f}", str(int(v)))


def rate_rows(s, tf, rates):
   now = utc_to_server(int(time.time()))
   return [
      fmt_row(s, tf, server_to_utc(int(r["time"])), r["open"], r["high"], r["low"], r["close"], r["tick_volume"])
      for r in rates
      if int(r["time"]) + tf * 60 <= now
   ]


def pull(st, s, tf, from_utc, to_utc):
   span = tf * 60 * 20000
   t = from_utc
   while t < to_utc:
      end = min(t + span, to_utc)
      rates = None
      for _ in range(3):
         with LOCK:
            rates = mt5.copy_rates_range(s.broker, TF[tf], dt(utc_to_server(t)), dt(utc_to_server(end)))
         if rates is not None:
            break
         time.sleep(2)
      if rates is not None and len(rates):
         st.upsert(rate_rows(s, tf, rates))
      t = end


def backfill(st, s):
   now = int(time.time())
   for tf in TFS:
      floor = now - (M1_DAYS if tf == 1 else BACKFILL_DAYS) * 86400
      last = st.last_ts(s.id, tf)
      pull(st, s, tf, max(last - tf * 180, floor) if last else floor, now)
   log.info("backfill done %s", s.name)


def repair(st, s):
   for tf in TFS:
      with LOCK:
         rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 1, 3)
      if rates is not None and len(rates):
         st.upsert(rate_rows(s, tf, rates))


def maintenance(syms):
   st = Store(DSN)
   pending = list(syms)
   while pending:
      try:
         backfill(st, pending[0])
         pending.pop(0)
      except Exception:
         log.exception("backfill failed")
         st.reset()
         time.sleep(5)
   loops = 0
   while True:
      time.sleep(REPAIR_SECONDS)
      try:
         for s in syms:
            repair(st, s)
         loops += 1
         if loops % 360 == 0:
            st.prune(M1_DAYS)
      except Exception:
         log.exception("repair failed")
         st.reset()


def load_symbols():
   st = Store(DSN)
   while True:
      try:
         rows = st.symbols()
         break
      except Exception:
         log.exception("db not ready")
         st.reset()
         time.sleep(3)

   out = []
   for sid, name, digits in rows:
      broker = SYMBOL_MAP.get(name, name + SUFFIX)
      with LOCK:
         ok = mt5.symbol_select(broker, True) and mt5.symbol_info(broker) is not None
      if ok:
         out.append(Sym(sid, name, broker, digits))
      else:
         log.warning("symbol %s (%s) not available at broker", name, broker)
   return out


def init_cursor(s):
   with LOCK:
      t = mt5.symbol_info_tick(s.broker)
   if t is None or not t.time_msc:
      return None
   return [int(t.time_msc), 1]


def new_ticks(s, cur):
   with LOCK:
      ticks = mt5.copy_ticks_from(s.broker, dt(cur[0] // 1000), 5000, mt5.COPY_TICKS_ALL)
   if ticks is None:
      return None
   ms, skip = cur
   out = []
   for t in ticks:
      tm = int(t["time_msc"])
      if tm < ms:
         continue
      if tm == ms and skip > 0:
         skip -= 1
         continue
      out.append(t)
   if out:
      last = int(out[-1]["time_msc"])
      n = sum(1 for t in out if int(t["time_msc"]) == last)
      cur[:] = [last, n + (cur[1] if last == cur[0] else 0)]
   return out


def seed(builder, s):
   now = utc_to_server(int(time.time()))
   for tf in TFS:
      with LOCK:
         rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 0, 1)
      if rates is None or not len(rates):
         continue
      r = rates[0]
      if int(r["time"]) + tf * 60 > now:
         builder.seed(
            s.name, tf, [int(r["time"]), float(r["open"]), float(r["high"]),
            float(r["low"]), float(r["close"]), int(r["tick_volume"])]
         )


def heartbeat():
   with LOCK:
      info = mt5.terminal_info()
   if info is not None and info.connected:
      with open(HEARTBEAT, "w") as f:
         f.write(str(int(time.time())))


def main():
   logging.basicConfig(
      level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s"
   )
   connect()
   syms = load_symbols()
   by_name = {s.name: s for s in syms}

   sink = Sink(DSN)
   sink.start()
   pub = Publisher(os.environ["CENTRIFUGO_URL"], os.environ["CENTRIFUGO_HTTP_API_KEY"], os.getenv("REDIS_URL", ""))
   pub.start()

   def on_close(name, tf, c):
      sink.put(fmt_row(by_name[name], tf, server_to_utc(c[0]), c[1], c[2], c[3], c[4], c[5]))

   builder = Builder(on_close)
   cursors = {s.name: init_cursor(s) for s in syms}
   for s in syms:
      seed(builder, s)
   threading.Thread(target=maintenance, args=(syms,), daemon=True, name="maintenance").start()

   fails = 0
   next_roll = 0.0
   log.info("market_data running with %d symbols", len(syms))
   while True:
      t0 = time.monotonic()
      msgs, px, live = {}, {}, {}
      alive = False

      for s in syms:
         cur = cursors[s.name]
         if cur is None:
            cursors[s.name] = init_cursor(s)
            continue
         ticks = new_ticks(s, cur)
         if ticks is None:
            continue
         alive = True
         last = None
         for t in ticks:
            bid = float(t["bid"])
            if bid <= 0:
               continue
            ask = float(t["ask"]) or bid
            tm = int(t["time_msc"])
            builder.add(s.name, bid, tm // 1000)
            last = (bid, ask, server_to_utc(tm // 1000) * 1000 + tm % 1000)
         if last is None:
            continue

         d = s.digits
         data = {"s": s.name, "b": f"{last[0]:.{d}f}", "a": f"{last[1]:.{d}f}", "t": last[2]}
         msgs[CHANNEL + s.name] = data
         px[s.name] = json.dumps(data, separators=(",", ":"))
         for tf, c in builder.cur[s.name].items():
            live[f"{s.name}:{tf}"] = (
               f"[{server_to_utc(c[0])},{c[1]:.{d}f},{c[2]:.{d}f},{c[3]:.{d}f},{c[4]:.{d}f},{c[5]}]"
            )

      if msgs:
         pub.submit(msgs, px, live)

      fails = 0 if alive else fails + 1
      if fails >= 50:
         log.error("mt5 unresponsive, reconnecting")
         with LOCK:
            mt5.shutdown()
         connect()
         fails = 0

      if t0 >= next_roll:
         next_roll = t0 + 1
         now = utc_to_server(int(time.time()))
         for s in syms:
            builder.roll(s.name, now)
         heartbeat()

      time.sleep(max(0.0, POLL - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()