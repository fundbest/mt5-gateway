import atexit
import logging
import os
import signal
import threading
import time

from . import feed, session
from .candles import Builder
from .publisher import CentrifugoPublisher, RedisPublisher
from .store import Sink, Store
from .timeconv import server_to_utc, utc_to_server

log = logging.getLogger("market_data")
STOP = feed.STOP

DSN = os.environ["DATABASE_URL"]
CHANNEL = os.getenv("CENTRIFUGO_PX_PREFIX", "market:px.")
REPAIR_SECONDS = int(os.getenv("REPAIR_SECONDS", "60"))
CENT_EVERY = int(os.getenv("CENT_EVERY_MS", "250")) / 1000
LIVE_EVERY = int(os.getenv("LIVE_EVERY_MS", "500")) / 1000
POLL = int(os.getenv("POLL_INTERVAL_MS", "100")) / 1000
INTERVAL = {"forex": POLL, "metal": POLL, "crypto": int(os.getenv("POLL_CRYPTO_MS", "250")) / 1000}
LOOP = 0.05

SINK = None
CLOSED = threading.Event()


def cleanup():
   if CLOSED.is_set():
      return
   CLOSED.set()
   STOP.set()
   if SINK is not None:
      SINK.close()
   feed.shutdown()
   log.info("market_data stopped")


def on_signal(signum, frame):
   log.info("signal %s received", signum)
   STOP.set()


def maintenance(syms):
   st = Store(DSN)
   STOP.wait(feed.HISTORY_DELAY)

   pending = list(syms)
   while pending and not STOP.is_set():
      try:
         feed.backfill(st, pending[0])
         pending.pop(0)
         STOP.wait(feed.SYMBOL_PAUSE)
      except Exception:
         log.exception("backfill failed")
         st.reset()
         STOP.wait(10)

   loops = 0
   while not STOP.wait(REPAIR_SECONDS):
      try:
         now = time.time()
         rows = []
         for s in syms:
            if session.awake(s.kind, now):
               rows += feed.repair_rows(s)
         st.upsert(rows)
         feed.refresh_markup(st)

         loops += 1
         if loops % 360 == 0:
            st.prune(feed.M1_DAYS)
      except Exception:
         log.exception("repair failed")
         st.reset()


def main():
   global SINK
   logging.basicConfig(
      level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s"
   )
   atexit.register(cleanup)
   for sig in (signal.SIGINT, signal.SIGTERM, getattr(signal, "SIGBREAK", None)):
      if sig is not None:
         signal.signal(sig, on_signal)

   if not feed.connect():
      return
   store = Store(DSN)
   names = feed.broker_names()
   while not STOP.is_set():
      try:
         feed.sync_symbols(store, names)
         break
      except Exception:
         log.exception("symbol sync failed")
         store.reset()
         STOP.wait(3)
   syms = feed.load_symbols(store, names)
   if not syms or STOP.is_set():
      log.error("no symbols to run")
      cleanup()
      return
   feed.resolve_rule(syms)
   by_name = {s.name: s for s in syms}

   SINK = Sink(DSN)
   SINK.start()
   cent_pub = CentrifugoPublisher(os.environ["CENTRIFUGO_URL"], os.environ["CENTRIFUGO_HTTP_API_KEY"])
   cent_pub.start()
   redis_url = os.getenv("REDIS_URL", "")
   redis_pub = RedisPublisher(redis_url) if redis_url else None
   if redis_pub is not None:
      redis_pub.start()

   def on_close(name, tf, c):
      SINK.put(feed.fmt_row(by_name[name], tf, server_to_utc(c[0]), c[1], c[2], c[3], c[4], c[5]))

   builder = Builder(on_close)
   cursors = {s.name: feed.init_cursor(s) for s in syms}
   for s in syms:
      feed.seed(builder, s)
   threading.Thread(target=maintenance, args=(syms,), daemon=True, name="maintenance").start()

   next_poll = {s.name: 0.0 for s in syms}
   next_cent, next_live, last_q, pending = {}, {}, {}, {}
   awake = {"forex": True, "metal": True, "crypto": True}
   active = None
   next_sched = next_roll = next_beat = next_check = 0.0
   fails = skew_hits = 0

   log.info("market_data running with %d symbols", len(syms))
   while not STOP.is_set():
      t0 = time.monotonic()

      if t0 >= next_sched:
         next_sched = t0 + 1
         now_utc = time.time()
         awake = {k: session.awake(k, now_utc) for k in INTERVAL}
         now_active = any(awake[s.kind] for s in syms)
         if now_active != active:
            active = now_active
            log.info("markets %s", "awake" if active else "closed, sleeping")

      msgs, px, live, out = {}, {}, {}, []
      polled = alive = False
      seen = None

      for s in syms:
         if not awake[s.kind] or t0 < next_poll[s.name]:
            continue
         next_poll[s.name] = t0 + INTERVAL[s.kind]
         cur = cursors[s.name]
         if cur is None:
            cursors[s.name] = feed.init_cursor(s)
            continue

         polled = True
         res = feed.new_ticks(s, cur)
         if res is None:
            continue
         alive = True
         bids, asks, tms = res
         if not bids:
            continue

         name, d = s.name, s.digits
         lq = last_q.get(name)
         b_s = a_s = t_ms = None
         for bid, ask, tm in zip(bids, asks, tms):
            sec = tm // 1000
            builder.add(name, bid, sec)
            if (bid, ask) == lq:
               continue
            lq = (bid, ask)
            b_s = f"{bid:.{d}f}"
            a_s = f"{ask:.{d}f}"
            t_ms = server_to_utc(sec) * 1000 + tm % 1000
            out.append(f'{{"s":"{name}","b":"{b_s}","a":"{a_s}","t":{t_ms}}}')
         last_q[name] = lq
         seen = tms[-1] // 1000
         if b_s is None:
            continue

         px[name] = out[-1]
         pending[name] = {"s": name, "b": b_s, "a": a_s, "t": t_ms}
         if t0 >= next_live.get(name, 0.0):
            next_live[name] = t0 + LIVE_EVERY
            for tf, c in builder.cur[name].items():
               live[f"{name}:{tf}"] = (
                  f"[{server_to_utc(c[0])},{c[1]:.{d}f},{c[2]:.{d}f},{c[3]:.{d}f},{c[4]:.{d}f},{c[5]}]"
               )

      for name, data in list(pending.items()):
         if t0 >= next_cent.get(name, 0.0):
            next_cent[name] = t0 + CENT_EVERY
            msgs[CHANNEL + name] = data
            del pending[name]
      if msgs:
         cent_pub.submit(msgs)
      if out and redis_pub is not None:
         redis_pub.submit(px, live, out)

      if polled:
         fails = 0 if alive else fails + 1
         if fails >= 50:
            log.error("mt5 unresponsive, reconnecting")
            fails = 0
            if not feed.reconnect():
               break

      if seen is not None and t0 >= next_check:
         next_check = t0 + 60
         skew = seen - utc_to_server(int(time.time()))
         skew_hits = skew_hits + 1 if abs(skew) > 1500 else 0
         if skew_hits >= 3:
            log.error("server time skew %ds, re-detecting rule", skew)
            skew_hits = 0
            feed.resolve_rule(syms, wait=0)

      if t0 >= next_roll:
         next_roll = t0 + 1
         now = utc_to_server(int(time.time()))
         for s in syms:
            builder.roll(s.name, now)
         if t0 >= next_beat:
            next_beat = t0 + 5
            feed.heartbeat()

      STOP.wait(max(0.001, LOOP - (time.monotonic() - t0)) if active else 1.0)

   cleanup()


if __name__ == "__main__":
   main()