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

log = logging.getLogger("market_data")
STOP = feed.STOP

DSN = os.environ["DATABASE_URL"]
CHANNEL = os.getenv("CENTRIFUGO_PX_PREFIX", "market:px.")
REPAIR_SECONDS = int(os.getenv("REPAIR_SECONDS", "60"))
CENT_EVERY = int(os.getenv("CENT_EVERY_MS", "250")) / 1000
LIVE_EVERY = int(os.getenv("LIVE_EVERY_MS", "0")) / 1000
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
   by_name = {s.name: s for s in syms}
   STOP.wait(feed.HISTORY_DELAY)

   pending = list(syms)
   while pending and not STOP.is_set():
      s = pending[0]
      if not s.clock.ready:
         pending.append(pending.pop(0))
         STOP.wait(1)
         continue
      try:
         feed.backfill(st, s)
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
            if s.clock.ready and session.awake(s.kind, now):
               rows += feed.repair_rows(s)
         st.upsert(rows)
         for _, name, _, _, markup in st.symbols():
            s = by_name.get(name)
            if s is not None:
               s.half = float(markup) / 2

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
   syms = feed.setup(Store(DSN))
   if not syms or STOP.is_set():
      log.error("no symbols to run")
      cleanup()
      return
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
      s = by_name[name]
      SINK.put(feed.fmt_row(s, tf, s.clock.to_utc(c[0]), c[1], c[2], c[3], c[4], c[5]))

   builder = Builder(on_close)
   for s in syms:
      s.cursor = feed.init_cursor(s)
   threading.Thread(target=maintenance, args=(syms,), daemon=True, name="maintenance").start()

   awake = {"forex": True, "metal": True, "crypto": True}
   active = None
   next_sched = next_roll = next_beat = 0.0
   fails = 0

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

      for s in syms:
         if not awake[s.kind] or t0 < s.next_poll:
            continue
         s.next_poll = t0 + INTERVAL[s.kind]
         if s.cursor is None:
            s.cursor = feed.init_cursor(s)
            continue

         polled = True
         res = feed.new_ticks(s, s.cursor)
         if res is None:
            continue
         alive = True
         bids, asks, tms = res
         if not bids:
            continue

         clock = s.clock
         if not clock.observe(tms[-1]) or not clock.ready:
            continue
         if not s.seeded:
            feed.seed(builder, s)
            s.seeded = True

         name, d, to_utc = s.name, s.digits, clock.to_utc
         lq = s.last_q
         b_s = a_s = t_ms = None
         for bid, ask, tm in zip(bids, asks, tms):
            sec = tm // 1000
            builder.add(name, bid, sec)
            if (bid, ask) == lq:
               continue
            lq = (bid, ask)
            b_s = f"{bid:.{d}f}"
            a_s = f"{ask:.{d}f}"
            t_ms = to_utc(sec) * 1000 + tm % 1000
            out.append(f'{{"s":"{name}","b":"{b_s}","a":"{a_s}","t":{t_ms}}}')
         s.last_q = lq
         if b_s is None:
            continue

         px[name] = out[-1]
         s.pending = {"s": name, "b": b_s, "a": a_s, "t": t_ms}
         if t0 >= s.next_live:
            s.next_live = t0 + LIVE_EVERY
            for tf, c in builder.cur[name].items():
               live[f"{name}:{tf}"] = (
                  f"[{to_utc(c[0])},{c[1]:.{d}f},{c[2]:.{d}f},{c[3]:.{d}f},{c[4]:.{d}f},{c[5]}]"
               )

      for s in syms:
         if s.pending is not None and t0 >= s.next_cent:
            s.next_cent = t0 + CENT_EVERY
            msgs[CHANNEL + s.name] = s.pending
            s.pending = None
      if msgs:
         cent_pub.submit(msgs)
      if out and redis_pub is not None:
         redis_pub.submit(px, live, out)

      if polled:
         fails = 0 if alive else fails + 1
         if fails >= 50:
            log.error("mt5 unresponsive, reconnecting")
            fails = 0
            if not feed.reconnect(syms):
               break

      if t0 >= next_roll:
         next_roll = t0 + 1
         now = int(time.time())
         for s in syms:
            if s.clock.ready:
               builder.roll(s.name, s.clock.to_server(now))
         if t0 >= next_beat:
            next_beat = t0 + 5
            feed.heartbeat()

      STOP.wait(max(0.001, LOOP - (time.monotonic() - t0)) if active else 1.0)

   cleanup()


if __name__ == "__main__":
   main()