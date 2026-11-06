import atexit
import json
import logging
import os
import signal
import threading
import time
from collections import namedtuple
from datetime import datetime, timezone

import MetaTrader5 as mt5  # type: ignore

from .candles import TFS, Builder
from .publisher import Publisher
from .store import Sink, Store
from .timeconv import (
    current_rule,
    offset_seconds,
    resolve_rule,
    server_to_utc,
    utc_to_server,
)

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
    1: mt5.TIMEFRAME_M1,
    5: mt5.TIMEFRAME_M5,
    15: mt5.TIMEFRAME_M15,
    30: mt5.TIMEFRAME_M30,
    60: mt5.TIMEFRAME_H1,
    240: mt5.TIMEFRAME_H4,
    1440: mt5.TIMEFRAME_D1,
}

HALF = {}

_stop = threading.Event()
_shutdown_once = threading.Lock()


def dt(utc_ts) -> datetime:
    return datetime.fromtimestamp(int(utc_ts), tz=timezone.utc)


def connect():
    while not _stop.is_set():
        with LOCK:
            if mt5.initialize() and mt5.account_info() is not None:
                return
            log.warning("mt5 not ready: %s", mt5.last_error())
        _stop.wait(5)


def _server_ts_from_syms(syms):
    def get():
        for s in syms:
            with LOCK:
                t = mt5.symbol_info_tick(s.broker)
            if t is not None and t.time:
                return float(t.time)
        return None

    return get


def resolve_rule_for(syms):
    resolve_rule(_server_ts_from_syms(syms))
    log.info("time rule: %s (offset %+ds)", current_rule(), offset_seconds())


def fmt_row(s, tf, ts, o, h, l, c, v):
    d = s.digits
    return (s.id, tf, ts, f"{o:.{d}f}", f"{h:.{d}f}", f"{l:.{d}f}", f"{c:.{d}f}", str(int(v)))


def force_history(s, tf, bars=5000):
    with LOCK:
        rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 0, bars)
    return rates


def rate_rows(s, tf, rates):
    now_server = utc_to_server(int(time.time()))
    h = HALF.get(s.name, 0.0)
    out = []
    for r in rates:
        ts_server = int(r["time"])
        if ts_server + tf * 60 > now_server:
            continue
        out.append(
            fmt_row(
                s,
                tf,
                server_to_utc(ts_server),
                r["open"] - h,
                r["high"] - h,
                r["low"] - h,
                r["close"] - h,
                r["tick_volume"],
            )
        )
    return out


def pull_pos(st, s, tf, min_utc):
    total = 0
    pos = 0
    chunk = 5000
    while not _stop.is_set():
        with LOCK:
            rates = mt5.copy_rates_from_pos(s.broker, TF[tf], pos, chunk)
        if rates is None or len(rates) == 0:
            break
        rows = rate_rows(s, tf, rates)
        rows = [r for r in rows if r[2] >= min_utc]
        if rows:
            st.upsert(rows)
            total += len(rows)
        if len(rates) < chunk:
            break
        oldest = int(rates[0]["time"])
        if server_to_utc(oldest) < min_utc:
            break
        pos += len(rates)
        time.sleep(0.05)
    log.info("pull_pos %s tf=%d upsert=%d", s.name, tf, total)


def pull(st, s, tf, from_utc, to_utc):
    if tf <= 5:
        force_history(s, tf, 20000)
        pull_pos(st, s, tf, from_utc)
        return

    need = 5000
    seeded = force_history(s, tf, need)
    if seeded is None or len(seeded) == 0:
        log.warning("force_history empty %s tf=%d err=%s", s.name, tf, mt5.last_error())

    max_bars = 8000
    span = max(tf * 60 * max_bars, 3600)
    t = from_utc
    while t < to_utc and not _stop.is_set():
        end = min(t + span, to_utc)
        rates = None
        for attempt in range(3):
            with LOCK:
                rates = mt5.copy_rates_range(s.broker, TF[tf], dt(t), dt(end))
            if rates is not None and len(rates) > 0:
                break
            with LOCK:
                rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 0, max_bars)
            if rates is not None and len(rates) > 0:
                break
            time.sleep(1 + attempt)

        n = 0 if rates is None else len(rates)
        if n:
            rows = rate_rows(s, tf, rates)
            if rows and rates is not None:
                filtered = [r for r in rows if t <= r[2] < end]
                rows = filtered or rows
            if rows:
                st.upsert(rows)
            log.info("pull %s tf=%d mt5=%d upsert=%d", s.name, tf, n, len(rows) if rows else 0)
        else:
            log.warning(
                "pull %s tf=%d empty %s→%s err=%s",
                s.name,
                tf,
                datetime.fromtimestamp(t, tz=timezone.utc).isoformat(),
                datetime.fromtimestamp(end, tz=timezone.utc).isoformat(),
                mt5.last_error(),
            )
        t = end


def backfill(st, s):
    now = int(time.time())
    for tf in TFS:
        if _stop.is_set():
            return
        floor = now - (M1_DAYS if tf == 1 else BACKFILL_DAYS) * 86400
        last = st.last_ts(s.id, tf)
        start = max(last - tf * 180, floor) if last else floor
        log.info(
            "backfill %s tf=%d last=%s from=%s to=%s (days=%.2f)",
            s.name,
            tf,
            datetime.fromtimestamp(last, tz=timezone.utc).isoformat() if last else "none",
            datetime.fromtimestamp(start, tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            (now - start) / 86400,
        )
        pull(st, s, tf, start, now)
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
    while pending and not _stop.is_set():
        try:
            backfill(st, pending[0])
            pending.pop(0)
        except Exception:
            log.exception("backfill failed")
            st.reset()
            _stop.wait(5)
    loops = 0
    while not _stop.is_set():
        _stop.wait(REPAIR_SECONDS)
        if _stop.is_set():
            break
        try:
            for s in syms:
                if _stop.is_set():
                    break
                repair(st, s)
            refresh_markup(st)
            loops += 1
            if loops % 360 == 0:
                st.prune(M1_DAYS)
        except Exception:
            log.exception("repair failed")
            st.reset()


def load_symbols():
    st = Store(DSN)
    while not _stop.is_set():
        try:
            rows = st.symbols()
            break
        except Exception:
            log.exception("db not ready")
            st.reset()
            _stop.wait(3)
    else:
        return []

    out = []
    for sid, name, digits, markup in rows:
        broker = SYMBOL_MAP.get(name, name + SUFFIX)
        with LOCK:
            ok = mt5.symbol_select(broker, True) and mt5.symbol_info(broker) is not None
        if ok:
            HALF[name] = float(markup) / 2
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
    server_sec = cur[0] // 1000
    utc_sec = server_to_utc(server_sec)
    with LOCK:
        ticks = mt5.copy_ticks_from(
            s.broker,
            dt(utc_sec),
            5000,
            mt5.COPY_TICKS_ALL,
        )
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
    h = HALF.get(s.name, 0.0)
    for tf in TFS:
        with LOCK:
            rates = mt5.copy_rates_from_pos(s.broker, TF[tf], 0, 1)
        if rates is None or not len(rates):
            continue
        r = rates[0]
        if int(r["time"]) + tf * 60 > now:
            builder.seed(
                s.name,
                tf,
                [
                    int(r["time"]),
                    float(r["open"]) - h,
                    float(r["high"]) - h,
                    float(r["low"]) - h,
                    float(r["close"]) - h,
                    int(r["tick_volume"]),
                ],
            )


def refresh_markup(st):
    for _, name, _, markup in st.symbols():
        if name in HALF:
            HALF[name] = float(markup) / 2


def heartbeat():
    with LOCK:
        info = mt5.terminal_info()
    if info is not None and info.connected:
        with open(HEARTBEAT, "w") as f:
            f.write(str(int(time.time())))


def shutdown_handler(signum=None, frame=None):
    if not _shutdown_once.acquire(blocking=False):
        return
    log.info("shutdown signal received (%s), stopping…", signum)
    _stop.set()
    try:
        with LOCK:
            mt5.shutdown()
    except Exception:
        log.exception("mt5.shutdown failed")


def _register_shutdown_handlers():
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    atexit.register(shutdown_handler)


def main():
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    _register_shutdown_handlers()

    connect()
    if _stop.is_set():
        return

    syms = load_symbols()
    if not syms or _stop.is_set():
        log.error("no symbols loaded, exiting")
        return

    resolve_rule_for(syms)
    by_name = {s.name: s for s in syms}

    sink = Sink(DSN)
    sink.start()
    pub = Publisher(
        os.environ["CENTRIFUGO_URL"],
        os.environ["CENTRIFUGO_HTTP_API_KEY"],
        os.getenv("REDIS_URL", ""),
    )
    pub.start()

    def on_close(name, tf, c):
        sink.put(
            fmt_row(
                by_name[name],
                tf,
                server_to_utc(c[0]),
                c[1],
                c[2],
                c[3],
                c[4],
                c[5],
            )
        )

    builder = Builder(on_close)
    cursors = {s.name: init_cursor(s) for s in syms}
    for s in syms:
        seed(builder, s)
    threading.Thread(target=maintenance, args=(syms,), daemon=True, name="maintenance").start()

    fails = 0
    next_roll = 0.0
    log.info("market_data running with %d symbols", len(syms))

    while not _stop.is_set():
        t0 = time.monotonic()
        msgs, px, live = {}, {}, {}
        alive = False

        for s in syms:
            if _stop.is_set():
                break
            cur = cursors[s.name]
            if cur is None:
                cursors[s.name] = init_cursor(s)
                continue
            ticks = new_ticks(s, cur)
            if ticks is None:
                continue

            alive = True
            h = HALF[s.name]
            last = None
            for t in ticks:
                raw_bid = float(t["bid"])
                if raw_bid <= 0:
                    continue
                bid = raw_bid - h
                ask = (float(t["ask"]) or raw_bid) + h
                tm = int(t["time_msc"])
                builder.add(s.name, bid, tm // 1000)
                last = (bid, ask, server_to_utc(tm // 1000) * 1000 + tm % 1000)
            if last is None:
                continue

            d = s.digits
            data = {
                "s": s.name,
                "b": f"{last[0]:.{d}f}",
                "a": f"{last[1]:.{d}f}",
                "t": last[2],
            }
            msgs[CHANNEL + s.name] = data
            px[s.name] = json.dumps(data, separators=(",", ":"))
            for tf, c in builder.cur.get(s.name, {}).items():
                live[f"{s.name}:{tf}"] = (
                    f"[{server_to_utc(c[0])},"
                    f"{c[1]:.{d}f},{c[2]:.{d}f},{c[3]:.{d}f},{c[4]:.{d}f},{c[5]}]"
                )

        if msgs:
            pub.submit(msgs, px, live)

        fails = 0 if alive else fails + 1
        if fails >= 50:
            log.error("mt5 unresponsive, reconnecting")
            with LOCK:
                mt5.shutdown()
            if not _stop.is_set():
                connect()
            fails = 0

        if t0 >= next_roll:
            next_roll = t0 + 1
            now = utc_to_server(int(time.time()))
            for s in syms:
                builder.roll(s.name, now)
            heartbeat()

        _stop.wait(timeout=max(0.0, POLL - (time.monotonic() - t0)))

    log.info("market_data stopped")


if __name__ == "__main__":
   main()