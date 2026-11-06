import json
import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache

log = logging.getLogger("market_data.tz")

STATE_FILE = os.getenv("TZ_STATE_FILE", "/tmp/market_data_tz.json")

# [kind, seconds]  kind: "fixed" | seconds = UTC offset of broker server
_rule = ["fixed", 0]


def set_rule(kind: str, seconds: int = 0) -> None:
    _rule[:] = [kind, int(seconds)]
    server_to_utc.cache_clear()
    log.info("time rule set: kind=%s offset=%+ds (%+.2fh)", kind, _rule[1], _rule[1] / 3600)


def current_rule():
    return tuple(_rule)


def offset_seconds() -> int:
    return int(_rule[1])


def utc_to_server(ts: int | float) -> int:
    return int(ts) + offset_seconds()


@lru_cache(maxsize=16384)
def server_to_utc(ts: int | float) -> int:
    return int(ts) - offset_seconds()


def load_state():
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
        return d["kind"], int(d["seconds"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def save_state(kind: str, seconds: int) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE) or ".", exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump({"kind": kind, "seconds": int(seconds)}, f)
    except OSError as e:
        log.warning("could not save tz state: %s", e)


def _round_offset(raw: float) -> int | None:
    """Round to 15-minute boundary; reject if residual > 2 minutes."""
    step = 900
    off = int(round(raw / step) * step)
    if abs(raw - off) <= 120:
        return off
    return None


def measure_from_tick(server_ts: float, utc_now: float | None = None) -> int | None:
    if utc_now is None:
        utc_now = time.time()
    return _round_offset(server_ts - utc_now)


def detect_offset(get_server_ts, samples: int = 8, gap: float = 0.4) -> int | None:
    """
    get_server_ts: callable () -> float | None
      should return broker server time in seconds (e.g. tick.time or time_msc/1000).
    Takes several samples and picks the median rounded offset.
    """
    vals = []
    for _ in range(samples):
        st = get_server_ts()
        now = time.time()
        if st is not None and st > 0:
            off = measure_from_tick(st, now)
            if off is not None:
                vals.append(off)
        time.sleep(gap)

    if not vals:
        return None

    vals.sort()
    return vals[len(vals) // 2]


def resolve_rule(get_server_ts, force: bool = False) -> None:
    """
    Auto-detect broker UTC offset.
    Priority: live sample → saved state → 0 (with warning).
    """
    mode = os.getenv("SERVER_TZ_MODE", "auto").lower()
    fixed = os.getenv("SERVER_UTC_OFFSET_SECONDS")

    if mode == "fixed" and fixed is not None:
        set_rule("fixed", int(fixed))
        return

    if mode != "auto" and not force:
        # allow explicit ny7 / athens only if you still want them
        if mode == "ny7":
            # NY offset + 7h — computed once at now
            from zoneinfo import ZoneInfo
            ny = ZoneInfo("America/New_York")
            sec = int(datetime.now(tz=ny).utcoffset().total_seconds()) + 25200
            set_rule("fixed", sec)
            return
        if mode == "athens":
            from zoneinfo import ZoneInfo
            sec = int(datetime.now(tz=ZoneInfo("Europe/Athens")).utcoffset().total_seconds())
            set_rule("fixed", sec)
            return

    off = detect_offset(get_server_ts)
    if off is not None:
        set_rule("fixed", off)
        save_state("fixed", off)
        return

    saved = load_state()
    if saved:
        set_rule(*saved)
        log.warning("no live tick for tz detect, using saved %s", saved)
        return

    set_rule("fixed", 0)
    log.error("tz detect failed and no saved state — using offset 0 (UTC). "
              "Historical bars may be wrong until a live tick is seen.")