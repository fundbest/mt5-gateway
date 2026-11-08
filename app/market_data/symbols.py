import os
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import MetaTrader5 as mt5  # type: ignore
import numpy as np

FIAT = {"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"}
METALS = {"XAU", "XAG", "XPT", "XPD"}
CRYPTO = {"BTC", "ETH"} # "XRP", "BCH", "ADA", "SOL"

ALIASES = {"GOLD": "XAUUSD", "SILVER": "XAGUSD"}

MARKUP_FACTOR = float(os.getenv("MARKUP_FACTOR", "1"))
SPREAD_LOOKBACK = timedelta(hours=float(os.getenv("SPREAD_LOOKBACK_HOURS", "1")))

KINDS = {
   "forex": {"commission": 7, "leverage": 500, "max_lot": 50},
   "metal": {"commission": 7, "leverage": 200, "max_lot": 20},
   "crypto": {"commission": 20, "leverage": 100, "max_lot": 10},
}

STEM = re.compile(r"[A-Z]+")


def canonical(info):
   m = STEM.match(info.name.upper())
   if not m:
      return None
   stem = m.group()
   if stem in ALIASES:
      return ALIASES[stem]
   if len(stem) < 6:
      return None
   quote = "USD" if stem[3:7] == "USDT" else stem[3:6]
   return stem[:3] + quote


def kind_of(name):
   base = name[:3]
   if base in METALS:
      return "metal"
   if base in CRYPTO:
      return "crypto"
   return "forex"


def accepted(name):
   base, quote = name[:3], name[3:]
   kind = kind_of(name)
   if kind == "forex":
      return base in FIAT and quote in FIAT and base != quote
   return quote == "USD"


def pick(infos):
   best = {}
   for i in infos:
      if i.trade_mode != mt5.SYMBOL_TRADE_MODE_FULL:
         continue
      name = canonical(i)
      if name is None or not accepted(name):
         continue
      rank = (i.name.upper() != name, len(i.name))
      cur = best.get(name)
      if cur is None or rank < cur[0]:
         best[name] = (rank, i)
   return {n: best[n][1] for n in sorted(best)}


def spread_points(info):
   ticks = mt5.copy_ticks_from(
      info.name, datetime.now(timezone.utc) - SPREAD_LOOKBACK, 2000, mt5.COPY_TICKS_INFO
   )
   if ticks is not None and len(ticks):
      ok = (ticks["bid"] > 0) & (ticks["ask"] > 0)
      if ok.any():
         gap = (ticks["ask"] - ticks["bid"])[ok]
         return max(int(round(float(np.median(gap)) / info.point)), 1)
   return max(int(info.spread), 1)


def row(name, info, spread_pts):
   cfg = KINDS[kind_of(name)]
   digits = int(info.digits)
   point = Decimal(str(info.point))
   markup_pts = max(int(round(spread_pts * MARKUP_FACTOR)), 0)
   stop_pts = max(int(info.trade_stops_level), spread_pts)
   max_lot = min(Decimal(str(info.volume_max)), Decimal(cfg["max_lot"]))
   return (
      name, kind_of(name), name[:3], name[3:], digits,
      str(Decimal(str(info.trade_contract_size))),
      str(Decimal(str(info.volume_min))), str(max_lot), str(Decimal(str(info.volume_step))),
      cfg["leverage"], str(cfg["commission"]),
      str(point * markup_pts), str(point * stop_pts),
   )