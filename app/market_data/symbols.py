from decimal import Decimal

import MetaTrader5 as mt5  # type: ignore

WATCH = {
   "EURUSD": 8, "GBPUSD": 12, "USDJPY": 12, "USDCHF": 12, "USDCAD": 14, "AUDUSD": 10,
   "NZDUSD": 14, "EURGBP": 12, "EURJPY": 20, "GBPJPY": 30, "AUDJPY": 25,
   "XAUUSD": 25, "BTCUSD": 0,
}

FIAT = {"USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"}
METAL = {"XAU", "XAG", "XPT", "XPD"}

KINDS = {
   "forex": {"commission": 7, "leverage": 500, "stop_points": 10, "max_lot": 50},
   "metal": {"commission": 7, "leverage": 200, "stop_points": 20, "max_lot": 20},
   "crypto": {"commission": 20, "leverage": 100, "stop_points": 500, "max_lot": 10},
}


def canonical(info):
   quote = "USD" if info.currency_profit == "USDT" else info.currency_profit
   name = info.currency_base + quote
   return name if len(name) == 6 else info.name.upper()


def kind_of(name):
   if name[:3] in METAL:
      return "metal"
   if name[:3] in FIAT and name[3:] in FIAT:
      return "forex"
   return "crypto"


def pick(infos):
   best = {}
   for i in infos:
      if i.trade_mode != mt5.SYMBOL_TRADE_MODE_FULL:
         continue
      name = canonical(i)
      if name not in WATCH:
         continue
      rank = (i.name != name, len(i.name))
      cur = best.get(name)
      if cur is None or rank < cur[0]:
         best[name] = (rank, i)
   return {n: best[n][1] for n in WATCH if n in best}


def row(name, info):
   kind = kind_of(name)
   cfg = KINDS[kind]
   digits = int(info.digits)
   point = Decimal(str(info.point))
   stop_points = max(int(info.trade_stops_level), cfg["stop_points"])
   max_lot = min(Decimal(str(info.volume_max)), Decimal(cfg["max_lot"]))
   return (
      name, kind, info.currency_base or name[:3], info.currency_profit or name[3:], digits,
      str(Decimal(str(info.trade_contract_size))),
      str(Decimal(str(info.volume_min))), str(max_lot), str(Decimal(str(info.volume_step))),
      cfg["leverage"], str(cfg["commission"]),
      str(point * WATCH[name]), str(point * stop_points),
   )