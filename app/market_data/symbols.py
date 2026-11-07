from decimal import Decimal

FOREX = {
   "EURUSD": 8, "GBPUSD": 12, "USDJPY": 12, "USDCHF": 12, "USDCAD": 14, "AUDUSD": 10,
   "NZDUSD": 14, "EURGBP": 12, "EURJPY": 20, "GBPJPY": 30, "AUDJPY": 25,
}
METALS = {"XAUUSD": 25}
CRYPTO = {"BTCUSD": 0}

KINDS = {
   "forex": {"commission": 7, "leverage": 500, "stop_points": 10, "max_lot": 50},
   "metal": {"commission": 7, "leverage": 200, "stop_points": 20, "max_lot": 20},
   "crypto": {"commission": 20, "leverage": 100, "stop_points": 500, "max_lot": 10},
}

OVERRIDES = {}

LIST = (
   [(n, "forex", p) for n, p in FOREX.items()]
   + [(n, "metal", p) for n, p in METALS.items()]
   + [(n, "crypto", p) for n, p in CRYPTO.items()]
)


def row(name, kind, points, info):
   cfg = dict(KINDS[kind])
   cfg.update(OVERRIDES.get(name, {}))
   digits = int(info.digits)
   base = info.currency_base or name[:3]
   quote = info.currency_profit or name[3:6]
   max_lot = min(Decimal(str(info.volume_max)), Decimal(cfg["max_lot"]))
   return (
      name, kind, base, quote, digits,
      str(Decimal(str(info.trade_contract_size))),
      str(Decimal(str(info.volume_min))), str(max_lot), str(Decimal(str(info.volume_step))),
      cfg["leverage"], str(cfg["commission"]),
      str(Decimal(points).scaleb(-digits)), str(Decimal(cfg["stop_points"]).scaleb(-digits)),
   )