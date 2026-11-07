import time

import MetaTrader5 as mt5


def main():
   try:
      if not mt5.initialize():
         print("MT5 initialization failed:", mt5.last_error())
         return

      for symbol in ("BTCUSD", "EURUSD"):
         tick = mt5.symbol_info_tick(symbol)

         if tick is None:
            print(symbol, "No tick:", mt5.last_error())
            continue

         print(symbol, round((tick.time - time.time()) / 3600, 2), "h")

      rates = mt5.copy_rates_from_pos(
         "EURUSD", mt5.TIMEFRAME_M1, 0, 1,
      )

      if rates is None or len(rates) == 0:
         print("No rates:", mt5.last_error())
         return

      print(time.strftime("%a %H:%M",time.gmtime(int(rates[0]["time"]))))

   finally:
      mt5.shutdown()


if __name__ == "__main__":
   main()