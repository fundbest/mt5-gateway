TFS = (1, 5, 15, 30, 60, 240, 1440)

class Builder:
   def __init__(self, on_close):
      self.cur = {}
      self.done = {}
      self.on_close = on_close

   def add(self, sym, price, ts):
      cs = self.cur.setdefault(sym, {})
      for tf in TFS:
         start = ts - ts % (tf * 60)
         c = cs.get(tf)
         if c is None:
            if start <= self.done.get((sym, tf), 0):
               continue
            cs[tf] = [start, price, price, price, price, 1]
         elif start > c[0]:
            self.on_close(sym, tf, c)
            self.done[(sym, tf)] = c[0]
            cs[tf] = [start, price, price, price, price, 1]
         elif start == c[0]:
            if price > c[2]:
               c[2] = price
            if price < c[3]:
               c[3] = price
            c[4] = price
            c[5] += 1

   def roll(self, sym, now):
      cs = self.cur.get(sym)
      if not cs:
         return
      for tf in list(cs):
         c = cs[tf]
         if c[0] + tf * 60 <= now:
            self.on_close(sym, tf, c)
            self.done[(sym, tf)] = c[0]
            del cs[tf]

   def seed(self, sym, tf, bar):
      self.cur.setdefault(sym, {})[tf] = bar