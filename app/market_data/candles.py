TFS = (1, 5, 15, 30, 60, 240, 1440)
SECS = tuple(tf * 60 for tf in TFS)
FULL = len(TFS)


class Builder:
   def __init__(self, on_close):
      self.cur = {}
      self.done = {}
      self.on_close = on_close

   def add(self, sym, price, ts):
      cs = self.cur.get(sym)
      if cs is None:
         cs = self.cur[sym] = {}
      elif len(cs) == FULL and ts - ts % 60 == cs[1][0]:
         for c in cs.values():
            if price > c[2]:
               c[2] = price
            if price < c[3]:
               c[3] = price
            c[4] = price
            c[5] += 1
         return

      done = self.done
      for tf, sec in zip(TFS, SECS):
         start = ts - ts % sec
         c = cs.get(tf)
         if c is None:
            if start <= done.get((sym, tf), 0):
               continue
            cs[tf] = [start, price, price, price, price, 1]
         elif start > c[0]:
            self.on_close(sym, tf, c)
            done[(sym, tf)] = c[0]
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