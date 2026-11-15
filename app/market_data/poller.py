from market.v1 import market_pb2 # type: ignore

from . import feed


def poll(s, builder):
   res = feed.new_ticks(s, s.cursor)
   if res is None:
      return None
   bids, asks, tms = res
   if not bids:
      return []
   if not s.clock.observe(tms[-1]) or not s.clock.ready:
      return []
   if not s.seeded:
      feed.seed(builder, s)
      s.seeded = True

   to_utc = s.clock.to_utc
   last = s.last_q
   out = []
   for bid, ask, tm in zip(bids, asks, tms):
      sec = tm // 1000
      builder.add(s.name, bid, sec)
      if (bid, ask) == last:
         continue
      last = (bid, ask)
      out.append(market_pb2.Tick(
         symbol_id=s.id, bid=round(bid * s.scale), ask=round(ask * s.scale),
         time_ms=to_utc(sec) * 1000 + tm % 1000,
      ).SerializeToString())
   s.last_q = last
   return out


def live_blobs(s, builder):
   to_utc = s.clock.to_utc
   return {
      "%s:%d" % (s.name, tf): market_pb2.Candle(
         ts=to_utc(c[0]), o=round(c[1] * s.scale), h=round(c[2] * s.scale),
         l=round(c[3] * s.scale), c=round(c[4] * s.scale), v=c[5],
      ).SerializeToString()
      for tf, c in builder.cur[s.name].items()
   }