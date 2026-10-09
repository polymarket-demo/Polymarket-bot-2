
from __future__ import annotations


class Book:
    def __init__(self):
        self.bids, self.asks = {}, {}

    @staticmethod
    def _k(p):
        return round(float(p), 4)

    def snapshot(self, bids, asks):
        self.bids = {
            self._k(l["price"]): float(l["size"])
            for l in bids if float(l["size"]) > 0
        }
        self.asks = {
            self._k(l["price"]): float(l["size"])
            for l in asks if float(l["size"]) > 0
        }

    def change(self, side, price, size):
        d = self.bids if str(side).upper() == "BUY" else self.asks
        k, s = self._k(price), float(size)

        if s > 0:
            d[k] = s
        else:
            d.pop(k, None)

    def best_bid(self):
        return (
            (max(self.bids), self.bids[max(self.bids)])
            if self.bids else None
        )

    def best_ask(self):
        return (
            (min(self.asks), self.asks[min(self.asks)])
            if self.asks else None
        )
