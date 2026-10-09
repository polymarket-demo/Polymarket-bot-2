from __future__ import annotations


class Book:
    def __init__(self):
        self.bids = {}
        self.asks = {}

    @staticmethod
    def _k(p):
        return round(float(p), 4)

    def snapshot(self, bids, asks):
        self.bids = {
            self._k(level["price"]): float(level["size"])
            for level in bids
            if float(level["size"]) > 0
        }
        self.asks = {
            self._k(level["price"]): float(level["size"])
            for level in asks
            if float(level["size"]) > 0
        }

    def change(self, side, price, size):
        book = self.bids if str(side).upper() == "BUY" else self.asks
        key = self._k(price)
        size = float(size)

        if size > 0:
            book[key] = size
        else:
            book.pop(key, None)

    def best_bid(self):
        if not self.bids:
            return None
        price = max(self.bids)
        return price, self.bids[price]

    def best_ask(self):
        if not self.asks:
            return None
        price = min(self.asks)
        return price, self.asks[price]
