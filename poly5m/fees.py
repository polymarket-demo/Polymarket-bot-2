FEE_RATE = 0.072
def taker_fee(shares: float, price: float, rate: float = FEE_RATE) -> float:
    return shares * rate * price * (1.0 - price)
def pair_edge(ask_up: float, ask_down: float, rate: float = FEE_RATE) -> float:
    return (
        1.0
        - ask_up
        - ask_down
        - taker_fee(1, ask_up, rate)
        - taker_fee(1, ask_down, rate)
    )
