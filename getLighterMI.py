import requests

MARKETS_URL = "https://explorer.elliot.ai/api/markets"

def fetch_lighter_market_indices():

    resp = requests.get(MARKETS_URL, timeout=10000)
    resp.raise_for_status()
    data = resp.json()

    symbol_to_index = {}
    index_to_symbol = {}

    for m in data:
        idx = (
            m.get("market_index")
            or m.get("market_id")
            or m.get("index")
        )
        sym = m.get("symbol") or m.get("name")

        if idx is None or sym is None:
            continue

        symbol_to_index[sym] = idx
        index_to_symbol[idx] = sym

    return symbol_to_index, index_to_symbol

if __name__ == "__main__":
    sym_to_idx, idx_to_sym = fetch_lighter_market_indices()
    print("Lighter Market Indices:")
    for sym, idx in sym_to_idx.items():
        print(f"  {sym}: {idx}")


    # ETH: 2048
    # BTC: 1
    # SOL: 2