from edgetrailresearch.data.storage.data_store import load_data
import pandas as pd

def get_price(ticker: str) -> float:
    data = load_data(ticker)
    return data.tail()

if __name__ == "__main__":
    ticker = "ES=F" 
    price = get_price(ticker)
    print(price) 