from edgetrailresearch.data.storage.data_store import load_data
import pandas as pd
from datetime import datetime

def get_price(ticker: str) -> float:
    data = load_data(ticker)
    return data.tail()

def get_yesterday_price(ticker: str) -> float:
    data = load_data(ticker)
    return data.iloc[-2] 

def percent_change(ticker: str) -> float:
    data = load_data(ticker)
    current_price = pd.to_numeric(data.iloc[-1], errors='coerce')
    yesterday_price = pd.to_numeric(data.iloc[-2], errors='coerce')
    pct_change = 100 * (current_price - yesterday_price) / yesterday_price
    pct_change = round(pct_change, 2)
    pct_change = f"+{pct_change}" if pct_change > 0 else f"{pct_change}"
    return pct_change

def week_percent_change(ticker: str) -> float:
    "Get the percent change of the ticker over the last 5 days" 
    data = load_data(ticker)
    week_change = 100 * (data.iloc[-1] - data.iloc[-6]) / data.iloc[-6]
    week_change = round(week_change, 2)
    week_change = f"+{week_change}" if week_change > 0 else f"{week_change}"
    return week_change

def month_percent_change(ticker: str) -> float:
    "Get the percent change of the ticker over the last 20 days"
    data = load_data(ticker)
    month_change = 100 * (data.iloc[-1] - data.iloc[-21]) / data.iloc[-21]
    month_change = round(month_change, 2)
    month_change = f"+{month_change}" if month_change > 0 else f"{month_change}"
    return month_change

def ytd_percent_change(ticker:str) -> float:
    "Get the pct change of the ticker from the start of the year"
    data = load_data(ticker)
    ytd_data = data[data.index.year == datetime.now().year]
    ytd_change = 100 * (ytd_data.iloc[-1] - ytd_data.iloc[0]) / ytd_data.iloc[0]
    ytd_change = round(ytd_change, 2)
    ytd_change = f"+{ytd_change}" if ytd_change > 0 else f"{ytd_change}"
    return ytd_change

def yoy_percent_change(ticker:str) -> float:
    "Get the pct change of the ticker from the same day last year"
    data = load_data(ticker)
    yoy_data = data[data.index.year == datetime.now().year - 1]
    # Convert to numeric values and handle any non-numeric data
    current_price = pd.to_numeric(data.iloc[-1], errors='coerce')
    last_year_price = pd.to_numeric(yoy_data.iloc[-1], errors='coerce')
    yoy_change = 100 * (current_price - last_year_price) / last_year_price
    yoy_change = round(yoy_change, 2)
    yoy_change = f"+{yoy_change}" if yoy_change > 0 else f"{yoy_change}"
    return yoy_change


if __name__ == "__main__":
    ticker = "^VVIX"
    yesterday_price = get_yesterday_price(ticker)
    print(yesterday_price)
        
    