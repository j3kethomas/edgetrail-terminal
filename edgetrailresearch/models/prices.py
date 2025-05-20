from edgetrailresearch.data.storage.data_store import load_data
import pandas as pd
from datetime import datetime

def get_price(ticker: str) -> float:
    data = load_data(ticker)
    print(f"{ticker}: {data.iloc[:,0].iloc[-1]}\n") 
    return data.iloc[:, 1].iloc[-1] 

def get_yesterday_price(ticker: str) -> float:
    data = load_data(ticker)
    print( f"{ticker}: {data.iloc[:,0].iloc[-2]}\n")
    return data.iloc[:, 1].iloc[-2]

def percent_change(ticker: str) -> float:
    current_price = get_price(ticker)
    yesterday_price = get_yesterday_price(ticker)
    pct_change = ((current_price - yesterday_price) / yesterday_price)*100
    pct_change = round(pct_change, 2)
    pct_change = f"+{pct_change}" if pct_change > 0 else f"{pct_change}"
    return pct_change

def week_percent_change(ticker: str) -> float:
    "Get the percent change of the ticker over the last 5 days" 
    data = load_data(ticker)
    week_change = 100 * (data.iloc[:,1].iloc[-1] - data.iloc[:,1].iloc[-6]) / data.iloc[:,1].iloc[-6] 
    week_change = round(week_change, 2)
    week_change = f"+{week_change}" if week_change > 0 else f"{week_change}"
    return week_change

def month_percent_change(ticker: str) -> float:
    "Get the percent change of the ticker over the last 20 days"
    data = load_data(ticker)
    month_change = 100 * (data.iloc[:,1].iloc[-1] - data.iloc[:,1].iloc[-21]) / data.iloc[:,1].iloc[-21]
    month_change = round(month_change, 2)
    month_change = f"+{month_change}" if month_change > 0 else f"{month_change}"
    return month_change

def ytd_percent_change(ticker:str) -> float:
    "Get the pct change of the ticker from the start of the year"
    data = load_data(ticker)
    data['Date'] = pd.to_datetime(data['Date'])
    ytd_data = data[data['Date'].dt.year == datetime.now().year] 
    ytd_change = 100 * (ytd_data['Close'].iloc[-1] - ytd_data['Close'].iloc[0]) / ytd_data['Close'].iloc[0]
    ytd_change = round(ytd_change, 2)
    ytd_change = f"+{ytd_change}" if ytd_change > 0 else f"{ytd_change}"
    return ytd_change

def yoy_percent_change(ticker:str) -> float:
    "Get the pct change of the ticker from the same day last year"
    data = load_data(ticker)
    data['Date'] = pd.to_datetime(data['Date'])
    
    # Get the last date from our data
    last_date = data['Date'].iloc[-1]
    last_year = last_date.year - 1
    
    # Filter data from last year
    yoy_data = data[data['Date'].dt.year == last_year]
    
    # Find the closest date to last_date in last year's data
    target_date = last_date.replace(year=last_year)
    closest_date = min(yoy_data['Date'], key=lambda x: abs((x - target_date).days))
    
    # Get the price from the closest date last year
    last_year_price = yoy_data[yoy_data['Date'] == closest_date].iloc[:,1].iloc[0]
    current_price = data.iloc[:,1].iloc[-1]
    
    yoy_change = 100 * (current_price - last_year_price) / last_year_price
    yoy_change = round(yoy_change, 2)
    yoy_change = f"+{yoy_change}" if yoy_change > 0 else f"{yoy_change}"
    return yoy_change


if __name__ == "__main__":
    ticker = "RTY=F"
    df = yoy_percent_change(ticker)
    print(df)  