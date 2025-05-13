import sqlite3
import yfinance as yf
from fredapi import Fred
import pandas as pd

DB_PATH = "C:\\Users\\Jake's Computer\\edgetrail-terminal\\edgetrailresearch\\data\\storage\\edgetrail.db"

def yf_pull(ticker):
   df = yf.download(ticker) 
   df.to_sql(f"{ticker}", sqlite3.connect(DB_PATH), if_exists="replace")

def fred_pull(series_id, fred_key = '4cc498a1d6344348aac3f6c8e6b8914c'):
   fred = Fred(api_key=fred_key) 
   data = fred.get_series(series_id)
   df = pd.DataFrame(data, columns = ["value"]) 
   df.to_sql(f"{series_id}", sqlite3.connect(DB_PATH), if_exists='replace')

def load_data(table_name, start=None,end=None):
   con = sqlite3.connect(DB_PATH) 
   query = f"SELECT * FROM '{table_name}'" 
   df = pd.read_sql(query, con)
   if start: 
      df = df[df.index >= start] 
   if end:
      df = df[df.index <= end]
   return df

print(load_data(table_name="GDPC1"))