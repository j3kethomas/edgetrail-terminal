import pandas as pd 
import yfinance as yf 

tickers = [
    "ES=F","NQ=F","RTY=F","YM=F",
    "NKD=F","^HSI","BTC=F","^VIX",
    "ZT=F","ZF=F","ZN=F","ZB=F",
    "UB=F","GC=F","SI=F","HG=F",
    "CL=F","NG=F","EURUSD=X","JPY=X",
    "GBPUSD=X","AUDUSD=X","CAD=X","CHF=X",
    "MXN=X","SEK=X","NZDUSD=X"
]

#init an emply list to store datframes
df_list = []

for t in tickers:
    try:
        df = yf.download(t,multi_level_index=False).dropna() 
        
        #skip if no data
        if df.empty:
            print(f"No data for {t}") 
            continue 
        df = df.reset_index()
        
        #add a ticker column
        df['ticker'] = t
        
        # Rename columns to match your schema
        df = df.rename(columns={
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume'
        })
        
        # Reorder columns
        df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']]
        
        # Append to the list
        df_list.append(df)
        
        print(f"Fetched data for {t}")
        
    except Exception as e:
        print(f"Error fetching data for {t}: {e}") 

# Combine all DataFrames into one
combined_df = pd.concat(df_list, ignore_index=True)

# Ensure 'date' is in datetime format
combined_df['date'] = pd.to_datetime(combined_df['date'])

# Sort by ticker and date for clarity
combined_df = combined_df.sort_values(['ticker', 'date'])

# Reset the index
combined_df = combined_df.reset_index(drop=True)

