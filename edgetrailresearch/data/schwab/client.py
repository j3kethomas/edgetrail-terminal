from dotenv import load_dotenv
import schwabdev
import os

load_dotenv()

client = schwabdev.Client(os.getenv("app_key"), os.getenv("app_secret"))

tickers = ["/ESU25", "/NQU25", "/RTYU25", "/YMU25",
           "/ZTU25", "/ZFU25", "/ZNU25", "/UBU25",
           "/GCU25", "/SIU25", "/HGU25", "/CLU25",
           "/NGU25", "EUR/USD", "USD/JPY", "GBP/USD",
           "USD/CAD", "USD/CHF", "USD/MXN", "USD/SEK", 
           "NZD/USD"]
for i in tickers:
    resp = client.quotes(symbols=i).json()    
    print(resp[i]['quote']['lastPrice']) 