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

tickers = [
   "ES=F","NQ=F","RTY=F","YM=F",
   "NKD=F","^HSI","BTC=F","^VIX",
   "ZT=F","ZF=F","ZN=F","ZB=F",
   "UB=F","GC=F","SI=F","HG=F",
   "CL=F","NG=F","EURUSD=X","JPY=X",
   "GBPUSD=X","AUDUSD=X","CAD=X","CHF=X",
   "MXN=X","SEK=X","NZDUSD=X","^MOVE",
   "^VVIX"] 

for i in tickers:
   try:
      yf_pull(ticker = i)
   except Exception as e:
      print(f"Error loading {i}")
   else:
      print(f"Successfully loaded {i}") 


fred_series = [
    "PCEPI", #Pce Price Index,
    "DGDSRG3M086SBEA", #PCE Goods Prices
    "DDURRG3M086SBEA", #PCE Durable Goods Prices
    "DNDGRG3M086SBEA", #PCE Nondurable Prices
    "DSERRG3M086SBEA", #PCE Services Prices
    "PCEPILFE", #PCE Prices Excluding Food & Energy
    "DFXARG3M086SBEA", #PCE Food Prices
    "DNRGRG3M086SBEA", #Energy Goods and Services Prices
    "DSPIC96", #Real Disposable Personal Income
    "PI", #Personal Income 
    "W209RC1", #Compensation of Employees
    "PIROA", #Personal Income Receipts on assets
    "PII", #Personal Interest income
    "ICSA",#Weekly Initial Claims
    "PCTR", #Personal Current Transfer Receipts
    "W055RC1", #Personal Current Taxes
    "DSPI", #Disposable Personal Income
    "A068RC1", #Personal Outlays
    "PCE", #Personal Consumption Expenditures
    "B069RC1", #Personal Interest Payments
    "PMSAVE", #Personal Saving
    "PCEC96", #Real PCE
    "HSN1F", #New Single Family Houses sold
    "PERMIT", #Building Permits 
    "AMDMTI", #Total Manufacturing Inventories: Durable goods
    "AMTMUO", #Manufacturers Unfilled Orders
    "AMDMVS", #Manufacturers Shipments: Durable Goods
    "BUSINV", #Total Business Inventories
    "RETAILIMSA", #Retail Inventories
    "WHLSLRIMSA", #Merchent Wholesalers Inventories
    "ISRATIO", #Total Business:Inventories to Sales Ratio
    "DGORDER", #Manufacturer's New Orders:Durable Goods
    "INDPRO", #Industrial Production:Total Index
    "IPMANSICS", #Industrial Production: Manufacturing
    "IPDMAN", #Industrial Production: Durable Manufacturing
    "IPNMAN", #Industrial Production: Nondurable Manufacturing
    "IPMINE", #Industrial Production: Mining
    "IPUTIL", #Industrial Production: Utilites
    "TCU", #Total Capacity Utilization
    "CUMFNS", #Capacity Utilization: Manufacturing
    "CAPUTLG21S", #Capacity Utilization: Mining
    "CAPUTLG2211A2S", #Capacity Utilization: Utilites
    "TTLCONS", #Total Construction Spending
    "TLRESCONS", #Residential Spending 
    "TLNRESCONS", #Nonresidential Spending 
    "TLPRVCONS", #Total Private Construction
    "PRRESCONS", #Total Private Construction Spending
    "PNRESCONS", #Total Private Nonresidential Spending 
    "TLPBLCONS", #Total Public Spending
    "RSAFS", #Advance Retail Sales: Retail Trade and Services
    "RSMVPD", #Advance Retail Sales: Motor Vehicle and Parts Dealers
    "RSEAS", #Advance Retail Sales: Electronic and Appliance stores
    "RSGASS", #Advance Retail Sales: Gasoline stores
    "RSDBS", #Advance Retail Sales: Food and Beverage Stores
    "CPIAUCSL", #Consumer Price Index
    "CPIFABSL", #CPI: Food and Beverages {Weight: 13.8%}
    "CPIHOSSL", #CPI: Housing 
    "CUSR0000SAH1", #CPI:Shelter {Weight: 33.5%}
    "CUSR0000SEHA", #CPI: Rent of Primary Residence 
    "CUSR0000SEHC", #CPI: Owner's Equivalent Rent {Weight:~24%} 
    "CUSR0000SAH2", #CPI: Housing Fuels and Utilites
    "CPIAPPSL", #CPI: Apparel
    "CPITRNSL", #CPI: Transportation {Weight: 16%} 
    "CUSR0000SETA", #CPI: New and Used Motor Vehicles
    "CUSR0000SETA01", #CPI: New Vehicles
    "CUSR0000SETA02", #CPI: Used cars and trucks
    "CUSR0000SETB", #CPI: Motor Fuel
    "CUSR0000SETC", #CPI: Motor Vehicle Parts and Equipment
    "CUSR0000SETD", #CPI: Motor Vehicle Maintenence and Repair 
    "CUSR0000SETG", #CPI: Public Transportation
    "CPIMEDSL", #CPI: Medical Care {Weight: 8%}
    "CUSR0000SAM2", #CPI: Medical Care Services
    "CPIRECSL", #CPI: Recreation
    "CPIEDUSL", #CPI: Education and Communication
    "CPIOGSSL", #CPI: Other Goods and Services {Weight: 7.9%}
    "PPIFIS", #PPI: Final Demand
    "PPIDGS", #PPI: Final Demand Goods
    "PPIDSS", #PPI: Final Demand Services 
    "IR", #Import Price Index
    "IQ", #Export Price Index
    "CSUSHPINSA", #Case-Shiller Home Prices
    "JTSJOL", #Job Openings 
    "JTSHIL", #Hires
    "JTSQUR", #Quits
    "JTSLDL", #Layoffs and Discharges
    "JTSTSL", #Total Seperations
    "CLF16OV", #Civilian Labor Force level
    "CIVPART", #LFPR
    "UNRATE", #Unemployment Rate (U-3)
    "U6RATE", #U-6 Unemployment (underemployment & discouraged workers)
    "U1RATE", #Structural Unemployment (discouraged workers)
    "U5RATE", #Discourged workers
    "PAYEMS", #Nonfarm Payrolls
    "USMINE", #Mining and Logging
    "USCONS", #Construction
    "MANEMP", #Manufacturing
    "USTPU", #Trade, Transportation, and Utilites
    "USINFO", #Information
    "USFIRE", #Financial Activities
    "USPBS", #Professional and Business Services
    "USEHS", #Education and Health Services
    "USLAH", #Leisure and Hospitality
    "USSERV", #Other Services
    "CES9091000001", #Federal Government
    "CES9092000001", #State Government
    "CES9093000001", #Local Government 
    "CES0500000003", #Average Hourly Earnings: Total Private
    "FRBATLWGT3MMAUMHWGO", #Atlanta Fed: 3-Month Moving avg. of unweighted median hourly wage growth
    "ADPMNUSNERSA", #ADP:Total Private Employment
    "MICH", #University of Michigan: Inflation Expectations
    "EXPINF1YR", #1 Year Expected Inflation
    "EXPINF2YR", #2 Year Expected Inflation
    "EXPINF1YR", #5 Year Expected Inflation
    "EXPINF10YR", #10 Year Expected Inflation
    "EXPINF30YR", #30 Year Expected Inflation
    "AMNMNO", #New Orders: Nondurable Goods
    "AMTMNO", #New Orders: Total Manufacturing
    "DGORDER", #New Orders: Durable Goods
    "AISRSA", #Auto Inventory/Sales Ratio 
]

for i in fred_series:
      try:
         fred_pull(series_id= i)
      except Exception as e:
         print(f"Error Loading data for {i}")
      else:
         print(f"Successfully loaded for {i}")