import yfinance as yf
from fredapi import Fred
import pandas as pd
import logging
from typing import Optional, List
from edgetrailresearch.data.storage.config import FRED_API_KEY
from edgetrailresearch.data.storage.db_manager import db_manager
from edgetrailresearch.data.storage.refresh_manager import refresh_manager
from edgetrailresearch.data.storage.validation import validate_dataframe

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def yf_pull(ticker: str) -> Optional[pd.DataFrame]:
    """Pull data from Yahoo Finance with validation."""
    try:
        df = pd.DataFrame(yf.download(ticker))
        df = df.reset_index()
        df = df.rename(columns={'index': 'Date'})
        
        # Validate data
        is_valid, error_msg = validate_dataframe(df)
        if not is_valid:
            logger.error(f"Data validation failed for {ticker}: {error_msg}")
            return None
            
        return df
    except Exception as e:
        logger.error(f"Error pulling data for {ticker}: {str(e)}")
        return None

def fred_pull(series_id: str) -> Optional[pd.DataFrame]:
    """Pull data from FRED with validation."""
    try:
        fred = Fred(api_key=FRED_API_KEY)
        data = fred.get_series(series_id)
        df = pd.DataFrame(data)
        df.columns = [f"{series_id}_value"]
        df = df.reset_index()
        df = df.rename(columns={'index': 'Date'})
        
        # Validate data
        is_valid, error_msg = validate_dataframe(df)
        if not is_valid:
            logger.error(f"Data validation failed for {series_id}: {error_msg}")
            return None
            
        return df
    except Exception as e:
        logger.error(f"Error pulling data for {series_id}: {str(e)}")
        return None

def load_data(table_name: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
    """Load data from the database with optional date filtering."""
    try:
        # Check if data needs refresh
        if refresh_manager.needs_refresh(table_name):
            logger.info(f"Refreshing data for {table_name}")
            if table_name in FRED_SERIES:
                df = fred_pull(table_name)
            else:
                df = yf_pull(table_name)
                
            if df is not None:
                refresh_manager.refresh_data(table_name, lambda: df)
        
        # Load data from database
        query = f"SELECT * FROM '{table_name}'"
        df = pd.read_sql(query, db_manager.engine)
        
        # Apply date filters if provided
        if start:
            df = df[df['Date'] >= start]
        if end:
            df = df[df['Date'] <= end]
            
        return df
    except Exception as e:
        logger.error(f"Error loading data for {table_name}: {str(e)}")
        raise

# Define your data series
YF_TICKERS = [
    "ES=F", "NQ=F", "RTY=F", "YM=F",
    "NKD=F", "^HSI", "BTC=F", "^VIX",
    "ZT=F", "ZF=F", "ZN=F", "ZB=F",
    "UB=F", "GC=F", "SI=F", "HG=F",
    "CL=F", "NG=F", "EURUSD=X", "JPY=X",
    "GBPUSD=X", "AUDUSD=X", "CAD=X", "CHF=X",
    "MXN=X", "SEK=X", "NZDUSD=X", "^MOVE",
    "^VVIX"
]

FRED_SERIES = [
    "PCEPI",  # Pce Price Index
    "DGDSRG3M086SBEA",  # PCE Goods Prices
    "DDURRG3M086SBEA",  # PCE Durable Goods Prices
    "DNDGRG3M086SBEA",  # PCE Nondurable Prices
    "DSERRG3M086SBEA",  # PCE Services Prices
    "PCEPILFE",  # PCE Prices Excluding Food & Energy
    "DFXARG3M086SBEA",  # PCE Food Prices
    "DNRGRG3M086SBEA",  # Energy Goods and Services Prices
    "DSPIC96",  # Real Disposable Personal Income
    "PI",  # Personal Income
    "W209RC1",  # Compensation of Employees
    "PIROA",  # Personal Income Receipts on assets
    "PII",  # Personal Interest income
    "ICSA",  # Weekly Initial Claims
    "PCTR",  # Personal Current Transfer Receipts
    "W055RC1",  # Personal Current Taxes
    "DSPI",  # Disposable Personal Income
    "A068RC1",  # Personal Outlays
    "PCE",  # Personal Consumption Expenditures
    "B069RC1",  # Personal Interest Payments
    "PMSAVE",  # Personal Saving
    "PCEC96",  # Real PCE
    "HSN1F",  # New Single Family Houses sold
    "PERMIT",  # Building Permits
    "AMDMTI",  # Total Manufacturing Inventories: Durable goods
    "AMTMUO",  # Manufacturers Unfilled Orders
    "AMDMVS",  # Manufacturers Shipments: Durable Goods
    "BUSINV",  # Total Business Inventories
    "RETAILIMSA",  # Retail Inventories
    "WHLSLRIMSA",  # Merchant Wholesalers Inventories
    "ISRATIO",  # Total Business:Inventories to Sales Ratio
    "DGORDER",  # Manufacturer's New Orders:Durable Goods
    "INDPRO",  # Industrial Production:Total Index
    "IPMANSICS",  # Industrial Production: Manufacturing
    "IPDMAN",  # Industrial Production: Durable Manufacturing
    "IPNMAN",  # Industrial Production: Nondurable Manufacturing
    "IPMINE",  # Industrial Production: Mining
    "IPUTIL",  # Industrial Production: Utilities
    "TCU",  # Total Capacity Utilization
    "CUMFNS",  # Capacity Utilization: Manufacturing
    "CAPUTLG21S",  # Capacity Utilization: Mining
    "CAPUTLG2211A2S",  # Capacity Utilization: Utilities
    "TTLCONS",  # Total Construction Spending
    "TLRESCONS",  # Residential Spending
    "TLNRESCONS",  # Nonresidential Spending
    "TLPRVCONS",  # Total Private Construction
    "PRRESCONS",  # Total Private Construction Spending
    "PNRESCONS",  # Total Private Nonresidential Spending
    "TLPBLCONS",  # Total Public Spending
    "RSAFS",  # Advance Retail Sales: Retail Trade and Services
    "RSMVPD",  # Advance Retail Sales: Motor Vehicle and Parts Dealers
    "RSEAS",  # Advance Retail Sales: Electronic and Appliance stores
    "RSGASS",  # Advance Retail Sales: Gasoline stores
    "RSDBS",  # Advance Retail Sales: Food and Beverage Stores
    "CPIAUCSL",  # Consumer Price Index
    "CPIFABSL",  # CPI: Food and Beverages {Weight: 13.8%}
    "CPIHOSSL",  # CPI: Housing
    "CUSR0000SAH1",  # CPI:Shelter {Weight: 33.5%}
    "CUSR0000SEHA",  # CPI: Rent of Primary Residence
    "CUSR0000SEHC",  # CPI: Owner's Equivalent Rent {Weight:~24%}
    "CUSR0000SAH2",  # CPI: Housing Fuels and Utilities
    "CPIAPPSL",  # CPI: Apparel
    "CPITRNSL",  # CPI: Transportation {Weight: 16%}
    "CUSR0000SETA",  # CPI: New and Used Motor Vehicles
    "CUSR0000SETA01",  # CPI: New Vehicles
    "CUSR0000SETA02",  # CPI: Used cars and trucks
    "CUSR0000SETB",  # CPI: Motor Fuel
    "CUSR0000SETC",  # CPI: Motor Vehicle Parts and Equipment
    "CUSR0000SETD",  # CPI: Motor Vehicle Maintenance and Repair
    "CUSR0000SETG",  # CPI: Public Transportation
    "CPIMEDSL",  # CPI: Medical Care {Weight: 8%}
    "CUSR0000SAM2",  # CPI: Medical Care Services
    "CPIRECSL",  # CPI: Recreation
    "CPIEDUSL",  # CPI: Education and Communication
    "CPIOGSSL",  # CPI: Other Goods and Services {Weight: 7.9%}
    "PPIFIS",  # PPI: Final Demand
    "PPIDGS",  # PPI: Final Demand Goods
    "PPIDSS",  # PPI: Final Demand Services
    "IR",  # Import Price Index
    "IQ",  # Export Price Index
    "CSUSHPINSA",  # Case-Shiller Home Prices
    "JTSJOL",  # Job Openings
    "JTSHIL",  # Hires
    "JTSQUR",  # Quits
    "JTSLDL",  # Layoffs and Discharges
    "JTSTSL",  # Total Separations
    "CLF16OV",  # Civilian Labor Force level
    "CIVPART",  # LFPR
    "UNRATE",  # Unemployment Rate (U-3)
    "U6RATE",  # U-6 Unemployment (underemployment & discouraged workers)
    "U1RATE",  # Structural Unemployment (discouraged workers)
    "U5RATE",  # Discouraged workers
    "PAYEMS",  # Nonfarm Payrolls
    "USMINE",  # Mining and Logging
    "USCONS",  # Construction
    "MANEMP",  # Manufacturing
    "USTPU",  # Trade, Transportation, and Utilities
    "USINFO",  # Information
    "USFIRE",  # Financial Activities
    "USPBS",  # Professional and Business Services
    "USEHS",  # Education and Health Services
    "USLAH",  # Leisure and Hospitality
    "USSERV",  # Other Services
    "CES9091000001",  # Federal Government
    "CES9092000001",  # State Government
    "CES9093000001",  # Local Government
    "CES0500000003",  # Average Hourly Earnings: Total Private
    "FRBATLWGT3MMAUMHWGO",  # Atlanta Fed: 3-Month Moving avg. of unweighted median hourly wage growth
    "ADPMNUSNERSA",  # ADP:Total Private Employment
    "MICH",  # University of Michigan: Inflation Expectations
    "EXPINF1YR",  # 1 Year Expected Inflation
    "EXPINF2YR",  # 2 Year Expected Inflation
    "EXPINF1YR",  # 5 Year Expected Inflation
    "EXPINF10YR",  # 10 Year Expected Inflation
    "EXPINF30YR",  # 30 Year Expected Inflation
    "AMNMNO",  # New Orders: Nondurable Goods
    "AMTMNO",  # New Orders: Total Manufacturing
    "DGORDER",  # New Orders: Durable Goods
    "AISRSA"  # Auto Inventory/Sales Ratio
]

def initialize_data():
    """Initialize all data series."""
    for ticker in YF_TICKERS:
        try:
            df = yf_pull(ticker)
            if df is not None:
                refresh_manager.refresh_data(ticker, lambda: df)
                logger.info(f"Successfully initialized {ticker}")
        except Exception as e:
            logger.error(f"Error initializing {ticker}: {str(e)}")
    
    for series in FRED_SERIES:
        try:
            df = fred_pull(series)
            if df is not None:
                refresh_manager.refresh_data(series, lambda: df)
                logger.info(f"Successfully initialized {series}")
        except Exception as e:
            logger.error(f"Error initializing {series}: {str(e)}")

if __name__ == "__main__":
    initialize_data()