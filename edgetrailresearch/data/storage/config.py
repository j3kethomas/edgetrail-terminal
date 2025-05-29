import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent.parent
DATA_DIR = BASE_DIR / "data" / "storage"
DB_PATH = str(DATA_DIR / "edgetrail.db")

# API Keys
FRED_API_KEY = os.getenv("FRED_API_KEY", "4cc498a1d6344348aac3f6c8e6b8914c")

# Data refresh settings
DATA_REFRESH_INTERVAL = 24  # hours
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Database settings
DB_POOL_SIZE = 5
DB_MAX_OVERFLOW = 10

# Data validation settings
MIN_DATA_POINTS = 100
MAX_MISSING_RATIO = 0.4  # 40% maximum missing data allowed 