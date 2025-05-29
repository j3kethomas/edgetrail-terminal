import time
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional, Dict, Callable
from edgetrailresearch.data.storage.config import DATA_REFRESH_INTERVAL, MAX_RETRIES, RETRY_DELAY
from edgetrailresearch.data.storage.db_manager import db_manager
from edgetrailresearch.data.storage.validation import validate_dataframe 
import logging

logger = logging.getLogger(__name__)

class DataRefreshManager:
    def __init__(self):
        self.last_refresh = {}
        self.refresh_interval = timedelta(hours=DATA_REFRESH_INTERVAL)
    
    def needs_refresh(self, table_name: str) -> bool:
        """Check if data needs to be refreshed."""
        if table_name not in self.last_refresh:
            return True
        
        time_since_refresh = datetime.now() - self.last_refresh[table_name]
        return time_since_refresh > self.refresh_interval
    
    def update_refresh_time(self, table_name: str):
        """Update the last refresh time for a table."""
        self.last_refresh[table_name] = datetime.now()
    
    def get_last_refresh_time(self, table_name: str) -> Optional[datetime]:
        """Get the last refresh time for a table."""
        return self.last_refresh.get(table_name)
    
    def refresh_data(self, table_name: str, data_func: Callable[[], pd.DataFrame]) -> bool:
        """Refresh data with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                df = data_func()
                if df is None:
                    logger.error(f"Data function returned None for {table_name}")
                    continue
                
                # Validate data
                is_valid, error_msg = validate_dataframe(df)
                if not is_valid:
                    logger.error(f"Data validation failed for {table_name}: {error_msg}")
                    continue
                
                # Save to database
                df.to_sql(table_name, db_manager.engine, if_exists='replace', index=False)
                
                # Update refresh time
                self.update_refresh_time(table_name)
                return True
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {table_name}: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
        
        return False

# Create a singleton instance
refresh_manager = DataRefreshManager() 