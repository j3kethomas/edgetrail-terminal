import pandas as pd
import numpy as np
from typing import Tuple, Optional
from edgetrailresearch.data.storage.config import MIN_DATA_POINTS, MAX_MISSING_RATIO

def validate_dataframe(df: pd.DataFrame, 
                      min_points: int = MIN_DATA_POINTS,
                      max_missing: float = MAX_MISSING_RATIO) -> Tuple[bool, Optional[str]]:
    """
    Validate a dataframe for data quality and completeness.
    
    Args:
        df: DataFrame to validate
        min_points: Minimum number of data points required
        max_missing: Maximum allowed ratio of missing values
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check if dataframe is empty
    if df.empty:
        return False, "DataFrame is empty"
    
    # Check minimum number of points
    if len(df) < min_points:
        return False, f"Insufficient data points: {len(df)} < {min_points}"
    
    # Check for missing values
    missing_ratio = df.isnull().sum().sum() / (df.shape[0] * df.shape[1])
    if missing_ratio > max_missing:
        return False, f"Too many missing values: {missing_ratio:.2%} > {max_missing:.2%}"
    
    # Check for infinite values
    if np.isinf(df.select_dtypes(include=np.number)).any().any():
        return False, "DataFrame contains infinite values"
    
    # Check for date column
    if 'Date' not in df.columns:
        return False, "Missing 'Date' column"
    
    # Check if dates are in chronological order
    if not df['Date'].is_monotonic_increasing:
        return False, "Dates are not in chronological order"
    
    return True, None