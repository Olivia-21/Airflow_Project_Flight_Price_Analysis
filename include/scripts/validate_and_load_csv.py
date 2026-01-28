"""
CSV Validation and MySQL Loading Script
Validates CSV structure and loads data into MySQL staging table with logging.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Expected columns from Kaggle dataset
EXPECTED_COLUMNS = [
    'Airline',
    'Source',
    'Source Name',
    'Destination',
    'Destination Name',
    'Departure Date & Time',
    'Arrival Date & Time',
    'Duration (hrs)',
    'Stopovers',
    'Aircraft Type',
    'Class',
    'Booking Source',
    'Base Fare (BDT)',
    'Tax & Surcharge (BDT)',
    'Total Fare (BDT)',
    'Seasonality',
    'Days Before Departure'
]

# Data type expectations from Kaggle metadata
EXPECTED_DTYPES = {
    'Airline': 'object',              # String
    'Source': 'object',               # String
    'Source Name': 'object',          # String
    'Destination': 'object',          # String
    'Destination Name': 'object',     # String
    'Departure Date & Time': 'object', # String
    'Arrival Date & Time': 'object',   # String
    'Duration (hrs)': 'float64',      # Float
    'Stopovers': 'object',            # String
    'Aircraft Type': 'object',        # String
    'Class': 'object',                # String
    'Booking Source': 'object',       # String
    'Base Fare (BDT)': 'float64',     # Float
    'Tax & Surcharge (BDT)': 'float64', # Float
    'Total Fare (BDT)': 'float64',    # Float
    'Seasonality': 'object',          # String
    'Days Before Departure': 'int64'  # Int
}


def validate_csv_structure(csv_path: str) -> Tuple[bool, List[str], pd.DataFrame]:
    """
    Validate CSV file structure and column names.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        Tuple of (is_valid, error_messages, dataframe)
    """
    errors = []
    df = None
    
    try:
        # Check if file exists
        if not Path(csv_path).exists():
            errors.append(f"File not found: {csv_path}")
            return False, errors, df
        
        # Check file size
        file_size = Path(csv_path).stat().st_size
        if file_size == 0:
            errors.append("CSV file is empty (0 bytes)")
            return False, errors, df
        
        logger.info(f"Starting CSV validation for {Path(csv_path).name}")
        logger.info(f"File size: {file_size / (1024*1024):.2f} MB")
        
        # Read CSV
        df = pd.read_csv(csv_path)
        
        # Check if file has data (not just headers)
        if len(df) == 0:
            errors.append("CSV file contains only headers, no data rows")
            return False, errors, df
        
        logger.info(f"CSV loaded: {len(df):,} rows, {len(df.columns)} columns")
        
        # Validate columns exist
        csv_columns = list(df.columns)
        missing_columns = [col for col in EXPECTED_COLUMNS if col not in csv_columns]
        extra_columns = [col for col in csv_columns if col not in EXPECTED_COLUMNS]
        
        if missing_columns:
            errors.append(f"Missing columns: {missing_columns}")
        
        if extra_columns:
            logger.warning(f"Extra columns found (will be ignored): {extra_columns}")
        
        if not missing_columns:
            logger.info(f"Column validation passed: {len(EXPECTED_COLUMNS)}/{len(EXPECTED_COLUMNS)} columns found")
        
        # Validate data types
        for col, expected_dtype in EXPECTED_DTYPES.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if expected_dtype == 'float64' and actual_dtype not in ['float64', 'float32', 'int64', 'int32']:
                    errors.append(f"Column '{col}' expected {expected_dtype}, got {actual_dtype}")
                elif expected_dtype == 'int64' and actual_dtype not in ['int64', 'int32', 'float64']:
                    errors.append(f"Column '{col}' expected {expected_dtype}, got {actual_dtype}")
        
        if not any('expected' in e for e in errors):
            logger.info("Data type validation passed")
        
        return len(errors) == 0, errors, df
        
    except pd.errors.EmptyDataError:
        errors.append("CSV file is empty or corrupted")
        return False, errors, df
    except pd.errors.ParserError as e:
        errors.append(f"CSV parsing error: {str(e)}")
        return False, errors, df
    except Exception as e:
        errors.append(f"Unexpected error during validation: {str(e)}")
        return False, errors, df


def load_csv_to_mysql(
    df: pd.DataFrame,
    mysql_config: dict,
    table_name: str = 'raw_flight_staging',
    batch_size: int = 5000
) -> Tuple[bool, int, Optional[str]]:
    """
    Load validated DataFrame into MySQL staging table.
    
    Args:
        df: Validated pandas DataFrame
        mysql_config: MySQL connection configuration
        table_name: Target table name
        batch_size: Number of rows per batch insert
        
    Returns:
        Tuple of (success, rows_loaded, error_message)
    """
    connection = None
    cursor = None
    rows_loaded = 0
    
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        logger.info(f"Connected to MySQL database: {mysql_config.get('database', 'unknown')}")
        
        # Truncate table for fresh load
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        logger.info(f"Truncated table {table_name}")
        
        # Prepare insert statement
        columns = EXPECTED_COLUMNS
        placeholders = ', '.join(['%s'] * len(columns))
        column_names = ', '.join([f'`{col}`' for col in columns])
        insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        
        # Select only expected columns and convert to list of tuples
        df_subset = df[EXPECTED_COLUMNS].copy()
        
        # Handle NaN values - replace with None for MySQL
        df_subset = df_subset.where(pd.notnull(df_subset), None)
        
        # Insert in batches
        total_rows = len(df_subset)
        for i in range(0, total_rows, batch_size):
            batch = df_subset.iloc[i:i+batch_size]
            batch_data = [tuple(row) for row in batch.values]
            cursor.executemany(insert_sql, batch_data)
            connection.commit()
            rows_loaded += len(batch_data)
            
            if (i + batch_size) % 10000 == 0 or i + batch_size >= total_rows:
                logger.info(f"Progress: {min(i + batch_size, total_rows):,}/{total_rows:,} rows loaded")
        
        logger.info(f"SUCCESS | Loaded {rows_loaded:,} rows into MySQL {table_name}")
        
        # Verify row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        mysql_count = cursor.fetchone()[0]
        
        if mysql_count == total_rows:
            logger.info(f"Row count verification: CSV={total_rows:,}, MySQL={mysql_count:,} ✓")
        else:
            logger.warning(f"Row count mismatch: CSV={total_rows:,}, MySQL={mysql_count:,}")
        
        return True, rows_loaded, None
        
    except Error as e:
        error_msg = f"MySQL error: {str(e)}"
        logger.error(error_msg)
        return False, rows_loaded, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during load: {str(e)}"
        logger.error(error_msg)
        return False, rows_loaded, error_msg
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def log_ingestion_result(
    mysql_config: dict,
    file_name: str,
    status: str,
    rows_expected: int,
    rows_loaded: int,
    error_message: Optional[str] = None,
    started_at: Optional[datetime] = None
):
    """Log ingestion result to MySQL ingestion_log table."""
    connection = None
    cursor = None
    
    try:
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        insert_sql = """
            INSERT INTO ingestion_log 
            (file_name, status, rows_expected, rows_loaded, error_message, started_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            file_name, status, rows_expected, rows_loaded, error_message, started_at
        ))
        connection.commit()
        logger.info(f"Ingestion log recorded: {status}")
        
    except Exception as e:
        logger.error(f"Failed to log ingestion result: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()


def validate_and_load(csv_path: str, mysql_config: dict) -> bool:
    """
    Main function to validate CSV and load to MySQL.
    
    Args:
        csv_path: Path to CSV file
        mysql_config: MySQL connection configuration
        
    Returns:
        True if successful, False otherwise
    """
    started_at = datetime.now()
    file_name = Path(csv_path).name
    
    # Step 1: Validate CSV
    is_valid, errors, df = validate_csv_structure(csv_path)
    
    if not is_valid:
        for error in errors:
            logger.error(error)
        logger.error(f"FAILURE | CSV ingestion aborted - validation errors detected")
        log_ingestion_result(
            mysql_config, file_name, 'FAILURE', 
            0, 0, '; '.join(errors), started_at
        )
        return False
    
    # Step 2: Load to MySQL
    success, rows_loaded, error_msg = load_csv_to_mysql(df, mysql_config)
    
    if success:
        log_ingestion_result(
            mysql_config, file_name, 'SUCCESS',
            len(df), rows_loaded, None, started_at
        )
        return True
    else:
        log_ingestion_result(
            mysql_config, file_name, 'FAILURE',
            len(df), rows_loaded, error_msg, started_at
        )
        return False


# For Airflow task usage
def run_validation_and_load(**context):
    """Airflow-callable function for CSV validation and MySQL load."""
    from airflow.hooks.base import BaseHook
    
    # Get MySQL connection from Airflow
    mysql_conn = BaseHook.get_connection('mysql_staging')
    mysql_config = {
        'host': mysql_conn.host,
        'port': mysql_conn.port or 3306,
        'user': mysql_conn.login,
        'password': mysql_conn.password,
        'database': mysql_conn.schema or 'flight_staging'
    }
    
    # CSV path
    csv_path = '/usr/local/airflow/dataset/Flight_Price_Dataset_of_Bangladesh.csv'
    
    success = validate_and_load(csv_path, mysql_config)
    
    if not success:
        raise Exception("CSV validation and load failed. Check logs for details.")
    
    return "CSV validation and MySQL load completed successfully"


if __name__ == '__main__':
    # For local testing
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'airflow',
        'password': 'airflow',
        'database': 'flight_staging'
    }
    
    csv_path = '../../dataset/Flight_Price_Dataset_of_Bangladesh.csv'
    validate_and_load(csv_path, mysql_config)
