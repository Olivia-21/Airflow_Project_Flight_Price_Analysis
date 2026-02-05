"""
CSV Validation and MySQL Loading Script
Validates CSV structure and loads data into MySQL staging table with logging.
Supports incremental and full load using row hashing for change detection.
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
import hashlib
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

# Load modes
LOAD_MODE_FULL = 'full'        # Truncate and reload all data
LOAD_MODE_INCREMENTAL = 'incremental'  # Only insert new/changed records


def generate_row_hash(row: pd.Series, columns: List[str]) -> str:
    """
    Generate a deterministic MD5 hash from ALL columns of a row.
    
    This hash is used for:
    1. Detecting new records during incremental load
    2. Detecting changed records (data modifications)
    3. Ensuring data integrity through deduplication
    
    Args:
        row: A pandas Series representing a single row
        columns: List of column names to include in hash (all columns)
        
    Returns:
        MD5 hash string of the row content
        
    Example:
        Input row: Airline='US-Bangla', Source='DAC', Destination='DXB', ...
        Output: 'a1b2c3d4e5f6789012345678901234567890abcd'
    """
    # Concatenate all column values into a single string
    # Use '|' as separator and handle None/NaN values
    values = []
    for col in columns:
        val = row.get(col, '')
        # Convert to string and handle None/NaN
        if pd.isna(val):
            val = 'NULL'
        else:
            val = str(val).strip()
        values.append(val)
    
    # Create the hash string from all values
    hash_string = '|'.join(values)
    
    # Generate MD5 hash
    return hashlib.md5(hash_string.encode('utf-8')).hexdigest()


def add_row_hashes(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Add a 'row_hash' column to the DataFrame using all specified columns.
    
    The hash enables:
    - Full Load: Truncate table and insert all records with hashes
    - Incremental Load: Compare hashes to identify new/changed records
    
    Args:
        df: DataFrame to add hashes to
        columns: List of columns to include in hash calculation
        
    Returns:
        DataFrame with additional 'row_hash' column
    """
    logger.info(f"Generating row hashes using {len(columns)} columns...")
    
    # Generate hash for each row using all columns
    df['row_hash'] = df.apply(lambda row: generate_row_hash(row, columns), axis=1)
    
    # Count unique hashes to detect potential duplicates
    unique_hashes = df['row_hash'].nunique()
    total_rows = len(df)
    
    if unique_hashes < total_rows:
        logger.warning(f"  {total_rows - unique_hashes} potential duplicate rows detected (same hash)")
    else:
        logger.info(f"  All {total_rows:,} rows have unique hashes")
    
    return df


def perform_eda(df: pd.DataFrame) -> dict:
    """
    Perform Exploratory Data Analysis on the dataset.
    Logs detailed statistics and returns a summary dictionary.
    
    Args:
        df: The DataFrame to analyze
        
    Returns:
        Dictionary with EDA summary statistics
    """
    logger.info("=" * 60)
    logger.info("EXPLORATORY DATA ANALYSIS (EDA)")
    logger.info("=" * 60)
    
    eda_summary = {}
    
    # 1. Basic Shape
    eda_summary['total_rows'] = len(df)
    eda_summary['total_columns'] = len(df.columns)
    logger.info(f"\n[DATA SHAPE]")
    logger.info(f"  Total Rows: {len(df):,}")
    logger.info(f"  Total Columns: {len(df.columns)}")
    
    # 2. Missing Values Analysis
    missing = df.isnull().sum()
    missing_cols = missing[missing > 0]
    eda_summary['missing_value_columns'] = len(missing_cols)
    logger.info(f"\n[MISSING VALUES]")
    if len(missing_cols) == 0:
        logger.info("  No missing values found - all columns complete")
    else:
        for col, count in missing_cols.items():
            pct = (count / len(df)) * 100
            logger.info(f"  {col}: {count:,} missing ({pct:.2f}%)")
    
    # 3. Seasonality Distribution
    logger.info(f"\n[SEASONALITY DISTRIBUTION]")
    seasonality_counts = df['Seasonality'].value_counts()
    eda_summary['seasonality_values'] = seasonality_counts.to_dict()
    for season, count in seasonality_counts.items():
        pct = (count / len(df)) * 100
        logger.info(f"  {season}: {count:,} ({pct:.1f}%)")
    
    # 4. Class Distribution
    logger.info(f"\n[CLASS DISTRIBUTION]")
    class_counts = df['Class'].value_counts()
    eda_summary['class_values'] = class_counts.to_dict()
    for cls, count in class_counts.items():
        pct = (count / len(df)) * 100
        logger.info(f"  {cls}: {count:,} ({pct:.1f}%)")
    
    # 5. Airline Distribution (Top 5)
    logger.info(f"\n[TOP 5 AIRLINES]")
    airline_counts = df['Airline'].value_counts().head(5)
    eda_summary['unique_airlines'] = df['Airline'].nunique()
    logger.info(f"  Total unique airlines: {df['Airline'].nunique()}")
    for airline, count in airline_counts.items():
        pct = (count / len(df)) * 100
        logger.info(f"  {airline}: {count:,} ({pct:.1f}%)")
    
    # 6. Stopovers Distribution
    logger.info(f"\n[STOPOVERS DISTRIBUTION]")
    stopover_counts = df['Stopovers'].value_counts()
    eda_summary['stopover_values'] = stopover_counts.to_dict()
    for stopover, count in stopover_counts.items():
        pct = (count / len(df)) * 100
        logger.info(f"  {stopover}: {count:,} ({pct:.1f}%)")
    
    # 7. Fare Statistics
    logger.info(f"\n[FARE STATISTICS]")
    logger.info(f"  Base Fare (BDT):")
    logger.info(f"    Min: {df['Base Fare (BDT)'].min():,.2f}")
    logger.info(f"    Max: {df['Base Fare (BDT)'].max():,.2f}")
    logger.info(f"    Mean: {df['Base Fare (BDT)'].mean():,.2f}")
    logger.info(f"  Total Fare (BDT):")
    logger.info(f"    Min: {df['Total Fare (BDT)'].min():,.2f}")
    logger.info(f"    Max: {df['Total Fare (BDT)'].max():,.2f}")
    logger.info(f"    Mean: {df['Total Fare (BDT)'].mean():,.2f}")
    eda_summary['avg_total_fare'] = df['Total Fare (BDT)'].mean()
    
    # 8. Duration Statistics
    logger.info(f"\n[DURATION STATISTICS]")
    logger.info(f"  Duration (hrs):")
    logger.info(f"    Min: {df['Duration (hrs)'].min():.2f}")
    logger.info(f"    Max: {df['Duration (hrs)'].max():.2f}")
    logger.info(f"    Mean: {df['Duration (hrs)'].mean():.2f}")
    eda_summary['avg_duration'] = df['Duration (hrs)'].mean()
    
    # 9. Route Analysis
    logger.info(f"\n[ROUTE ANALYSIS]")
    unique_sources = df['Source'].nunique()
    unique_destinations = df['Destination'].nunique()
    unique_routes = df.groupby(['Source', 'Destination']).ngroups
    eda_summary['unique_sources'] = unique_sources
    eda_summary['unique_destinations'] = unique_destinations
    eda_summary['unique_routes'] = unique_routes
    logger.info(f"  Unique source airports: {unique_sources}")
    logger.info(f"  Unique destination airports: {unique_destinations}")
    logger.info(f"  Unique routes (source-destination pairs): {unique_routes}")
    
    # 10. Data Quality Issues Detection
    logger.info(f"\n[DATA QUALITY CHECK]")
    issues = []
    
    # Check for negative fares
    negative_fares = len(df[df['Base Fare (BDT)'] < 0])
    if negative_fares > 0:
        issues.append(f"Negative base fares: {negative_fares}")
        logger.warning(f"  WARNING: {negative_fares} records with negative base fares")
    
    # Check for zero total fares
    zero_fares = len(df[df['Total Fare (BDT)'] <= 0])
    if zero_fares > 0:
        issues.append(f"Zero/negative total fares: {zero_fares}")
        logger.warning(f"  WARNING: {zero_fares} records with zero/negative total fares")
    
    # Check for invalid durations
    invalid_duration = len(df[(df['Duration (hrs)'] < 0) | (df['Duration (hrs)'] > 48)])
    if invalid_duration > 0:
        issues.append(f"Invalid durations: {invalid_duration}")
        logger.warning(f"  WARNING: {invalid_duration} records with invalid durations (< 0 or > 48 hrs)")
    
    if len(issues) == 0:
        logger.info("  No data quality issues detected")
    
    eda_summary['data_quality_issues'] = issues
    
    logger.info("=" * 60)
    logger.info("EDA COMPLETE")
    logger.info("=" * 60)
    
    return eda_summary


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
    batch_size: int = 5000,
    load_mode: str = LOAD_MODE_FULL
) -> Tuple[bool, int, int, Optional[str]]:
    """
    Load validated DataFrame into MySQL staging table with hash-based change detection.
    
    Supports two load modes:
    - FULL: Truncate table and reload all data (always includes row_hash)
    - INCREMENTAL: Only insert records with new hashes (skip existing ones)
    
    Args:
        df: Validated pandas DataFrame
        mysql_config: MySQL connection configuration
        table_name: Target table name
        batch_size: Number of rows per batch insert
        load_mode: 'full' or 'incremental'
        
    Returns:
        Tuple of (success, rows_loaded, rows_skipped, error_message)
    """
    connection = None
    cursor = None
    rows_loaded = 0
    rows_skipped = 0
    
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        logger.info(f"Connected to MySQL database: {mysql_config.get('database', 'unknown')}")
        logger.info(f"Load mode: {load_mode.upper()}")
        
        # Generate row hashes using ALL columns
        df_with_hash = add_row_hashes(df.copy(), EXPECTED_COLUMNS)
        
        # Columns to insert (including row_hash)
        insert_columns = EXPECTED_COLUMNS + ['row_hash']
        
        if load_mode == LOAD_MODE_FULL:
            # FULL LOAD: Truncate and reload all data
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            logger.info(f"Truncated table {table_name} for full load")
            
            # Prepare insert statement with row_hash
            placeholders = ', '.join(['%s'] * len(insert_columns))
            column_names = ', '.join([f'`{col}`' for col in insert_columns])
            insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Select columns and convert to list of tuples
            df_subset = df_with_hash[insert_columns].copy()
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
        
        elif load_mode == LOAD_MODE_INCREMENTAL:
            # INCREMENTAL LOAD: Only insert rows with new hashes
            logger.info("Fetching existing hashes from database...")
            
            cursor.execute(f"SELECT `row_hash` FROM {table_name}")
            existing_hashes = set(row[0] for row in cursor.fetchall())
            logger.info(f"Found {len(existing_hashes):,} existing records in database")
            
            # Filter to only new records (hash not in existing)
            df_subset = df_with_hash[insert_columns].copy()
            df_subset = df_subset[~df_subset['row_hash'].isin(existing_hashes)]
            df_subset = df_subset.where(pd.notnull(df_subset), None)
            
            new_records = len(df_subset)
            rows_skipped = len(df_with_hash) - new_records
            
            if new_records == 0:
                logger.info("No new records to insert - all records already exist")
                return True, 0, rows_skipped, None
            
            logger.info(f"Found {new_records:,} new records to insert, {rows_skipped:,} existing records skipped")
            
            # Prepare insert statement
            placeholders = ', '.join(['%s'] * len(insert_columns))
            column_names = ', '.join([f'`{col}`' for col in insert_columns])
            insert_sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            
            # Insert only new records in batches
            total_rows = len(df_subset)
            for i in range(0, total_rows, batch_size):
                batch = df_subset.iloc[i:i+batch_size]
                batch_data = [tuple(row) for row in batch.values]
                cursor.executemany(insert_sql, batch_data)
                connection.commit()
                rows_loaded += len(batch_data)
                
                if (i + batch_size) % 10000 == 0 or i + batch_size >= total_rows:
                    logger.info(f"Progress: {min(i + batch_size, total_rows):,}/{total_rows:,} new rows loaded")
        
        logger.info(f"SUCCESS | Loaded {rows_loaded:,} rows into MySQL {table_name}")
        if rows_skipped > 0:
            logger.info(f"         Skipped {rows_skipped:,} existing rows (hash match)")
        
        # Verify row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        mysql_count = cursor.fetchone()[0]
        
        if load_mode == LOAD_MODE_FULL:
            expected = len(df_with_hash)
            if mysql_count == expected:
                logger.info(f"Row count verification: CSV={expected:,}, MySQL={mysql_count:,} ✓")
            else:
                logger.warning(f"Row count mismatch: CSV={expected:,}, MySQL={mysql_count:,}")
        else:
            logger.info(f"Total records in MySQL after incremental load: {mysql_count:,}")
        
        return True, rows_loaded, rows_skipped, None
        
    except Error as e:
        error_msg = f"MySQL error: {str(e)}"
        logger.error(error_msg)
        return False, rows_loaded, rows_skipped, error_msg
    except Exception as e:
        error_msg = f"Unexpected error during load: {str(e)}"
        logger.error(error_msg)
        return False, rows_loaded, rows_skipped, error_msg
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


def validate_and_load(csv_path: str, mysql_config: dict, load_mode: str = LOAD_MODE_FULL) -> bool:
    """
    Main function to validate CSV and load to MySQL with hash-based change detection.
    
    Args:
        csv_path: Path to CSV file
        mysql_config: MySQL connection configuration
        load_mode: 'full' (truncate and reload) or 'incremental' (only new records)
        
    Returns:
        True if successful, False otherwise
    """
    started_at = datetime.now()
    file_name = Path(csv_path).name
    
    logger.info("=" * 60)
    logger.info(f"STARTING {'FULL' if load_mode == LOAD_MODE_FULL else 'INCREMENTAL'} LOAD")
    logger.info("=" * 60)
    
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
    
    # Step 2: Perform EDA
    logger.info(f"\n[RECORD COUNT] CSV Raw Data: {len(df):,} rows")
    eda_summary = perform_eda(df)
    
    # Step 3: Load to MySQL with hashing
    success, rows_loaded, rows_skipped, error_msg = load_csv_to_mysql(
        df, mysql_config, load_mode=load_mode
    )
    
    logger.info(f"[RECORD COUNT] Loaded to MySQL: {rows_loaded:,} rows")
    if rows_skipped > 0:
        logger.info(f"[RECORD COUNT] Skipped (already exist): {rows_skipped:,} rows")
    
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
    """
    Airflow-callable function for CSV validation and MySQL load.
    
    Supports both full and incremental load modes via DAG params:
    - Full Load (default): Truncates table and reloads all data with hashes
    - Incremental Load: Only inserts records with new hashes
    
    To trigger incremental load, pass params={'load_mode': 'incremental'} to DAG trigger.
    """
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
    csv_path = '/opt/airflow/dataset/Flight_Price_Dataset_of_Bangladesh.csv'
    
    # Get load mode from DAG params (default to 'full')
    params = context.get('params', {})
    load_mode = params.get('load_mode', LOAD_MODE_FULL)
    
    # Validate load mode
    if load_mode not in [LOAD_MODE_FULL, LOAD_MODE_INCREMENTAL]:
        logger.warning(f"Invalid load_mode '{load_mode}', defaulting to 'full'")
        load_mode = LOAD_MODE_FULL
    
    success = validate_and_load(csv_path, mysql_config, load_mode=load_mode)
    
    if not success:
        raise Exception("CSV validation and load failed. Check logs for details.")
    
    return f"CSV validation and MySQL {load_mode} load completed successfully"


if __name__ == '__main__':
    # For local testing
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate and load CSV to MySQL')
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full',
                        help='Load mode: full (truncate and reload) or incremental (new records only)')
    args = parser.parse_args()
    
    mysql_config = {
        'host': 'localhost',
        'port': 3306,
        'user': 'airflow',
        'password': 'airflow',
        'database': 'flight_staging'
    }
    
    csv_path = '../../dataset/Flight_Price_Dataset_of_Bangladesh.csv'
    validate_and_load(csv_path, mysql_config, load_mode=args.mode)

