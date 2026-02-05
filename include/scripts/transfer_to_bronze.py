"""
MySQL to PostgreSQL Bronze Transfer Script
Transfers validated data from MySQL staging to PostgreSQL Bronze layer with column renaming.
Supports incremental and full load modes using row hashing for change detection.
"""

import pandas as pd
import mysql.connector
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import logging
from datetime import datetime
from typing import Tuple, Optional, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Column mapping: MySQL (original) -> PostgreSQL (snake_case)
COLUMN_MAPPING = {
    'Airline': 'airline',
    'Source': 'source_code',
    'Source Name': 'source_name',
    'Destination': 'destination_code',
    'Destination Name': 'destination_name',
    'Departure Date & Time': 'departure_datetime',
    'Arrival Date & Time': 'arrival_datetime',
    'Duration (hrs)': 'duration_hours',
    'Stopovers': 'stopovers',
    'Aircraft Type': 'aircraft_type',
    'Class': 'booking_class',
    'Booking Source': 'booking_source',
    'Base Fare (BDT)': 'base_fare_bdt',
    'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
    'Total Fare (BDT)': 'total_fare_bdt',
    'Seasonality': 'seasonality',
    'Days Before Departure': 'days_before_departure',
    'row_hash': 'row_hash'  # Preserve hash for incremental load detection
}

# Load modes
LOAD_MODE_FULL = 'full'        # Truncate and reload all data
LOAD_MODE_INCREMENTAL = 'incremental'  # Only insert new/changed records


def extract_from_mysql(mysql_config: dict) -> Tuple[bool, Optional[pd.DataFrame], Optional[str]]:
    """
    Extract data from MySQL staging table.
    
    Args:
        mysql_config: MySQL connection configuration
        
    Returns:
        Tuple of (success, dataframe, error_message)
    """
    connection = None
    
    try:
        connection = mysql.connector.connect(**mysql_config)
        logger.info(f"Connected to MySQL database: {mysql_config.get('database', 'unknown')}")
        
        # Read all data from staging table
        query = "SELECT * FROM raw_flight_staging"
        df = pd.read_sql(query, connection)
        
        logger.info(f"Extracted {len(df):,} rows from MySQL raw_flight_staging")
        
        return True, df, None
        
    except Exception as e:
        error_msg = f"MySQL extraction error: {str(e)}"
        logger.error(error_msg)
        return False, None, error_msg
    finally:
        if connection and connection.is_connected():
            connection.close()


def transform_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns from MySQL format to PostgreSQL snake_case format.
    Preserves row_hash for incremental load change detection.
    
    Args:
        df: DataFrame with MySQL column names
        
    Returns:
        DataFrame with PostgreSQL column names (including row_hash)
    """
    # Drop MySQL-specific columns (keep row_hash for incremental detection)
    columns_to_drop = ['id', 'ingested_at']
    df = df.drop(columns=[c for c in columns_to_drop if c in df.columns], errors='ignore')
    
    # Rename columns (including row_hash mapping)
    df = df.rename(columns=COLUMN_MAPPING)
    
    # Verify row_hash is present
    if 'row_hash' in df.columns:
        logger.info(f"row_hash column preserved for change detection")
    else:
        logger.warning(f"row_hash column not found in source data")
    
    logger.info(f"Transformed columns: {list(COLUMN_MAPPING.keys())[:3]}... -> {list(COLUMN_MAPPING.values())[:3]}...")
    
    return df


def load_to_postgres(
    df: pd.DataFrame,
    postgres_config: dict,
    schema: str = 'bronze',
    table_name: str = 'raw_flight_data',
    batch_size: int = 5000,
    load_mode: str = LOAD_MODE_FULL
) -> Tuple[bool, int, int, Optional[str]]:
    """
    Load transformed data into PostgreSQL Bronze table with hash-based change detection.
    
    Supports two load modes:
    - FULL: Truncate table and reload all data
    - INCREMENTAL: Only insert records with new row_hash values
    
    Args:
        df: Transformed DataFrame (must include row_hash column)
        postgres_config: PostgreSQL connection configuration
        schema: Target schema
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
        connection = psycopg2.connect(**postgres_config)
        cursor = connection.cursor()
        
        logger.info(f"Connected to PostgreSQL database: {postgres_config.get('database', 'unknown')}")
        logger.info(f"Load mode: {load_mode.upper()}")
        
        # Get column names
        columns = list(df.columns)
        
        if load_mode == LOAD_MODE_FULL:
            # FULL LOAD: Truncate and reload all data
            cursor.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            ))
            connection.commit()
            logger.info(f"Truncated table {schema}.{table_name} for full load")
            
            # Replace NaN with None
            df_subset = df.where(pd.notnull(df), None)
            total_rows = len(df_subset)
            
            # Insert in batches
            for i in range(0, total_rows, batch_size):
                batch = df_subset.iloc[i:i+batch_size]
                values = [tuple(row) for row in batch.values]
                
                insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                
                execute_values(cursor, insert_sql.as_string(cursor), values)
                connection.commit()
                rows_loaded += len(values)
                
                if (i + batch_size) % 10000 == 0 or i + batch_size >= total_rows:
                    logger.info(f"Progress: {min(i + batch_size, total_rows):,}/{total_rows:,} rows transferred")
        
        elif load_mode == LOAD_MODE_INCREMENTAL:
            # INCREMENTAL LOAD: Only insert rows with new hashes
            if 'row_hash' not in df.columns:
                raise ValueError("row_hash column required for incremental load but not found")
            
            logger.info("Fetching existing hashes from PostgreSQL...")
            
            cursor.execute(sql.SQL("SELECT row_hash FROM {}.{}").format(
                sql.Identifier(schema),
                sql.Identifier(table_name)
            ))
            existing_hashes = set(row[0] for row in cursor.fetchall() if row[0])
            logger.info(f"Found {len(existing_hashes):,} existing records in PostgreSQL")
            
            # Filter to only new records (hash not in existing)
            df_new = df[~df['row_hash'].isin(existing_hashes)]
            df_new = df_new.where(pd.notnull(df_new), None)
            
            new_records = len(df_new)
            rows_skipped = len(df) - new_records
            
            if new_records == 0:
                logger.info("No new records to insert - all records already exist in PostgreSQL")
                return True, 0, rows_skipped, None
            
            logger.info(f"Found {new_records:,} new records to insert, {rows_skipped:,} existing records skipped")
            
            # Insert only new records in batches
            total_rows = len(df_new)
            for i in range(0, total_rows, batch_size):
                batch = df_new.iloc[i:i+batch_size]
                values = [tuple(row) for row in batch.values]
                
                insert_sql = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                    sql.Identifier(schema),
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns))
                )
                
                execute_values(cursor, insert_sql.as_string(cursor), values)
                connection.commit()
                rows_loaded += len(values)
                
                if (i + batch_size) % 10000 == 0 or i + batch_size >= total_rows:
                    logger.info(f"Progress: {min(i + batch_size, total_rows):,}/{total_rows:,} new rows transferred")
        
        logger.info(f"SUCCESS | Transferred {rows_loaded:,} rows to PostgreSQL {schema}.{table_name}")
        if rows_skipped > 0:
            logger.info(f"         Skipped {rows_skipped:,} existing rows (hash match)")
        
        # Verify row count
        cursor.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table_name)
        ))
        pg_count = cursor.fetchone()[0]
        
        if load_mode == LOAD_MODE_FULL:
            if pg_count == len(df):
                logger.info(f"Row count verification: Source={len(df):,}, PostgreSQL={pg_count:,} ✓")
            else:
                logger.warning(f"Row count mismatch: Source={len(df):,}, PostgreSQL={pg_count:,}")
        else:
            logger.info(f"Total records in PostgreSQL after incremental load: {pg_count:,}")
        
        return True, rows_loaded, rows_skipped, None
        
    except Exception as e:
        error_msg = f"PostgreSQL load error: {str(e)}"
        logger.error(error_msg)
        if connection:
            connection.rollback()
        return False, rows_loaded, rows_skipped, error_msg
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def log_transfer_result(
    postgres_config: dict,
    source_table: str,
    target_table: str,
    status: str,
    rows_transferred: int,
    error_message: Optional[str] = None
):
    """Log transfer result to PostgreSQL transfer_log table."""
    connection = None
    cursor = None
    
    try:
        connection = psycopg2.connect(**postgres_config)
        cursor = connection.cursor()
        
        insert_sql = """
            INSERT INTO bronze.transfer_log 
            (source_table, target_table, status, rows_transferred, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(insert_sql, (
            source_table, target_table, status, rows_transferred, error_message
        ))
        connection.commit()
        logger.info(f"Transfer log recorded: {status}")
        
    except Exception as e:
        logger.error(f"Failed to log transfer result: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def transfer_mysql_to_bronze(
    mysql_config: dict, 
    postgres_config: dict,
    load_mode: str = LOAD_MODE_FULL
) -> bool:
    """
    Main function to transfer data from MySQL to PostgreSQL Bronze with hash-based change detection.
    
    Args:
        mysql_config: MySQL connection configuration
        postgres_config: PostgreSQL connection configuration
        load_mode: 'full' (truncate and reload) or 'incremental' (only new records)
        
    Returns:
        True if successful, False otherwise
    """
    source_table = 'flight_staging.raw_flight_staging'
    target_table = 'bronze.raw_flight_data'
    
    logger.info("=" * 60)
    logger.info(f"STARTING {'FULL' if load_mode == LOAD_MODE_FULL else 'INCREMENTAL'} TRANSFER")
    logger.info("=" * 60)
    
    # Step 1: Extract from MySQL
    success, df, error_msg = extract_from_mysql(mysql_config)
    
    if not success:
        logger.error(f"FAILURE | MySQL extraction failed")
        log_transfer_result(
            postgres_config, source_table, target_table,
            'FAILURE', 0, error_msg
        )
        return False
    
    if df is None or len(df) == 0:
        logger.error("FAILURE | No data extracted from MySQL")
        log_transfer_result(
            postgres_config, source_table, target_table,
            'FAILURE', 0, 'No data in source table'
        )
        return False
    
    # Step 2: Transform columns (preserves row_hash)
    df_transformed = transform_columns(df)
    
    # Step 3: Load to PostgreSQL with hash-based change detection
    success, rows_loaded, rows_skipped, error_msg = load_to_postgres(
        df_transformed, postgres_config, load_mode=load_mode
    )
    
    if success:
        log_transfer_result(
            postgres_config, source_table, target_table,
            'SUCCESS', rows_loaded, None
        )
        return True
    else:
        log_transfer_result(
            postgres_config, source_table, target_table,
            'FAILURE', rows_loaded, error_msg
        )
        return False


# For Airflow task usage
def run_transfer_to_bronze(**context):
    """
    Airflow-callable function for MySQL to PostgreSQL transfer.
    
    Supports both full and incremental load modes via DAG params:
    - Full Load (default): Truncates Bronze table and reloads all data
    - Incremental Load: Only inserts records with new row_hash values
    
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
    
    # Get PostgreSQL connection from Airflow
    pg_conn = BaseHook.get_connection('postgres_analytics')
    postgres_config = {
        'host': pg_conn.host,
        'port': pg_conn.port or 5432,
        'user': pg_conn.login,
        'password': pg_conn.password,
        'database': pg_conn.schema or 'flight_analytics'
    }
    
    # Get load mode from DAG params (default to 'full')
    params = context.get('params', {})
    load_mode = params.get('load_mode', LOAD_MODE_FULL)
    
    # Validate load mode
    if load_mode not in [LOAD_MODE_FULL, LOAD_MODE_INCREMENTAL]:
        logger.warning(f"Invalid load_mode '{load_mode}', defaulting to 'full'")
        load_mode = LOAD_MODE_FULL
    
    success = transfer_mysql_to_bronze(mysql_config, postgres_config, load_mode=load_mode)
    
    if not success:
        raise Exception("MySQL to PostgreSQL transfer failed. Check logs for details.")
    
    return f"MySQL to PostgreSQL Bronze {load_mode} transfer completed successfully"


if __name__ == '__main__':
    # For local testing
    import argparse
    
    parser = argparse.ArgumentParser(description='Transfer data from MySQL to PostgreSQL Bronze')
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
    
    postgres_config = {
        'host': 'localhost',
        'port': 5433,
        'user': 'analytics',
        'password': 'analytics',
        'database': 'flight_analytics'
    }
    
    transfer_mysql_to_bronze(mysql_config, postgres_config, load_mode=args.mode)

