"""
Flight Price Analysis Pipeline DAG

This DAG orchestrates the complete data pipeline for Bangladesh flight price analysis:
1. Validates and loads CSV data into MySQL staging
2. Transfers validated data to PostgreSQL Bronze layer
3. Runs dbt models for Silver (staging, dimensions, facts) and Gold (KPIs) layers

Technologies: Airflow, MySQL, PostgreSQL, dbt
Architecture: Medallion (Bronze -> Silver -> Gold)

LOAD MODES (via DAG params):
- Full Load (default): Truncates tables and reloads all data with row hashes
- Incremental Load: Only inserts records with new row hashes (skips existing)

To trigger in incremental mode, use:
    airflow dags trigger flight_price_pipeline --conf '{"load_mode": "incremental"}'
    
Or via the Airflow UI, add {"load_mode": "incremental"} to the trigger params.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import custom scripts
import sys
sys.path.insert(0, '/opt/airflow/include/scripts')
from validate_and_load_csv import run_validation_and_load
from transfer_to_bronze import run_transfer_to_bronze


# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Paths
DBT_PROJECT_PATH = '/opt/airflow/dbt/flight_analytics'


def generate_summary_report(**context):
    """Generate a summary report of the pipeline execution."""
    import logging
    from airflow.hooks.base import BaseHook
    import psycopg2
    
    logger = logging.getLogger(__name__)
    
    try:
        # Get PostgreSQL connection
        pg_conn = BaseHook.get_connection('postgres_analytics')
        conn = psycopg2.connect(
            host=pg_conn.host,
            port=pg_conn.port or 5432,
            user=pg_conn.login,
            password=pg_conn.password,
            database=pg_conn.schema or 'flight_analytics'
        )
        cursor = conn.cursor()
        
        # Get summary statistics
        report = []
        report.append("=" * 60)
        report.append("FLIGHT PRICE ANALYSIS PIPELINE - EXECUTION REPORT")
        report.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 60)
        
        # Bronze layer stats
        cursor.execute("SELECT COUNT(*) FROM bronze.raw_flight_data")
        bronze_count = cursor.fetchone()[0]
        report.append(f"\n BRONZE LAYER:")
        report.append(f"   Total records: {bronze_count:,}")
        
        # Silver layer stats
        cursor.execute("SELECT COUNT(*) FROM bronze_silver.stg_flight_bookings")
        silver_count = cursor.fetchone()[0]
        report.append(f"\n SILVER LAYER:")
        report.append(f"   Validated records: {silver_count:,}")
        report.append(f"   Validation rate: {(silver_count/bronze_count*100):.1f}%")
        
        # Dimension counts
        cursor.execute("SELECT COUNT(*) FROM bronze_silver.dim_airline")
        airline_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM bronze_silver.dim_route")
        route_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM bronze_silver.dim_date")
        date_count = cursor.fetchone()[0]
        report.append(f"   Unique airlines: {airline_count}")
        report.append(f"   Unique routes: {route_count}")
        report.append(f"   Date range entries: {date_count}")
        
        # Gold layer KPIs
        report.append(f"\n GOLD LAYER (KPIs):")
        
        # Top airline by bookings
        cursor.execute("""
            SELECT airline, total_bookings, market_share_pct 
            FROM bronze_gold.kpi_booking_count_by_airline 
            ORDER BY total_bookings DESC LIMIT 3
        """)
        top_airlines = cursor.fetchall()
        report.append("   Top 3 Airlines by Bookings:")
        for i, (airline, bookings, share) in enumerate(top_airlines, 1):
            report.append(f"     {i}. {airline}: {bookings:,} ({share}%)")
        
        # Top routes
        cursor.execute("""
            SELECT route_code, total_bookings, avg_fare_bdt 
            FROM bronze_gold.kpi_most_popular_routes 
            ORDER BY total_bookings DESC LIMIT 3
        """)
        top_routes = cursor.fetchall()
        report.append("   Top 3 Popular Routes:")
        for i, (route, bookings, fare) in enumerate(top_routes, 1):
            report.append(f"     {i}. {route}: {bookings:,} bookings (Avg: ৳{fare:,.0f})")
        
        # Seasonal comparison
        cursor.execute("""
            SELECT season_type, SUM(total_bookings) as bookings, 
                   AVG(avg_total_fare_bdt) as avg_fare
            FROM bronze_gold.kpi_seasonal_fare_variation 
            GROUP BY season_type
        """)
        seasonal = cursor.fetchall()
        report.append("   Seasonal Fare Comparison:")
        for season_type, bookings, avg_fare in seasonal:
            report.append(f"     {season_type}: {int(bookings):,} bookings, Avg ৳{avg_fare:,.0f}")
        
        report.append("\n" + "=" * 60)
        report.append("Pipeline completed successfully! ")
        report.append("=" * 60)
        
        # Log the report
        full_report = "\n".join(report)
        logger.info(full_report)
        
        cursor.close()
        conn.close()
        
        return full_report
        
    except Exception as e:
        logger.error(f"Error generating report: {str(e)}")
        raise


# Define the DAG
with DAG(
    dag_id='flight_price_pipeline',
    default_args=default_args,
    description='End-to-end flight price analysis pipeline with Medallion architecture',
    schedule_interval=None,  # Manual trigger for now
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['flight', 'analytics', 'medallion', 'dbt'],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Validate and Load CSV to MySQL
    validate_and_load_csv = PythonOperator(
        task_id='validate_and_load_csv',
        python_callable=run_validation_and_load,
        doc_md="""
        Validates CSV structure and loads data into MySQL staging table.
        - Checks all 17 required columns exist
        - Validates data types match Kaggle metadata
        - Logs success/failure to ingestion_log table
        """,
    )
    
    # Task 2: Transfer MySQL to PostgreSQL Bronze
    transfer_to_bronze = PythonOperator(
        task_id='transfer_to_bronze',
        python_callable=run_transfer_to_bronze,
        doc_md="""
        Transfers validated data from MySQL to PostgreSQL Bronze layer.
        - Renames columns to snake_case convention
        - Logs transfer results to bronze.transfer_log
        """,
    )
    
    # Task 3: dbt Dependencies and Silver Models
    dbt_silver = BashOperator(
        task_id='dbt_silver',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt deps && dbt run --select silver',
        doc_md="""
        Runs dbt Silver layer models:
        - Installs dbt packages
        - Creates staging, dimension, and fact tables
        """,
    )
    
    # Task 4: dbt Gold Models (KPIs)
    dbt_gold = BashOperator(
        task_id='dbt_gold',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --select gold',
        doc_md="""
        Runs dbt Gold layer KPI models:
        - Average Fare by Airline
        - Booking Count by Airline
        - Most Popular Routes
        - Seasonal Fare Variation
        """,
    )
    
    # Task 5: Generate Summary Report
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_summary_report,
        doc_md="""
        Generates a summary report of the pipeline execution.
        - Bronze layer record counts
        - Silver layer validation statistics
        - Gold layer KPI highlights
        """,
    )
    
    # Define task dependencies (5 tasks in sequence)
    validate_and_load_csv >> transfer_to_bronze >> dbt_silver >> dbt_gold >> generate_report
    
