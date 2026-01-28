"""
Flight Price Analysis Pipeline DAG

This DAG orchestrates the complete data pipeline for Bangladesh flight price analysis:
1. Validates and loads CSV data into MySQL staging
2. Transfers validated data to PostgreSQL Bronze layer
3. Runs dbt models for Silver (staging, dimensions, facts) and Gold (KPIs) layers

Technologies: Airflow, MySQL, PostgreSQL, dbt, Astronomer Cosmos
Architecture: Medallion (Bronze -> Silver -> Gold)
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Cosmos imports for dbt integration
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Import custom scripts
import sys
sys.path.insert(0, '/usr/local/airflow/include/scripts')
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
DBT_PROJECT_PATH = Path('/usr/local/airflow/dbt/flight_analytics')
DBT_EXECUTABLE_PATH = '/usr/local/airflow/.local/bin/dbt'

# dbt Profile Configuration
profile_config = ProfileConfig(
    profile_name='flight_analytics',
    target_name='dev',
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='postgres_analytics',
        profile_args={
            'schema': 'bronze',
        },
    ),
)

# dbt Execution Configuration
execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)


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
        report.append(f"\n📦 BRONZE LAYER:")
        report.append(f"   Total records: {bronze_count:,}")
        
        # Silver layer stats
        cursor.execute("SELECT COUNT(*) FROM silver.stg_flight_bookings")
        silver_count = cursor.fetchone()[0]
        report.append(f"\n🥈 SILVER LAYER:")
        report.append(f"   Validated records: {silver_count:,}")
        report.append(f"   Validation rate: {(silver_count/bronze_count*100):.1f}%")
        
        # Dimension counts
        cursor.execute("SELECT COUNT(*) FROM silver.dim_airline")
        airline_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM silver.dim_route")
        route_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM silver.dim_date")
        date_count = cursor.fetchone()[0]
        report.append(f"   Unique airlines: {airline_count}")
        report.append(f"   Unique routes: {route_count}")
        report.append(f"   Date range entries: {date_count}")
        
        # Gold layer KPIs
        report.append(f"\n🥇 GOLD LAYER (KPIs):")
        
        # Top airline by bookings
        cursor.execute("""
            SELECT airline, total_bookings, market_share_pct 
            FROM gold.kpi_booking_count_by_airline 
            ORDER BY total_bookings DESC LIMIT 3
        """)
        top_airlines = cursor.fetchall()
        report.append("   Top 3 Airlines by Bookings:")
        for i, (airline, bookings, share) in enumerate(top_airlines, 1):
            report.append(f"     {i}. {airline}: {bookings:,} ({share}%)")
        
        # Top routes
        cursor.execute("""
            SELECT route_code, total_bookings, avg_fare_bdt 
            FROM gold.kpi_most_popular_routes 
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
            FROM gold.kpi_seasonal_fare_variation 
            GROUP BY season_type
        """)
        seasonal = cursor.fetchall()
        report.append("   Seasonal Fare Comparison:")
        for season_type, bookings, avg_fare in seasonal:
            report.append(f"     {season_type}: {int(bookings):,} bookings, Avg ৳{avg_fare:,.0f}")
        
        report.append("\n" + "=" * 60)
        report.append("Pipeline completed successfully! ✅")
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
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
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
    
    # Task Group: dbt Staging Models (Silver Layer - Validation)
    with TaskGroup(group_id='dbt_staging') as dbt_staging:
        dbt_staging_models = DbtTaskGroup(
            group_id='staging_models',
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            operator_args={
                'install_deps': True,
            },
            default_args={
                'retries': 2,
            },
            select=['path:models/staging'],
        )
    
    # Task Group: dbt Marts Models (Silver Layer - Dimensions/Facts)
    with TaskGroup(group_id='dbt_marts') as dbt_marts:
        dbt_marts_models = DbtTaskGroup(
            group_id='marts_models',
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            default_args={
                'retries': 2,
            },
            select=['path:models/marts'],
        )
    
    # Task Group: dbt KPI Models (Gold Layer)
    with TaskGroup(group_id='dbt_kpis') as dbt_kpis:
        dbt_kpi_models = DbtTaskGroup(
            group_id='kpi_models',
            project_config=ProjectConfig(
                dbt_project_path=DBT_PROJECT_PATH,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            default_args={
                'retries': 2,
            },
            select=['path:models/kpis'],
        )
    
    # Task: Generate Summary Report
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
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies
    (
        start
        >> validate_and_load_csv
        >> transfer_to_bronze
        >> dbt_staging
        >> dbt_marts
        >> dbt_kpis
        >> generate_report
        >> end
    )
