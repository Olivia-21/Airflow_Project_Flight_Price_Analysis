# Flight Price Analysis Pipeline

An end-to-end data pipeline for analyzing Bangladesh flight prices using **Apache Airflow**, **dbt**, **MySQL**, **PostgreSQL**, and **Astronomer**.

## 🏗️ Architecture

This project implements the **Medallion Architecture** (Bronze → Silver → Gold) for progressive data quality:

```
┌─────────────┐     ┌─────────────────┐     ┌─────────────────────────────────────────┐
│   CSV File  │────▶│  MySQL Staging  │────▶│           PostgreSQL Analytics          │
│   (Source)  │     │  (Validation)   │     │ ┌─────────┐ ┌─────────┐ ┌─────────────┐ │
└─────────────┘     └─────────────────┘     │ │ Bronze  │▶│ Silver  │▶│    Gold     │ │
                                            │ │  Raw    │ │ Dims/   │ │    KPIs     │ │
                                            │ │  Data   │ │ Facts   │ │             │ │
                                            │ └─────────┘ └─────────┘ └─────────────┘ │
                                            └─────────────────────────────────────────┘
```

### Data Flow

| Layer | Database | Description |
|-------|----------|-------------|
| **Staging** | MySQL | Raw CSV ingestion with validation logging |
| **Bronze** | PostgreSQL | Validated data with renamed columns (snake_case) |
| **Silver** | PostgreSQL | Clean data with dimensions and fact tables |
| **Gold** | PostgreSQL | Business KPIs and analytics |

## 📋 Prerequisites

- **Docker Desktop** (with WSL2 on Windows)
- **Astro CLI** - Astronomer command line tool
- **Git** (optional, for version control)

### Install Astro CLI (Windows)

```powershell
# Using winget
winget install -e --id Astronomer.Astro

# Verify installation
astro version
```

## 🚀 Quick Start

### 1. Initialize and Start Airflow

```bash
# Navigate to project directory
cd c:\Users\OliviaDosimey\Desktop\Airflow_Flight_Price_Analysis

# Start Airflow environment
astro dev start

# Check running containers
astro dev ps
```

### 2. Start Additional Databases

The MySQL and PostgreSQL analytics databases are configured in `docker-compose.override.yml`:

```bash
# Start MySQL and PostgreSQL analytics containers
docker-compose -f docker-compose.override.yml up -d
```

### 3. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: admin
- **Password**: admin

### 4. Trigger the Pipeline

1. Navigate to DAGs in the Airflow UI
2. Find `flight_price_pipeline`
3. Toggle the DAG ON
4. Click "Trigger DAG" to run manually

## 📁 Project Structure

```
Airflow_Flight_Price_Analysis/
├── dags/
│   └── flight_price_pipeline.py      # Main Airflow DAG
├── include/
│   ├── sql/
│   │   ├── mysql_staging_schema.sql  # MySQL staging DDL
│   │   └── bronze_schema.sql         # PostgreSQL Bronze DDL
│   └── scripts/
│       ├── validate_and_load_csv.py  # CSV validation & MySQL load
│       └── transfer_to_bronze.py     # MySQL to PostgreSQL transfer
├── dbt/
│   └── flight_analytics/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── packages.yml
│       ├── models/
│       │   ├── staging/              # Silver: Validation models
│       │   ├── marts/                # Silver: Dimension & Fact tables
│       │   └── kpis/                 # Gold: KPI models
│       └── macros/
├── dataset/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
├── Dockerfile
├── docker-compose.override.yml
├── requirements.txt
├── packages.txt
├── .env
└── README.md
```

## 🔄 Pipeline Tasks

| Task | Description |
|------|-------------|
| `validate_and_load_csv` | Validates CSV structure, loads to MySQL staging |
| `transfer_to_bronze` | Transfers from MySQL to PostgreSQL Bronze |
| `dbt_staging` | Runs dbt staging models (validation, cleaning) |
| `dbt_marts` | Builds dimension and fact tables |
| `dbt_kpis` | Computes KPI metrics |
| `generate_report` | Creates execution summary report |

## 📊 KPI Definitions

### 1. Average Fare by Airline
- **Metrics**: Average base fare, tax, total fare by airline
- **Additional**: Market share, fare statistics, rankings

### 2. Seasonal Fare Variation
- **Peak Seasons**: Eid ul-Fitr, Eid ul-Adha, Winter Holidays
- **Comparison**: Peak vs Non-Peak fare differences
- **Metrics**: Average fares, booking counts by season

### 3. Booking Count by Airline
- **Breakdown**: By class (Economy/Business/First), booking source
- **Metrics**: Total bookings, market share, revenue

### 4. Most Popular Routes
- **Top Routes**: By booking count
- **Metrics**: Average fare, duration, direct vs connecting flights

## 🗄️ Database Connection Details

### MySQL (Staging)
- **Host**: localhost
- **Port**: 3306
- **Database**: flight_staging
- **User**: airflow
- **Password**: airflow

### PostgreSQL (Analytics)
- **Host**: localhost
- **Port**: 5433
- **Database**: flight_analytics
- **User**: analytics
- **Password**: analytics

## 🛠️ Useful Commands

```bash
# Airflow Commands
astro dev start          # Start Airflow
astro dev stop           # Stop Airflow
astro dev restart        # Restart all services
astro dev ps             # List containers
astro dev logs           # View logs
astro dev bash           # Enter container shell
astro dev parse          # Validate DAG syntax

# dbt Commands (run inside container)
astro dev bash
cd /usr/local/airflow/dbt/flight_analytics
dbt deps                 # Install dependencies
dbt run                  # Run all models
dbt test                 # Run tests
dbt run --select staging # Run only staging models
dbt docs generate        # Generate documentation
```

## 📝 Data Validation

### MySQL Staging Validation
- All 17 required columns exist
- Column data types match Kaggle metadata
- Row count verification after load
- Success/failure logging to `ingestion_log` table

### Silver Layer Validation (dbt)
- Non-null checks on required fields
- Fare values > 0
- Duration within valid range (0-48 hours)
- Duplicate removal
- Failed rows logged to `stg_validation_failures`

## 🔍 Troubleshooting

### Container Issues
```bash
# Check container status
astro dev ps

# View container logs
astro dev logs

# Restart containers
astro dev restart
```

### DAG Issues
```bash
# Validate DAG syntax
astro dev parse

# Check import errors
astro dev bash
python -c "from dags.flight_price_pipeline import dag; print('DAG OK')"
```

### dbt Issues
```bash
# Enter container
astro dev bash

# Debug dbt
cd /usr/local/airflow/dbt/flight_analytics
dbt debug
dbt deps
dbt compile
```

## 📈 Sample Queries

### Check Bronze Data
```sql
SELECT COUNT(*) FROM bronze.raw_flight_data;
SELECT * FROM bronze.raw_flight_data LIMIT 10;
```

### Check Silver Data
```sql
SELECT * FROM silver.dim_airline;
SELECT * FROM silver.dim_route LIMIT 10;
SELECT * FROM silver.fct_flight_bookings LIMIT 10;
```

### Check Gold KPIs
```sql
SELECT * FROM gold.kpi_avg_fare_by_airline;
SELECT * FROM gold.kpi_seasonal_fare_variation;
SELECT * FROM gold.kpi_booking_count_by_airline;
SELECT * FROM gold.kpi_most_popular_routes;
```

## 📄 License

This project is for educational purposes.

## 👤 Author

Olivia Dosimey
