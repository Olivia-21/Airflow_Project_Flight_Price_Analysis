# Flight Price Analysis Pipeline

An end-to-end data pipeline for analyzing Bangladesh flight prices using **Apache Airflow**, **dbt**, **MySQL**, **PostgreSQL**, and **Docker**.

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
- **Git** (optional, for version control)

## 🚀 Quick Start

### 1. Start Docker Containers

```bash
cd c:\Users\OliviaDosimey\Desktop\Airflow_Flight_Price_Analysis

# Build and start all containers
docker-compose up -d --build

# Check container status
docker-compose ps
```

### 2. Access Airflow UI

- **URL**: http://localhost:8080
- **Username**: airflow
- **Password**: airflow

### 3. Trigger the Pipeline

```bash
# Unpause and trigger the DAG
docker exec flight_airflow_webserver airflow dags unpause flight_price_pipeline
docker exec flight_airflow_webserver airflow dags trigger flight_price_pipeline
```

## 📁 Project Structure

```
Airflow_Flight_Price_Analysis/
├── dags/
│   └── flight_price_pipeline.py       # Main Airflow DAG (5 tasks)
├── include/
│   ├── sql/
│   │   ├── mysql_staging_schema.sql   # MySQL staging DDL
│   │   └── bronze_schema.sql          # PostgreSQL Bronze DDL
│   └── scripts/
│       ├── validate_and_load_csv.py   # CSV validation & MySQL load
│       └── transfer_to_bronze.py      # MySQL to PostgreSQL transfer
├── dbt/
│   └── flight_analytics/
│       ├── dbt_project.yml
│       ├── profiles.yml               # Database connection config
│       ├── packages.yml               # dbt dependencies
│       └── models/
│           ├── silver/                # Staging, Dimension & Fact tables
│           │   ├── stg_flight_bookings.sql
│           │   ├── stg_validation_failures.sql
│           │   ├── dim_airline.sql
│           │   ├── dim_route.sql
│           │   ├── dim_date.sql
│           │   └── fct_flight_bookings.sql
│           └── gold/                  # KPI models
│               ├── kpi_avg_fare_by_airline.sql
│               ├── kpi_booking_count_by_airline.sql
│               ├── kpi_most_popular_routes.sql
│               └── kpi_seasonal_fare_variation.sql
├── dataset/
│   └── Flight_Price_Dataset_of_Bangladesh.csv
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env
└── README.md
```

## 🔄 Pipeline Tasks (5 Tasks)

| Task | Description |
|------|-------------|
| `validate_and_load_csv` | Validates CSV structure, loads to MySQL staging |
| `transfer_to_bronze` | Transfers from MySQL to PostgreSQL Bronze |
| `dbt_silver` | Runs dbt deps + Silver models (staging, dims, facts) |
| `dbt_gold` | Builds Gold KPI models |
| `generate_report` | Creates execution summary report |

## 📊 KPI Definitions

### 1. Average Fare by Airline
- Metrics: Average base fare, tax, total fare by airline
- Additional: Market share, fare statistics, rankings

### 2. Seasonal Fare Variation
- Peak Seasons: Eid ul-Fitr, Eid ul-Adha, Winter Holidays
- Comparison: Peak vs Non-Peak fare differences

### 3. Booking Count by Airline
- Breakdown: By class (Economy/Business/First), booking source
- Metrics: Total bookings, market share, revenue

### 4. Most Popular Routes
- Top Routes: By booking count (top 50)
- Metrics: Average fare, duration, direct vs connecting flights

## 🗄️ Database Connection Details

### MySQL (Staging)
- **Host**: localhost
- **Port**: 3307
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
# Container Management
docker-compose up -d --build    # Start all services
docker-compose down             # Stop all services
docker-compose ps               # List containers
docker-compose logs -f          # View logs

# Trigger DAG
docker exec flight_airflow_webserver airflow dags trigger flight_price_pipeline

# Check DAG Status
docker exec flight_airflow_webserver airflow dags list-runs -d flight_price_pipeline

# Run dbt manually
docker exec flight_airflow_scheduler sh -c "cd /opt/airflow/dbt/flight_analytics && dbt run"
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

## 📈 Sample Queries

### Check Bronze Data
```sql
SELECT COUNT(*) FROM bronze.raw_flight_data;
```

### Check Silver Data
```sql
SELECT * FROM bronze_silver.dim_airline;
SELECT * FROM bronze_silver.fct_flight_bookings LIMIT 10;
```

### Check Gold KPIs
```sql
SELECT * FROM bronze_gold.kpi_avg_fare_by_airline;
SELECT * FROM bronze_gold.kpi_seasonal_fare_variation;
SELECT * FROM bronze_gold.kpi_booking_count_by_airline;
SELECT * FROM bronze_gold.kpi_most_popular_routes LIMIT 10;
```

## 📄 License

This project is for educational purposes.

## 👤 Author

Olivia Dosimey
