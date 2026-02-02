-- PostgreSQL Bronze Schema for Flight Analytics
-- This schema stores the raw data transferred from MySQL with renamed columns (snake_case)

-- Create bronze schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Create silver schema
CREATE SCHEMA IF NOT EXISTS silver;

-- Create gold schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Drop table if exists for clean re-runs
DROP TABLE IF EXISTS bronze.raw_flight_data;

-- Bronze Layer: Raw flight data with renamed columns
CREATE TABLE bronze.raw_flight_data (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(200),
    destination_code VARCHAR(10),
    destination_name VARCHAR(200),
    departure_datetime VARCHAR(50),
    arrival_datetime VARCHAR(50),
    duration_hours FLOAT,
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(50),
    booking_class VARCHAR(50),
    booking_source VARCHAR(50),
    base_fare_bdt FLOAT,
    tax_surcharge_bdt FLOAT,
    total_fare_bdt FLOAT,
    seasonality VARCHAR(50),
    days_before_departure INT,
    source_system VARCHAR(50) DEFAULT 'mysql_staging',
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for Bronze table
CREATE INDEX IF NOT EXISTS idx_bronze_airline ON bronze.raw_flight_data(airline);
CREATE INDEX IF NOT EXISTS idx_bronze_route ON bronze.raw_flight_data(source_code, destination_code);
CREATE INDEX IF NOT EXISTS idx_bronze_seasonality ON bronze.raw_flight_data(seasonality);

-- Transfer log table
DROP TABLE IF EXISTS bronze.transfer_log;

CREATE TABLE bronze.transfer_log (
    id SERIAL PRIMARY KEY,
    source_table VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_transferred INT,
    error_message TEXT,
    transferred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


