-- MySQL Staging Schema for Raw Flight Data
-- This table matches the original CSV structure exactly with data types from Kaggle metadata

CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

-- Drop table if exists for clean re-runs
DROP TABLE IF EXISTS raw_flight_staging;

-- Create staging table with original column names
CREATE TABLE raw_flight_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Airline` VARCHAR(100),                    -- String
    `Source` VARCHAR(10),                      -- String
    `Source Name` VARCHAR(200),                -- String
    `Destination` VARCHAR(10),                 -- String
    `Destination Name` VARCHAR(200),           -- String
    `Departure Date & Time` VARCHAR(50),       -- String (from Kaggle metadata)
    `Arrival Date & Time` VARCHAR(50),         -- String (from Kaggle metadata)
    `Duration (hrs)` FLOAT,                    -- Float (from Kaggle metadata)
    `Stopovers` VARCHAR(50),                   -- String
    `Aircraft Type` VARCHAR(50),               -- String
    `Class` VARCHAR(50),                       -- String
    `Booking Source` VARCHAR(50),              -- String
    `Base Fare (BDT)` FLOAT,                   -- Float (from Kaggle metadata)
    `Tax & Surcharge (BDT)` FLOAT,             -- Float (from Kaggle metadata)
    `Total Fare (BDT)` FLOAT,                  -- Float (from Kaggle metadata)
    `Seasonality` VARCHAR(50),                 -- String
    `Days Before Departure` INT,               -- Int (from Kaggle metadata)
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for common queries
    INDEX idx_airline (`Airline`),
    INDEX idx_source (`Source`),
    INDEX idx_destination (`Destination`),
    INDEX idx_seasonality (`Seasonality`)
);

-- Ingestion log table for tracking success/failure
DROP TABLE IF EXISTS ingestion_log;

CREATE TABLE ingestion_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    status ENUM('SUCCESS', 'FAILURE', 'PARTIAL') NOT NULL,
    rows_expected INT,
    rows_loaded INT,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
