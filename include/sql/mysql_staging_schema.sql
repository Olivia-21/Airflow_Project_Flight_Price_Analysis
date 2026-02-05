-- MySQL Staging Schema for Raw Flight Data
-- This table matches the original CSV structure exactly with data types from Kaggle metadata
-- Includes row_hash for incremental/full load change detection

CREATE DATABASE IF NOT EXISTS flight_staging;
USE flight_staging;

-- Drop table if exists for clean re-runs
DROP TABLE IF EXISTS raw_flight_staging;

-- Create staging table with original column names as in CSV
-- row_hash is computed from ALL columns for change detection
CREATE TABLE raw_flight_staging (
    id INT AUTO_INCREMENT PRIMARY KEY,
    `Airline` VARCHAR(100),                    
    `Source` VARCHAR(10),                      
    `Source Name` VARCHAR(200),                
    `Destination` VARCHAR(10),                 
    `Destination Name` VARCHAR(200),           
    `Departure Date & Time` VARCHAR(50),       
    `Arrival Date & Time` VARCHAR(50),         
    `Duration (hrs)` FLOAT,                    
    `Stopovers` VARCHAR(50),                   
    `Aircraft Type` VARCHAR(50),               
    `Class` VARCHAR(50),                       
    `Booking Source` VARCHAR(50),              
    `Base Fare (BDT)` FLOAT,                   
    `Tax & Surcharge (BDT)` FLOAT,             
    `Total Fare (BDT)` FLOAT,                  
    `Seasonality` VARCHAR(50),                 
    `Days Before Departure` INT,
    `row_hash` VARCHAR(32),                    -- MD5 hash of all columns for change detection
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for common queries
    INDEX idx_airline (`Airline`),
    INDEX idx_source (`Source`),
    INDEX idx_destination (`Destination`),
    INDEX idx_seasonality (`Seasonality`),
    INDEX idx_row_hash (`row_hash`)            -- Index for fast hash lookups during incremental load
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
