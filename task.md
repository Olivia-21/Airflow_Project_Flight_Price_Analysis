Airflow Project: Flight Price Analysis
 
Objective
Develop an end-to-end data pipeline using Apache Airflow to process and analyze flight price data for Bangladesh. The pipeline must ingest raw CSV data, validate and transform it, compute key performance indicators (KPIs), and store the final results in a PostgreSQL database for further analysis.

Technologies
Airflow (workflow orchestration)

MySQL (staging database)

PostgreSQL (analytics database)

Python (data processing)

CSV File Source: Flight Price Dataset of Bangladesh (Kaggle)

Pipeline Components & Requirements
1. Data Ingestion

Input: Flight_Price_Dataset_of_Bangladesh.csv

Task: Load the CSV data into a staging table in MySQL.

Validation: Ensure all data from the CSV is correctly inserted with appropriate column types matching the original structure.

2. Data Validation

Checks to Implement:

All required columns exist: Airline, Source, Destination, Base Fare, Tax & Surcharge, Total Fare.

Handle missing or null values.

Validate data types (e.g., numeric fare values, non-empty strings for categorical fields).

Flag or correct any inconsistencies (e.g., negative fares or invalid city names).

3. Data Transformation & KPI Computation

Transformation:

If not already present, calculate Total Fare = Base Fare + Tax & Surcharge.

KPI Metrics to Compute:

Average Fare by Airline

Seasonal Fare Variation:

Define peak seasons (e.g., Eid, Winter holidays).

Compare average fares during peak vs. non-peak seasons.

Booking Count by Airline

Most Popular Routes:

Identify top source-destination pairs by booking count.

4. Data Loading into PostgreSQL

Task: Transfer the transformed and enriched data to a PostgreSQL database.

Goal: Ensure minimal latency and data consistency during transfer.

Documentation
A comprehensive report including:

Pipeline architecture and execution flow.

Description of each Airflow DAG/task.

KPI definitions and computation logic.

Challenges encountered and how they were resolved