# Use official Apache Airflow image
FROM apache/airflow:2.10.4-python3.11

USER root

# Install system dependencies for psycopg2 build
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dbt project
COPY --chown=airflow:root dbt/flight_analytics /opt/airflow/dbt/flight_analytics

# Set dbt profiles directory
ENV DBT_PROFILES_DIR=/opt/airflow/dbt/flight_analytics
