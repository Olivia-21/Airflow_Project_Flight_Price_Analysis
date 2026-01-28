FROM quay.io/astronomer/astro-runtime:12.4.0

# Install dbt and database connectors
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.7.1 \
    dbt-postgres==1.9.0 \
    mysql-connector-python==9.1.0 \
    pymysql==1.1.1 \
    pandas==2.2.3 \
    psycopg2-binary==2.9.10

# Copy dbt project
COPY dbt/flight_analytics /usr/local/airflow/dbt/flight_analytics
