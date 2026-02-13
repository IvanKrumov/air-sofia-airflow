#!/bin/bash
set -e

echo "Creating airquality database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE airquality'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airquality')\gexec

    \connect airquality

    CREATE TABLE IF NOT EXISTS air_quality_measurements (
        station_code VARCHAR(50),
        pollutant VARCHAR(20),
        datetime TIMESTAMP,
        value FLOAT,
        unit VARCHAR(20),
        averaging_time VARCHAR(50),
        validity INTEGER,
        verification INTEGER,
        data_capture FLOAT,
        country_code VARCHAR(10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (station_code, pollutant, datetime)
    );

    CREATE INDEX IF NOT EXISTS idx_datetime ON air_quality_measurements(datetime);
    CREATE INDEX IF NOT EXISTS idx_station ON air_quality_measurements(station_code);
    CREATE INDEX IF NOT EXISTS idx_pollutant ON air_quality_measurements(pollutant);

    GRANT ALL PRIVILEGES ON DATABASE airquality TO airflow;
    GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
EOSQL

echo "airquality database and table created successfully."
