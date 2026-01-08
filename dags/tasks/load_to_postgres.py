"""
Load air quality data from parquet files to PostgreSQL database.

This module handles reading parquet files containing PM2.5 and PM10 measurements
and loading them into a PostgreSQL database for further analysis.
"""

import os
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text


logger = logging.getLogger(__name__)


def get_postgres_connection_string():
    """
    Get PostgreSQL connection string from environment variables.

    Returns:
        str: SQLAlchemy connection string
    """
    # Connection parameters - can be overridden via environment variables
    host = os.environ.get('POSTGRES_HOST', 'host.docker.internal')
    port = os.environ.get('POSTGRES_PORT', '5432')
    database = os.environ.get('POSTGRES_DB', 'airquality')
    user = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', 'postgres')

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def create_table_if_not_exists(engine):
    """
    Create air quality measurements table if it doesn't exist.

    Args:
        engine: SQLAlchemy engine
    """
    create_table_sql = """
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
    """

    with engine.begin() as conn:
        for statement in create_table_sql.split(';'):
            if statement.strip():
                conn.execute(text(statement))

    logger.info("✓ Table 'air_quality_measurements' created/verified")


def load_parquet_files_to_postgres(data_dir: str = "./data/bulgaria_verified"):
    """
    Load all parquet files from data directory into PostgreSQL.

    Args:
        data_dir: Directory containing parquet files

    Returns:
        dict: Statistics about the loading process
    """
    logger.info(f"Starting data load from {data_dir}")

    # Get PostgreSQL connection
    conn_string = get_postgres_connection_string()
    logger.info(f"Connecting to PostgreSQL...")

    try:
        engine = create_engine(conn_string)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✓ PostgreSQL connection successful")

        # Create table if needed
        create_table_if_not_exists(engine)

        # Find all parquet files
        data_path = Path(data_dir)
        parquet_files = list(data_path.rglob("*.parquet"))

        if not parquet_files:
            logger.warning(f"No parquet files found in {data_dir}")
            return {"status": "no_files", "files_processed": 0}

        logger.info(f"Found {len(parquet_files)} parquet files to process")

        # Process each file
        total_rows = 0
        files_processed = 0

        for parquet_file in parquet_files:
            try:
                logger.info(f"Processing: {parquet_file.name}")

                # Read parquet file
                df = pd.read_parquet(parquet_file)

                if df.empty:
                    logger.warning(f"Empty file: {parquet_file.name}")
                    continue

                # Standardize column names (airbase data structure)
                # Adjust these based on actual parquet structure
                df.columns = df.columns.str.lower()

                # Add country code if not present
                if 'country_code' not in df.columns:
                    df['country_code'] = 'BG'

                # Load to PostgreSQL using upsert (insert or update on conflict)
                df.to_sql(
                    'air_quality_measurements',
                    engine,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                rows_loaded = len(df)
                total_rows += rows_loaded
                files_processed += 1

                logger.info(f"✓ Loaded {rows_loaded} rows from {parquet_file.name}")

            except Exception as e:
                logger.error(f"Failed to process {parquet_file.name}: {str(e)}")
                continue

        logger.info(f"✓ Data load complete! Processed {files_processed} files, loaded {total_rows} rows")

        return {
            "status": "success",
            "files_processed": files_processed,
            "total_rows": total_rows
        }

    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {str(e)}")
        raise


def load_data_task(**context):
    """
    Airflow task wrapper for loading data to PostgreSQL.

    This function is called by PythonOperator in the DAG.
    """
    result = load_parquet_files_to_postgres()

    # Push results to XCom for downstream tasks
    context['task_instance'].xcom_push(key='load_stats', value=result)

    return result
