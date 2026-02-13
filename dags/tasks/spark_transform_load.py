"""
PySpark task: Read parquet files, select/rename columns, write to PostgreSQL.

Uses Spark local mode for the column selection and renaming transformation.
"""

import os
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

COLUMN_MAPPING = {
    "samplingpoint": "station_code",
    "pollutant": "pollutant",
    "start": "datetime",
    "value": "value",
    "unit": "unit",
    "aggtype": "averaging_time",
    "validity": "validity",
    "verification": "verification",
    "datacapture": "data_capture",
}

TABLE_NAME = "air_quality_measurements"


def get_jdbc_url():
    host = os.environ.get("AIRQUALITY_DB_HOST", "postgres")
    port = os.environ.get("AIRQUALITY_DB_PORT", "5432")
    db = os.environ.get("AIRQUALITY_DB_NAME", "airquality")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_properties():
    return {
        "user": os.environ.get("AIRQUALITY_DB_USER", "airflow"),
        "password": os.environ.get("AIRQUALITY_DB_PASSWORD", "airflow"),
        "driver": "org.postgresql.Driver",
    }


def spark_transform_and_load(**context):
    """
    Airflow PythonOperator callable.

    1. Creates a local-mode SparkSession
    2. Reads all parquet files
    3. Selects and renames columns (the PySpark "modeling")
    4. Adds country_code column
    5. Writes to PostgreSQL via JDBC
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    data_dir = "/opt/airflow/data/bulgaria_verified"
    parquet_path = os.path.join(data_dir, "BG", "*.parquet")

    bg_dir = Path(data_dir) / "BG"
    parquet_files = list(bg_dir.glob("*.parquet")) if bg_dir.exists() else []
    if not parquet_files:
        logger.warning(f"No parquet files found in {bg_dir}")
        return {"status": "no_files", "files_processed": 0, "total_rows": 0}

    logger.info(f"Found {len(parquet_files)} parquet files in {bg_dir}")

    spark = SparkSession.builder \
        .appName("AirQualityTransformLoad") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.3.jar") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()

    try:
        logger.info(f"Reading parquet files from {parquet_path}")
        df = spark.read.parquet(parquet_path)

        logger.info(f"Raw columns: {df.columns}")
        raw_count = df.count()
        logger.info(f"Raw row count: {raw_count}")

        # Lowercase all column names
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, col_name.lower())

        # Select and rename columns
        select_exprs = []
        for src_col, tgt_col in COLUMN_MAPPING.items():
            if src_col in [c.lower() for c in df.columns]:
                select_exprs.append(F.col(src_col).alias(tgt_col))
            else:
                logger.warning(f"Column '{src_col}' not found in parquet, skipping")

        df_transformed = df.select(select_exprs)

        # Add country_code
        df_transformed = df_transformed.withColumn("country_code", F.lit("BG"))

        # Drop rows where datetime is null (part of primary key)
        df_transformed = df_transformed.filter(F.col("datetime").isNotNull())

        # Cast pollutant to string
        df_transformed = df_transformed.withColumn(
            "pollutant", F.col("pollutant").cast("string")
        )

        row_count = df_transformed.count()
        logger.info(f"Transformed row count: {row_count}")

        # Write to PostgreSQL via JDBC (overwrite = full refresh each run)
        jdbc_url = get_jdbc_url()
        jdbc_props = get_jdbc_properties()

        logger.info(f"Writing {row_count} rows to PostgreSQL table '{TABLE_NAME}'")

        df_transformed.write \
            .mode("overwrite") \
            .jdbc(url=jdbc_url, table=TABLE_NAME, properties=jdbc_props)

        logger.info(f"Successfully wrote {row_count} rows to {TABLE_NAME}")

        result = {
            "status": "success",
            "files_processed": len(parquet_files),
            "total_rows": row_count,
        }
        context["task_instance"].xcom_push(key="load_stats", value=result)
        return result

    finally:
        spark.stop()
        logger.info("SparkSession stopped")
