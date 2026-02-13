"""
Air Quality Data Pipeline for Sofia, Bulgaria

DAG: download parquet -> PySpark transform + load to PostgreSQL -> create indexes
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pendulum

from tasks.spark_transform_load import spark_transform_and_load

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}


def download_air_quality_data():
    """Download verified PM2.5 and PM10 air quality data for Bulgaria."""
    import airbase
    import os

    data_dir = "/opt/airflow/data/bulgaria_verified"
    os.makedirs(data_dir, exist_ok=True)
    logger.info(f"Data directory: {data_dir}")

    logger.info("Initializing Airbase client...")
    client = airbase.AirbaseClient()

    logger.info("Creating download request for Bulgaria PM2.5 and PM10 data...")
    request = client.request("Verified", "BG", poll=["PM2.5", "PM10"])

    logger.info(f"Starting download to {data_dir}/")
    request.download(dir=data_dir, skip_existing=True)

    logger.info("Download complete!")
    return "Download successful"


def create_indexes():
    """Recreate indexes after Spark overwrite drops them."""
    from sqlalchemy import create_engine, text
    import os

    host = os.environ.get("AIRQUALITY_DB_HOST", "postgres")
    port = os.environ.get("AIRQUALITY_DB_PORT", "5432")
    db = os.environ.get("AIRQUALITY_DB_NAME", "airquality")
    user = os.environ.get("AIRQUALITY_DB_USER", "airflow")
    password = os.environ.get("AIRQUALITY_DB_PASSWORD", "airflow")

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(conn_string)

    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_datetime ON air_quality_measurements(datetime)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_station ON air_quality_measurements(station_code)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_pollutant ON air_quality_measurements(pollutant)"))

    logger.info("Indexes created successfully")


with DAG(
    dag_id='bulgaria_air_quality_pipeline',
    default_args=default_args,
    description='Daily pipeline: download PM2.5/PM10 data, transform with PySpark, load to PostgreSQL',
    schedule='@daily',
    start_date=pendulum.yesterday('UTC'),
    catchup=False,
    tags=['air-quality', 'bulgaria', 'pm25', 'pm10', 'pyspark'],
) as dag:

    download_task = PythonOperator(
        task_id='download_bulgaria_air_quality',
        python_callable=download_air_quality_data,
    )

    spark_transform_load_task = PythonOperator(
        task_id='spark_transform_and_load',
        python_callable=spark_transform_and_load,
    )

    create_indexes_task = PythonOperator(
        task_id='create_indexes',
        python_callable=create_indexes,
    )

    download_task >> spark_transform_load_task >> create_indexes_task
