"""
Air Quality Data Pipeline for Sofia, Bulgaria

This DAG downloads verified PM2.5 and PM10 air quality measurements
from the European Air Quality Portal for Bulgaria on a daily schedule,
then loads the data into PostgreSQL.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import pendulum

# Import task functions from tasks module
from tasks.load_to_postgres import load_data_task

# Default arguments for the DAG
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
    """
    Download verified PM2.5 and PM10 air quality data for Bulgaria
    from the European Air Quality Portal.
    """
    import airbase
    import os

    logger = logging.getLogger(__name__)

    try:
        # Ensure data directory exists
        data_dir = "./data/bulgaria_verified"
        os.makedirs(data_dir, exist_ok=True)
        logger.info(f"Data directory: {data_dir}")

        logger.info("Initializing Airbase client...")
        client = airbase.AirbaseClient()

        logger.info("Creating download request for Bulgaria PM2.5 and PM10 data...")
        request = client.request(
            "Verified",
            "BG",
            poll=["PM2.5", "PM10"]
        )

        logger.info("Request created successfully!")
        logger.info(f"Starting download to {data_dir}/")

        # Download the data
        request.download(
            dir=data_dir,
            skip_existing=True
        )

        logger.info(" Download complete! Air quality data has been updated.")
        return "Download successful"

    except Exception as e:
        logger.error(f"Failed to download air quality data: {str(e)}")
        raise


# Define the DAG
with DAG(
    dag_id='bulgaria_air_quality_pipeline',
    default_args=default_args,
    description='Daily download of verified PM2.5 and PM10 air quality data for Bulgaria',
    schedule='@daily',  # Run once per day at midnight
    start_date=pendulum.yesterday('UTC'),
    catchup=False,  # Don't run for past dates
    tags=['air-quality', 'bulgaria', 'pm25', 'pm10', 'environment'],
) as dag:

    # Task 1: Download air quality data
    download_task = PythonOperator(
        task_id='download_bulgaria_air_quality',
        python_callable=download_air_quality_data,
        doc_md="""
        ### Download Bulgaria Air Quality Data

        This task downloads verified PM2.5 and PM10 particulate matter measurements
        for Bulgaria from the European Air Quality Portal.

        **Data Source:** European Environment Agency (EEA)
        **Pollutants:** PM2.5, PM10
        **Country:** Bulgaria (BG)
        **Data Type:** Verified measurements
        **Output Format:** Parquet files
        **Output Directory:** /opt/airflow/data/bulgaria_verified/

        The task will:
        1. Connect to the Airbase API
        2. Request verified data for Bulgaria
        3. Download new/updated files
        4. Skip existing files to avoid redundant downloads
        """,
    )

    # Task 2: Load data to PostgreSQL
    load_to_db_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data_task,
        doc_md="""
        ### Load Air Quality Data to PostgreSQL

        This task reads the downloaded parquet files and loads them into PostgreSQL.

        **Database:** External PostgreSQL container (postgres-sql-workshop)
        **Table:** air_quality_measurements
        **Connection:** Configured via environment variables

        The task will:
        1. Connect to PostgreSQL database
        2. Create table if it doesn't exist
        3. Read all parquet files from data directory
        4. Load data into database
        5. Report statistics on loaded data
        """,
    )

    # Task dependencies: download first, then load to database
    download_task >> load_to_db_task
