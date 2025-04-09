from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p"
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

logger.info(f"Current working directory: {os.getcwd()}")
logger.info(f"Initial Python path: {sys.path}")

load_dotenv("/opt/airflow/.env")

sys.path.append('/opt/airflow/dags')
sys.path.append('/opt/airflow/src')
logger.info(f"Updated Python path: {sys.path}")

from tasks.etl import (
    create_schemas,
    load_grammys_csv_to_db,
    extract_spotify,
    extract_grammys,
    transform_spotify,
    transform_grammys,
    merge_data,
    load_data,
    store_data,
    extract_spotify_api,
    transform_spotify_api
)

def execute_extract_grammys():
    from src.extract.grammys_extract import extract_grammys_data
    try:
        logger.info("Starting Grammy data extraction")
        dataframes = extract_grammys_data()
        logger.info(f"Grammy data extraction completed. Keys: {list(dataframes.keys())}")
        
        if 'grammy_awards' not in dataframes:
            raise ValueError("Expected 'grammy_awards' table not found in extracted data")
        
        df = dataframes['grammy_awards']
        logger.info(f"Grammy data type: {type(df)}")
        return df.to_json(orient="records")
    except Exception as e:
        logger.error(f"Error extracting Grammy data: {e}", exc_info=True)
        raise

with DAG(
    dag_id='workshop_002_etl_pipeline',
    description='ETL pipeline for Spotify and Grammy Awards data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 4, 9),
    catchup=False,
    tags=['etl', 'spotify', 'grammys'],
    is_paused_upon_creation=False,
) as dag:

    create_schemas_task = PythonOperator(
        task_id='create_schemas',
        python_callable=create_schemas,
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    load_grammys_csv_task = PythonOperator(
        task_id='load_grammys_csv',
        python_callable=load_grammys_csv_to_db,
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    extract_spotify_task = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_spotify,
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    extract_spotify_api_task = PythonOperator(
        task_id='extract_spotify_api',
        python_callable=extract_spotify_api,
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    extract_grammys_task = PythonOperator(
        task_id='extract_grammys',
        python_callable=execute_extract_grammys,
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    transform_spotify_task = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_spotify,
        op_args=['{{ ti.xcom_pull(task_ids="extract_spotify") }}'],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    transform_spotify_api_task = PythonOperator(
        task_id='transform_spotify_api',
        python_callable=transform_spotify_api,
        op_args=['{{ ti.xcom_pull(task_ids="extract_spotify_api") }}'],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    transform_grammys_task = PythonOperator(
        task_id='transform_grammys',
        python_callable=transform_grammys,
        op_args=['{{ ti.xcom_pull(task_ids="extract_grammys") }}'],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    merge_data_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        op_args=[
            '{{ ti.xcom_pull(task_ids="transform_spotify") }}',
            '{{ ti.xcom_pull(task_ids="transform_grammys") }}',
            '{{ ti.xcom_pull(task_ids="transform_spotify_api") }}'
        ],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=['{{ ti.xcom_pull(task_ids="merge_data") }}'],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    store_data_task = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        op_args=['{{ ti.xcom_pull(task_ids="load_data") }}'],
        provide_context=True,
        owner='sebasbelmos',
        depends_on_past=False,
        email_on_failure=False,
        email_on_retry=False,
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    create_schemas_task >> load_grammys_csv_task
    create_schemas_task >> extract_spotify_task
    create_schemas_task >> extract_spotify_api_task
    load_grammys_csv_task >> extract_grammys_task
    extract_spotify_task >> transform_spotify_task
    extract_spotify_api_task >> transform_spotify_api_task
    extract_grammys_task >> transform_grammys_task
    [transform_spotify_task, transform_grammys_task, transform_spotify_api_task] >> merge_data_task
    merge_data_task >> load_data_task
    load_data_task >> store_data_task