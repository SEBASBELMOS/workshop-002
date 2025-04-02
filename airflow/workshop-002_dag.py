from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from airflow.tasks.etl import (
    extract_spotify,
    extract_grammys,
    transform_spotify,
    transform_grammys,
    merge_data,
    load_data,
    store_data
)

default_args = {
    'owner': 'sebasbelmos',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='workshop_002_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Spotify and Grammy Awards data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 29),
    catchup=False,
    tags=['etl', 'spotify', 'grammys']
) as dag:

    extract_spotify_task = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_spotify,
        provide_context=True,
    )

    extract_grammys_task = PythonOperator(
        task_id='extract_grammys',
        python_callable=extract_grammys,
        provide_context=True,
    )

    transform_spotify_task = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_spotify,
        op_args=['{{ ti.xcom_pull(task_ids="extract_spotify") }}'],
        provide_context=True,
    )

    transform_grammys_task = PythonOperator(
        task_id='transform_grammys',
        python_callable=transform_grammys,
        op_args=['{{ ti.xcom_pull(task_ids="extract_grammys") }}'],
        provide_context=True,
    )

    merge_data_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        op_args=[
            '{{ ti.xcom_pull(task_ids="transform_spotify") }}',
            '{{ ti.xcom_pull(task_ids="transform_grammys") }}'
        ],
        provide_context=True,
    )

    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_args=['{{ ti.xcom_pull(task_ids="merge_data") }}'],
        provide_context=True,
    )

    store_data_task = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        op_args=['{{ ti.xcom_pull(task_ids="merge_data") }}'],
        provide_context=True,
    )

    (
        [extract_spotify_task, extract_grammys_task]
        >> [transform_spotify_task, transform_grammys_task]
        >> merge_data_task
        >> load_data_task
        >> store_data_task
    )