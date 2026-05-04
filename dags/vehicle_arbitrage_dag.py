from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'avirukth',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'australian_auto_arbitrage_pipeline',
    default_args=default_args,
    description='Automated pipeline from Kaggle to Gold Layer',
    # Daily pipeline execution
    schedule_interval='@daily', 
    start_date=datetime(2026, 5, 1),
    catchup=False,
    tags=['data_engineering', 'australia_auto'],
) as dag:

    # Ingest Kaggle data to BigQuery bronze layer
    task_ingest_raw = BashOperator(
        task_id='ingest_kaggle_to_bq',
        bash_command='python /opt/airflow/scripts/ingest_kaggle_to_bq.py',
    )

    # Execute dbt models for silver and gold layers
    # dbt build handles materialization and data tests
    task_dbt_build = BashOperator(
        task_id='dbt_build_marts',
        bash_command='cd /opt/airflow/auto_arbitrage && dbt build --profiles-dir .',
    )
    # Define task dependencies
    task_ingest_raw >> task_dbt_build