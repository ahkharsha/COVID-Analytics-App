from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='covid_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['covid', 'pyspark'],
) as dag:

    run_pyspark = BashOperator(
        task_id='run_pyspark_job',
        bash_command='python /opt/airflow/scripts/process_covid_data.py',
    )

    run_pyspark
