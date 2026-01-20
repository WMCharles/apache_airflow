from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'masinde',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 10  # 10 seconds
}

# Define the DAG
with DAG(
    dag_id="brian_dag",
    default_args=default_args,
    start_date=datetime(2026, 1, 10),
    schedule='@daily',  # manual trigger
    catchup=False,
    tags=["example"]
) as dag:

    # BashOperator task
    echo_task = BashOperator(
        task_id="echo_masinde",
        bash_command="echo brian masinde"
    )
