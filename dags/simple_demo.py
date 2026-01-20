import random
from datetime import datetime
from airflow.decorators import dag, task
# from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 10  # 10 seconds
}

# 1. Define the DAG
@dag(
    dag_id='simple_demo_pipeline',
    description='A simple demo pipeline that generates, processes, and reports a number.',
    default_args=default_args,
    schedule='* * * * *',  # This is the Cron expression for "every minute"
    start_date=datetime(2026, 1, 1),
    catchup=False,               # Don't run for past dates
    tags=['learning', 'basics']
)
def my_simple_pipeline():

    # 2. Define Task 1: Generate Data
    @task()
    def get_number():
        num = random.randint(1, 100)
        print(f"Generated number: {num}")
        return num

    # 3. Define Task 2: Process Data
    @task()
    def double_number(value):
        doubled = value * 2
        print(f"Doubled value: {doubled}")
        return doubled

    # 4. Define Task 3: Final Output
    @task()
    def report_result(final_value):
        status = "High" if final_value > 100 else "Low"
        print(f"The result {final_value} is {status}!")

    # 5. Build the flow (Dependencies)
    # This automatically handles passing the data between tasks
    val = get_number()
    doubled_val = double_number(val)
    report_result(doubled_val)

# 6. Instantiate the DAG
my_simple_pipeline()