from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id='name_age_sentence_dag_v2',
    description='A DAG that constructs a sentence using name and age.',
    # schedule='* * * * *',  
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=True,
    tags=["example", "taskflow"]
)
def name_age_sentence_dag():

    @task
    def get_name():
        return "John"

    @task
    def get_age():
        return 30

    @task
    def build_sentence(name: str, age: int):
        return f"My name is {name} and I am {age} years old."

    name = get_name()
    age = get_age()
    sentence = build_sentence(name, age)

name_age_sentence_dag()
