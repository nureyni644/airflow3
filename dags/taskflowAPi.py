from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.python import get_current_context
@dag(start_date=datetime(2025,9,3),schedule="@once")
def decorator_dag():

    @task(retries=3)
    def start(**context):
        print(f"context dagRun: {context}")

        print(f"\n context dagRun: {get_current_context()}")
        return "success"
    
    @task.branch
    def choose_task(next_task:str):
        return next_task
    
    @task
    def success(retries=1):
        print("Success Task")
    
    @task
    def failure():
        print("Fail Task")
    
    choose_task(start()) >> [success(), failure()]

decorator_dag()