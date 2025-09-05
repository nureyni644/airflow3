from airflow import DAG
from airflow.decorators import task

from datetime import datetime

def create_dag(filename:str):
    with DAG(dag_id=filename, start_date=datetime(2025,9,3), schedule="@daily", catchup=False) as dag:

        @task
        def extract(file:str):
            return filename

        @task
        def process(filename:str):
            # print(f"Processing {filename}")
            return filename
        @task
        def send_email(filename:str):
            print(f"Sending email with {filename}")
            return filename
        send_email(process(extract(filename)))
    return dag

for filename in ["file1.csv","file2.csv"]:
    globals()[f"dag_{filename.replace(".","_")}"] = create_dag(filename)