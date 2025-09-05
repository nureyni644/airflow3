from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(dag_id="process_file_a", start_date=datetime(2025,9,3),
          schedule="@daily", catchup=False) as dag:
    @task
    def extract(filename:str):
        return filename

    @task
    def process(filename:str):
        # print(f"Processing {filename}")
        return filename
    @task
    def send_email(filename:str):
        print(f"Sending email with {filename}")
        return filename
    
    send_email(process(extract("file_a.csv")))