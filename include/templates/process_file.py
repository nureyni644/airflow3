from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(dag_id="process_DAG_ID_HOLDER", start_date=datetime(2025,9,3),
          schedule="SCHEDULE_INTERVAL_HOLDER", catchup=False) as dag:
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
    
    send_email(process(extract("INPUT_HOLDER")))