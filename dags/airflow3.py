from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from airflow.operators.bash import BashOperator
import random
with DAG(dag_id="airflow3", start_date=datetime(2025,9,3), schedule="@daily", catchup=False) as dag:

    @task
    def get_file():
        return [f"file_{nb}" for nb in range(random.randint(3,5))]

    @task
    def download_files(folder:str,file:str):
        print(f"Downloading file {folder}/{file}")
        path = f"{folder}/{file}"
        return path
    
    files = download_files.partial(folder="/usr/local").expand(file=get_file())

    print_files = BashOperator(
        task_id="print_files",
        bash_command="echo '{{ti.xcom_pull(dag_id='airflow3', task_ids='download_files', key='return_value') | list}}'"
    )

    files >> print_files