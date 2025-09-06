from airflow.decorators import dag, task, task_group

from datetime import datetime



@dag(start_date=datetime(2025,9,3),schedule="@once")
def task_group_dag():
    
    @task
    def t1():
        print("Task 1")
    
    
    @task
    def t2():
        print("Task 2")
    
    @task_group
    def etl_group():
        @task
        def extract():
            print("extract")
        
        @task
        def transform():
            print("Transform")
        @task
        def load():
            print("Load")
        
        extract() >> transform() >> load()
    
    t1() >> etl_group()
    etl_group() >> t2()

task_group_dag()