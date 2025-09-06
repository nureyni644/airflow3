from airflow.decorators import dag,task
from datetime import datetime

@dag(start_date=datetime(2025,9,3),schedule="@once")
def branching_dag():
    
    @task
    def start():
        print("Start")
    # BRANCHING TASK
    # @task.branch
    # def branch():
    #     import random
    #     choice = random.choice(['path_a','path_b'])
    #     print(f"Branching to {choice}")
    #     # print(f"XCom value: {choice}")
    #     return choice
    
    # @task
    # def path_a():
    #     print("Executing Path A")
    
    # @task
    # def path_b():
    #     print("Executing Path B")
    
    # @task
    # def join():
    #     print("Joining paths")
    
    # start_task = start()
    # branch_task = branch()
    
    # path_a_task = path_a()
    # path_b_task = path_b()
    
    # join_task = join()
    
    # start_task >> branch_task>> path_a_task >> join_task
    # branch_task >> path_b_task >> join_task

    # # SHORT CIRCUITING EXAMPLE
    # @task.short_circuit
    # def check_condition():
    #     import random
    #     condition = random.choice([True, False,False])
    #     print(f"Condition is {condition}")
    #     return condition
    # @task
    # def task_if_true():
    #     print("Condition was True, executing this task.")
    # start_task = start()
    # condition_task = check_condition()
    # true_task = task_if_true()
    # start_task >> condition_task >> true_task

    # THE SPECIALIZED OPERATORS: handle complex scenarios with minimal code.
        # 1 BranchSQLOperator
    
    from airflow.providers.common.sql.operators.sql import BranchSQLOperator
    @task
    def large_data_path():
        print("Handling large data path")
    @task
    def small_data_path():
        print("Handling small data path")

    # large_task = large_data_path()
    # small_task = small_data_path()
    branch_sql = BranchSQLOperator(
        task_id="branch_sql",
        conn_id="postgres_default",
        # sql= "SELECT COUNT(*) > 1000 FROM daily_data WHERE date = CURRENT_DATE",
        sql="SELECT TRUE",
        follow_task_ids_if_true=["large_data_path"],
        follow_task_ids_if_false=["small_data_path"]
    )
    branch_sql>>[large_data_path(),small_data_path()]
        # 2. BranchDayOfWeekOperator
    # from airflow.prividers.standard.operators.weekday import BranchDayOfWeekOperator

    # weekend_branch = BranchDayOfWeekOperator(
    #     task_id="is_weekend",
    #     week_day={5,6},
    #     follow_task_ids_if_true=["weekend_task"],
    #     follow_task_ids_if_false=["weekday_task"]
    # )
    
        # 3. BranchDatetimeOperator
    from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
    from datetime import datetime, time, timedelta

    night_branch = BranchDateTimeOperator(
        task_id="is_night_time",
        target_lower = time(22,0), # 10 PM
        target_upper = time(6,0),  # 6 AM
        follow_task_ids_if_true=["night_task"],
        follow_task_ids_if_false=["day_task"],
    )
branching_dag()