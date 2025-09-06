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

    # NOTE DE COURS
# Résumé
# Leçon 1 : Introduction à la ramification
# La ramification permet aux flux de travail de prendre des décisions intelligentes et de choisir des chemins d'exécution en fonction des conditions d'exécution
# Utilisez la ramification pour les flux de travail de validation des données, le traitement spécifique à l'environnement, les décisions basées sur le temps et l'optimisation des ressources
# La ramification élimine le gaspillage de ressources et le traitement unique et inflexible
# Les tâches ignorées sont marquées comme « ignorées » (non échouées) lorsque les branches ne sont pas sélectionnées
# Leçon 2 : Le décorateur @task.branch
# Méthode de ramification la plus flexible : accepte toute fonction Python qui renvoie des ID de tâche.
# La fonction doit renvoyer des ID de tâche valides qui sont directement en aval de la tâche de ramification
# Peut renvoyer un ID de tâche unique (chaîne), une liste d'ID de tâche ou Aucun (ignorer tout en aval)
# Idéal pour une logique personnalisée complexe, des conditions multiples et une prise de décision dynamique
# Leçon 3 : Le décorateur @task.short_circuit
# Décisions simples « continuer ou arrêter » utilisant des valeurs de retour booléennes (Vrai/Faux)
# Vrai = continuer avec toutes les tâches en aval, Faux = ignorer toutes les tâches en aval
# Idéal pour les points de contrôle de validation, les contrôles de disponibilité des données et les conditions temporelles
# À utiliser lorsque vous devez prendre des décisions d'aller ou de ne pas aller plutôt que de choisir entre différents chemins
# Leçon 4 : Opérateurs de succursales spécialisées
# BranchSQLOperator : décisions basées sur une base de données utilisant les résultats des requêtes SQL
# BranchDayOfWeekOperator : branche basée sur le jour actuel de la semaine
# BranchDateTimeOperator : Branche basée sur des fenêtres temporelles (heures ouvrables, fenêtres de maintenance)
# Toutes les utilisations follow_task_ids_if_trueet follow_task_ids_if_falseparamètres
# Leçon 5 : Règles de déclenchement et bonnes pratiques
# La règle de déclenchement par défaut « all_success » provoque des problèmes de ramification (ignoré ≠ réussi)
# Utilisez « none_failed_min_one_success » pour joindre les tâches après la ramification
# Gardez les fonctions de ramification légères : la logique complexe appartient aux tâches en amont
# Testez tous les chemins de branche et planifiez les règles de déclenchement à l'avance
# Conception pour une gestion élégante des dépendances ignorée
branching_dag()