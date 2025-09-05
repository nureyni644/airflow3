import os
import shutil
import fileinput
import json
TEMPLATE_FILE =  "include/templates/process_file.py"

for filename in os.listdir("include/data/"):
    if  filename.endswith(".json"):

        config = json.load(open(os.path.join("include/data",filename)))
        new_dag_file = os.path.join("dags",f"process_{config['dag_id']}.py")
        shutil.copyfile(TEMPLATE_FILE,new_dag_file)
        for line in fileinput.input(new_dag_file,inplace=True):
            line = line.replace("DAG_ID_HOLDER",config['dag_id'])
            line = line.replace("SCHEDULE_INTERVAL_HOLDER",config['schedule_interval'])
            line = line.replace("INPUT_HOLDER",config['input'])
            print(line,end="")