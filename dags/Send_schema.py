from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess
from airflow.utils.dates import days_ago
def execute_the_schema():
    path=r"/opt/airflow/schema/SchemaReg.py"
    subprocess.run(['python', path], check=True)

default_args = {
    'owner': 'YNS_Bousetta',
    'start_date': days_ago(0),
    'retries': 1,
}

# Instantiate the DAG
dag = DAG(
    'Send_schema_to_instance',
    default_args=default_args,
    description='A simple DAG to run a Python script',
    schedule_interval='@daily',
)

# Define the task
run_script_task = PythonOperator(
    task_id='Schema',
    python_callable=execute_the_schema,
    dag=dag,
)

# Set the task in the DAG
run_script_task