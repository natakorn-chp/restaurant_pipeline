from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

#Define default arguments
default_args = {
    'owner': 'de_user',
    'start_date': datetime(2024, 1, 7),
    'retries': 1,
}

# Instantiate your DAG
dag = DAG ('restaurant_ingestion'
    , default_args=default_args
    , schedule_interval="@daily"
)

task_1 = BashOperator(
    task_id='ingest_postgres', 
    bash_command='python3 /home/user/de_project/restaurant_ingest_postgres.py',
    dag=dag
)

task_2 = BashOperator(
    task_id='ingest_hive', 
    bash_command='python3 /home/user/de_project/restaurant_ingest_hive.py',
    dag=dag
)

# Set task dependencies
task_1 >> task_2
