from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Task functions
def start_task():
    print("Start task executed")

def middle_task():
    print("Middle task executed")

def end_task():
    print("End task executed")

# Define DAG
with DAG(
    dag_id="multi_task_dag2",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Define tasks
    t1 = PythonOperator(
        task_id="start",
        python_callable=start_task
    )

    t2 = PythonOperator(
        task_id="middle",
        python_callable=middle_task
    )

    t3 = PythonOperator(
        task_id="end",
        python_callable=end_task
    )

    # Define dependencies
    t1 >> [t2 , t3]  
