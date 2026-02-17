from airflow.sdk import dag ,task
from datetime import datetime

@dag(
    dag_id = "Taskapi",
    start_date = datetime(2024,3,4),
    schedule = None,
    catchup = False
    
)

def dag_task():
    @task
    def extract():
        print("Extracing")
        return {"value":45}
    @task
    def transform(a):
        print("Transforming")
        return a["value"]*2 
    @task
    def load(a):
        print(f"The Data is {a}")
        
    a = extract()
    b = transform(a)
    load(b)
    
dag = dag_task()