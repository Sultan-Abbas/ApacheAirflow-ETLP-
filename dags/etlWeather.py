from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

citys = ['Lodhran', 'Lahore', 'Dubai', 'Islamabad']

@dag(
    dag_id='WeatherApi_ETL',
    start_date=datetime(2025, 12, 30),
    schedule='@hourly',
    catchup=False,
    default_args={
        'retries': 3,
        "retry_delay": timedelta(minutes=5)
    }
)
def dag_task():
  
    @task
    def extract_data():
        weather_data = []
        conn = BaseHook.get_connection("Weather_API")
        api_key = conn.password
        
        hook = HttpHook(method="GET", http_conn_id="Weather_API")
        
        for city in citys:
            endpoint = f"/data/2.5/weather?q={city}&appid={api_key}&units=metric"
            response = hook.run(endpoint)
            data = response.json()
            weather_data.append(data)
            
        return weather_data

    @task 
    def Load(weather_data):
        hook = PostgresHook(postgres_conn_id="Postgres")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(100),
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            description VARCHAR(100),
            humidity FLOAT,      
            pressure FLOAT,      
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        data = weather_data
        for i in data:
            cursor.execute(""" 
                INSERT INTO weather_data (city, temperature, windspeed, winddirection, humidity, pressure, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    i["name"],
                    i['main']['temp'],
                    i['wind']['speed'],
                    i['wind']['deg'],
                    i['main']['humidity'],
                    i['main']['pressure'],
                    i['weather'][0]['description']
                )
            )
        
        conn.commit()
        cursor.close()
        conn.close()

    data = extract_data()
    Load(data)

dag = dag_task()