from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("BOOOOOTCAMP")

dag = DAG(
    'BOOTCAMP',
    description='LALALALLALALALALLALA',
    schedule_interval='@once',
    start_date=datetime(2025, 6, 10),
    catchup=False
)

bootcamp = PythonOperator(
    task_id='bootcamp',
    python_callable=hello_world,
    dag=dag
)
