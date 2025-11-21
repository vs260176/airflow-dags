from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to run
def hello_airflow():
    print("Hello Airflow!")

# Define the DAG
with DAG(
    dag_id="dag_hello_airflow_v",
    start_date=datetime(2025, 11, 20),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    task1 = PythonOperator(
        task_id="task_hello_airflow_v2",
        python_callable=hello_airflow
    )

    task1
