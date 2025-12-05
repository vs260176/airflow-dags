# Minimal test DAG
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='postgres_import_test_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_query = PostgresOperator(
        task_id="test_connection",
        postgres_conn_id="my_postgres_local", # Убедитесь, что Connection ID правильный
        sql="SELECT 1;"
    )
