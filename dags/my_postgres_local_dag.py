from airflow.decorators import dag, task
from datetime import datetime

# Мы будем использовать PostgresHook внутри задачи, а не импортировать оператор сверху
# from airflow.providers.postgres.operators.postgres import PostgresOperator # Закомментировано

POSTGRES_CONN_ID = 'my_postgres_local'

@dag(
    dag_id='postgres_import_workaround_dag',
    start_date=datetime(2023, 1, 1),
    # schedule_interval=None,  <-- Старый параметр
    schedule=None,           # <-- Новый, правильный параметр
    catchup=False,
)
def process_hourly_db_partition_dag_workaround():

    @task
    def run_test_query_task():
        # Импорт Хука происходит внутри функции, когда задача уже запущена
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql_query = "SELECT 1 as test_result;"
        
        # Выполнение запроса
        result = pg_hook.get_records(sql_query)
        print(f"Query executed successfully. Result: {result}")

    # Определение последовательности выполнения задач
    run_test_query_task()

# Регистрация DAG
process_hourly_db_partition_dag_workaround()
