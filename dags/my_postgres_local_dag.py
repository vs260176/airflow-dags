from datetime import datetime
from airflow.decorators import dag, task

# ЭТОТ ИМПОРТ УДАЛЕН ИЗ ВЕРХНЕГО УРОВНЯ:
# from airflow.providers.postgres.operators.postgres import PostgresOperator 

POSTGRES_CONN_ID = 'my_postgres_local' 

@dag(
    dag_id='process_hourly_db_partition_dag_v2',
    start_date=datetime(2023, 11, 1),
    schedule='@hourly',
    catchup=False,
)
def process_hourly_db_partition_dag_v2():

    # Задача 1: Создать целевую таблицу, если ее нет
    @task
    def create_summary_table_task():
        # Импорт Хука происходит внутри функции, когда задача уже запущена
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        sql_create = """
            CREATE TABLE IF NOT EXISTS hourly_summary (
                interval_start TIMESTAMP PRIMARY KEY,
                total_events INT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        pg_hook.run(sql_create)
        print("Summary table ensured to exist.")


    # Задача 2: Обработать данные за конкретный интервал и вставить их
    @task
    def aggregate_and_insert_data(**kwargs):
        # Импорт Хука происходит внутри функции, когда задача уже запущена
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        ds_start = kwargs['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
        ds_end = kwargs['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')

        sql_query = f"""
            INSERT INTO hourly_summary (interval_start, total_events)
            SELECT 
                '{ds_start}'::timestamp AS interval_start, 
                COUNT(*) AS total_events
            FROM 
                raw_events
            WHERE 
                event_time >= '{ds_start}'::timestamp AND 
                event_time < '{ds_end}'::timestamp
        """
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql_query)
        print("Data aggregation complete.")


    # Определение последовательности выполнения задач
    create_summary_table_task() >> aggregate_and_insert_data()

# Регистрация DAG
process_hourly_db_partition_dag_v2()
