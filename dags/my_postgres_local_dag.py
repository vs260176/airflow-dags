from datetime import datetime
from airflow.decorators import dag, task
# Теперь этот импорт должен работать без ModuleNotFoundError!
from airflow.providers.postgres.operators.postgres import PostgresOperator 

# Указываем Airflow, какой Connection ID использовать по умолчанию для PostgresOperator и Hook
POSTGRES_CONN_ID = 'my_postgres_local' 

@dag(
    dag_id='process_hourly_db_partition_dag',
    start_date=datetime(2023, 11, 1),
    schedule='@hourly',  # Запускается каждый час, используем современный 'schedule'
    catchup=False, # Не запускать пропущенные интервалы с 1 ноября
)
def process_hourly_db_partition_dag():

    # 1. Задача: Создать целевую таблицу, если ее нет 
    # (Используем PostgresOperator напрямую)
    create_table_if_not_exists = PostgresOperator(
        task_id="create_summary_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
            CREATE TABLE IF NOT EXISTS hourly_summary (
                interval_start TIMESTAMP PRIMARY KEY,
                total_events INT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. Задача: Обработать данные за конкретный интервал и вставить их
    @task
    def aggregate_and_insert_data(**kwargs):
        # Получаем временные метки интервала из контекста Airflow
        # Форматируем их в строки, понятные PostgreSQL
        ds_start = kwargs['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
        ds_end = kwargs['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')

        print(f"Processing data between {ds_start} and {ds_end}")

        # SQL-запрос, который ВЫБИРАЕТ данные только из нужной ПАРТИЦИИ ВРЕМЕНИ
        # Примечание: Убедитесь, что в вашей таблице raw_events есть столбец event_time
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
            -- ON CONFLICT (interval_start) DO UPDATE SET total_events = EXCLUDED.total_events; 
            -- Раскомментируйте, если хотите сделать задачу полностью идемпотентной при перезапуске
        """
        
        # Используем хук (Hook) Airflow для выполнения SQL внутри Python-задачи
        # Импорт здесь - это лучшая практика (нет кода верхнего уровня)
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        pg_hook.run(sql_query)

        print("Data aggregation complete.")


    # Определение последовательности выполнения задач
    # create_table_if_not_exists выполняется первой, затем aggregate_and_insert_data
    create_table_if_not_exists >> aggregate_and_insert_data()

# Регистрация DAG
process_hourly_db_partition_dag()
