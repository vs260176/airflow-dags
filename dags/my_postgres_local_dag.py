# Импортируем класс datetime и timedelta из стандартной библиотеки Python 
# для работы с датами и временем.
from datetime import datetime, timedelta
# Импортируем декораторы dag и task из airflow.decorators. Это современный 
# способ определения DAG и задач.
from airflow.decorators import dag, task

# ЭТОТ ИМПОРТ УДАЛЕН ИЗ ВЕРХНЕГО УРОВНЯ:
# from airflow.providers.postgres.operators.postgres import PostgresOperator 
# Закомментированная строка, которая изначально могла присутствовать, но была 
# удалена. 
# Это указывает на изменение подхода: вместо использования операторов верхнего 
# уровня (Top-Level Operators) 
# для задач используются Python-функции с декоратором @task и Хуки (Hooks) 
# внутри них.

# Определяем константу для хранения идентификатора подключения к базе данных 
# Postgres.
# Этот идентификатор должен быть предварительно настроен в веб-интерфейсе 
# Airflow.
POSTGRES_CONN_ID = 'my_postgres_local' 


# Применяем декоратор @dag к функции для регистрации ее в Airflow как DAG.
@dag(
    # Уникальный идентификатор (название) DAG.
    dag_id='process_hourly_db_partition_dag_v2',
    # Дата начала выполнения DAG. Airflow начнет планировать запуски с этого 
    # момента.
    start_date=datetime(2023, 11, 1),
    # Расписание выполнения. '@hourly' означает запуск каждый час.
    schedule='@hourly',
    # Параметр catchup=False отключает "наверстывание" пропущенных запусков 
    # между start_date и текущим моментом.
    catchup=False,
)
# Определяем функцию, которая содержит логику всего DAG. Название функции 
# совпадает с dag_id.
def process_hourly_db_partition_dag_v2():

    # Задача 1: Создать целевую таблицу, если ее нет
    # Применяем декоратор @task к функции, превращая ее в задачу Airflow.
    @task
    def create_summary_table_task():
        # Импорт Хука происходит внутри функции, когда задача уже запущена
        # Ленивый импорт: модуль загружается в память только при вызове 
        # функции (во время выполнения задачи).
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        # Создаем экземпляр PostgresHook, используя заданный ранее 
        # connection ID.
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Определяем SQL-запрос в виде многострочной строки.
        sql_create = """
            CREATE TABLE IF NOT EXISTS hourly_summary (
                interval_start TIMESTAMP PRIMARY KEY,
                interval_end TIMESTAMP NOT NULL,
                total_events INT,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        # Выполняем SQL-запрос с помощью Хука.
        pg_hook.run(sql_create)
        # Выводим сообщение в логи задачи Airflow.
        print("Summary table ensured to exist.")

    # Задача 2: Обработать данные за конкретный интервал и вставить их
    # Применяем декоратор @task, превращая функцию в задачу Airflow.
    # **kwargs используется для доступа к контекстным переменным Airflow 
    # (например, data_interval_start).

    @task
    def aggregate_and_insert_data(
        **kwargs
    ):
        # Импорт Хука происходит внутри функции, когда задача уже запущена
        # Ленивый импорт для оптимизации загрузки DAG файла.
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        end = kwargs["data_interval_end"]
        start = end - timedelta(hours=1)

        ds_start = start.strftime('%Y-%m-%d %H:%M:%S')
        ds_end = end.strftime('%Y-%m-%d %H:%M:%S')
        
        # Получаем время начала интервала выполнения DAG из контекста Airflow 
        # и форматируем его в строку SQL.
        ds_start = start.strftime('%Y-%m-%d %H:%M:%S')
        # Получаем время окончания интервала выполнения DAG из контекста 
        # Airflow и форматируем его в строку SQL.
        ds_end = end.strftime('%Y-%m-%d %H:%M:%S')

        # Определяем SQL-запрос для агрегации и вставки данных.
        # Используем f-строку Python для динамической вставки переменных 
        # ds_start и ds_end.
        sql_query = (
            f"INSERT INTO hourly_summary "
            f"(interval_start, interval_end, total_events)\n"
            f"SELECT \n"
            f"    '{ds_start}'::timestamp AS interval_start, \n"
            f"    '{ds_end}'::timestamp AS interval_end,\n"
            f"    COUNT(*) AS total_events\n"
            f"FROM \n"
            f"    raw_events\n"
            f"WHERE \n"
            f"    event_time >= '{ds_start}'::timestamp AND \n"
            f"    event_time < '{ds_end}'::timestamp\n"
        )
        
        # Создаем экземпляр PostgresHook.
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # Выполняем SQL-запрос.
        pg_hook.run(sql_query)
        # Выводим сообщение в логи.
        print("Data aggregation complete.")

    # Определение последовательности выполнения задач
    # Определяем зависимости между задачами с помощью оператора >> 
    # (task_1 >> task_2),
    # что означает: task_1 должна успешно завершиться перед запуском task_2.
    create_summary_table_task() >> aggregate_and_insert_data()

# Вызываем функцию-фабрику DAG, чтобы зарегистрировать его в глобальном 
# пространстве имен Airflow.

process_hourly_db_partition_dag_v2()

