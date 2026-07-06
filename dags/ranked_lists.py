from datetime import datetime, timedelta
import re
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Константы
BASE_URL = "https://www.kubsu.ru"
START_URL = f"{BASE_URL}/ru/abitlist"
BUCKET_NAME = "ranked-lists"
AWS_CONN_ID = "minio_logs"
POSTGRES_CONN_ID = "postgre_k8s"

TARGET_FACULTIES = [
    "Факультет компьютерных технологий и прикладной математики",
    "Экономический факультет"
]

# Набор заголовков для мимикрии под обычный браузер Chrome
BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kubsu_abitlist_to_postgres',
    default_args=default_args,
    description='Парсинг списков абитуриентов КубГУ и загрузка в MinIO S3',
    schedule='@daily',
    catchup=False,
    tags=['kubsu', 'parser', 's3'],
) as dag:

    @task
    def extract_and_upload_to_s3():
        # Отключаем IPv6, так как files.kubsu.ru может не иметь IPv6 маршрутов в вашем K8s
        requests.packages.urllib3.util.connection.HAS_IPV6 = False
        requests.packages.urllib3.disable_warnings()

        import sys
        print(f"--- НАСТОЯЩАЯ ВЕРСИЯ PYTHON НА ВОРКЕРЕ: {sys.version} ---")

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

        # Настраиваем стратегию автоматических повторов (Retries)
        from requests.adapters import HTTPAdapter
        from urllib3.util import Retry

        retry_strategy = Retry(
            total=3,                # Количество попыток для каждой страницы
            backoff_factor=2,       # Пауза между попытками увеличивается: 2с -> 4с -> 8с
            status_forcelist=[500, 502, 503, 504], # Перезапускать при серверных ошибках
            raise_on_status=False
        )

        # Создаем сессию и монтируем в нее стратегию ретраев для http и https
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.verify = False  # Игнорируем проблемы с SSL-сертификатами вуза

        print(f"Запрос главной страницы: {START_URL}")
        response = session.get(START_URL, headers=BROWSER_HEADERS, timeout=15) # Уменьшили таймаут до 15с, чтобы ретраи срабатывали быстрее
        response.encoding = 'utf-8'
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        for faculty_name in TARGET_FACULTIES:
            faculty_element = soup.find(string=lambda text: text and faculty_name in text)

            if not faculty_element:
                print(f"Факультет не найден на странице: {faculty_name}")
                continue

            parent_tag = faculty_element.parent
            print(f"Успешно найден факультет: {faculty_name} в теге <{parent_tag.name}>")

            links_list = parent_tag.find_next('ul')
            if not links_list:
                print(f"Не найден список ссылок для факультета: {faculty_name}")
                continue

            links = links_list.find_all('a', href=True)
            print(f"Найдено ссылок для обработки: {len(links)}")

            for link in links:
                link_text = link.get_text(strip=True)
                link_href = link['href']

                if "иностранные граждане и лица без гражданства" in link_text.lower():
                    print(f"Пропущена ссылка (иностранцы): {link_text}")
                    continue

                # Формируем абсолютный URL (обрабатываем и относительные, и абсолютные ссылки)
                if link_href.startswith('/'):
                    full_page_url = BASE_URL + link_href
                elif link_href.startswith('http'):
                    full_page_url = link_href
                else:
                    continue

                try:
                    print(f"Скачивание страницы: {full_page_url} ({link_text})")
                    # Сессия сама сделает до 3 попыток, если files.kubsu.ru выбросит ConnectTimeout
                    page_res = session.get(full_page_url, headers=BROWSER_HEADERS, timeout=15)
                    page_res.encoding = 'utf-8'
                    page_res.raise_for_status()

                    page_soup = BeautifulSoup(page_res.text, 'html.parser')

                    hr_tag = page_soup.find('hr')
                    if hr_tag:
                        for sibling in list(hr_tag.find_next_siblings()):
                            if hasattr(sibling, 'extract'):
                                sibling.extract()
                        hr_tag.extract()

                    final_html = str(page_soup)

                    safe_filename = re.sub(r'[^\w\-_.]', '_', link_text)
                    if not safe_filename:
                        safe_filename = link_href.split('/')[-1] or 'index'
                    s3_key = f"{faculty_name}/{safe_filename}.html"

                    s3_hook.load_string(
                        string_data=final_html,
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True
                    )
                    print(f"Успешно загружено в S3: {s3_key}")

                except Exception as e:
                    # Если даже после 3 попыток страница не скачалась, пишем ошибку, но НЕ ломаем цикл для других файлов
                    print(f"Ошибка после повторных попыток для страницы {full_page_url}: {e}")

    @task
    def load_s3_to_postgres_staging():
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Очищаем staging-таблицу перед новой загрузкой, чтобы данные не дублировались
        print("Очистка staging таблицы...")
        pg_hook.run("TRUNCATE TABLE staging_kubsu_ranked_lists;")

        # Получаем список всех файлов из нашего бакета
        print(f"Сканирование бакета {BUCKET_NAME}...")
        keys = s3_hook.list_keys(bucket_name=BUCKET_NAME)

        if not keys:
            print("В S3 не найдено файлов для загрузки в БД.")
            return

        for key in keys:
            # Извлекаем название факультета и файла из структуры S3 (Факультет/Файл.html)
            if '/' in key:
                faculty, filename = key.split('/', 1)
                list_name = filename.replace('.html', '').replace('_', ' ')
            else:
                continue

            print(f"Читаем из S3 и пишем в Postgres: {key}")
            # Читаем HTML-контент файла из MinIO
            raw_html = s3_hook.read_key(key, bucket_name=BUCKET_NAME)

            # Записываем сырой HTML в Postgres
            sql = """
                INSERT INTO staging_kubsu_ranked_lists (faculty, list_name, raw_html)
                VALUES (%s, %s, %s);
            """
            pg_hook.run(sql, parameters=(faculty, list_name, raw_html))

        print("Все файлы успешно перенесены в staging_kubsu_ranked_lists!")

    @task
    def transform_staging_to_dim():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        print("Очистка витрины dim_ranked_applicants...")
        pg_hook.run("TRUNCATE TABLE dim_ranked_applicants;")
        
        print("Выгрузка сырых данных из staging...")
        records = pg_hook.get_records("SELECT faculty, list_name, raw_html FROM staging_kubsu_ranked_lists;")
        
        if not records:
            print("В staging таблице нет данных для парсинга.")
            return

        inserted_count = 0
        
        # [Уровень 1] Главный цикл по страницам из базы
        for faculty, list_name, raw_html in records:
            soup = BeautifulSoup(raw_html, 'html.parser')
            
            tables = soup.find_all('table')
            if not tables:
                continue
                
            print(f"Парсинг списка '{list_name}' ({faculty}). Найдено таблиц: {len(tables)}")
            
            # [Уровень 2] Цикл по таблицам внутри ОДНОЙ страницы (СДВИГАЕМ НА 12 ПРОБЕЛОВ)
            for table in tables:
                # 1. Сбор метаданных (тип конкурса и дата среза) — с защитой от None
                list_type = "На места по общему конкурсу"
                list_date_str = "Неизвестная дата"
                
                table_container = table.find_parent('div', class_='table') or table
                current_sibling = table_container.find_previous_sibling() if table_container else None
                
                # [Уровень 3] Петля сбора текста над текущей таблицей
                while current_sibling:
                    if current_sibling.name == 'table' or current_sibling.find('table'):
                        break
                    
                    current_classes = current_sibling.get('class', []) if hasattr(current_sibling, 'get') else []
                    if 'table' in current_classes:
                        break
                    
                    p_tags = current_sibling.find_all('p') if current_sibling.name != 'p' else [current_sibling]
                    for p in p_tags:
                        txt = p.get_text(strip=True)
                        if not txt:
                            continue
                        if "в рамках" in txt.lower() or "конкурсу" in txt.lower():
                            list_type = txt.replace('(', '').replace(')', '').strip()
                        if "на " in txt.lower() and ("г." in txt.lower() or ":" in txt.lower()) and "заявление" not in txt.lower():
                            list_date_str = txt.strip()
                            
                    if hasattr(current_sibling, 'find_previous_sibling'):
                        current_sibling = current_sibling.find_previous_sibling()
                    else:
                        break
                # [Конец Уровня 3] Вышли из while. Теперь мы знаем точные list_type и list_date_str!

                # 2. Парсинг шапки таблицы (На уровне 16 пробелов, ВНЕ цикла while!)
                header_row = table.find('tr')
                if not header_row:
                    continue
                    
                headers = [th.get_text(strip=True).strip().lower() for th in header_row.find_all(['th', 'td'])]
                
                idx_1 = headers.index('1') if '1' in headers else None
                idx_2 = headers.index('2') if '2' in headers else None
                idx_3 = headers.index('3') if '3' in headers else None
                idx_4 = headers.index('4') if '4' in headers else None
                
                idx_priority = None
                idx_agreement = None
                
                for i, h in enumerate(headers):
                    if "приоритет" in h:
                        idx_priority = i
                    if "согласие" in h or "договор" in h or "оригинал" in h:
                        idx_agreement = i

                if idx_agreement is None:
                    idx_agreement = len(headers) - 1

                # 3. Парсинг строк абитуриентов (На уровне 16 пробелов)
                rows = table.find_all('tr')
                rows_to_insert = []
                
                for row in rows:
                    if row.parent.name == 'thead' or (row.find('th') and "идентификатор" in row.get_text().lower()):
                        continue
                        
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 4:
                        continue
                        
                    try:
                        ord_cell = row.find(class_='ord')
                        fio_cell = row.find(class_='fio')
                        note_cells = row.find_all(class_='note')
                        
                        pos_number_raw = ord_cell.get_text(strip=True) if ord_cell else cells[0].get_text(strip=True)
                        snils_or_id = fio_cell.get_text(strip=True).replace('*', '').strip() if fio_cell else cells[1].get_text(strip=True).replace('*', '').strip()
                        total_score_raw = note_cells[0].get_text(strip=True) if note_cells else cells[2].get_text(strip=True)
                        
                        pos_number = int(pos_number_raw) if pos_number_raw.isdigit() else None
                        total_score = int(total_score_raw) if total_score_raw.isdigit() else 0
                        
                        def get_score_by_idx(idx):
                            if idx is not None and idx < len(cells):
                                txt = cells[idx].get_text(strip=True)
                                return int(txt) if txt.isdigit() else 0
                            return None
                        
                        score_1 = get_score_by_idx(idx_1)
                        score_2 = get_score_by_idx(idx_2)
                        score_3 = get_score_by_idx(idx_3)
                        score_4 = get_score_by_idx(idx_4)
                        priority = get_score_by_idx(idx_priority)
                        
                        has_original_documents = False
                        if idx_agreement is not None and idx_agreement < len(cells):
                            agreement_txt = cells[idx_agreement].get_text(strip=True).lower()
                            if "да" in agreement_txt or "оригинал" in agreement_txt:
                                has_original_documents = True
                        
                        if pos_number is not None and snils_or_id:
                            rows_to_insert.append((
                                faculty, list_name, list_type, list_date_str,
                                pos_number, snils_or_id, total_score, 
                                score_1, score_2, score_3, score_4, 
                                priority, has_original_documents
                            ))
                    except Exception as cell_err:
                        print(f"Ошибка строки: {cell_err}")
                        continue
                
                # 4. Вставка пачки текущей таблицы в Postgres
                if rows_to_insert:
                    pg_hook.insert_rows(
                        table='dim_ranked_applicants', 
                        rows=rows_to_insert, 
                        target_fields=[
                            'faculty', 'direction', 'list_type', 'list_date_str', 
                            'pos_number', 'snils_or_id', 'total_score', 
                            'subject_1', 'subject_2', 'subject_3', 'subject_4', 
                            'priority', 'has_original_documents'
                        ]
                    )
                    inserted_count += len(rows_to_insert)

        print(f"Парсинг успешно завершён! В витрину записано {inserted_count} строк.")

    # Строим граф зависимостей
    extract_and_upload_to_s3() >> load_s3_to_postgres_staging() >> transform_staging_to_dim()
