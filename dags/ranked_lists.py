from datetime import datetime, timedelta
import re
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Константы
BASE_URL = "https://kubsu.ru"
START_URL = f"{BASE_URL}/ru/abitlist"
BUCKET_NAME = "ranked-lists"
AWS_CONN_ID = "minio_logs"
POSTGRES_CONN_ID = "postgre_k8s"

TARGET_FACULTIES = [
    "Факультет компьютерных технологий и прикладной математики",
    "Экономический факультет"
]

BROWSER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Connection': 'keep-alive'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kubsu_abitlist_to_postgres_v3',  # Переходим на v3 для чистой истории
    default_args=default_args,
    description='Парсинг списков КубГУ с сохранением ежедневной истории',
    schedule='@daily',
    catchup=False,
    tags=['kubsu', 'history', 's3', 'postgres'],
) as dag:

    @task
    def parse_links_and_upload(ds=None):  # ds автоматически содержит дату рана 'YYYY-MM-DD'
        requests.packages.urllib3.util.connection.HAS_IPV6 = False
        requests.packages.urllib3.disable_warnings()
        
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        from requests.adapters import HTTPAdapter
        from urllib3.util import Retry
        retry_strategy = Retry(total=3, backoff_factor=2, raise_on_status=False)
        
        session = requests.Session()
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.verify = False
        
        print(f"Запрос главной страницы: {START_URL}")
        response = session.get(START_URL, headers=BROWSER_HEADERS, timeout=15)
        response.encoding = 'utf-8'
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for faculty_name in TARGET_FACULTIES:
            faculty_element = soup.find(string=lambda text: text and faculty_name in text)
            if not faculty_element:
                continue
            
            parent_tag = faculty_element.parent
            
            # --- ОБНОВЛЕННАЯ ЛОГИКА НАВИГАЦИИ ПО НОВОЙ СТРУКТУРЕ ---
            # 1. Находим первый тег <hr /> после заголовка факультета
            first_hr = parent_tag.find_next('hr')
            if not first_hr:
                print(f"Предупреждение: для {faculty_name} не найден первый <hr />")
                continue
            
            # 2. Собираем все теги <a> из списков <ul> строго до второго <hr />
            links = []
            for sibling in first_hr.find_next_siblings():
                if sibling.name == 'hr':  # Дошли до второго hr — останавливаем сбор (бакалавриат закончился)
                    break
                if sibling.name == 'ul':  # Нашли список со специальностями между hr
                    links.extend(sibling.find_all('a', href=True))
            
            if not links:
                print(f"Предупреждение: для {faculty_name} не найдены ссылки между <hr />")
                continue
            # ------------------------------------------------------
            
            for link in links:
                link_text = link.get_text(strip=True)
                link_href = link['href']
                
                if "иностранные граждане и лица без гражданства" in link_text.lower():
                    continue
                
                if link_href.startswith('/'):
                    full_page_url = BASE_URL + link_href
                elif link_href.startswith('http'):
                    full_page_url = link_href
                else:
                    continue
                    
                try:
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
                    
                    # 💡 ИСПРАВЛЕНО ДЛЯ S3: сохраняем в отдельную папочку с датой дня
                    s3_key = f"{ds}/{faculty_name}/{safe_filename}.html"
                    
                    s3_hook.load_string(
                        string_data=final_html,
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True
                    )
                    print(f"Успешно загружено в S3: {s3_key}")
                    
                except Exception as e:
                    print(f"Ошибка страницы {full_page_url}: {e}")

    @task
    def load_s3_to_postgres_staging(ds=None):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 💡 ИСПРАВЛЕНО ДЛЯ ИДЕМПОТЕНТНОСТИ: Стираем данные ТОЛЬКО за текущий день
        print(f"Очистка старых данных staging за дату {ds}...")
        pg_hook.run("DELETE FROM staging_kubsu_ranked_lists WHERE snapshot_date = %s;", parameters=(ds,))
        
        # Сканируем префикс S3 строго для текущей даты рана
        s3_prefix = f"{ds}/"
        print(f"Сканирование папки S3: {BUCKET_NAME}/{s3_prefix}...")
        keys = s3_hook.list_keys(bucket_name=BUCKET_NAME, prefix=s3_prefix)
        
        if not keys:
            print(f"В S3 не найдено файлов за дату {ds}.")
            return

        for key in keys:
            # Парсим структуру: ds/Факультет/Имя_файла.html
            parts = key.split('/')
            if len(parts) >= 3:
                faculty = parts[1]
                filename = parts[2]
                list_name = filename.replace('.html', '').replace('_', ' ')
            else:
                continue
                
            raw_html = s3_hook.read_key(key, bucket_name=BUCKET_NAME)
            
            # Пишем сырой html с указанием snapshot_date
            sql = """
                INSERT INTO staging_kubsu_ranked_lists (faculty, list_name, raw_html, snapshot_date)
                VALUES (%s, %s, %s, %s);
            """
            pg_hook.run(sql, parameters=(faculty, list_name, raw_html, ds))
            
        print(f"Данные за {ds} успешно перенесены в staging!")

    @task
    def transform_staging_to_dim(ds=None):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # 💡 ИСПРАВЛЕНО ДЛЯ ИДЕМПОТЕНТНОСТИ: Удаляем расчеты только за текущий день запуска
        print(f"Очистка витрины dim_ranked_applicants за дату {ds}...")
        pg_hook.run("DELETE FROM dim_ranked_applicants WHERE snapshot_date = %s;", parameters=(ds,))
        
        # Извлекаем из staging данные СТРОГО за текущий день
        print(f"Выгрузка сырых данных за дату {ds}...")
        sql_select = "SELECT faculty, list_name, raw_html FROM staging_kubsu_ranked_lists WHERE snapshot_date = %s;"
        records = pg_hook.get_records(sql_select, parameters=(ds,))
        
        if not records:
            print(f"В staging за дату {ds} нет данных для парсинга.")
            return

        inserted_count = 0
        
        for faculty, list_name, raw_html in records:
            soup = BeautifulSoup(raw_html, 'html.parser')
            tables = soup.find_all('table')
            if not tables:
                continue
                
            for table in tables:
                list_type = "На места по общему конкурсу"
                list_date_str = "Неизвестная дата"
                
                table_container = table.find_parent('div', class_='table') or table
                current_sibling = table_container.find_previous_sibling() if table_container else None
                
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
                        txt_lower = txt.lower()
                        if "особого права" in txt_lower:
                            list_type = "На места в рамках особого права"
                        elif "отдельной квоты" in txt_lower:
                            list_type = "На места в рамках отдельной квоты"
                        elif "целевой квоты" in txt_lower:
                            list_type = "На места в рамках целевой квоты"
                        elif "общему конкурсу" in txt_lower:
                            list_type = "На места по общему конкурсу"
                        
                        if txt_lower.startswith("на ") and ("г." in txt_lower or ":" in txt_lower) and len(txt) < 35:
                            list_date_str = txt.strip()
                            
                    if hasattr(current_sibling, 'find_previous_sibling'):
                        current_sibling = current_sibling.find_previous_sibling()
                    else:
                        break

                header_row = table.find('tr')
                if not header_row:
                    continue
                    
                headers = [th.get_text(strip=True).strip().lower() for th in header_row.find_all(['th', 'td'])]
                
                # 2. Расчет индексов колонок приоритетов
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

                # 3. Парсинг строк абитуриентов
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
                            # 💡 ДОБАВИЛИ ДАТУ СНИМКА В СТРОКУ К ЗАПИСИ
                            rows_to_insert.append((
                                faculty, list_name, list_type, list_date_str,
                                pos_number, snils_or_id, total_score, 
                                score_1, score_2, score_3, score_4, 
                                priority, has_original_documents, ds
                            ))
                    except Exception as cell_err:
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
                            'priority', 'has_original_documents', 'snapshot_date'
                        ]
                    )
                    inserted_count += len(rows_to_insert)

        print(f"Парсинг успешно завершён! За дату {ds} записано {inserted_count} строк.")

    # Вызовы тасок
    extract_task = parse_links_and_upload()
    staging_task = load_s3_to_postgres_staging()
    dim_task = transform_staging_to_dim()

    extract_task >> staging_task >> dim_task
