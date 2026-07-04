from datetime import datetime, timedelta
import json
import logging
import requests
from datetime import datetime, timedelta
import re
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Константы
BASE_URL = "https://www.kubsu.ru"
START_URL = f"{BASE_URL}/ru/abitlist"
BUCKET_NAME = "ranked-lists"
AWS_CONN_ID = "aws_minio"

# Целевые факультеты
TARGET_FACULTIES = [
    "Факультет компьютерных технологий и прикладной математики",
    "Экономический факультет"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_ some_user@example.com': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kubsu_abitlist_to_minio',
    default_args=default_args,
    description='Парсинг списков абитуриентов КубГУ и загрузка в MinIO S3',
    schedule='@daily',
    catchup=False,
    tags=['kubsu', 'parser', 's3'],
) as dag:

    @task
    def parse_links_and_upload():
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        # 1. Скачиваем главную страницу со списками
        print(f"Запрос главной страницы: {START_URL}")
        response = requests.get(START_URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Находим блоки факультетов (обычно заголовки h2/h3/h4 или блоки с названиями)
        # Ищем элементы, содержащие названия нужных факультетов
        for faculty_name in TARGET_FACULTIES:
            faculty_element = soup.find(lambda tag: tag.name in ['h2', 'h3', 'h4', 'p', 'strong'] and faculty_name in tag.text)
            
            if not faculty_element:
                print(f"Факультет не найден на странице: {faculty_name}")
                continue
                
            print(f"Обработка факультета: {faculty_name}")
            
            # Собираем ссылки, идущие ПОСЛЕ заголовка факультета, но ДО следующего крупного блока/разделителя
            # В структуре КубГУ ссылки обычно лежат в списках или идут друг за другом
            current_element = faculty_element.next_sibling
            
            while current_element:
                # Если дошли до следующего факультета (заголовка), останавливаемся
                if current_element.name in ['h2', 'h3', 'h4']:
                    break
                
                # Ищем все теги <a> внутри текущего контейнера/элемента
                if hasattr(current_element, 'find_all'):
                    links = current_element.find_all('a', href=True) if current_element.name != 'a' else [current_element]
                    
                    for link in links:
                        link_text = link.get_text(strip=True)
                        link_href = link['href']
                        
                        # Условие-фильтр: пропускаем "иностранные граждане и лица без гражданства"
                        if "иностранные граждане и лица без гражданства" in link_text.lower():
                            print(f"Пропущена ссылка (иностранцы): {link_text}")
                            continue
                        
                        # Формируем полный URL
                        if link_href.startswith('/'):
                            full_page_url = BASE_URL + link_href
                        elif link_href.startswith('http'):
                            full_page_url = link_href
                        else:
                            continue
                            
                        # 2. Скачиваем целевую страницу по ссылке
                        try:
                            print(f"Скачивание страницы: {full_page_url} ({link_text})")
                            page_res = requests.get(full_page_url, timeout=30)
                            page_res.raise_for_status()
                            
                            # Переводим в BeautifulSoup для обрезки контента
                            page_soup = BeautifulSoup(page_res.text, 'html.parser')
                            
                            # 3. Обрезаем контент ДО тега <hr />
                            hr_tag = page_soup.find('hr')
                            if hr_tag:
                                # Удаляем сам тег hr и всё, что идет после него
                                for sibling in list(hr_tag.find_next_siblings()):
                                    sibling.extract()
                                hr_tag.extract() # удаляем сам hr
                            
                            # Получаем финальный HTML в виде строки
                            final_html = str(page_soup)
                            
                            # Формируем безопасное имя файла на основе текста ссылки и URL
                            safe_filename = re.sub(r'[^\w\-_.]', '_', link_text)
                            if not safe_filename:
                                safe_filename = link_href.split('/')[-1] or 'index'
                            s3_key = f"{faculty_name}/{safe_filename}.html"
                            
                            # 4. Загружаем обработанный HTML напрямую в MinIO S3
                            s3_hook.load_string(
                                string_data=final_html,
                                key=s3_key,
                                bucket_name=BUCKET_NAME,
                                replace=True
                            )
                            print(f"Успешно загружено в S3: {s3_key}")
                            
                        except Exception as e:
                            print(f"Ошибка при обработке страницы {full_page_url}: {e}")
                            
                current_element = current_element.next_sibling

    # Вызов таски
    parse_links_and_upload()
