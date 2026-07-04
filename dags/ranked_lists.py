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

TARGET_FACULTIES = [
    "Факультет компьютерных технологий и прикладной математики",
    "Экономический факультет"
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
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

        # Мимикрируем под обычный браузер Chrome на Windows
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        print(f"Запрос главной страницы: {START_URL}")
        response = requests.get(START_URL, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for faculty_name in TARGET_FACULTIES:
            # Исправленный поиск: ищем элемент, текст которого содержит название факультета
            faculty_element = soup.find(string=lambda text: text and faculty_name in text)
            
            if not faculty_element:
                print(f"Факультет не найден на странице: {faculty_name}")
                continue
            
            # Поднимаемся к родительскому тегу, чтобы от него искать следующий список <ul>
            parent_tag = faculty_element.parent
            print(f"Успешно найден факультет: {faculty_name} в теге <{parent_tag.name}>")
            
            # Находим первый список <ul>, который идет сразу после этого факультета
            links_list = parent_tag.find_next('ul')
            if not links_list:
                print(f"Не найден список ссылок для факультета: {faculty_name}")
                continue
                
            # Извлекаем все ссылки <a> внутри этого списка
            links = links_list.find_all('a', href=True)
            print(f"Найдено ссылок для обработки: {len(links)}")
            
            for link in links:
                link_text = link.get_text(strip=True)
                link_href = link['href']
                
                # Фильтр: пропускаем иностранных граждан
                if "иностранные граждане и лица без гражданства" in link_text.lower():
                    print(f"Пропущена ссылка (иностранцы): {link_text}")
                    continue
                
                # Формируем абсолютный URL
                if link_href.startswith('/'):
                    full_page_url = BASE_URL + link_href
                elif link_href.startswith('http'):
                    full_page_url = link_href
                else:
                    continue
                    
                # Скачивание целевой страницы списка
                try:
                    print(f"Скачивание страницы: {full_page_url} ({link_text})")
                    page_res = requests.get(full_page_url, timeout=30)
                    page_res.raise_for_status()
                    
                    page_soup = BeautifulSoup(page_res.text, 'html.parser')
                    
                    # Обрезка контента ДО тега <hr />
                    hr_tag = page_soup.find('hr')
                    if hr_tag:
                        # Удаляем все элементы, идущие после тега <hr />
                        for sibling in list(hr_tag.find_next_siblings()):
                            if hasattr(sibling, 'extract'):
                                sibling.extract()
                        hr_tag.extract() # удаляем сам <hr />
                    
                    final_html = str(page_soup)
                    
                    # Генерация безопасного имени файла
                    safe_filename = re.sub(r'[^\w\-_.]', '_', link_text)
                    if not safe_filename:
                        safe_filename = link_href.split('/')[-1] or 'index'
                    s3_key = f"{faculty_name}/{safe_filename}.html"
                    
                    # Загрузка в MinIO S3
                    s3_hook.load_string(
                        string_data=final_html,
                        key=s3_key,
                        bucket_name=BUCKET_NAME,
                        replace=True
                    )
                    print(f"Успешно загружено в S3: {s3_key}")
                    
                except Exception as e:
                    print(f"Ошибка при обработке страницы {full_page_url}: {e}")

    parse_links_and_upload()
