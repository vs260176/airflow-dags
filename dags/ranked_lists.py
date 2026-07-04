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
AWS_CONN_ID = "minio_logs"

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
    dag_id='kubsu_abitlist_to_minio',
    default_args=default_args,
    description='Парсинг списков абитуриентов КубГУ и загрузка в MinIO S3',
    schedule='@daily',
    catchup=False,
    tags=['kubsu', 'parser', 's3'],
) as dag:

    @task
    def parse_links_and_upload():
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

    parse_links_and_upload()
