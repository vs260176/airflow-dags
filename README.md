# Airflow DAGs

## Настройка Airflow в VSCode

Для настройки подсветки синтаксиса, автодополнения (IntelliSense) и проверки ошибок для Airflow 3.2.0 в VSCode необходимо связать редактор с тем окружением Python, в котором установлен Airflow. Так как Airflow — это обычная библиотека Python, VSCode не сможет распознавать его синтаксис (включая обновленный синтаксис Airflow 3.x), пока не увидит исходный код пакета.

### Базовые расширения VSCode

- Python (от Microsoft) — обеспечивает базовую поддержку языка, линтинг и навигацию.
- Pylance (устанавливается вместе с Python) — отвечает за быстрое автодополнение, статический анализ и валидацию типов.

### Развёртывание Airflow 3.2.0 локально

Чтобы VSCode подтягивал автодополнение для модулей вроде airflow.sdk или новых провайдеров, необходимо установить Airflow 3.2.0 в виртуальное окружение проекта:

```bash
# 0. Узнать установлена ли версия 3.12
python3.12 --version

# 1. Удалить старое окружение (если что-то пошло не так)
rm -rf .venv

# 2. Создать новое с Python 3.12
python3.12 -m venv .venv

# 3. Активировать
source .venv/bin/activate

# 4. Проверить версию Python (должна быть 3.12.13)
python --version

# 5. Обновить pip
pip install --upgrade pip

# 6. Установить Airflow 3.2.0
pip install "apache-airflow==3.2.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.12.txt"

# 7. Проверить версии Airflow
airflow version
# Должно вывести: 3.2.0

# 8. Проверить установленные пакеты
pip list | grep apache-airflow
```

Этот URL формируется по шаблону, который используется в официальной документации Airflow

```bash
https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt
```

Где:

${AIRFLOW_VERSION} — это версия Airflow, которую необходимо установить (текущая версия 3.2.0). Ветка constraints-3.2.0 в репозитории Apache Airflow на GitHub содержит все файлы ограничений для этой версии.

${PYTHON_VERSION} — это версия Python (текущая версия 3.14.3).

### Указать VSCode правильный интерпретатор

Если VSCode продолжает подчеркивать import airflow красным цветом:

Нужно наажать комбинацию клавиш Ctrl + Shift + P (или Cmd + Shift + P на macOS).
Ввсти в поиск: Python: Select Interpreter.
Выбрать из списка интерпретатор папки .venv (он должен иметь пометку ('venv': ./venv)).
После этого Pylance проиндексирует библиотеку Airflow 3.2.0, и заработает автокомплит для всех операторов и декораторов TaskFlow.

### Локальный запуск через Docker

Так как целевая среда — Kubernetes, запуск в Docker-контейнерах на macOS гарантирует, что не возникнет проблем из-за разницы операционных систем (например, путей к файлам или бинарных зависимостей).

Вариант А: Использование Astro CLI (Самый простой и популярный)

Astro CLI от компании Astronomer — это стандарт де-факто для локальной разработки под Airflow.
Он автоматически разворачивает Airflow в Docker.

```bash
# 1. Установить Astro CLI на macOS через Homebrew
brew install astronomer/tap/astro

# 2. Перейти в рабочую директорию
cd airflow-dags

# 3. Инициализировать проект в папке с DAG:
astro dev init

# 4. Запустить локальный Airflow
astro dev start
```

В итоге запускается работающий веб-интерфейс Airflow по адресу localhost:8080, где можно визуально протестировать и запустить ваш DAG.

Вариант Б:

Использование официального docker-compose

Если необходимо использовать чистый Apache Airflow 3.2.0

Скачать официальный манифест:bashcurl -LfO 'https://apache.org'

Создать папки ./dags, ./logs, ./plugins.

Запустить контейнеры: docker compose up -d.

Важный нюанс для Kubernetes: KubernetesPodOperator

Если в DAG используется KubernetesPodOperator (который запускает отдельные Docker-контейнеры для задач), обычный локальный запуск через airflow tasks test выдаст ошибку, так как в macOS нет доступа к кластеру Kubernetes.

Как это обойти:
Установить Docker Desktop на macOS и включить в его настройках встроенный локальный Kubernetes (minikube / k3s).
Настроить in_cluster=False и указать config_file='~/.kube/config' в параметрах оператора для локального тестирования.
Перед отправкой в Git убедиться, что в проде оператор будет использовать in_cluster=True.

## Настройка DAG ranked_lists.py

```bash
pip install beautifulsoup4 apache-airflow-providers-amazon apache-airflow
```
