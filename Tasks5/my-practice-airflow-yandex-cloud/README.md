
## Чтобы узнать IP-адреса узлов Airflow кластера в Yandex Cloud, можно использовать следующие команды:

yc managed-airflow cluster list

yc managed-airflow cluster get c9qs3h6jlqtcgqroi76d


Этот Makefile предназначен для автоматизации операций с Apache Airflow в Yandex Cloud. Вот что делает каждая команда:

## Основные настройки
- **SHELL := /bin/bash** - использует bash как оболочку
- **include .env** - подключает переменные окружения из файла .env
- **.EXPORT_ALL_VARIABLES:** - экспортирует все переменные в подпроцессы

## Команды для работы с Airflow

### `setup-airflow-variables`
- Подключается к Airflow серверу по SSH
- Запускает скрипт `setup_airflow_variables.sh` для настройки переменных Airflow

### `upload-dags-to-airflow`
- Загружает DAG файлы (Python скрипты) на Airflow сервер через SCP
- Копирует файлы в директорию `/home/airflow/dags/`

## Команды для работы с S3 хранилищем

### `upload-dags-to-bucket`
- Загружает DAG файлы в S3 bucket

### `upload-src-to-bucket`
- Загружает исходный код из папки `src/` в S3 bucket

### `upload-data-to-bucket`
- Загружает входные CSV файлы из `data/input_data/` в S3 bucket

### `upload-all`
- Комбинированная команда для загрузки всех компонентов (данные, код, DAGs)

### `download-output-data-from-bucket`
- Скачивает обработанные данные из S3 bucket в локальную папку `data/output_data/`

## Команды для управления инфраструктурой

### `instance-list`
- Показывает список всех инстансов в Yandex Cloud

### `git-push-secrets`
- Запускает Python скрипт для отправки секретов в GitHub репозиторий

## Команды синхронизации

### `sync-repo`
- Синхронизирует код с удаленным сервером через rsync
- Исключает временные файлы (.venv, .terraform, .tfstate, .backup, .json)

### `sync-env`
- Синхронизирует файл .env с удаленного сервера

### `airflow-cluster-mon`
- Мониторит логи Airflow кластера в реальном времени

Этот Makefile обеспечивает полный цикл работы с MLOps pipeline: от загрузки данных и кода до мониторинга выполнения задач в Airflow.