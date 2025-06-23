
Файл `data_pipeline.py` представляет собой **Airflow DAG (Directed Acyclic Graph)** для обработки данных с использованием Yandex Cloud Dataproc и PySpark. Вот что он делает:

## Основное назначение
Этот DAG автоматизирует процесс обработки данных в облаке Yandex Cloud, используя временный Spark кластер.

## Структура и функциональность

### 1. **Настройка подключений** (setup_connections)
- Создает подключения к Yandex Cloud Object Storage (S3-совместимое хранилище)
- Настраивает подключение к Yandex Cloud для работы с Dataproc
- Все настройки берутся из переменных Airflow (Variables)

### 2. **Создание Dataproc кластера** (create_spark_cluster)
- Создает временный Spark кластер с уникальным именем (`tmp-dp-{uuid}`)
- Конфигурация кластера:
  - **Master node**: s3-c2-m8 (2 vCPU, 8 GB RAM), 20 GB SSD
  - **Data nodes**: 2 узла s3-c4-m16 (4 vCPU, 16 GB RAM), 50 GB SSD каждый
  - **Compute nodes**: 0 (не используются)
  - **Сервисы**: YARN, SPARK, HDFS, MAPREDUCE

### 3. **Запуск PySpark задания** (poke_spark_processing)
- Выполняет PySpark скрипт `pyspark_script.py` из S3 bucket
- Передает параметр `--bucket` с именем S3 bucket
- Обрабатывает данные с помощью Spark

### 4. **Удаление кластера** (delete_spark_cluster)
- Удаляет временный Dataproc кластер после завершения обработки
- Использует `TriggerRule.ALL_DONE` - удаляет кластер независимо от успеха/неудачи предыдущих задач

## Расписание выполнения
- **Запуск**: 21 июня 2025 года
- **Интервал**: каждые 60 минут
- **Catchup**: отключен (не выполняет пропущенные запуски)



## Архитектура
```
setup_connections → create_spark_cluster → poke_spark_processing → delete_spark_cluster
```

Этот подход позволяет эффективно использовать облачные ресурсы - кластер создается только на время обработки данных и автоматически удаляется после завершения, что экономит затраты на инфраструктуру.

Отличный вопрос! Доступ происходит в несколько этапов, и ключевую роль здесь играет **сервисный аккаунт (Service Account)**, который передается от Airflow в Data Proc.

Вот пошаговая цепочка, как это работает:

### 1. Airflow DAG (`data_pipeline.py`): Подготовка и передача прав

1.  **Определение Сервисного Аккаунта:** В DAG'е вы получаете ID сервисного аккаунта из Airflow Variables:
    ```python
    # dags/data_pipeline.py
    DP_SA_ID = Variable.get("DP_SA_ID")
    ```
    Этот сервисный аккаунт должен иметь права на чтение/запись в нужный S3 бакет (например, роль `storage.editor`).

2.  **Создание кластера Data Proc:** При создании кластера вы явно указываете, что он должен использовать этот сервисный аккаунт:
    ```python
    # dags/data_pipeline.py
    create_spark_cluster = DataprocCreateClusterOperator(
        # ...
        service_account_id=DP_SA_ID, # <--- Вот здесь передаются права
        # ...
    )
    ```
    Теперь все виртуальные машины внутри созданного кластера Data Proc "живут" от имени этого сервисного аккаунта и наследуют все его права.

### 2. Запуск PySpark-задания

1.  **Передача аргументов:** DAG запускает PySpark-скрипт и передает ему имя бакета в качестве аргумента командной строки:
    ```python
    # dags/data_pipeline.py
    poke_spark_processing = DataprocCreatePysparkJobOperator(
        main_python_file_uri=f"s3a://{S3_SRC_BUCKET}/src/pyspark_script3.py",
        args=["--bucket", S3_BUCKET_NAME], # <--- Передаем имя бакета
        # ...
    )
    ```

2.  **Получение аргументов в скрипте:** `pyspark_script3.py` принимает этот аргумент:
    ```python
    # src/pyspark_script3.py
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket
    ```

### 3. PySpark (`pyspark_script3.py`): Непосредственный доступ к S3

1.  **Формирование пути:** Скрипт строит полный путь к данным, используя префикс `s3a://`:
    ```python
    # src/pyspark_script3.py
    input_path = f"s3a://{bucket_name}/input_data/*.csv"
    ```

2.  **Магия доступа:** Когда Spark видит путь `s3a://`, он делает следующее:
    *   Он понимает, что нужно обратиться к S3-совместимому хранилищу.
    *   Поскольку скрипт запущен на кластере Yandex Data Proc, Spark автоматически (без дополнительного кода) обращается к **Metadata Service** виртуальной машины.
    *   Metadata Service выдает временный IAM-токен для того самого **сервисного аккаунта (`DP_SA_ID`)**, с которым был создан кластер.
    *   С этим токеном Spark успешно аутентифицируется в Yandex Object Storage и получает доступ к файлам.

### Итог
Доступ происходит **не напрямую** из Airflow. Airflow лишь **оркестрирует** процесс:

`Airflow` → `создает кластер Data Proc с правами Сервисного Аккаунта` → `запускает на нем PySpark-скрипт` → `Spark внутри кластера использует права Сервисного Аккаунта для доступа к S3`.

Поэтому для успешного выполнения **ключевым моментом** является наличие у сервисного аккаунта `DP_SA_ID` необходимых IAM-ролей для работы с S3-бакетом.