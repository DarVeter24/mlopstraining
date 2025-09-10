"""
ШАГ 4 (Tasks10): ПАРАЛЛЕЛЬНЫЙ Spark Streaming для МАКСИМАЛЬНОЙ HTTP нагрузки
Создает 5 параллельных Spark Streaming jobs для максимальной нагрузки на ML API
"""

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from spark_stream_inference_http import run_spark_streaming_http, test_ml_api_connection, cleanup_spark_checkpoints

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DAG arguments
default_args = {
    'owner': 'mlops-tasks10-iteration5',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=35),
}

# Создаем DAG для параллельных Spark jobs
dag = DAG(
    dag_id='tasks10_spark_streaming_parallel_v1',
    default_args=default_args,
    description='Tasks10: 5 Параллельных Spark Streaming jobs для МАКСИМАЛЬНОЙ нагрузки на ML API',
    schedule=None,
    start_date=datetime(2024, 12, 20),
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'tasks10', 'parallel-spark', 'maximum-load', 'v1']
)

# Тест подключения
test_api_task = PythonOperator(
    task_id='test_ml_api_connection',
    python_callable=test_ml_api_connection,
    dag=dag
)

# Очистка checkpoints
cleanup_task = PythonOperator(
    task_id='cleanup_spark_checkpoints',
    python_callable=cleanup_spark_checkpoints,
    dag=dag
)

# Создаем 5 параллельных Spark Streaming tasks
parallel_tasks = []
for i in range(1, 6):
    task = PythonOperator(
        task_id=f'spark_streaming_job_{i}',
        python_callable=run_spark_streaming_http,
        dag=dag,
        doc_md=f"""
        ## 🚀 Parallel Spark Streaming Job {i}/5
        
        Один из 5 параллельных Spark Streaming jobs для создания максимальной нагрузки на ML API.
        Общая нагрузка: 5x Spark jobs = 5x HTTP requests/sec к ML API
        """
    )
    parallel_tasks.append(task)

# Зависимости
test_api_task >> cleanup_task
for task in parallel_tasks:
    cleanup_task >> task

dag.doc_md = """
# 🚀 ПАРАЛЛЕЛЬНАЯ АТАКА: 5x Spark Streaming Jobs

## 🎯 Стратегия
Запускает **5 параллельных Spark Streaming jobs**, каждый из которых:
- Читает из Kafka топика `transactions-input`
- Делает HTTP POST запросы к ML API
- Создает **5x нагрузку** по сравнению с одним job

## 📊 Ожидаемый эффект
- **Нагрузка на ML API**: 5x увеличение HTTP requests/sec
- **CPU utilization**: Должен превысить 80% от requests
- **HPA scaling**: Быстрое масштабирование до 6 подов
- **Alert trigger**: Срабатывание алерта администратора

## ⚠️ ВНИМАНИЕ
Этот DAG создает ЭКСТРЕМАЛЬНУЮ нагрузку на систему!
Используйте только для тестирования HPA и алертов.
"""
