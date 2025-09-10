"""
ШАГ 4 (Tasks10): Spark Streaming Job для HTTP запросов к ML API (ITERATION 5) - EXTENDED RUNTIME v7.0

🚨 ESCALATING ATTACK: Этот DAG модифицирован для Tasks10 Iteration 5
Вместо локального inference делает HTTP POST запросы к ML API для создания реальной нагрузки.

Этот DAG управляет Spark Streaming приложением которое:
- Читает транзакции из Kafka топика transactions-input
- ❌ НЕ загружает MLflow модель локально
- ✅ Делает HTTP POST запросы к http://tasks10-ml-api.darveter.com/predict
- Записывает результаты в Kafka топик fraud-predictions
- Создает РЕАЛЬНУЮ нагрузку на ML API для тестирования HPA и алертов

🎯 ЦЕЛЬ: Довести ML API до 6 подов + CPU>80% → 🚨 АЛЕРТ АДМИНИСТРАТОРА!

Автор: MLOps Task 10 - Iteration 5 - HTTP Attack Mode
"""

import logging
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import subprocess
import signal
import psutil

# Airflow imports
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context

# Spark imports (будут доступны в Spark executors)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import requests  # ✅ НОВОЕ: Для HTTP запросов к ML API
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logging.warning("⚠️ Spark/requests не найдены в Airflow - будут доступны в Spark cluster")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =====================================================================================
# 🔧 КОНФИГУРАЦИЯ TASKS10 - HTTP MODE
# =====================================================================================

# 🌐 HTTP API Configuration
ML_API_URL = "http://tasks10-ml-service-service.mlops-tasks10.svc.cluster.local:80/predict"
ML_API_TIMEOUT = 10  # секунд на запрос
ML_API_RETRY_COUNT = 2

# 📊 Kafka Configuration (аналогично Tasks8)
KAFKA_BOOTSTRAP_SERVERS = "kafka.kafka.svc.cluster.local:9092"
INPUT_TOPIC = "transactions-input"
OUTPUT_TOPIC = "fraud-predictions"

# 🔥 Spark Configuration для HTTP режима
SPARK_CONFIG = {
    "spark.app.name": "Tasks10-HTTP-Fraud-Detection-Streaming",
    "spark.master": "local[*]",
    "spark.sql.streaming.checkpointLocation": "/tmp/spark-checkpoints-tasks10",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.streaming.forceDeleteTempCheckpointLocation": "true",
    
    # 🚀 Kafka Integration - CRITICAL FIX!
    "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
    
    # 🚀 HTTP оптимизации
    "spark.task.maxFailures": "3",
    "spark.sql.streaming.kafka.consumer.pollTimeoutMs": "5000",
    "spark.sql.execution.arrow.pyspark.enabled": "true",
    "spark.sql.streaming.statefulOperator.checkCorrectness.enabled": "false",
}

# =====================================================================================
# 🌐 HTTP ML API CLIENT FUNCTIONS
# =====================================================================================

def make_ml_api_request(transaction_data: dict) -> dict:
    """
    Делает HTTP POST запрос к ML API для получения предсказания мошенничества
    
    Args:
        transaction_data: Данные транзакции в формате JSON
        
    Returns:
        dict: Результат предсказания или ошибка
    """
    try:
        # Подготавливаем данные для ML API - извлекаем только нужные поля
        transaction_fields = transaction_data.get("data", {})
        payload = {
            "transaction_id": str(transaction_fields.get("transaction_id")),  # Преобразуем в строку!
            "customer_id": transaction_fields.get("customer_id"),
            "terminal_id": transaction_fields.get("terminal_id"),
            "tx_amount": transaction_fields.get("tx_amount"),
            "tx_time_seconds": transaction_fields.get("tx_time_seconds"),
            "tx_time_days": transaction_fields.get("tx_time_days"),
            "tx_fraud_scenario": transaction_fields.get("tx_fraud_scenario")
        }
        
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Tasks10-Spark-Streaming/1.0"
        }
        
        # Делаем HTTP POST запрос
        response = requests.post(
            ML_API_URL,
            json=payload,
            headers=headers,
            timeout=ML_API_TIMEOUT
        )
        
        if response.status_code == 200:
            result = response.json()
            return {
                "transaction_id": transaction_data.get("transaction_id", "unknown"),
                "prediction": result.get("predictions", [{}])[0],
                "http_status": response.status_code,
                "response_time_ms": response.elapsed.total_seconds() * 1000,
                "timestamp": datetime.now().isoformat(),
                "api_url": ML_API_URL,
                "success": True
            }
        else:
            return {
                "transaction_id": transaction_data.get("transaction_id", "unknown"),
                "error": f"HTTP {response.status_code}: {response.text}",
                "http_status": response.status_code,
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "api_url": ML_API_URL
            }
            
    except requests.exceptions.Timeout:
        return {
            "transaction_id": transaction_data.get("transaction_id", "unknown"),
            "error": f"Request timeout after {ML_API_TIMEOUT}s",
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "api_url": ML_API_URL
        }
    except requests.exceptions.RequestException as e:
        return {
            "transaction_id": transaction_data.get("transaction_id", "unknown"),
            "error": f"Request failed: {str(e)}",
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "api_url": ML_API_URL
        }
    except Exception as e:
        return {
            "transaction_id": transaction_data.get("transaction_id", "unknown"),
            "error": f"Unexpected error: {str(e)}",
            "success": False,
            "timestamp": datetime.now().isoformat(),
            "api_url": ML_API_URL
        }

def batch_http_inference(batch_df, batch_id):
    """
    Обрабатывает batch данных из Kafka, делая HTTP запросы к ML API
    
    Args:
        batch_df: Spark DataFrame с транзакциями
        batch_id: ID батча
    """
    logger.info(f"🔥 Processing batch {batch_id} with {batch_df.count()} transactions")
    
    # Собираем все транзакции из batch
    transactions = batch_df.collect()
    results = []
    
    start_time = time.time()
    success_count = 0
    error_count = 0
    
    for row in transactions:
        try:
            # Парсим JSON данные транзакции
            transaction_data = json.loads(row.value)
            
            # Делаем HTTP запрос к ML API
            result = make_ml_api_request(transaction_data)
            
            if result["success"]:
                success_count += 1
            else:
                error_count += 1
                logger.warning(f"❌ ML API error for transaction {result['transaction_id']}: {result.get('error')}")
            
            results.append(result)
            
        except json.JSONDecodeError as e:
            error_count += 1
            logger.error(f"❌ JSON parse error: {e}")
            results.append({
                "error": f"JSON parse error: {str(e)}",
                "success": False,
                "timestamp": datetime.now().isoformat(),
                "raw_value": str(row.value)
            })
        except Exception as e:
            error_count += 1
            logger.error(f"❌ Processing error: {e}")
            results.append({
                "error": f"Processing error: {str(e)}",
                "success": False,
                "timestamp": datetime.now().isoformat()
            })
    
    processing_time = time.time() - start_time
    
    logger.info(f"""
    📊 Batch {batch_id} completed:
    ✅ Successful HTTP requests: {success_count}
    ❌ Failed HTTP requests: {error_count}
    ⏱️ Total processing time: {processing_time:.2f}s
    🌐 ML API URL: {ML_API_URL}
    📈 Average time per request: {processing_time/len(transactions):.3f}s
    """)
    
    # Отправляем результаты в выходной Kafka топик
    if results:
        try:
            # Создаем Spark DataFrame из результатов
            spark = SparkSession.getActiveSession()
            results_rdd = spark.sparkContext.parallelize(results)
            results_df = spark.read.json(results_rdd)
            
            # Отправляем в Kafka
            results_df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("topic", OUTPUT_TOPIC) \
                .save()
                
            logger.info(f"✅ Sent {len(results)} results to Kafka topic {OUTPUT_TOPIC}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send results to Kafka: {e}")

# =====================================================================================
# 🚀 SPARK STREAMING APPLICATION
# =====================================================================================

def run_spark_streaming_http(**context) -> str:
    """
    Запускает Spark Streaming приложение для HTTP запросов к ML API
    
    Returns:
        str: Статус выполнения
    """
    logger.info("🚀 Starting Spark Streaming HTTP ML API client...")
    
    try:
        # Создаем Spark Session
        spark = SparkSession.builder \
            .appName(SPARK_CONFIG["spark.app.name"])
        
        # Применяем конфигурацию
        for key, value in SPARK_CONFIG.items():
            if key != "spark.app.name":
                spark = spark.config(key, value)
        
        spark = spark.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"✅ Spark Session created: {spark.version}")
        logger.info(f"🌐 ML API URL: {ML_API_URL}")
        logger.info(f"📥 Input Kafka topic: {INPUT_TOPIC}")
        logger.info(f"📤 Output Kafka topic: {OUTPUT_TOPIC}")
        
        # Читаем из Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", INPUT_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        logger.info("📥 Connected to Kafka input stream")
        
        # Обрабатываем данные и делаем HTTP запросы
        query = kafka_df \
            .writeStream \
            .foreachBatch(batch_http_inference) \
            .outputMode("append") \
            .option("checkpointLocation", SPARK_CONFIG["spark.sql.streaming.checkpointLocation"]) \
            .trigger(processingTime="10 seconds") \
            .start()
        
        logger.info("🔥 Spark Streaming HTTP job started!")
        logger.info("📊 Monitoring ML API load generation...")
        
        # Мониторим выполнение
        start_time = time.time()
        max_runtime = 1800  # 30 минут для HPA тестирования!
        
        while query.isActive and (time.time() - start_time) < max_runtime:
            time.sleep(10)
            runtime_minutes = (time.time() - start_time) / 60
            progress = query.lastProgress
            if progress:
                logger.info(f"""
                📈 Streaming Progress (⏱️ {runtime_minutes:.1f}min / {max_runtime/60:.0f}min):
                - Batch ID: {progress.get('batchId', 'N/A')}
                - Input rows/sec: {progress.get('inputRowsPerSecond', 0):.1f}
                - Processing time: {progress.get('durationMs', {}).get('triggerExecution', 0)}ms
                - 🌐 HTTP requests to ML API: {progress.get('inputRowsPerSecond', 0):.1f}/sec
                - 🚀 HPA тестирование: Проверьте kubectl get hpa -n mlops-tasks10
                """)
        
        # Останавливаем стрим
        logger.info("🛑 Stopping Spark Streaming job...")
        query.stop()
        query.awaitTermination(timeout=30)
        
        spark.stop()
        
        runtime = time.time() - start_time
        logger.info(f"✅ Spark Streaming HTTP job completed successfully in {runtime:.1f}s")
        
        return f"SUCCESS: HTTP ML API load generation completed in {runtime:.1f}s"
        
    except Exception as e:
        logger.error(f"❌ Spark Streaming HTTP job failed: {e}")
        try:
            if 'spark' in locals():
                spark.stop()
        except:
            pass
        raise e

# =====================================================================================
# 🔧 HELPER FUNCTIONS
# =====================================================================================

def test_ml_api_connection(**context) -> str:
    """Тестирует подключение к ML API"""
    logger.info(f"🧪 Testing ML API connection to {ML_API_URL}")
    
    try:
        # Тестовая транзакция
        test_transaction = {
            "transaction_id": "test_001",
            "amount": 100.0,
            "merchant_category": "grocery",
            "hour_of_day": 14,
            "day_of_week": 2,
            "is_weekend": False,
            "user_age": 30,
            "transaction_frequency": 1.5
        }
        
        result = make_ml_api_request(test_transaction)
        
        if result["success"]:
            logger.info(f"✅ ML API test successful: {result}")
            return f"SUCCESS: ML API responding in {result.get('response_time_ms', 0):.1f}ms"
        else:
            logger.error(f"❌ ML API test failed: {result}")
            return f"FAILED: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"❌ ML API test error: {e}")
        return f"ERROR: {str(e)}"

def cleanup_spark_checkpoints(**context) -> str:
    """Очищает Spark checkpoints"""
    checkpoint_dir = SPARK_CONFIG["spark.sql.streaming.checkpointLocation"]
    try:
        import shutil
        if os.path.exists(checkpoint_dir):
            shutil.rmtree(checkpoint_dir)
            logger.info(f"✅ Cleaned up checkpoint directory: {checkpoint_dir}")
        return f"SUCCESS: Cleaned up {checkpoint_dir}"
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return f"ERROR: {str(e)}"

# =====================================================================================
# 🎯 AIRFLOW DAG DEFINITION
# =====================================================================================

# DAG arguments
default_args = {
    'owner': 'mlops-tasks10-iteration5',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=35),  # Увеличено для 30-минутного теста
}

# Создаем DAG
dag = DAG(
    dag_id='tasks10_spark_streaming_http_v7',
    default_args=default_args,
    description='Tasks10 Iteration 5: Spark Streaming HTTP ML API Load Generator v7 - Extended runtime for HPA testing (30min)',
    schedule=None,  # Запускается вручную или через escalating attack
    start_date=datetime(2024, 12, 20),
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'tasks10', 'iteration5', 'spark-streaming', 'http-api', 'load-generation', 'v7', 'extended-runtime-30min']
)

# Task 1: Тест подключения к ML API
test_api_task = PythonOperator(
    task_id='test_ml_api_connection',
    python_callable=test_ml_api_connection,
    dag=dag,
    doc_md="""
    ## 🧪 Test ML API Connection
    
    Проверяет доступность ML API перед запуском нагрузочного тестирования.
    
    **Что проверяется:**
    - Доступность http://tasks10-ml-api.darveter.com/predict
    - Время ответа API
    - Формат ответа
    
    **Ожидаемый результат:** HTTP 200 с предсказанием мошенничества
    """
)

# Task 2: Очистка checkpoints
cleanup_task = PythonOperator(
    task_id='cleanup_spark_checkpoints',
    python_callable=cleanup_spark_checkpoints,
    dag=dag,
    doc_md="""
    ## 🧹 Cleanup Spark Checkpoints
    
    Очищает старые Spark Streaming checkpoints для чистого запуска.
    """
)

# Task 3: Основной Spark Streaming HTTP job
streaming_task = PythonOperator(
    task_id='run_spark_streaming_http',
    python_callable=run_spark_streaming_http,
    dag=dag,
    doc_md="""
    ## 🚀 Spark Streaming HTTP ML API Load Generator
    
    **ГЛАВНАЯ ЗАДАЧА ITERATION 5:**
    Генерирует HTTP нагрузку на ML API для тестирования HPA и алертов.
    
    **Процесс:**
    1. Читает транзакции из Kafka топика `transactions-input`
    2. Для каждой транзакции делает HTTP POST к ML API
    3. Отправляет результаты в топик `fraud-predictions`
    4. Мониторит производительность и ошибки
    
    **Цель:** Довести ML API до 6 подов + CPU>80% → 🚨 АЛЕРТ АДМИНИСТРАТОРА!
    
    **Конфигурация:**
    - ML API: http://tasks10-ml-api.darveter.com/predict
    - Timeout: 10 секунд на запрос
    - Retry: 2 попытки при ошибке
    - Batch interval: 10 секунд
    """
)

# Определяем зависимости
test_api_task >> cleanup_task >> streaming_task

# =====================================================================================
# 🎯 DOCUMENTATION
# =====================================================================================

dag.doc_md = """
# 🚨 Tasks10 Iteration 5: HTTP ML API Load Generator

## 🎯 Цель
Создать **реальную HTTP нагрузку** на ML API для тестирования:
- Horizontal Pod Autoscaler (HPA)
- AlertManager правила
- Критический алерт `AdminNotification_MaxScaleHighCPU`

## 🔥 Escalating Attack Strategy
1. **Kafka Producer** генерирует транзакции с возрастающим TPS: 50→200→500→1500
2. **Spark Streaming** читает из Kafka и делает HTTP POST к ML API
3. **ML API** обрабатывает запросы → CPU нагрузка → HPA масштабирование
4. **Prometheus** мониторит метрики → AlertManager → 🚨 АЛЕРТ АДМИНИСТРАТОРА!

## 📊 Мониторинг
- **Grafana Dashboard**: http://grafana.darveter.com/d/tasks10-ml-dashboard
- **ML API URL**: http://tasks10-ml-api.darveter.com/predict
- **Prometheus**: Метрики CPU, Memory, HTTP requests

## 🚨 Ожидаемый результат
При достижении 1500 TPS:
- ML API масштабируется до 6 подов (максимум)
- CPU usage > 80% в течение 5+ минут
- Срабатывает алерт `AdminNotification_MaxScaleHighCPU`
- Администратор получает уведомление о критической нагрузке

## 🔧 Технические детали
- **Spark Local Mode**: Обходит проблемы с кластером
- **HTTP Client**: requests с timeout и retry
- **Error Handling**: Подробное логирование ошибок
- **Kafka Integration**: Вход/выход через Kafka топики
"""

if __name__ == "__main__":
    print("🚀 Tasks10 Spark Streaming HTTP ML API Load Generator")
    print(f"🌐 ML API URL: {ML_API_URL}")
    print(f"📥 Input topic: {INPUT_TOPIC}")
    print(f"📤 Output topic: {OUTPUT_TOPIC}")
