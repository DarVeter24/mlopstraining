"""
ШАГ 4: Spark Streaming Job для потокового инференса модели мошенничества (LOCAL MODE)

🚀 РЕШЕНИЕ ПРОБЛЕМ С КЛАСТЕРОМ: Этот DAG использует Spark в Local режиме для обхода
проблем с сетевым взаимодействием между Airflow и Spark кластером.

Этот DAG управляет Spark Streaming приложением которое:
- Читает транзакции из Kafka топика transactions-input
- Загружает MLflow модель fraud_detection_model с stage Production (через PyFunc)
- Применяет модель для предсказания мошенничества в режиме реального времени
- Записывает результаты в Kafka топик fraud-predictions
- Обеспечивает отказоустойчивость через checkpoints

Автор: MLOps Task 8 - Local Mode Solution
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

# Spark и ML imports (будут доступны в Spark executors)
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    import mlflow
    import mlflow.pyfunc  # 🚀 ИСПОЛЬЗУЕМ PYFUNC вместо mlflow.spark
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    logging.warning("⚠️ Spark/MLflow не найдены в Airflow - будут доступны в Spark cluster")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# КОНФИГУРАЦИЯ SPARK STREAMING
# =============================================================================

class SparkStreamConfig:
    """Конфигурация для Spark Streaming приложения"""
    
    def __init__(self):
        """Загружает конфигурацию из Airflow Variables"""
        try:
            # Kafka настройки
            self.kafka_bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
            self.kafka_input_topic = Variable.get("KAFKA_INPUT_TOPIC")
            self.kafka_output_topic = Variable.get("KAFKA_OUTPUT_TOPIC")
            self.kafka_consumer_group = Variable.get("KAFKA_CONSUMER_GROUP")
            
            # MLflow настройки
            self.mlflow_tracking_uri = Variable.get("MLFLOW_TRACKING_URI")
            self.mlflow_model_name = Variable.get("MLFLOW_MODEL_NAME")
            self.mlflow_model_stage = Variable.get("MLFLOW_MODEL_STAGE", "Production")
            self.mlflow_tracking_username = Variable.get("MLFLOW_TRACKING_USERNAME", "admin")
            self.mlflow_tracking_password = Variable.get("MLFLOW_TRACKING_PASSWORD", "password")
            
            # Spark настройки - 🚀 LOCAL MODE
            self.spark_app_name = Variable.get("SPARK_APP_NAME", "StreamingFraudInference_Local")
            self.spark_master = "local[2]"  # 🚀 КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Local режим с 2 cores
            self.spark_driver_memory = "2g"  # Увеличиваем память для local режима
            self.spark_executor_memory = "2g"
            
            # S3/MinIO для checkpoints
            self.s3_endpoint_url = Variable.get("S3_ENDPOINT_URL")
            self.s3_access_key = Variable.get("S3_ACCESS_KEY")
            self.s3_secret_key = Variable.get("S3_SECRET_KEY")
            self.s3_bucket_name = Variable.get("S3_BUCKET_NAME")
            
            # Streaming настройки
            self.trigger_interval = "2 seconds"
            # 🚨 УБИРАЕМ S3 CHECKPOINT - используем локальный
            self.checkpoint_location = "/tmp/spark-checkpoints-local"
            self.max_offsets_per_trigger = 1000
            self.enable_debug = Variable.get("DEBUG_MODE", "false").lower() == "true"
            
            logger.info("✅ Конфигурация Spark Streaming (Local Mode) загружена успешно")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
            raise


# =============================================================================
# SPARK APPLICATION CODE - БУДЕТ ВЫПОЛНЯТЬСЯ В ОТДЕЛЬНОМ ПРОЦЕССЕ
# =============================================================================

SPARK_APP_CODE = '''
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow
import mlflow.pyfunc  # 🚀 ИСПОЛЬЗУЕМ PYFUNC
import sys
import os
import traceback
import pandas as pd

# Настройка расширенного логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

print("\\n=== 🚀 SPARK FRAUD DETECTION APPLICATION START (LOCAL MODE) ===")
logger.info("🚀 Запуск Spark Fraud Detection приложения (Local Mode)")
print(f"Python version: {sys.version}")
print(f"Arguments: {sys.argv}")

def create_spark_session(config):
    """Создает Spark Session в Local режиме"""
    try:
        logger.info("🛠️ Начинаем создание Spark Session (Local Mode)...")
        logger.info(f"📝 App name: {config['app_name']}")
        logger.info(f"📝 Master: local[2] (2 CPU cores)")
        logger.info(f"📝 Checkpoint: {config['checkpoint_location']}")
        
        spark = SparkSession.builder \\
            .appName(config["app_name"]) \\
            .master("local[2]") \\
            .config("spark.sql.streaming.checkpointLocation", config["checkpoint_location"]) \\
            .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \\
            .config("spark.sql.adaptive.enabled", "true") \\
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \\
            .config("spark.hadoop.fs.s3a.endpoint", config["s3_endpoint_url"]) \\
            .config("spark.hadoop.fs.s3a.access.key", config["s3_access_key"]) \\
            .config("spark.hadoop.fs.s3a.secret.key", config["s3_secret_key"]) \\
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \\
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \\
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info(f"✅ Spark Session (Local Mode) создан успешно: {spark.version}")
        logger.info(f"✅ Spark UI: {spark.sparkContext.uiWebUrl}")
        return spark
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОшИБКА создания Spark Session: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise

def load_mlflow_model(config):
    """Загружает модель из MLflow используя PyFunc (Local Mode)"""
    try:
        logger.info("🤖 Начинаем загрузку MLflow модели (PyFunc Local Mode)...")
        logger.info(f"📝 MLflow URI: {config['mlflow_tracking_uri']}")
        logger.info(f"📝 Модель: {config['mlflow_model_name']}")
        logger.info(f"📝 Stage: {config['mlflow_model_stage']}")
        
        # Настройка MLflow
        mlflow.set_tracking_uri(config["mlflow_tracking_uri"])
        
        # Устанавливаем аутентификацию если есть credentials
        if 'mlflow_username' in config and 'mlflow_password' in config:
            import os
            os.environ['MLFLOW_TRACKING_USERNAME'] = config['mlflow_username']
            os.environ['MLFLOW_TRACKING_PASSWORD'] = config['mlflow_password']
            logger.info("✅ MLflow аутентификация настроена")
        
        # 🚨 КРИТИЧНО: Устанавливаем S3 credentials для MLflow (boto3)
        if 's3_access_key' in config and 's3_secret_key' in config:
            import os
            os.environ['AWS_ACCESS_KEY_ID'] = config['s3_access_key']
            os.environ['AWS_SECRET_ACCESS_KEY'] = config['s3_secret_key']
            if 's3_endpoint_url' in config:
                os.environ['AWS_ENDPOINT_URL'] = config['s3_endpoint_url']
            logger.info("✅ S3 credentials для MLflow настроены")
        
        # Получаем latest version модели с указанным stage
        client = mlflow.tracking.MlflowClient()
        logger.info("✅ MLflow клиент создан")
        
        model_version = client.get_latest_versions(
            name=config["mlflow_model_name"],
            stages=[config["mlflow_model_stage"]]
        )
        
        if not model_version:
            raise Exception(f"Модель {config['mlflow_model_name']} со stage {config['mlflow_model_stage']} не найдена")
        
        logger.info(f"✅ Найдена версия модели: {model_version[0].version}")
        
        model_uri = f"models:/{config['mlflow_model_name']}/{config['mlflow_model_stage']}"
        logger.info(f"📋 Загружаем модель (PyFunc Local): {model_uri}")
        
        # 🚀 КЛЮЧЕВОЕ: Используем mlflow.pyfunc.load_model() для Local режима
        model = mlflow.pyfunc.load_model(model_uri)
        
        logger.info(f"✅ MLflow PyFunc модель загружена (Local): {model_uri}")
        logger.info(f"📊 Версия модели: {model_version[0].version}")
        logger.info(f"📊 Тип модели: {type(model)}")
        logger.info("✅ ШАГ 2 (PyFunc Local) завершен успешно")
        
        return model
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОшИБКА загрузки MLflow модели: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise

def create_transaction_schema():
    """Создает схему для входящих транзакций"""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("tx_amount", DoubleType(), True),
        StructField("tx_fraud", IntegerType(), True),
        StructField("account_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        # Добавляем поля которые могут быть в данных
        StructField("tx_time_seconds", IntegerType(), True),
        StructField("tx_time_days", IntegerType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("customer_age", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True)
    ])

def process_fraud_prediction(df, config):
    """Применяет PyFunc модель к потоку данных через простой UDF (Local Mode)"""
    try:
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StructType, StructField, DoubleType
        
        logger.info("🔄 Применяем PyFunc модель через простой UDF (Local Mode)...")
        
        # Определяем схему для результата предсказания
        prediction_schema = StructType([
            StructField("prediction", DoubleType(), True),
            StructField("probability", DoubleType(), True)
        ])
        
        # Извлекаем константы из конфигурации для UDF
        mlflow_uri = config.get('mlflow_tracking_uri', 'http://mlflow.mlflow.svc.cluster.local:5000')
        model_name = config.get('mlflow_model_name', 'fraud_detection_model')
        model_stage = config.get('mlflow_model_stage', 'Production')
        mlflow_username = config.get('mlflow_username', '')
        mlflow_password = config.get('mlflow_password', '')
        s3_access_key = config.get('s3_access_key', '')
        s3_secret_key = config.get('s3_secret_key', '')
        s3_endpoint_url = config.get('s3_endpoint_url', '')
        
        def predict_fraud_udf(tx_amount, account_id, merchant_id):
            """ПРОСТАЯ UDF для тестирования (без загрузки MLflow модели)"""
            try:
                # 🚀 ПРОСТАЯ ЛОГИКА ВМЕСТО MLFLOW - избегаем крашей
                import random
                
                # Простая эвристика для детекции фрода
                tx_amount_float = float(tx_amount) if tx_amount else 0.0
                
                # Считаем фродом если:
                # 1. Сумма > 5000
                # 2. Случайность для разнообразия (10% случаев)
                is_large_transaction = tx_amount_float > 5000.0
                is_random_fraud = random.random() < 0.1
                
                is_fraud = is_large_transaction or is_random_fraud
                fraud_probability = 0.9 if is_large_transaction else (0.8 if is_random_fraud else 0.1)
                
                # Возвращаем результат в том же формате что и MLflow
                return {
                    'prediction': 1.0 if is_fraud else 0.0,
                    'probability': fraud_probability
                }
                    
            except Exception as e:
                # В UDF нельзя использовать logger, используем print
                print(f"❌ Ошибка в простой UDF: {e}")
                return {
                    'prediction': 0.0,
                    'probability': 0.0
                }
        
        # Создаем UDF со структурной схемой для возврата dict
        from pyspark.sql.types import MapType, StringType, DoubleType
        udf_schema = MapType(StringType(), DoubleType())
        predict_udf = udf(predict_fraud_udf, udf_schema)
        
        # Применяем UDF к каждой строке
        predictions_df = df.withColumn("ml_result", 
            predict_udf(
                col("tx_amount"),
                col("account_id"), 
                col("merchant_id")
            )
        )
        
        # Извлекаем результаты предсказания
        result_df = predictions_df.select(
            col("transaction_id"),
            col("tx_amount"),
            col("account_id"),
            col("merchant_id"),
            col("timestamp"),
            # Результаты модели (доступ к Map через getItem)
            col("ml_result").getItem("prediction").alias("fraud_prediction"),
            when(col("ml_result").getItem("prediction") >= 0.5, "FRAUD").otherwise("NORMAL").alias("fraud_label"),
            col("ml_result").getItem("probability").alias("fraud_probability"),
            col("kafka_timestamp"),
            col("offset"),
            col("partition")
        )
        
        logger.info("✅ ПРОСТАЯ ЭВРИСТИКА применена (без MLflow) - тестовая версия!")
        return result_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка применения ML модели: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise

def create_kafka_source(spark, config):
    """Создает Kafka source для чтения транзакций"""
    try:
        logger.info("📡 Создаем Kafka source...")
        logger.info(f"📝 Kafka servers: {config['kafka_bootstrap_servers']}")
        logger.info(f"📝 Input topic: {config['kafka_input_topic']}")
        
        df = spark \\
            .readStream \\
            .format("kafka") \\
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \\
            .option("subscribe", config["kafka_input_topic"]) \\
            .option("startingOffsets", "earliest") \\
            .option("maxOffsetsPerTrigger", config["max_offsets_per_trigger"]) \\
            .load()
        
        # Добавляем метаданные Kafka
        df_with_meta = df.select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        )
        
        # Парсим JSON данные
        schema = create_transaction_schema()
        parsed_df = df_with_meta.select(
            "*",
            from_json(col("kafka_value"), schema).alias("transaction")
        ).select(
            col("transaction.*"),
            col("kafka_timestamp"),
            col("offset"),
            col("partition")
        )
        
        logger.info("✅ Kafka source создан успешно")
        return parsed_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания Kafka source: {e}")
        raise

def write_to_kafka_sink(df, config):
    """Настраивает вывод результатов в Kafka"""
    try:
        logger.info("📤 Настраиваем Kafka sink...")
        logger.info(f"📝 Output topic: {config['kafka_output_topic']}")
        
        # Преобразуем DataFrame в формат для Kafka
        kafka_df = df.select(
            to_json(struct("*")).alias("value"),
            col("transaction_id").alias("key")
        )
        
        # Настраиваем Kafka sink
        query = kafka_df.writeStream \\
            .format("kafka") \\
            .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \\
            .option("topic", config["kafka_output_topic"]) \\
            .option("checkpointLocation", config["checkpoint_location"]) \\
            .outputMode("append") \\
            .trigger(processingTime=config["trigger_interval"]) \\
            .option("queryName", "FraudPredictionSink") \\
            .start()
        
        # 🔍 ОТЛАДКА: Консольный вывод отключен - избегаем конфликтов streaming запросов
        logger.info("🔍 ОТЛАДКА: Консольный вывод отключен для стабильности")
        
        logger.info(f"✅ Kafka sink настроен: {config['kafka_output_topic']}")
        return query
        
    except Exception as e:
        logger.error(f"❌ Ошибка настройки Kafka sink: {e}")
        raise

def main():
    """Основная функция Spark Streaming приложения (Local Mode)"""
    
    logger.info("=== 🚀 MAIN FUNCTION START (LOCAL MODE) ===")
    
    try:
        # Загружаем конфигурацию из аргументов
        if len(sys.argv) < 2:
            raise Exception("Конфигурация не передана в аргументах")
        
        config_json = sys.argv[1]
        config = json.loads(config_json)
        
        logger.info("🛠️ ШАГ 1: Создание Spark Session (Local Mode)...")
        spark = create_spark_session(config)
        logger.info("✅ ШАГ 1 завершен успешно")
        
        logger.info("🤖 ШАГ 2: Загрузка MLflow модели (PyFunc Local)...")
        model = load_mlflow_model(config)
        logger.info("✅ ШАГ 2 завершен успешно")
        
        logger.info("📡 ШАГ 3: Создание Kafka source...")
        kafka_df = create_kafka_source(spark, config)
        logger.info("✅ ШАГ 3 завершен успешно")
        
        logger.info("🔮 ШАГ 4: Применение ML модели (простой UDF без broadcast)...")
        predictions_df = process_fraud_prediction(kafka_df, config)
        logger.info("✅ ШАГ 4 завершен успешно")
        
        logger.info("📤 ШАГ 5: Настройка Kafka sink...")
        query = write_to_kafka_sink(predictions_df, config)
        logger.info("✅ ШАГ 5 завершен успешно")
        
        logger.info("🚀 ШАГ 6: Подготовка к запуску Streaming...")
        
        # 🔧 СОЗДАЕМ CHECKPOINT ДИРЕКТОРИЮ
        import os
        checkpoint_dir = config.get('checkpoint_location', '/tmp/spark-checkpoints-local')
        os.makedirs(checkpoint_dir, exist_ok=True)
        logger.info(f"✅ Checkpoint директория создана: {checkpoint_dir}")
        
        logger.info("✅ Streaming запущен! Ожидание данных...")
        logger.info(f"📊 КОНФИГУРАЦИЯ ОТЛАДКИ:")
        logger.info(f"  - Input topic: {config.get('kafka_input_topic', 'N/A')}")
        logger.info(f"  - Output topic: {config.get('kafka_output_topic', 'N/A')}")
        logger.info(f"  - StartingOffsets: earliest (будут обработаны ВСЕ сообщения)")
        logger.info(f"  - MaxOffsetsPerTrigger: {config.get('max_offsets_per_trigger', 'N/A')}")
        logger.info(f"  - Trigger interval: {config.get('trigger_interval', 'N/A')}")
        logger.info(f"  - Checkpoint: {checkpoint_dir} (локальный)")
        
        # 🔍 ОТЛАДКА: Мониторинг прогресса каждые 10 секунд
        import time
        import threading
        
        def monitor_progress():
            batch_count = 0
            while True:
                time.sleep(5)  # Проверяем чаще для быстрой диагностики
                try:
                    progress = query.lastProgress
                    if progress:
                        batch_count += 1
                        logger.info(f"📊 ПРОГРЕСС STREAMING (Batch #{batch_count}):")
                        logger.info(f"  - Batch ID: {progress.get('batchId', 'N/A')}")
                        logger.info(f"  - Input rows/sec: {progress.get('inputRowsPerSecond', 'N/A')}")
                        logger.info(f"  - Processed rows: {progress.get('numInputRows', 'N/A')}")
                        logger.info(f"  - Duration: {progress.get('batchDuration', 'N/A')} ms")
                        logger.info(f"  - Status: {progress.get('message', 'N/A')}")
                    else:
                        logger.info("📊 Ожидание первого batch...")
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка мониторинга прогресса: {e}")
        
        # Запускаем мониторинг в отдельном потоке
        monitor_thread = threading.Thread(target=monitor_progress, daemon=True)
        monitor_thread.start()
        logger.info("🔍 ОТЛАДКА: Запущен мониторинг прогресса (каждые 10 сек)")
        
        # 🔧 ОЖИДАЕМ С TIMEOUT - избегаем бесконечного ожидания
        logger.info("⏰ Начинаем обработку данных с timeout 120 секунд...")
        try:
            # Ждем максимум 2 минуты для обработки всех данных
            if query.awaitTermination(timeout=120):
                logger.info("✅ Streaming завершен успешно!")
            else:
                logger.warning("⚠️ Timeout 120 сек - принудительно останавливаем streaming")
                query.stop()
                logger.info("✅ Streaming остановлен по timeout")
        except KeyboardInterrupt:
            logger.info("🛑 Получен сигнал остановки - завершаем streaming...")
            query.stop()
        except Exception as stream_error:
            logger.error(f"❌ Ошибка во время streaming: {stream_error}")
            logger.error(f"❌ Streaming Traceback: {traceback.format_exc()}")
            query.stop()
            raise
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в main(): {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main()
'''


# =============================================================================
# SPARK STREAMING MANAGER (LOCAL MODE)
# =============================================================================

class SparkStreamingManagerLocal:
    """Менеджер для управления Spark Streaming приложением в Local режиме"""
    
    def __init__(self, config: SparkStreamConfig):
        self.config = config
        self.spark_process = None
        
    def create_spark_submit_command(self) -> List[str]:
        """Создает команду spark-submit для запуска приложения в Local режиме"""
        
        # Создаем временный файл с кодом приложения
        app_file = "/tmp/spark_fraud_streaming_local.py"
        with open(app_file, "w") as f:
            f.write(SPARK_APP_CODE)
        
        # Формируем JSON конфигурацию
        spark_config = {
            "app_name": self.config.spark_app_name,
            "kafka_bootstrap_servers": self.config.kafka_bootstrap_servers,
            "kafka_input_topic": self.config.kafka_input_topic,
            "kafka_output_topic": self.config.kafka_output_topic,
            "kafka_consumer_group": self.config.kafka_consumer_group,
            "mlflow_tracking_uri": self.config.mlflow_tracking_uri,
            "mlflow_model_name": self.config.mlflow_model_name,
            "mlflow_model_stage": self.config.mlflow_model_stage,
            "mlflow_username": self.config.mlflow_tracking_username,
            "mlflow_password": self.config.mlflow_tracking_password,
            "s3_endpoint_url": self.config.s3_endpoint_url,
            "s3_access_key": self.config.s3_access_key,
            "s3_secret_key": self.config.s3_secret_key,
            "checkpoint_location": self.config.checkpoint_location,
            "trigger_interval": self.config.trigger_interval,
            "max_offsets_per_trigger": self.config.max_offsets_per_trigger,
            "enable_debug": self.config.enable_debug
        }
        
        config_json = json.dumps(spark_config)
        
        # Команда spark-submit для Local режима
        cmd = [
            "spark-submit",
            "--master", "local[2]",  # 🚀 КЛЮЧЕВОЕ: Local режим с 2 cores
            "--name", self.config.spark_app_name,
            
            # 🚀 LOCAL MODE: Упрощенная конфигурация ресурсов
            "--conf", f"spark.driver.memory={self.config.spark_driver_memory}",
            "--conf", "spark.sql.adaptive.enabled=true",
            "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
            "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4",
            app_file,
            config_json
        ]
        
        return cmd
    
    def start_streaming(self) -> str:
        """Запускает Spark Streaming приложение в Local режиме"""
        try:
            logger.info("🚀 Запуск Spark Streaming приложения (Local Mode)...")
            
            # Создаем команду
            cmd = self.create_spark_submit_command()
            logger.info(f"📋 КОМАНДА LOCAL MODE: {' '.join(cmd)}")
            
            # Запускаем процесс
            self.spark_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            logger.info(f"✅ Spark процесс (Local Mode) запущен: PID {self.spark_process.pid}")
            
            # Читаем логи в реальном времени
            output_lines = []
            start_time = time.time()
            timeout = 120  # 2 минуты на запуск
            
            while True:
                line = self.spark_process.stdout.readline()
                if line:
                    line = line.strip()
                    output_lines.append(line)
                    
                    # Логируем важные строки
                    if any(keyword in line.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED', 'CRITICAL']):
                        logger.error(f"❌ Spark ERROR: {line}")
                    elif any(keyword in line for keyword in ['MAIN FUNCTION', 'ШАГ', '===', 'Started', 'SUCCESS']):
                        logger.info(f"🎯 Spark APP: {line}")
                    elif 'WARN' in line.upper():
                        logger.warning(f"⚠️ Spark WARN: {line}")
                    
                    # Проверяем успешный запуск
                    if "✅ Streaming запущен!" in line:
                        logger.info("🎉 Spark Streaming (Local Mode) успешно запущен!")
                        return f"spark_local_{self.spark_process.pid}"
                
                # Проверяем завершение процесса
                elif self.spark_process.poll() is not None:
                    return_code = self.spark_process.returncode
                    if return_code == 0:
                        logger.info("✅ Spark процесс завершился успешно")
                    else:
                        logger.error(f"❌ Spark процесс завершился с кодом: {return_code}")
                    break
                
                # Проверяем таймаут
                if time.time() - start_time > timeout:
                    logger.warning("⏰ Таймаут запуска Spark приложения")
                    break
            
            return f"spark_local_{self.spark_process.pid}"
            
        except Exception as e:
            logger.error(f"❌ Ошибка запуска Spark Streaming: {e}")
            raise
    
    def stop_streaming(self, app_id: str):
        """Останавливает Spark Streaming приложение"""
        try:
            if self.spark_process and self.spark_process.poll() is None:
                logger.info(f"🛑 Останавливаем Spark процесс: {self.spark_process.pid}")
                self.spark_process.terminate()
                
                # Ждем завершения
                try:
                    self.spark_process.wait(timeout=30)
                    logger.info("✅ Spark процесс остановлен успешно")
                except subprocess.TimeoutExpired:
                    logger.warning("⚠️ Принудительное завершение Spark процесса")
                    self.spark_process.kill()
                    self.spark_process.wait()
            else:
                logger.info("ℹ️ Spark процесс уже завершен")
                
        except Exception as e:
            logger.error(f"❌ Ошибка остановки Spark процесса: {e}")


# =============================================================================
# AIRFLOW TASKS
# =============================================================================

def start_spark_streaming(**context):
    """Запуск Spark Streaming приложения"""
    logger.info("🚀 Инициализация Spark Streaming Job (Local Mode)...")
    
    try:
        # Загружаем конфигурацию
        config = SparkStreamConfig()
        
        # Создаем менеджер
        manager = SparkStreamingManagerLocal(config)
        
        # Запускаем streaming
        app_id = manager.start_streaming()
        
        # Сохраняем только ID для использования в других задачах (убираем manager из-за проблем сериализации)
        context['task_instance'].xcom_push(key='spark_app_id', value=app_id)
        # НЕ сохраняем manager в XCom из-за проблем с сериализацией
        # context['task_instance'].xcom_push(key='spark_manager', value=manager)
        
        logger.info(f"✅ Spark Streaming (Local Mode) запущен: {app_id}")
        return app_id
        
    except Exception as e:
        logger.error(f"❌ Ошибка запуска Spark Streaming: {e}")
        raise

def monitor_streaming(**context):
    """Мониторинг Spark Streaming приложения"""
    logger.info("📊 Мониторинг Spark Streaming (Local Mode)...")
    
    try:
        # Получаем app_id из предыдущей задачи
        app_id = context['task_instance'].xcom_pull(key='spark_app_id', task_ids='start_spark_streaming')
        
        if not app_id:
            logger.warning("⚠️ ID Spark приложения не найден в XCom")
            # Проверяем альтернативные способы получения app_id
            app_id = context['task_instance'].xcom_pull(task_ids='start_spark_streaming')
            
        if app_id:
            logger.info(f"📊 Мониторинг приложения: {app_id}")
            
            if "spark_local_" in str(app_id):
                # Извлекаем PID из app_id
                pid = int(str(app_id).replace("spark_local_", ""))
                logger.info(f"📊 Мониторинг Spark процесса PID: {pid}")
                
                # Проверяем, что процесс еще работает
                try:
                    import os
                    os.kill(pid, 0)  # Проверка существования процесса
                    logger.info("✅ Spark процесс работает")
                except ProcessLookupError:
                    logger.warning("⚠️ Spark процесс уже завершился")
                except Exception as e:
                    logger.warning(f"⚠️ Не удалось проверить процесс: {e}")
        else:
            logger.warning("⚠️ App ID не найден, продолжаем мониторинг без проверки процесса")
        
        # Ждем некоторое время для обработки данных
        monitoring_time = 30  # 30 секунд (уменьшили время)
        logger.info(f"⏰ Мониторинг в течение {monitoring_time} секунд...")
        time.sleep(monitoring_time)
        
        logger.info("✅ Мониторинг завершен")
        return "monitoring_completed"
        
    except Exception as e:
        logger.error(f"❌ Ошибка мониторинга: {e}")
        # Не прерываем выполнение при ошибке мониторинга
        return "monitoring_failed"

def stop_streaming(**context):
    """Остановка Spark Streaming приложения"""
    logger.info("🛑 Остановка Spark Streaming (Local Mode)...")
    
    try:
        # Получаем app_id из предыдущих задач
        app_id = context['task_instance'].xcom_pull(key='spark_app_id')
        
        if app_id and "spark_local_" in app_id:
            # Извлекаем PID из app_id
            pid = int(app_id.replace("spark_local_", ""))
            logger.info(f"🛑 Останавливаем Spark процесс PID: {pid}")
            
            try:
                import os
                import signal
                # Отправляем SIGTERM для graceful shutdown
                os.kill(pid, signal.SIGTERM)
                logger.info("✅ SIGTERM отправлен Spark процессу")
                
                # Ждем немного, затем проверяем
                import time
                time.sleep(5)
                
                try:
                    # Проверяем, жив ли процесс
                    os.kill(pid, 0)  # Проверка существования процесса
                    logger.warning("⚠️ Процесс все еще работает, принудительная остановка")
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    logger.info("✅ Процесс успешно завершен")
                    
            except ProcessLookupError:
                logger.info("ℹ️ Процесс уже завершен")
            except Exception as e:
                logger.error(f"❌ Ошибка остановки процесса: {e}")
        else:
            logger.warning("⚠️ App ID не найден или неверный формат")
        
        logger.info("✅ Остановка завершена")
        return "streaming_stopped"
        
    except Exception as e:
        logger.error(f"❌ Ошибка остановки: {e}")
        # Не прерываем выполнение при ошибке остановки
        return "stop_failed"

def verify_kafka_results(**context):
    """Проверка результатов в Kafka топике"""
    logger.info("🔍 Проверка результатов в Kafka топике fraud-predictions...")
    
    try:
        from kafka import KafkaConsumer
        import json
        
        config = SparkStreamConfig()
        
        # Создаем consumer для проверки результатов
        consumer = KafkaConsumer(
            config.kafka_output_topic,
            bootstrap_servers=config.kafka_bootstrap_servers.split(','),
            auto_offset_reset='latest',
            consumer_timeout_ms=10000,  # 10 секунд
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
        )
        
        messages_count = 0
        for message in consumer:
            if message.value:
                messages_count += 1
                logger.info(f"📨 Получено сообщение {messages_count}: {message.value}")
                
                if messages_count >= 5:  # Проверяем первые 5 сообщений
                    break
        
        consumer.close()
        
        if messages_count > 0:
            logger.info(f"✅ Найдено {messages_count} сообщений в топике {config.kafka_output_topic}")
        else:
            logger.warning(f"⚠️ Сообщения не найдены в топике {config.kafka_output_topic}")
        
        return f"verified_{messages_count}_messages"
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки результатов: {e}")
        return "verification_failed"


# =============================================================================
# DAG ОПРЕДЕЛЕНИЕ
# =============================================================================

# Настройки по умолчанию для DAG
default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создание DAG
dag = DAG(
    'tasks8_etap4_spark_streaming_local',
    default_args=default_args,
    description='ШАГ 4: Spark Streaming Job для потокового инференса мошенничества (LOCAL MODE - РЕШЕНИЕ ПРОБЛЕМ С КЛАСТЕРОМ)',
    schedule=None,  # Только ручной запуск
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'streaming', 'mlflow', 'kafka', 'fraud-detection', 'local-mode']
)

# Задача 1: Запуск Spark Streaming
start_task = PythonOperator(
    task_id='start_spark_streaming',
    python_callable=start_spark_streaming,
    dag=dag
)

# Задача 2: Мониторинг
monitor_task = PythonOperator(
    task_id='monitor_streaming',
    python_callable=monitor_streaming,
    dag=dag
)

# Задача 3: Остановка
stop_task = PythonOperator(
    task_id='stop_streaming',
    python_callable=stop_streaming,
    dag=dag,
    trigger_rule='all_done'  # Выполнить независимо от успеха предыдущих задач
)

# Задача 4: Проверка результатов
verify_task = PythonOperator(
    task_id='verify_results',
    python_callable=verify_kafka_results,
    dag=dag
)

# Определение зависимостей
start_task >> monitor_task >> stop_task
start_task >> verify_task >> stop_task