"""
ШАГ 4: Spark Streaming Job для потокового инференса модели мошенничества

Этот DAG управляет Spark Streaming приложением которое:
- Читает транзакции из Kafka топика transactions-input
- Загружает MLflow модель fraud_detection_model с stage Production
- Применяет модель для предсказания мошенничества в режиме реального времени
- Записывает результаты в Kafka топик fraud-predictions
- Обеспечивает отказоустойчивость через checkpoints

Автор: MLOps Task 8
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
    import mlflow.spark
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
            
            # Spark настройки
            self.spark_app_name = Variable.get("SPARK_APP_NAME", "StreamingFraudInference")
            self.spark_master = Variable.get("SPARK_MASTER", "k8s://https://kubernetes.default.svc:443")
            self.spark_namespace = Variable.get("SPARK_NAMESPACE", "spark")
            self.spark_driver_memory = Variable.get("SPARK_DRIVER_MEMORY", "1g")
            self.spark_executor_memory = Variable.get("SPARK_EXECUTOR_MEMORY", "1g") 
            self.spark_executor_cores = Variable.get("SPARK_EXECUTOR_CORES", "1")
            self.spark_max_executors = Variable.get("SPARK_MAX_EXECUTORS", "2")
            
            # S3/MinIO для checkpoints
            self.s3_endpoint_url = Variable.get("S3_ENDPOINT_URL")
            self.s3_access_key = Variable.get("S3_ACCESS_KEY")
            self.s3_secret_key = Variable.get("S3_SECRET_KEY")
            self.s3_bucket_name = Variable.get("S3_BUCKET_NAME")
            
            # Streaming настройки
            self.trigger_interval = "2 seconds"
            self.checkpoint_location = f"s3a://{self.s3_bucket_name}/spark-checkpoints/fraud-streaming"
            self.max_offsets_per_trigger = 1000
            self.enable_debug = Variable.get("DEBUG_MODE", "false").lower() == "true"
            
            logger.info("✅ Конфигурация Spark Streaming загружена успешно")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
            raise


# =============================================================================
# SPARK STREAMING APPLICATION CODE
# =============================================================================

SPARK_APP_CODE = '''
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import mlflow
import mlflow.spark
import sys
import os
import traceback

# Настройка расширенного логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    stream=sys.stdout  # Принудительно в stdout
)
logger = logging.getLogger(__name__)

print("\\n=== 🚀 SPARK FRAUD DETECTION APPLICATION START ===")
logger.info("🚀 Запуск Spark Fraud Detection приложения")
print(f"Python version: {sys.version}")
print(f"Arguments: {sys.argv}")

def create_spark_session(config):
    """Создает Spark Session с оптимизированными настройками"""
    try:
        logger.info("🛠️ Начинаем создание Spark Session...")
        logger.info(f"📝 App name: {config['app_name']}")
        logger.info(f"📝 Checkpoint: {config['checkpoint_location']}")
        logger.info(f"📝 S3 endpoint: {config['s3_endpoint_url']}")
        
        spark = SparkSession.builder \\
            .appName(config["app_name"]) \\
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
        logger.info(f"✅ Spark Session создан успешно: {spark.version}")
        logger.info(f"✅ Spark UI: {spark.sparkContext.uiWebUrl}")
        return spark
        
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОшИБКА создания Spark Session: {e}")
        logger.error(f"❌ Traceback: {traceback.format_exc()}")
        raise

def load_mlflow_model(config):
    """Загружает модель из MLflow"""
    try:
        logger.info("🤖 Начинаем загрузку MLflow модели...")
        logger.info(f"📝 MLflow URI: {config['mlflow_tracking_uri']}")
        logger.info(f"📝 Модель: {config['mlflow_model_name']}")
        logger.info(f"📝 Stage: {config['mlflow_model_stage']}")
        
        # Устанавливаем MLflow tracking URI
        mlflow.set_tracking_uri(config["mlflow_tracking_uri"])
        logger.info("✅ MLflow tracking URI установлен")
        
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
            logger.info(f"📋 AWS_ACCESS_KEY_ID: {config['s3_access_key'][:4]}***")
            logger.info(f"📋 AWS_ENDPOINT_URL: {config.get('s3_endpoint_url', 'default')}")
        else:
            logger.warning("⚠️ S3 credentials НЕ найдены в конфигурации!")
        
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
        logger.info(f"📋 Загружаем модель: {model_uri}")
        
        model = mlflow.spark.load_model(model_uri)
        
        logger.info(f"✅ MLflow модель загружена: {model_uri}")
        logger.info(f"📊 Версия модели: {model_version[0].version}")
        logger.info(f"📊 Тип модели: {type(model)}")
        logger.info("✅ ШАГ 2 завершен успешно")
        
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

def process_fraud_prediction(df, model):
    """Применяет модель к потоку данных"""
    try:
        # Применяем модель для предсказания
        predictions_df = model.transform(df)
        
        # Создаем выходной формат с результатами предсказания
        result_df = predictions_df.select(
            col("transaction_id"),
            col("tx_amount"),
            col("account_id"),
            col("merchant_id"),
            col("timestamp"),
            # Результаты модели
            col("prediction").alias("fraud_prediction"),
            when(col("prediction") == 1.0, "FRAUD").otherwise("NORMAL").alias("fraud_label"),
            col("probability").alias("fraud_probability"),
            col("kafka_timestamp"),
            col("offset"),
            col("partition")
        )
        
        logger.info("✅ ML модель применена к данным")
        return result_df
        
    except Exception as e:
        logger.error(f"❌ Ошибка применения ML модели: {e}")
        raise

def write_to_kafka_sink(df, config):
    """Настраивает вывод результатов в Kafka"""
    try:
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
            .start()
        
        logger.info(f"✅ Kafka sink настроен: {config['kafka_output_topic']}")
        return query
        
    except Exception as e:
        logger.error(f"❌ Ошибка настройки Kafka sink: {e}")
        raise

def main():
    """Основная функция Spark Streaming приложения"""
    
    print("\\n=== 🚀 MAIN FUNCTION START ===")
    logger.info("🚀 ОСНОВНАЯ функция main() запущена")
    
    try:
        # Получаем конфигурацию из аргументов командной строки
        logger.info("📋 Парсинг аргументов командной строки...")
        logger.info(f"📋 sys.argv: {sys.argv}")
        
        if len(sys.argv) <= 1:
            raise Exception("Не передана конфигурация JSON в качестве аргумента")
        
        config_json = sys.argv[1]
        logger.info(f"📋 Конфигурация JSON: {config_json[:200]}...")  # Первые 200 символов
        
        config = json.loads(config_json)
        logger.info("✅ Конфигурация JSON успешно распарсена")
        logger.info(f"📋 App name: {config.get('app_name', 'N/A')}")
        logger.info(f"📋 Kafka servers: {config.get('kafka_bootstrap_servers', 'N/A')}")
        logger.info(f"📋 Input topic: {config.get('kafka_input_topic', 'N/A')}")
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА парсинга JSON конфигурации: {e}")
        logger.error(f"❌ JSON строка: {sys.argv[1] if len(sys.argv) > 1 else 'None'}")
        raise
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА получения конфигурации: {e}")
        raise
    
    logger.info("🚀 Начинаем инициализацию Spark Streaming приложения")
    
    spark = None
    try:
        # ШАГ 1: Создаем Spark Session
        logger.info("🛠️ ШАГ 1: Создание Spark Session...")
        spark = create_spark_session(config)
        logger.info("✅ ШАГ 1 завершен успешно")
        
        # ШАГ 2: Загружаем MLflow модель
        logger.info("🤖 ШАГ 2: Загрузка MLflow модели...")
        model = load_mlflow_model(config)
        logger.info("✅ ШАГ 2 завершен успешно")
        
        # ШАГ 3: Создаем схему
        logger.info("📋 ШАГ 3: Создание схемы данных...")
        schema = create_transaction_schema()
        logger.info(f"✅ ШАГ 3: Схема создана: {len(schema.fields)} полей")
        
        # ШАГ 4: Подключаем Kafka source
        logger.info("🌊 ШАГ 4: Подключение к Kafka source...")
        logger.info(f"📋 Kafka servers: {config['kafka_bootstrap_servers']}")
        logger.info(f"📋 Input topic: {config['kafka_input_topic']}")
        logger.info(f"📋 Consumer group: {config['kafka_consumer_group']}")
        logger.info(f"📋 Max offsets per trigger: {config.get('max_offsets_per_trigger', 1000)}")
        
        try:
            logger.info("🔧 Создаем Kafka DataStreamReader...")
            
            kafka_df = spark \\
                .readStream \\
                .format("kafka") \\
                .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \\
                .option("subscribe", config["kafka_input_topic"]) \\
                .option("kafka.group.id", config["kafka_consumer_group"]) \\
                .option("startingOffsets", "latest") \\
                .option("maxOffsetsPerTrigger", config.get("max_offsets_per_trigger", 1000)) \\
                .option("failOnDataLoss", "false") \\
                .option("kafka.session.timeout.ms", "30000") \\
                .option("kafka.request.timeout.ms", "40000") \\
                .option("kafka.connection.max.idle.ms", "300000") \\
                .load()
            
            logger.info("✅ DataStreamReader создан успешно")
            
            # Проверяем что можем прочитать схему
            logger.info("🔍 Проверяем схему Kafka источника...")
            kafka_schema = kafka_df.schema
            logger.info(f"📊 Kafka schema: {kafka_schema}")
            
            logger.info(f"✅ ШАГ 4: Kafka source подключен: {config['kafka_input_topic']}")
            
        except Exception as kafka_error:
            logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА подключения к Kafka: {kafka_error}")
            logger.error(f"❌ Kafka servers: {config['kafka_bootstrap_servers']}")
            logger.error(f"❌ Topic: {config['kafka_input_topic']}")
            logger.error(f"❌ Consumer group: {config['kafka_consumer_group']}")
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            raise Exception(f"Не удалось подключиться к Kafka: {kafka_error}")
        
        # ШАГ 5: Парсим JSON
        logger.info("📋 ШАГ 5: Парсинг JSON данных из Kafka...")
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
            col("offset"),
            col("partition")
        ).select("data.*", "kafka_timestamp", "offset", "partition")
        logger.info("✅ ШАГ 5: JSON парсер настроен")
        
        # ШАГ 6: Применяем модель
        logger.info("🤖 ШАГ 6: Применение ML модели...")
        predictions_df = process_fraud_prediction(parsed_df, model)
        logger.info("✅ ШАГ 6: ML pipeline настроен")
        
        # ШАГ 7: Настраиваем Kafka sink
        logger.info("🌊 ШАГ 7: Настройка Kafka sink...")
        logger.info(f"📋 Output topic: {config['kafka_output_topic']}")
        output_query = write_to_kafka_sink(predictions_df, config)
        logger.info("✅ ШАГ 7: Kafka sink настроен")
        
        # ШАГ 8: Опциональный debug
        debug_query = None
        if config.get("enable_debug", False):
            logger.info("🔍 ШАГ 8: Настройка debug консоли...")
            debug_query = predictions_df.writeStream \\
                .outputMode("append") \\
                .format("console") \\
                .option("truncate", "false") \\
                .option("numRows", 10) \\
                .trigger(processingTime=config["trigger_interval"]) \\
                .start()
            logger.info("✅ ШАГ 8: Debug консоль включена")
        else:
            logger.info("🔍 ШАГ 8: Debug отключен")
        
        logger.info("✅ ВСЕ ШАГИ УСПЕШНО ЗАВЕРШЕНЫ!")
        logger.info("🚀 Spark Streaming запущен успешно!")
        logger.info("📊 Ожидание данных из Kafka...")
        print("=== ✅ STREAMING STARTED SUCCESSFULLY ===")
        
        # ШАГ 9: Ожидаем завершения
        logger.info("⏳ Ожидание завершения streaming job...")
        output_query.awaitTermination()
        
        if debug_query:
            debug_query.stop()
            
    except Exception as e:
        logger.error(f"❌ КРИТИЧЕСКАЯ ОшИБКА в main(): {e}")
        logger.error(f"❌ Полный traceback: {traceback.format_exc()}")
        print(f"=== ❌ CRITICAL ERROR: {e} ===")
        raise
    finally:
        if spark is not None:
            logger.info("🛑 Останавливаем Spark Session...")
            spark.stop()
            logger.info("✅ Spark Session остановлен")
        print("=== 🛑 MAIN FUNCTION END ===")

if __name__ == "__main__":
    main()
'''


# =============================================================================
# SPARK STREAMING MANAGER
# =============================================================================

class SparkStreamingManager:
    """Менеджер для управления Spark Streaming приложением"""
    
    def __init__(self, config: SparkStreamConfig):
        self.config = config
        self.spark_process = None
        self.app_id = None
        
    def create_spark_submit_command(self) -> List[str]:
        """Создает команду spark-submit для запуска приложения"""
        
        # Создаем временный файл с кодом приложения
        app_file = "/tmp/spark_fraud_streaming.py"
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
        
        # Команда spark-submit
        cmd = [
            "spark-submit",
            "--master", self.config.spark_master,
            "--deploy-mode", "client",
            "--name", self.config.spark_app_name,

            # 🚀 ВАРИАНТ A: ГАРАНТИРОВАННЫЕ РЕСУРСЫ для MLflow модели в кластере
            "--conf", "spark.driver.memory=1g",
            "--conf", "spark.executor.memory=1g", 
            "--conf", "spark.executor.cores=2",
            "--conf", "spark.dynamicAllocation.maxExecutors=2",
            "--conf", "spark.dynamicAllocation.minExecutors=1",  # ❗ КЛЮЧЕВОЕ: Гарантируем минимум 1 executor
            "--conf", "spark.dynamicAllocation.initialExecutors=1",  # ❗ КЛЮЧЕВОЕ: Сразу запускаем 1 executor 
            "--conf", "spark.dynamicAllocation.enabled=true",
            "--conf", "spark.dynamicAllocation.shuffleTracking.enabled=true",
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4",
            # Убираем --py-files так как pyspark.zip не существует в контейнере Airflow
            # PySpark уже установлен через pip и доступен в PYTHONPATH
            app_file,
            config_json
        ]
        
        return cmd
    
    def start_streaming(self) -> str:
        """Запускает Spark Streaming приложение"""
        try:
            logger.info("🚀 Запуск Spark Streaming приложения...")
            
            # Создаем команду
            cmd = self.create_spark_submit_command()
            logger.info(f"📋 ПОЛНАЯ КОМАНДА: {' '.join(cmd)}")
            
            # Создаем файл для сохранения всех логов
            import threading
            self.log_file_path = "/tmp/spark_streaming_full_logs.txt"
            logger.info(f"📝 Все логи Spark будут сохранены в: {self.log_file_path}")
            
            # Запускаем процесс
            self.spark_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            logger.info(f"✅ Spark процесс запущен: PID {self.spark_process.pid}")
            
            # Создаем поток для непрерывного чтения и сохранения логов
            self.all_logs = []
            self.log_lock = threading.Lock()
            
            def continuous_log_reader():
                """Непрерывно читает и сохраняет все логи Spark"""
                with open(self.log_file_path, "w") as log_file:
                    while True:
                        line = self.spark_process.stdout.readline()
                        if not line:
                            if self.spark_process.poll() is not None:
                                # Процесс завершился
                                logger.error(f"❌ Spark процесс завершился с кодом: {self.spark_process.returncode}")
                                break
                            continue
                        
                        line = line.strip()
                        with self.log_lock:
                            self.all_logs.append(line)
                            log_file.write(line + "\n")
                            log_file.flush()
                        
                        # Логируем важные строки
                        if any(keyword in line.upper() for keyword in ['ERROR', 'EXCEPTION', 'FAILED', 'CRITICAL', 'TRACEBACK']):
                            logger.error(f"❌ Spark ERROR: {line}")
                        elif any(keyword in line for keyword in ['SPARK FRAUD DETECTION', 'MAIN FUNCTION', 'ШАГ', '===', 'Started', 'Creating']):
                            logger.info(f"🎯 Spark APP: {line}")
                        elif 'WARN' in line.upper():
                            logger.warning(f"⚠️ Spark WARN: {line}")
            
            # Запускаем фоновый поток для чтения логов
            log_thread = threading.Thread(target=continuous_log_reader)
            log_thread.daemon = True
            log_thread.start()
            
            # Читаем начальные логи для диагностики
            logger.info("📋 Читаем начальные логи Spark приложения...")
            initial_logs = []
            start_time = time.time()
            
            while time.time() - start_time < 90:  # 90 секунд на инициализацию чтобы увидеть больше логов
                if self.spark_process.poll() is not None:
                    # Процесс завершился
                    remaining_output = self.spark_process.stdout.read()
                    logger.error(f"❌ Spark процесс завершился неожиданно!")
                    logger.error(f"❌ STDOUT/STDERR: {remaining_output}")
                    return "streaming_failed"
                
                # Читаем доступные строки (упрощенный подход)
                try:
                    # Проверяем есть ли данные для чтения (неблокирующий режим)
                    import fcntl
                    import os
                    fd = self.spark_process.stdout.fileno()
                    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                    
                    try:
                        line = self.spark_process.stdout.readline()
                        if line:
                            line = line.strip()
                            initial_logs.append(line)
                            logger.info(f"📊 Spark: {line}")
                            
                            # Проверяем на критические ошибки
                            if any(error in line.upper() for error in ['EXCEPTION', 'ERROR', 'FAILED TO']):
                                logger.error(f"❌ КРИТИЧЕСКАЯ ОШИБКА в Spark: {line}")
                            
                            # Проверяем успешный старт
                            if 'Started streaming query' in line or 'StreamingQuery started' in line:
                                logger.info("✅ Spark Streaming query запущен успешно!")
                                return "streaming_started"
                    except (BlockingIOError, IOError):
                        # Нет данных для чтения - это нормально
                        pass
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка настройки неблокирующего чтения: {e}")
                    # Fallback к простому readline
                    try:
                        line = self.spark_process.stdout.readline()
                        if line:
                            line = line.strip()
                            logger.info(f"📊 Spark: {line}")
                    except:
                        pass
                        
                time.sleep(0.5)
            
            # Проверяем финальный статус после таймаута
            if self.spark_process.poll() is None:
                logger.info("✅ Spark процесс все еще работает после 45 секунд")
                
                # Выводим последние логи из накопленного буфера
                if hasattr(self, 'all_logs') and self.all_logs:
                    num_lines = 300 if len(self.all_logs) > 300 else len(self.all_logs)
                    logger.info(f"📋 Последние {num_lines} строк логов Spark:")
                    logger.info("=" * 60)
                    for line in self.all_logs[-20:]:
                        logger.info(f"  {line}")
                    logger.info("=" * 60)
                
                return "streaming_started"
            else:
                logger.error(f"❌ Spark процесс завершился во время инициализации с кодом: {self.spark_process.returncode}")
                
                # Выводим все накопленные логи
                if hasattr(self, 'all_logs') and self.all_logs:
                    logger.error(f"📋 ВСЕ ЛОГИ SPARK (всего {len(self.all_logs)} строк):")
                    logger.error("=" * 60)
                    for line in self.all_logs:
                        logger.error(f"  {line}")
                    logger.error("=" * 60)
                    
                    # Ищем ошибки
                    error_lines = [l for l in self.all_logs if any(e in l.upper() for e in ['ERROR', 'EXCEPTION', 'FAILED', 'TRACEBACK'])]
                    if error_lines:
                        logger.error("❌ НАЙДЕНЫ ОШИБКИ:")
                        for line in error_lines:
                            logger.error(f"  >>> {line}")
                
                return "streaming_failed"
                
        except Exception as e:
            logger.error(f"❌ Ошибка запуска Spark Streaming: {e}")
            return "streaming_error"
    
    def monitor_streaming(self, duration_minutes: int = 10) -> Dict:
        """Мониторинг работы Spark Streaming приложения"""
        try:
            if not self.spark_process or self.spark_process.poll() is not None:
                logger.error("❌ Spark процесс не запущен или завершился")
                
                # УСИЛЕННАЯ ДИАГНОСТИКА ЛОГОВ
                logger.info("🔍 ДИАГНОСТИКА: Проверяем наличие файла с логами...")
                
                # Проверяем разные возможные пути к файлу
                possible_log_paths = [
                    "/tmp/spark_streaming_full_logs.txt",
                    getattr(self, 'log_file_path', None),
                    "/tmp/spark_streaming_logs.txt"
                ]
                
                for log_path in possible_log_paths:
                    if log_path:
                        logger.info(f"🔍 Проверяем файл: {log_path}")
                        try:
                            import os
                            if os.path.exists(log_path):
                                file_size = os.path.getsize(log_path)
                                logger.info(f"✅ Файл найден! Размер: {file_size} байт")
                                
                                with open(log_path, 'r') as f:
                                    saved_logs = f.readlines()
                                    logger.info(f"📊 Найдено {len(saved_logs)} строк логов")
                                    
                                    if saved_logs:
                                        # Выводим ВСЕ логи, не только последние 50
                                        logger.info("📋 ВСЕ ЛОГИ SPARK:")
                                        logger.info("=" * 80)
                                        for i, line in enumerate(saved_logs, 1):
                                            logger.info(f"{i:4d}: {line.strip()}")
                                        logger.info("=" * 80)
                                        
                                        # Ищем ошибки
                                        error_lines = [l for l in saved_logs if any(e in l.upper() for e in ['ERROR', 'EXCEPTION', 'FAILED', 'TRACEBACK'])]
                                        if error_lines:
                                            logger.error("❌ НАЙДЕНЫ ОШИБКИ В ЛОГАХ:")
                                            logger.error("=" * 80)
                                            for line in error_lines:
                                                logger.error(f"  >>> {line.strip()}")
                                            logger.error("=" * 80)
                                    else:
                                        logger.warning("⚠️ Файл пустой!")
                                break
                            else:
                                logger.warning(f"⚠️ Файл не найден: {log_path}")
                        except Exception as e:
                            logger.error(f"❌ Ошибка чтения {log_path}: {e}")
                else:
                    logger.error("❌ НИ ОДИН ФАЙЛ С ЛОГАМИ НЕ НАЙДЕН!")
                
                # Дополнительно проверяем все логи из памяти
                if hasattr(self, 'all_logs') and self.all_logs:
                    logger.info(f"📊 Найдены логи в памяти: {len(self.all_logs)} строк")
                    logger.info("📋 ЛОГИ ИЗ ПАМЯТИ:")
                    logger.info("=" * 80)
                    for i, line in enumerate(self.all_logs, 1):
                        logger.info(f"{i:4d}: {line}")
                    logger.info("=" * 80)
                else:
                    logger.warning("⚠️ Логи в памяти не найдены!")
                
                return {"status": "not_running"}
            
            logger.info(f"📊 Мониторинг Spark Streaming в течение {duration_minutes} минут...")
            
            start_time = time.time()
            end_time = start_time + (duration_minutes * 60)
            
            logs = []
            
            while time.time() < end_time and self.spark_process.poll() is None:
                # Читаем логи процесса
                try:
                    line = self.spark_process.stdout.readline()
                    if line:
                        logs.append(line.strip())
                        logger.info(f"Spark: {line.strip()}")
                        
                        # Ищем важные события в логах
                        if "Started streaming query" in line:
                            logger.info("✅ Streaming query запущен")
                        elif "Processed" in line and "rows" in line:
                            logger.info(f"📊 {line.strip()}")
                        elif "ERROR" in line or "Exception" in line:
                            logger.error(f"❌ Ошибка в Spark: {line.strip()}")
                            
                except Exception as read_error:
                    logger.warning(f"⚠️ Ошибка чтения логов: {read_error}")
                
                time.sleep(5)  # Проверяем каждые 5 секунд
            
            # Проверяем финальный статус
            if self.spark_process.poll() is None:
                status = "running"
                logger.info("✅ Spark Streaming все еще работает")
            else:
                status = "completed"
                logger.info("🏁 Spark Streaming завершился")
            
            return {
                "status": status,
                "monitoring_duration": duration_minutes,
                "logs_count": len(logs),
                "last_logs": logs[-10:] if logs else []
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка мониторинга: {e}")
            return {"status": "monitoring_error", "error": str(e)}
    
    def stop_streaming(self) -> str:
        """Останавливает Spark Streaming приложение"""
        try:
            if not self.spark_process:
                logger.info("ℹ️ Spark процесс не найден")
                return "not_running"
            
            if self.spark_process.poll() is not None:
                logger.info("ℹ️ Spark процесс уже завершен")
                return "already_stopped"
            
            logger.info("🛑 Останавливаем Spark Streaming...")
            
            # Graceful shutdown
            self.spark_process.terminate()
            
            # Ждем graceful завершения
            try:
                self.spark_process.wait(timeout=30)
                logger.info("✅ Spark Streaming остановлен gracefully")
                return "stopped_gracefully"
            except subprocess.TimeoutExpired:
                # Принудительно убиваем
                logger.warning("⚠️ Принудительное завершение Spark процесса")
                self.spark_process.kill()
                self.spark_process.wait()
                return "stopped_forcefully"
                
        except Exception as e:
            logger.error(f"❌ Ошибка остановки Spark Streaming: {e}")
            return "stop_error"


# =============================================================================
# AIRFLOW TASK FUNCTIONS
# =============================================================================

def start_spark_streaming_job(**context):
    """Запуск Spark Streaming приложения"""
    logger.info("🚀 Инициализация Spark Streaming Job...")
    
    try:
        # Загружаем конфигурацию
        config = SparkStreamConfig()
        manager = SparkStreamingManager(config)
        
        # Запускаем Spark Streaming
        result = manager.start_streaming()
        
        if result == "streaming_started":
            # Сохраняем manager в XCom для других задач
            context['task_instance'].xcom_push(key='spark_manager', value={
                'status': 'started',
                'pid': manager.spark_process.pid if manager.spark_process else None,
                'app_name': config.spark_app_name
            })
            
            logger.info("✅ Spark Streaming Job запущен успешно")
            return "streaming_job_started"
        else:
            logger.error("❌ Не удалось запустить Spark Streaming Job")
            return f"streaming_job_failed_{result}"
            
    except Exception as e:
        logger.error(f"❌ Ошибка запуска Spark Streaming Job: {e}")
        raise

def monitor_spark_streaming(**context):
    """Мониторинг работы Spark Streaming приложения"""
    logger.info("📊 Запуск мониторинга Spark Streaming...")
    
    try:
        # Восстанавливаем конфигурацию
        config = SparkStreamConfig()
        manager = SparkStreamingManager(config)
        
        # Получаем информацию о запущенном процессе из XCom
        spark_info = context['task_instance'].xcom_pull(
            task_ids='start_spark_streaming',
            key='spark_manager'
        )
        
        if not spark_info or spark_info.get('status') != 'started':
            logger.error("❌ Spark Streaming не запущен или недоступен")
            return "streaming_not_available"
        
        # Проводим мониторинг
        monitoring_duration = 5  # 5 минут мониторинга
        result = manager.monitor_streaming(monitoring_duration)
        
        # Сохраняем результаты мониторинга
        context['task_instance'].xcom_push(key='monitoring_results', value=result)
        
        logger.info("=" * 60)
        logger.info("📊 ОТЧЕТ МОНИТОРИНГА SPARK STREAMING")
        logger.info("=" * 60)
        logger.info(f"🎯 Статус: {result.get('status', 'unknown')}")
        logger.info(f"⏰ Длительность: {result.get('monitoring_duration', 0)} минут")
        logger.info(f"📋 Логов собрано: {result.get('logs_count', 0)}")
        
        if result.get('last_logs'):
            logger.info("📄 Последние логи:")
            for log_line in result['last_logs']:
                logger.info(f"   {log_line}")
        
        logger.info("=" * 60)
        
        if result.get('status') == 'running':
            logger.info("✅ Мониторинг завершен успешно")
            return "monitoring_successful"
        else:
            logger.warning("⚠️ Spark Streaming завершился во время мониторинга")
            return "streaming_completed_early"
            
    except Exception as e:
        logger.error(f"❌ Ошибка мониторинга: {e}")
        raise

def stop_spark_streaming(**context):
    """Остановка Spark Streaming приложения"""
    logger.info("🛑 Остановка Spark Streaming Job...")
    
    try:
        config = SparkStreamConfig()
        manager = SparkStreamingManager(config)
        
        result = manager.stop_streaming()
        
        logger.info(f"🏁 Результат остановки: {result}")
        
        # Очищаем XCom данные
        context['task_instance'].xcom_push(key='spark_manager', value={'status': 'stopped'})
        
        return f"streaming_{result}"
        
    except Exception as e:
        logger.error(f"❌ Ошибка остановки Spark Streaming: {e}")
        raise


# =============================================================================
# DAG ОПРЕДЕЛЕНИЕ
# =============================================================================

# Параметры DAG
default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Создание DAG
dag = DAG(
    'tasks8_etap4_spark_streaming_v10',
    default_args=default_args,
    description='ШАГ 4: Spark Streaming Job для потокового инференса мошенничества (ВАРИАНТ A: увеличенные ресурсы)',
    schedule=None,  # Только ручной запуск
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'spark', 'streaming', 'fraud-detection', 'tasks8']
)

# =============================================================================
# ЗАДАЧИ DAG
# =============================================================================

# Задача 1: Запуск Spark Streaming
start_spark_streaming = PythonOperator(
    task_id='start_spark_streaming',
    python_callable=start_spark_streaming_job,
    dag=dag,
    doc_md="""
    ## Запуск Spark Streaming Job
    
    Запускает Spark Streaming приложение для потокового инференса модели мошенничества:
    - Подключается к Kafka топику transactions-input
    - Загружает MLflow модель fraud_detection_model@Production
    - Применяет модель к потоку транзакций
    - Записывает результаты в топик fraud-predictions
    
    **Настройки:**
    - Consumer group: fraud-detection-group
    - Trigger interval: 2 seconds
    - Checkpoint: S3/MinIO
    """
)

# Задача 2: Мониторинг Spark Streaming
monitor_streaming = PythonOperator(
    task_id='monitor_streaming',
    python_callable=monitor_spark_streaming,
    dag=dag,
    doc_md="""
    ## Мониторинг Spark Streaming
    
    Отслеживает работу Spark Streaming приложения в течение 5 минут:
    - Мониторинг процесса Spark
    - Сбор логов выполнения
    - Анализ производительности
    - Выявление ошибок
    """
)

# Задача 3: Остановка Spark Streaming
stop_streaming = PythonOperator(
    task_id='stop_streaming',
    python_callable=stop_spark_streaming,
    dag=dag,
    doc_md="""
    ## Остановка Spark Streaming
    
    Graceful остановка Spark Streaming приложения:
    - Попытка graceful shutdown
    - При необходимости принудительная остановка
    - Очистка ресурсов
    """
)

# Задача 4: Проверка результатов в Kafka
def verify_kafka_results(**context):
    """Проверка результатов в выходном топике Kafka"""
    logger.info("🔍 Проверка результатов в Kafka топике fraud-predictions...")
    
    try:
        from kafka import KafkaConsumer
        import json
        
        config = SparkStreamConfig()
        
        # Подключаемся к Kafka consumer для чтения результатов
        consumer = KafkaConsumer(
            config.kafka_output_topic,
            bootstrap_servers=config.kafka_bootstrap_servers,
            group_id='fraud-results-checker',
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=30000,  # 30 секунд timeout
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info(f"✅ Подключились к топику {config.kafka_output_topic}")
        
        # Читаем сообщения
        message_count = 0
        fraud_predictions = 0
        normal_predictions = 0
        
        for message in consumer:
            message_count += 1
            prediction_data = message.value
            
            fraud_label = prediction_data.get('fraud_label', 'UNKNOWN')
            if fraud_label == 'FRAUD':
                fraud_predictions += 1
            elif fraud_label == 'NORMAL':
                normal_predictions += 1
            
            if message_count >= 100:  # Ограничиваем чтение первых 100 сообщений
                break
        
        consumer.close()
        
        # Анализируем результаты
        logger.info("=" * 60)
        logger.info("📊 РЕЗУЛЬТАТЫ ПРОВЕРКИ KAFKA PREDICTIONS")
        logger.info("=" * 60)
        logger.info(f"📨 Всего сообщений: {message_count}")
        logger.info(f"🚨 FRAUD предсказаний: {fraud_predictions}")
        logger.info(f"✅ NORMAL предсказаний: {normal_predictions}")
        
        if message_count > 0:
            fraud_rate = (fraud_predictions / message_count) * 100
            logger.info(f"📈 Доля FRAUD: {fraud_rate:.1f}%")
        
        logger.info("=" * 60)
        
        # Сохраняем результаты
        results = {
            'total_messages': message_count,
            'fraud_predictions': fraud_predictions,
            'normal_predictions': normal_predictions,
            'fraud_rate': fraud_rate if message_count > 0 else 0
        }
        
        context['task_instance'].xcom_push(key='kafka_results', value=results)
        
        if message_count > 0:
            logger.info("✅ Результаты инференса найдены в Kafka")
            return "results_found"
        else:
            logger.warning("⚠️ Результаты инференса не найдены")
            return "no_results"
            
    except Exception as e:
        logger.error(f"❌ Ошибка проверки результатов: {e}")
        return "verification_error"

verify_results = PythonOperator(
    task_id='verify_results',
    python_callable=verify_kafka_results,
    dag=dag,
    doc_md="""
    ## Проверка результатов инференса
    
    Проверяет что результаты инференса записались в Kafka топик fraud-predictions:
    - Читает сообщения из выходного топика
    - Анализирует распределение предсказаний (FRAUD vs NORMAL)
    - Подсчитывает общую статистику
    """
)

# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# Последовательное выполнение задач
start_spark_streaming >> monitor_streaming >> stop_streaming >> verify_results

# Документация DAG
dag.doc_md = """
# ШАГ 4: Spark Streaming Job для инференса

Этот DAG управляет Spark Streaming приложением для потокового инференса модели мошенничества.

## Архитектура:

1. **Spark Streaming** читает транзакции из Kafka топика `transactions-input`
2. **MLflow модель** загружается с stage `Production` для предсказания мошенничества
3. **Потоковый инференс** применяет модель к каждой транзакции в режиме реального времени
4. **Результаты** записываются в Kafka топик `fraud-predictions`

## Задачи DAG:

1. **start_spark_streaming** - Запуск Spark Streaming приложения
2. **monitor_streaming** - 5-минутный мониторинг работы
3. **stop_streaming** - Graceful остановка приложения
4. **verify_results** - Проверка результатов в выходном топике

## Настройки Spark:

- **Master**: Kubernetes cluster
- **Consumer group**: fraud-detection-group  
- **Trigger interval**: 2 seconds
- **Checkpoints**: S3/MinIO для отказоустойчивости
- **Dynamic allocation**: включено для автоматического масштабирования

## Мониторинг:

- Логи Spark процесса в реальном времени
- Статистика обработанных сообщений
- Автоматическая проверка результатов в Kafka
- Graceful shutdown с cleanup ресурсов

## Использование:

Этот DAG запускается вручную или из координирующего DAG после запуска Producer.
Для полного тестирования используйте его вместе с Producer DAG и мониторингом Consumer Lag.
"""

if __name__ == "__main__":
    logger.info("DAG ШАГ 4: Spark Streaming Inference загружен успешно")