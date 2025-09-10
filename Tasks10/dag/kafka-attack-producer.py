#!/usr/bin/env python3
"""
Producer для replay исторических транзакций в Kafka
Домашнее задание №8 - Инференс на потоке
Этап 1: Создание компонента генерации данных
Переделано для работы с Airflow DAG и Variables
"""

import os
import sys
import json
import time
import logging
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

# S3 и данные
import boto3
import pandas as pd

# PyArrow для партиционированных parquet
try:
    import pyarrow.parquet as pq
    import pyarrow.fs as fs
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    # Предупреждение будет в логах позже

# Kafka
from kafka import KafkaProducer, errors as kafka_errors

# Airflow Variables (если доступны)
try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    Variable = None

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class ProducerConfig:
    """Конфигурация Producer'а из Airflow Variables"""
    # Kafka настройки
    kafka_bootstrap_servers: str = "kafka.kafka.svc.cluster.local:9092"
    kafka_topic: str = "transactions-input"
    kafka_consumer_group: str = "fraud-detection-group"
    
    # Producer настройки
    tps_target: int = 50
    batch_size: int = 16384
    linger_ms: int = 10
    compression_type: str = "gzip"
    acks: str = "all"
    retries: int = 3
    timeout: int = 1
    max_in_flight: int = 5
    
    # Данные
    data_path: str = "s3a://otus/clean/fraud_transactions_fixed_new.parquet"
    max_records: Optional[int] = None
    
    # S3 настройки
    s3_endpoint_url: str = "http://192.168.31.201:9000"
    s3_access_key: str = "admin"
    s3_secret_key: str = "password"
    
    # Мониторинг
    stats_interval: int = 10
    enable_performance_logging: bool = True


def get_config_from_airflow_variables(tps_override: Optional[int] = None,
                                      topic_override: Optional[str] = None,
                                      max_records_override: Optional[int] = None) -> ProducerConfig:
    """
    Получение конфигурации из Airflow Variables
    
    Args:
        tps_override: Переопределение TPS
        topic_override: Переопределение топика
        max_records_override: Переопределение количества записей
    
    Returns:
        ProducerConfig с настройками из Airflow Variables
    """
    config = ProducerConfig()
    
    if not AIRFLOW_AVAILABLE or not Variable:
        logger.warning("⚠️ Airflow Variables недоступны, используем defaults")
        if tps_override:
            config.tps_target = tps_override
        if topic_override:
            config.kafka_topic = topic_override
        if max_records_override:
            config.max_records = max_records_override
        return config
    
    try:
        # Kafka настройки
        config.kafka_bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var=config.kafka_bootstrap_servers)
        config.kafka_topic = Variable.get("KAFKA_INPUT_TOPIC", default_var=config.kafka_topic)
        config.kafka_consumer_group = Variable.get("KAFKA_CONSUMER_GROUP", default_var=config.kafka_consumer_group)
        
        # Producer настройки
        config.tps_target = int(Variable.get("PRODUCER_DEFAULT_TPS", default_var=str(config.tps_target)))
        config.batch_size = int(Variable.get("PRODUCER_BATCH_SIZE", default_var=str(config.batch_size)))
        config.linger_ms = int(Variable.get("PRODUCER_LINGER_MS", default_var=str(config.linger_ms)))
        config.compression_type = Variable.get("PRODUCER_COMPRESSION_TYPE", default_var=config.compression_type)
        config.acks = Variable.get("PRODUCER_ACKS", default_var=config.acks)
        config.retries = int(Variable.get("PRODUCER_RETRIES", default_var=str(config.retries)))
        config.timeout = int(Variable.get("PRODUCER_TIMEOUT", default_var=str(config.timeout)))
        config.max_in_flight = int(Variable.get("PRODUCER_MAX_IN_FLIGHT", default_var=str(config.max_in_flight)))
        
        # Данные
        config.data_path = Variable.get("CLEAN_DATA_PATH", default_var=config.data_path)
        
        # S3 настройки
        config.s3_endpoint_url = Variable.get("S3_ENDPOINT_URL", default_var=config.s3_endpoint_url)
        config.s3_access_key = Variable.get("S3_ACCESS_KEY", default_var=config.s3_access_key)
        config.s3_secret_key = Variable.get("S3_SECRET_KEY", default_var=config.s3_secret_key)
        
        # Мониторинг
        config.stats_interval = int(Variable.get("MONITORING_STATS_INTERVAL", default_var=str(config.stats_interval)))
        config.enable_performance_logging = Variable.get("ENABLE_PERFORMANCE_LOGGING", default_var="true").lower() == "true"
        
        # Переопределения из параметров
        if tps_override:
            config.tps_target = tps_override
        if topic_override:
            config.kafka_topic = topic_override
        if max_records_override:
            config.max_records = max_records_override
            
        logger.info("✅ Конфигурация загружена из Airflow Variables")
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения переменных из Airflow: {e}")
        logger.info("⚠️ Используем конфигурацию по умолчанию")
        
        # Применяем переопределения если есть
        if tps_override:
            config.tps_target = tps_override
        if topic_override:
            config.kafka_topic = topic_override
        if max_records_override:
            config.max_records = max_records_override
    
    return config


@dataclass
class ProducerStats:
    """Статистика producer'а"""
    start_time: datetime
    messages_sent: int = 0
    messages_error: int = 0
    bytes_sent: int = 0
    last_tps: float = 0.0
    avg_tps: float = 0.0
    kafka_lag: int = 0
    avg_latency: float = 0.0


class TransactionProducer:
    """Producer для отправки транзакций в Kafka с контролем TPS"""
    
    def __init__(self, config: ProducerConfig):
        self.config = config
        self.stats = ProducerStats(start_time=datetime.now())
        self.running = False
        
        # Kafka producer
        self.producer = None
        self._init_kafka_producer()
        
        # S3 клиент для MinIO
        self.s3_client = None
        self._init_s3_client()
        
    def _init_kafka_producer(self):
        """Инициализация Kafka Producer с настройками из config"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                batch_size=self.config.batch_size,
                linger_ms=self.config.linger_ms,
                compression_type=self.config.compression_type,
                max_in_flight_requests_per_connection=self.config.max_in_flight,
                acks=self.config.acks,
                retries=self.config.retries
            )
            logger.info(f"✅ Kafka Producer инициализирован: {self.config.kafka_bootstrap_servers}")
            logger.info(f"📊 Параметры: batch_size={self.config.batch_size}, compression={self.config.compression_type}")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Kafka Producer: {e}")
            raise
    
    def _init_s3_client(self):
        """Инициализация S3 клиента для подключения к MinIO"""
        try:
            # Устанавливаем переменные окружения
            os.environ["AWS_ACCESS_KEY_ID"] = self.config.s3_access_key
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.config.s3_secret_key
            
            self.s3_client = boto3.client(
                's3',
                endpoint_url=self.config.s3_endpoint_url,
                aws_access_key_id=self.config.s3_access_key,
                aws_secret_access_key=self.config.s3_secret_key,
                region_name='us-east-1'  # Для MinIO
            )
            logger.info(f"✅ S3 клиент инициализирован: {self.config.s3_endpoint_url}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации S3 клиента: {e}")
            logger.error("🚨 S3 обязателен для работы!")
            raise
    
    def _load_data_smart(self, bucket_name: str, key: str) -> pd.DataFrame:
        """
        Умная загрузка данных: поддерживает как обычные файлы, так и партиционированные datasets
        
        Args:
            bucket_name: Имя S3 bucket
            key: Ключ (путь к файлу/директории)
            
        Returns:
            DataFrame с данными
        """
        if not PYARROW_AVAILABLE:
            logger.warning("⚠️ PyArrow недоступен - fallback на first part-файл для партиций")
        else:
            logger.info("✅ PyArrow доступен - поддержка партиционированных datasets")
            
        try:
            # Проверяем что существует в S3
            try:
                # Пытаемся получить информацию об объекте как о файле
                self.s3_client.head_object(Bucket=bucket_name, Key=key)
                is_single_file = True
                logger.info(f"📄 Обнаружен одиночный файл: {key}")
                
            except self.s3_client.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    # Возможно это партиционированная директория
                    is_single_file = False
                    logger.info(f"📁 Возможно партиционированная директория: {key}")
                else:
                    raise
            
            if is_single_file:
                # Загружаем обычный файл
                logger.info("📥 Загрузка одиночного parquet файла...")
                temp_file = "/tmp/fraud_data.parquet"
                self.s3_client.download_file(bucket_name, key, temp_file)
                df = pd.read_parquet(temp_file)
                
                # Удаляем временный файл
                cleanup_temp = os.getenv("CLEANUP_TEMP_FILES", "true").lower() == "true"
                if cleanup_temp:
                    os.remove(temp_file)
                    logger.info("🧹 Временный файл удален")
                
            else:
                # Пытаемся загрузить как партиционированный dataset
                if not PYARROW_AVAILABLE:
                    # Fallback: пытаемся найти part-файлы и загрузить первый
                    logger.warning("⚠️ PyArrow недоступен, ищем part-файлы...")
                    objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=key)
                    
                    if 'Contents' in objects:
                        part_files = [obj['Key'] for obj in objects['Contents'] 
                                    if obj['Key'].endswith('.parquet')]
                        if part_files:
                            first_part = part_files[0]
                            logger.info(f"📄 Используем первый part-файл: {first_part}")
                            temp_file = "/tmp/fraud_data.parquet"
                            self.s3_client.download_file(bucket_name, first_part, temp_file)
                            df = pd.read_parquet(temp_file)
                            
                            # Удаляем временный файл
                            cleanup_temp = os.getenv("CLEANUP_TEMP_FILES", "true").lower() == "true"
                            if cleanup_temp:
                                os.remove(temp_file)
                                logger.info("🧹 Временный файл удален")
                        else:
                            raise ValueError("❌ Не найдены parquet файлы в директории")
                    else:
                        raise ValueError("❌ Директория пустая или не существует")
                
                else:
                    # Используем PyArrow для партиционированного dataset
                    logger.info("📊 Загрузка партиционированного dataset через PyArrow...")
                    
                    # Создаем S3 filesystem для PyArrow
                    s3_fs = fs.S3FileSystem(
                        endpoint_override=self.config.s3_endpoint_url.replace('http://', '').replace('https://', ''),
                        access_key=self.config.s3_access_key,
                        secret_key=self.config.s3_secret_key,
                        scheme='http'
                    )
                    
                    # Читаем партиционированный dataset
                    dataset_path = f"{bucket_name}/{key}"
                    dataset = pq.ParquetDataset(dataset_path, filesystem=s3_fs)
                    table = dataset.read()
                    df = table.to_pandas()
                    
                    logger.info(f"✅ Загружено {len(df):,} строк из партиционированного dataset")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка умной загрузки данных: {e}")
            raise
    
    def load_transaction_data(self) -> pd.DataFrame:
        """
        Загрузка данных о транзакциях из S3 MinIO
        
        Returns:
            DataFrame с транзакциями
        """
        data_path = self.config.data_path
        max_records = self.config.max_records
        
        logger.info(f"📥 Загрузка данных из S3: {data_path}")
        
        try:
            # Проверяем что путь S3
            if not data_path.startswith('s3a://'):
                raise ValueError(f"Поддерживается только S3 путь (s3a://), получен: {data_path}")
            
            # Преобразуем s3a:// в s3://
            s3_path = data_path.replace('s3a://', 's3://')
            bucket_name = s3_path.split('/')[2]
            key = '/'.join(s3_path.split('/')[3:])
            
            logger.info(f"📡 S3 загрузка: bucket={bucket_name}, key={key}")
            
            # Умная загрузка: поддержка как обычных файлов, так и партиционированных данных
            df = self._load_data_smart(bucket_name, key)
            
            # Ограничиваем количество записей если нужно
            if max_records and len(df) > max_records:
                df = df.sample(n=max_records, random_state=42)
                logger.info(f"📊 Ограничено до {max_records} записей")
            
            logger.info(f"📊 Загружено {len(df):,} транзакций из S3")
            logger.info(f"📋 Колонки: {list(df.columns)}")
            
            # Проверяем наличие целевой переменной
            if 'tx_fraud' in df.columns:
                fraud_count = df['tx_fraud'].sum()
                fraud_pct = (fraud_count / len(df)) * 100
                logger.info(f"🔍 Мошеннических транзакций: {fraud_count:,} ({fraud_pct:.1f}%)")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных из S3: {e}")
            raise
    
    def prepare_transaction_message(self, row: pd.Series, timestamp: datetime) -> Dict[str, Any]:
        """
        Подготовка сообщения транзакции для отправки в Kafka
        
        Args:
            row: Строка из DataFrame с данными транзакции
            timestamp: Временная метка для сообщения
            
        Returns:
            Словарь с данными транзакции
        """
        # Преобразуем pandas Series в dict, исключая NaN значения
        transaction = {}
        for key, value in row.items():
            if pd.notna(value):  # Исключаем NaN
                # Преобразуем numpy типы в Python типы для JSON сериализации
                if hasattr(value, 'item'):
                    transaction[key] = value.item()
                else:
                    transaction[key] = value
        
        # Добавляем метаданные для потока
        message = {
            "transaction_id": f"tx_{int(time.time() * 1000000)}_{self.stats.messages_sent}",
            "stream_timestamp": timestamp.isoformat(),
            "producer_id": "producer_replay_transactions",
            "producer_config": {
                "tps_target": self.config.tps_target,
                "topic": self.config.kafka_topic
            },
            "data": transaction
        }
        
        return message
    
    def send_transaction(self, message: Dict[str, Any]) -> bool:
        """
        Отправка транзакции в Kafka
        
        Args:
            message: Сообщение для отправки
            
        Returns:
            True если отправлено успешно
        """
        try:
            # Используем transaction_id как key для партиционирования
            key = message["transaction_id"]
            
            # Асинхронная отправка
            future = self.producer.send(
                topic=self.config.kafka_topic,
                key=key,
                value=message
            )
            
            # Получаем метаданные (блокирующий вызов с timeout)
            record_metadata = future.get(timeout=self.config.timeout)
            
            # Обновляем статистику
            self.stats.messages_sent += 1
            self.stats.bytes_sent += len(json.dumps(message).encode('utf-8'))
            
            return True
            
        except kafka_errors.KafkaTimeoutError:
            logger.warning("⏰ Kafka timeout при отправке сообщения")
            self.stats.messages_error += 1
            return False
            
        except Exception as e:
            logger.error(f"❌ Ошибка отправки в Kafka: {e}")
            self.stats.messages_error += 1
            return False
    
    def calculate_stats(self):
        """Вычисление текущей статистики"""
        elapsed = (datetime.now() - self.stats.start_time).total_seconds()
        
        if elapsed > 0:
            self.stats.avg_tps = self.stats.messages_sent / elapsed
            
            # Вычисляем TPS за последние интервал
            if hasattr(self, '_last_stats_time') and hasattr(self, '_last_messages_count'):
                time_diff = time.time() - self._last_stats_time
                msg_diff = self.stats.messages_sent - self._last_messages_count
                if time_diff > 0:
                    self.stats.last_tps = msg_diff / time_diff
            
            # Сохраняем для следующего вычисления
            self._last_stats_time = time.time()
            self._last_messages_count = self.stats.messages_sent
    
    def print_stats(self, total_records: int):
        """Вывод статистики производительности"""
        if not self.config.enable_performance_logging:
            return
            
        self.calculate_stats()
        
        elapsed = (datetime.now() - self.stats.start_time).total_seconds()
        progress = (self.stats.messages_sent / total_records) * 100 if total_records > 0 else 0
        success_rate = (self.stats.messages_sent / (self.stats.messages_sent + self.stats.messages_error) * 100 
                       if (self.stats.messages_sent + self.stats.messages_error) > 0 else 0)
        
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 📊 Producer Statistics:")
        print(f"  ├── TPS Target: {self.config.tps_target} | Current: {self.stats.last_tps:.1f} | Average: {self.stats.avg_tps:.1f}")
        print(f"  ├── Messages: {self.stats.messages_sent:,} sent | {self.stats.messages_error} errors | {success_rate:.1f}% success")
        print(f"  ├── Progress: {self.stats.messages_sent:,}/{total_records:,} ({progress:.1f}%)")
        print(f"  ├── Topic: {self.config.kafka_topic}")
        print(f"  ├── Data Size: {self.stats.bytes_sent / 1024 / 1024:.1f} MB")
        print(f"  └── Runtime: {elapsed:.1f}s")
        
        # Предупреждения о производительности
        if self.stats.last_tps < self.config.tps_target * 0.9:
            print(f"  ⚠️  WARNING: TPS below target ({self.stats.last_tps:.1f} < {self.config.tps_target})")
        if self.stats.messages_error > 10:
            print(f"  🚨 ALERT: High error count ({self.stats.messages_error})")
    
    def replay_transactions(self) -> Dict[str, Any]:
        """
        Основной метод для replay транзакций с контролем TPS
        
        Returns:
            Словарь с результатами выполнения
        """
        logger.info(f"🚀 Запуск Producer Replay: TPS={self.config.tps_target}, Topic={self.config.kafka_topic}")
        
        # Загружаем данные
        df = self.load_transaction_data()
        total_records = len(df)
        
        if total_records == 0:
            error_msg = "❌ Нет данных для отправки"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        # Параметры для контроля TPS
        target_interval = 1.0 / self.config.tps_target  # Интервал между сообщениями
        self.running = True
        stats_interval = self.config.stats_interval
        last_stats_time = time.time()
        
        logger.info(f"📡 Начинаем отправку {total_records:,} транзакций с TPS={self.config.tps_target}")
        logger.info(f"⏱️  Интервал между сообщениями: {target_interval*1000:.1f}ms")
        
        try:
            for idx, (_, row) in enumerate(df.iterrows()):
                if not self.running:
                    break
                
                # Подготавливаем сообщение
                current_time = datetime.now()
                message = self.prepare_transaction_message(row, current_time)
                
                # Отправляем в Kafka
                send_start = time.time()
                success = self.send_transaction(message)
                send_duration = time.time() - send_start
                
                # Контроль TPS - ждем нужный интервал
                sleep_time = max(0, target_interval - send_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
                # Выводим статистику
                if time.time() - last_stats_time >= stats_interval:
                    self.print_stats(total_records)
                    last_stats_time = time.time()
            
            # Финальная статистика
            logger.info("✅ Отправка завершена")
            self.print_stats(total_records)
            
            # Итоговый отчет
            result = self.get_final_report(total_records)
            
            return result
            
        except KeyboardInterrupt:
            logger.info("⏹️  Остановка по Ctrl+C")
            self.running = False
            return {"status": "interrupted", "message": "Stopped by user"}
        
        except Exception as e:
            error_msg = f"❌ Критическая ошибка: {e}"
            logger.error(error_msg)
            return {"status": "error", "message": error_msg}
        
        finally:
            # Закрываем producer
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("✅ Kafka Producer закрыт")
    
    def get_final_report(self, total_records: int) -> Dict[str, Any]:
        """Итоговый отчет о производительности"""
        elapsed = (datetime.now() - self.stats.start_time).total_seconds()
        success_rate = (self.stats.messages_sent / (self.stats.messages_sent + self.stats.messages_error) * 100 
                       if (self.stats.messages_sent + self.stats.messages_error) > 0 else 0)
        
        # Анализ производительности
        tps_achieved = self.stats.avg_tps >= self.config.tps_target * 0.95
        reliability_ok = success_rate >= 99
        
        report = {
            "status": "success",
            "config": {
                "target_tps": self.config.tps_target,
                "total_records": total_records,
                "kafka_topic": self.config.kafka_topic,
                "data_path": self.config.data_path
            },
            "results": {
                "messages_sent": self.stats.messages_sent,
                "messages_error": self.stats.messages_error,
                "success_rate": round(success_rate, 1),
                "avg_tps": round(self.stats.avg_tps, 1),
                "data_size_mb": round(self.stats.bytes_sent / 1024 / 1024, 1),
                "runtime_seconds": round(elapsed, 1)
            },
            "analysis": {
                "tps_achieved": tps_achieved,
                "reliability_ok": reliability_ok,
                "overall_success": tps_achieved and reliability_ok
            }
        }
        
        if self.config.enable_performance_logging:
            print("\n" + "="*60)
            print("📋 ИТОГОВЫЙ ОТЧЕТ О ПРОИЗВОДИТЕЛЬНОСТИ")
            print("="*60)
            print(f"Параметры теста:")
            print(f"  ├── Target TPS: {self.config.tps_target}")
            print(f"  ├── Всего записей: {total_records:,}")
            print(f"  ├── Topic: {self.config.kafka_topic}")
            print(f"  └── Время выполнения: {elapsed:.1f}s")
            print(f"\nРезультаты:")
            print(f"  ├── Отправлено: {self.stats.messages_sent:,} транзакций")
            print(f"  ├── Ошибок: {self.stats.messages_error}")
            print(f"  ├── Успешность: {success_rate:.1f}%")
            print(f"  ├── Средний TPS: {self.stats.avg_tps:.1f}")
            print(f"  └── Объем данных: {self.stats.bytes_sent / 1024 / 1024:.1f} MB")
            
            print(f"\nАнализ производительности:")
            if tps_achieved:
                print(f"  ✅ Цель TPS достигнута ({self.stats.avg_tps:.1f} ≥ {self.config.tps_target * 0.95:.1f})")
            else:
                print(f"  ⚠️  Цель TPS не достигнута ({self.stats.avg_tps:.1f} < {self.config.tps_target * 0.95:.1f})")
            
            if reliability_ok:
                print(f"  ✅ Высокая надежность ({success_rate:.1f}% успешных)")
            else:
                print(f"  ⚠️  Есть проблемы с надежностью ({success_rate:.1f}% успешных)")
        
        return report


# === ФУНКЦИИ ДЛЯ AIRFLOW DAG ===

def run_producer_test(tps: int = 50, topic: str = "transactions-input-test", max_records: int = 1000) -> Dict[str, Any]:
    """
    Функция для запуска producer'а из Airflow DAG
    
    Args:
        tps: Целевой TPS
        topic: Kafka топик 
        max_records: Максимальное количество записей
    
    Returns:
        Результат выполнения
    """
    logger.info(f"🚀 Запуск producer test: TPS={tps}, Topic={topic}, MaxRecords={max_records}")
    
    try:
        # Получаем конфигурацию из Airflow Variables
        config = get_config_from_airflow_variables(
            tps_override=tps,
            topic_override=topic,
            max_records_override=max_records
        )
        
        # Создаем и запускаем producer
        producer = TransactionProducer(config)
        result = producer.replay_transactions()
        
        logger.info(f"✅ Producer test завершен: {result['status']}")
        return result
        
    except Exception as e:
        error_msg = f"❌ Ошибка producer test: {e}"
        logger.error(error_msg)
        return {"status": "error", "message": error_msg}


def run_producer_tps_50(**context) -> str:
    """TPS=50 согласно Этапу 1 (исправлено с tps=10)"""
    result = run_producer_test(tps=50, topic="transactions-input", max_records=1000)
    if result["status"] == "success":
        print(f"✅ TPS=50: достигнуто {result['results']['avg_tps']:.1f} avg TPS")
        return "success"
    else:
        raise Exception(f"TPS=50 test failed: {result.get('message', 'Unknown error')}")


def run_producer_small_load(**context) -> str:
    """Функция для малой нагрузки (для Airflow PythonOperator) - DEPRECATED, используйте run_producer_tps_50"""
    return run_producer_tps_50(**context)


def run_producer_tps_100(**context) -> str:  
    """TPS=100 согласно Этапу 1"""
    result = run_producer_test(tps=100, topic="transactions-input", max_records=1500)
    if result["status"] == "success":
        print(f"✅ TPS=100: достигнуто {result['results']['avg_tps']:.1f} avg TPS")
        return "success"
    else:
        raise Exception(f"TPS=100 test failed: {result.get('message', 'Unknown error')}")


def run_producer_medium_load(**context) -> str:
    """Функция для средней нагрузки (для Airflow PythonOperator) - DEPRECATED, используйте run_producer_tps_100"""  
    return run_producer_tps_100(**context)


def run_producer_tps_200(**context) -> str:
    """TPS=200 согласно Этапу 1"""
    result = run_producer_test(tps=200, topic="transactions-input", max_records=2000) 
    if result["status"] == "success":
        print(f"✅ TPS=200: достигнуто {result['results']['avg_tps']:.1f} avg TPS")
        return "success"
    else:
        raise Exception(f"TPS=200 test failed: {result.get('message', 'Unknown error')}")


def run_producer_tps_400(**context) -> str:
    """TPS=400 согласно Этапу 1 (PEAK LOAD)"""  
    result = run_producer_test(tps=400, topic="transactions-input", max_records=3000)
    if result["status"] == "success":
        print(f"🚀 TPS=400 PEAK: достигнуто {result['results']['avg_tps']:.1f} avg TPS")
        return "success"
    else:
        raise Exception(f"TPS=400 test failed: {result.get('message', 'Unknown error')}")


def run_producer_high_load(**context) -> str:
    """Функция для высокой нагрузки (для Airflow PythonOperator) - DEPRECATED, используйте run_producer_tps_400"""
    return run_producer_tps_400(**context)


def run_escalating_attack(**context) -> str:
    """
    🚨 ESCALATING ATTACK для Tasks10 - Итерация 5
    Постепенное наращивание нагрузки: 50→200→500→1500 TPS
    Цель: довести ML API до 6 подов + CPU>80% → алерт администратора
    """
    logger.info("🚨 Запуск ESCALATING ATTACK для Tasks10!")
    
    # Фазы атаки с увеличивающейся интенсивностью - МАКСИМАЛЬНАЯ НАГРУЗКА
    attack_phases = [
        {"name": "Фаза 1: Быстрый старт", "tps": 500, "duration_sec": 60, "records": 30000},
        {"name": "Фаза 2: Интенсивная атака", "tps": 1000, "duration_sec": 120, "records": 120000},
        {"name": "Фаза 3: КРИТИЧЕСКАЯ АТАКА", "tps": 2000, "duration_sec": 180, "records": 360000},
        {"name": "Фаза 4: МАКСИМАЛЬНАЯ АТАКА", "tps": 5000, "duration_sec": 300, "records": 1500000}
    ]
    
    results = []
    
    for phase in attack_phases:
        logger.info(f"🔥 {phase['name']}: {phase['tps']} TPS на {phase['duration_sec']} секунд")
        print(f"\n{'='*60}")
        print(f"🔥 {phase['name']}")
        print(f"   TPS: {phase['tps']}")
        print(f"   Длительность: {phase['duration_sec']} секунд")
        print(f"   Ожидаемые записи: {phase['records']:,}")
        print(f"{'='*60}")
        
        try:
            # Запускаем фазу атаки
            result = run_producer_test(
                tps=phase['tps'], 
                topic="transactions-input", 
                max_records=phase['records']
            )
            
            if result['status'] == 'success':
                actual_tps = result['results']['avg_tps']
                logger.info(f"✅ {phase['name']} завершена: {actual_tps:.1f} TPS достигнуто")
                print(f"✅ Фаза успешна: {actual_tps:.1f} TPS")
                
                # Пауза между фазами для стабилизации системы
                if phase != attack_phases[-1]:  # Не ждем после последней фазы
                    logger.info("⏸️  Пауза 30 секунд для стабилизации системы...")
                    print("⏸️  Пауза 30 секунд...")
                    time.sleep(30)
                    
            else:
                logger.error(f"❌ {phase['name']} провалена: {result.get('message')}")
                print(f"❌ Фаза провалена: {result.get('message')}")
                
            results.append({
                "phase": phase['name'],
                "target_tps": phase['tps'],
                "actual_tps": result['results']['avg_tps'] if result['status'] == 'success' else 0,
                "status": result['status'],
                "message": result.get('message', 'OK')
            })
            
        except Exception as e:
            logger.error(f"💥 Ошибка в {phase['name']}: {str(e)}")
            print(f"💥 Ошибка: {str(e)}")
            results.append({
                "phase": phase['name'],
                "target_tps": phase['tps'],
                "actual_tps": 0,
                "status": "error",
                "message": str(e)
            })
    
    # Итоговый отчет
    print(f"\n{'='*60}")
    print("📊 ИТОГОВЫЙ ОТЧЕТ ESCALATING ATTACK")
    print(f"{'='*60}")
    
    total_success = 0
    for result in results:
        status_icon = "✅" if result['status'] == 'success' else "❌"
        print(f"{status_icon} {result['phase']}: {result['actual_tps']:.1f}/{result['target_tps']} TPS")
        if result['status'] == 'success':
            total_success += 1
    
    attack_success = total_success >= 3  # Минимум 3 из 4 фаз должны быть успешными
    
    print(f"\n🎯 Результат атаки: {total_success}/4 фаз успешны")
    if attack_success:
        print("🚨 ESCALATING ATTACK УСПЕШНА! Ожидайте алерт администратора через 5 минут при CPU>80% + 6 подов!")
        logger.info("🚨 ESCALATING ATTACK завершена успешно - система должна масштабироваться до критического уровня")
    else:
        print("⚠️  ESCALATING ATTACK частично провалена - возможно система не достигнет критического уровня")
        logger.warning("⚠️  ESCALATING ATTACK частично провалена")
    
    return f"ESCALATING ATTACK: {total_success}/4 фаз успешны - {'УСПЕХ' if attack_success else 'ЧАСТИЧНЫЙ ПРОВАЛ'}"


# === LEGACY MAIN ДЛЯ STANDALONE ЗАПУСКА ===

def main():
    """Основная функция для standalone запуска (с базовыми параметрами)"""
    logger.info("🚀 Producer Replay Transactions - Standalone режим")
    logger.info("ℹ️ Для DAG используйте функции run_producer_*")
    
    try:
        # Получаем базовую конфигурацию
        config = get_config_from_airflow_variables()
        
        # Создаем и запускаем producer
        producer = TransactionProducer(config)
        result = producer.replay_transactions()
        
        if result["status"] == "success":
            logger.info("✅ Producer выполнен успешно")
        else:
            logger.error(f"❌ Producer завершился с ошибкой: {result.get('message')}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())


# === AIRFLOW DAG ДЛЯ ЭТАПА 1 ===

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    
    # Настройки DAG
    default_args = {
        'owner': 'mlops-tasks8-etap1',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    }

    # Создаем DAG
    dag = DAG(
        dag_id='tasks8_etap1_producer_test_v4',
        default_args=default_args,
        description='Этап 1: Producer с TPS 50,100,200,400',
        schedule=None,  # Только ручной запуск (исправлено для новых версий Airflow)
        start_date=datetime(2024, 8, 10),
        catchup=False,
        tags=['mlops', 'tasks8', 'etap1', 'producer', 'tps']
    )

    # Создаем задачи для всех TPS из Этапа 1
    test_50_task = PythonOperator(
        task_id='test_tps_50', 
        python_callable=run_producer_tps_50,
        dag=dag
    )

    test_100_task = PythonOperator(
        task_id='test_tps_100',
        python_callable=run_producer_tps_100, 
        dag=dag
    )

    test_200_task = PythonOperator(
        task_id='test_tps_200',
        python_callable=run_producer_tps_200,
        dag=dag
    )

    test_400_task = PythonOperator(
        task_id='test_tps_400', 
        python_callable=run_producer_tps_400,
        dag=dag
    )

    # Зависимости: выполняются по очереди для контроля нагрузки
    test_50_task >> test_100_task >> test_200_task >> test_400_task

    # === DAG ДЛЯ TASKS10 ESCALATING ATTACK ===
    
    # Создаем отдельный DAG для Tasks10 Итерация 5
    tasks10_dag = DAG(
        dag_id='tasks10_escalating_attack_v1',
        default_args={
            'owner': 'mlops-tasks10-iteration5',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Tasks10 Итерация 5: Escalating Attack для тестирования алерта администратора',
        schedule=None,  # Только ручной запуск
        start_date=datetime(2024, 12, 20),
        catchup=False,
        tags=['mlops', 'tasks10', 'iteration5', 'escalating-attack', 'kafka']
    )
    
    # Задача escalating attack
    escalating_attack_task = PythonOperator(
        task_id='run_escalating_attack',
        python_callable=run_escalating_attack,
        dag=tasks10_dag,
        doc_md="""
        ## 🚨 ESCALATING ATTACK для Tasks10
        
        **Цель**: Протестировать имитацию атаки с постепенным наращиванием нагрузки 
        до срабатывания алерта администратора (6 экземпляров + CPU>80% в течение 5 минут).
        
        **Фазы атаки**:
        1. **Фаза 1**: 50 TPS на 60 секунд (базовая нагрузка)
        2. **Фаза 2**: 200 TPS на 120 секунд (начало HPA масштабирования)  
        3. **Фаза 3**: 500 TPS на 180 секунд (4-5 подов)
        4. **Фаза 4**: 1500 TPS на 300 секунд (6 подов + CPU>80% → 🚨 АЛЕРТ!)
        
        **Ожидаемый результат**: Срабатывание алерта `AdminNotification_MaxScaleHighCPU` 
        через 5 минут после достижения критических условий.
        
        **Мониторинг**: 
        ```bash
        watch kubectl get pods -n mlops-tasks10
        kubectl top pods -n mlops-tasks10
        ```
        """
    )

except ImportError:
    # Если Airflow недоступен (standalone режим), игнорируем
    logger.info("ℹ️ Airflow недоступен - работаем в standalone режиме")
    dag = None
    tasks10_dag = None