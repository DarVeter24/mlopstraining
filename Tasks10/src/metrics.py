"""
Модуль метрик Prometheus для ML модели Tasks10.

Содержит определения всех метрик согласно vision.md:
- Кастомные метрики модели
- Метрики запросов и производительности  
- Метрики ошибок
- Метрики Kafka очереди
- Системные метрики
"""

from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
import time
import psutil
import os
from functools import wraps
from typing import Callable, Any

# Создаем отдельный реестр для наших метрик
REGISTRY = CollectorRegistry()

# =============================================================================
# КАСТОМНЫЕ МЕТРИКИ МОДЕЛИ
# =============================================================================

# Общее количество предсказаний с метками версии модели и результата
ml_model_predictions_total = Counter(
    'ml_model_predictions_total',
    'Total number of ML model predictions',
    ['model_version', 'prediction_result'],
    registry=REGISTRY
)

# Гистограмма времени выполнения предсказаний
ml_model_prediction_duration = Histogram(
    'ml_model_prediction_duration_seconds',
    'Time spent on ML model predictions',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    registry=REGISTRY
)

# Распределение вероятностей мошенничества
ml_model_fraud_probability = Histogram(
    'ml_model_fraud_probability',
    'Distribution of fraud probability scores',
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
    registry=REGISTRY
)

# Распределение уровней уверенности модели
ml_model_confidence_score = Histogram(
    'ml_model_confidence_score',
    'Distribution of model confidence scores',
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
    registry=REGISTRY
)

# =============================================================================
# МЕТРИКИ ЗАПРОСОВ И ПРОИЗВОДИТЕЛЬНОСТИ
# =============================================================================

# Общее количество HTTP запросов с метками метода, статуса, эндпоинта
http_requests_total = Counter(
    'http_requests_total',
    'Total number of HTTP requests',
    ['method', 'endpoint', 'status_code'],
    registry=REGISTRY
)

# Гистограмма времени обработки HTTP запросов
http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'Time spent processing HTTP requests',
    ['method', 'endpoint'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
    registry=REGISTRY
)

# Количество активных HTTP запросов в данный момент
http_requests_in_progress = Gauge(
    'http_requests_in_progress',
    'Number of HTTP requests currently being processed',
    registry=REGISTRY
)

# =============================================================================
# МЕТРИКИ ОШИБОК
# =============================================================================

# Общее количество ошибок с метками типа ошибки
ml_model_errors_total = Counter(
    'ml_model_errors_total',
    'Total number of ML model errors',
    ['error_type'],
    registry=REGISTRY
)

# Количество HTTP ошибок по статус-кодам
http_errors_total = Counter(
    'http_errors_total',
    'Total number of HTTP errors',
    ['status_code'],
    registry=REGISTRY
)

# Количество неудачных предсказаний
ml_model_failed_predictions_total = Counter(
    'ml_model_failed_predictions_total',
    'Total number of failed ML model predictions',
    ['failure_reason'],
    registry=REGISTRY
)

# =============================================================================
# МЕТРИКИ KAFKA ОЧЕРЕДИ
# =============================================================================

# Текущая длина очереди Kafka
kafka_queue_length = Gauge(
    'kafka_queue_length',
    'Current length of Kafka queue',
    ['topic'],
    registry=REGISTRY
)

# Задержка потребителя Kafka
kafka_consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Kafka consumer lag',
    ['topic', 'consumer_group'],
    registry=REGISTRY
)

# Общее количество обработанных сообщений из Kafka
kafka_messages_consumed_total = Counter(
    'kafka_messages_consumed_total',
    'Total number of messages consumed from Kafka',
    ['topic'],
    registry=REGISTRY
)

# Общее количество отправленных сообщений в Kafka
kafka_messages_produced_total = Counter(
    'kafka_messages_produced_total',
    'Total number of messages produced to Kafka',
    ['topic'],
    registry=REGISTRY
)

# =============================================================================
# СИСТЕМНЫЕ МЕТРИКИ
# =============================================================================

# Использование памяти в байтах
ml_model_memory_usage = Gauge(
    'ml_model_memory_usage_bytes',
    'Memory usage in bytes',
    registry=REGISTRY
)

# Процент использования CPU
ml_model_cpu_usage = Gauge(
    'ml_model_cpu_usage_percent',
    'CPU usage percentage',
    registry=REGISTRY
)

# Количество активных запросов
ml_model_active_requests = Gauge(
    'ml_model_active_requests',
    'Number of active requests being processed',
    registry=REGISTRY
)

# =============================================================================
# ДЕКОРАТОРЫ ДЛЯ АВТОМАТИЧЕСКОГО СБОРА МЕТРИК
# =============================================================================

def track_prediction_metrics(model_version: str = "unknown"):
    """
    Декоратор для автоматического отслеживания метрик предсказаний.
    
    Args:
        model_version: Версия модели
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            start_time = time.time()
            
            try:
                # Выполняем предсказание
                result = await func(*args, **kwargs)
                
                # Записываем метрики успешного предсказания
                duration = time.time() - start_time
                ml_model_prediction_duration.observe(duration)
                
                # Определяем результат предсказания
                prediction_result = "fraud" if result.is_fraud else "legitimate"
                ml_model_predictions_total.labels(
                    model_version=model_version,
                    prediction_result=prediction_result
                ).inc()
                
                # Записываем распределения
                if hasattr(result, "fraud_probability") and result.fraud_probability is not None:
                    ml_model_fraud_probability.observe(result.fraud_probability)
                
                if hasattr(result, "confidence") and result.confidence is not None:
                    ml_model_confidence_score.observe(result.confidence)
                
                return result
                
            except Exception as e:
                # Записываем метрики ошибок
                error_type = type(e).__name__
                ml_model_errors_total.labels(error_type=error_type).inc()
                ml_model_failed_predictions_total.labels(failure_reason=error_type).inc()
                raise
                
        return wrapper
    return decorator


def track_http_metrics(endpoint: str):
    """
    Декоратор для автоматического отслеживания HTTP метрик.
    
    Args:
        endpoint: Название эндпоинта
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(request, *args, **kwargs) -> Any:
            start_time = time.time()
            method = request.method
            
            # Увеличиваем счетчик активных запросов
            http_requests_in_progress.inc()
            ml_model_active_requests.inc()
            
            try:
                # Выполняем запрос
                response = await func(request, *args, **kwargs)
                status_code = "200"  # Default success status
                
                # Записываем метрики успешного запроса
                duration = time.time() - start_time
                http_request_duration_seconds.labels(
                    method=method,
                    endpoint=endpoint
                ).observe(duration)
                
                http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status_code=status_code
                ).inc()
                
                # Записываем метрики (предполагаем успех для FastAPI responses)
                # FastAPI responses не имеют прямого доступа к status_code в этом контексте
                
                return response
                
            except Exception as e:
                # Записываем метрики ошибок
                http_errors_total.labels(status_code="500").inc()
                http_requests_total.labels(
                    method=method,
                    endpoint=endpoint,
                    status_code="500"
                ).inc()
                raise
            finally:
                # Уменьшаем счетчик активных запросов
                http_requests_in_progress.dec()
                ml_model_active_requests.dec()
                
        return wrapper
    return decorator


# =============================================================================
# ФУНКЦИИ ДЛЯ ОБНОВЛЕНИЯ СИСТЕМНЫХ МЕТРИК
# =============================================================================

def update_system_metrics():
    """Обновляет системные метрики (CPU, память)."""
    try:
        # Получаем текущий процесс
        process = psutil.Process(os.getpid())
        
        # Обновляем метрики памяти
        memory_info = process.memory_info()
        ml_model_memory_usage.set(memory_info.rss)  # RSS - Resident Set Size
        
        # Обновляем метрики CPU
        cpu_percent = process.cpu_percent()
        ml_model_cpu_usage.set(cpu_percent)
        
    except Exception as e:
        # Записываем ошибку сбора системных метрик
        ml_model_errors_total.labels(error_type="system_metrics_error").inc()


def get_metrics() -> str:
    """
    Возвращает все метрики в формате Prometheus.
    
    Returns:
        Строка с метриками в формате Prometheus
    """
    # Обновляем системные метрики перед экспортом
    update_system_metrics()
    
    # Генерируем метрики в формате Prometheus
    return generate_latest(REGISTRY).decode('utf-8')


# =============================================================================
# ФУНКЦИИ ДЛЯ KAFKA МЕТРИК (заглушки для будущей интеграции)
# =============================================================================

def update_kafka_metrics(topic: str, queue_length: int, consumer_lag: int = 0):
    """
    Обновляет метрики Kafka.
    
    Args:
        topic: Название топика
        queue_length: Длина очереди
        consumer_lag: Задержка потребителя
    """
    kafka_queue_length.labels(topic=topic).set(queue_length)
    if consumer_lag > 0:
        kafka_consumer_lag.labels(topic=topic, consumer_group="ml-service").set(consumer_lag)


def increment_kafka_consumed(topic: str):
    """Увеличивает счетчик обработанных сообщений Kafka."""
    kafka_messages_consumed_total.labels(topic=topic).inc()


def increment_kafka_produced(topic: str):
    """Увеличивает счетчик отправленных сообщений Kafka."""
    kafka_messages_produced_total.labels(topic=topic).inc()
