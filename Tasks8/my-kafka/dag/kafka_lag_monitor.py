"""
шаг 3: мониторинг consumer lag для системы потокового инференса мошенничества

этот dag обеспечивает мониторинг производительности kafka потребителей и топиков:
- мониторинг consumer lag для группы fraud-detection-group
- отслеживание throughput входного и выходного топиков
- определение критического tps и генерация алертов
- backpressure сигналы для producer компонентов

автор: mlops task 8
"""

import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import time

# Airflow imports
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

# Kafka Admin API
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import kafka.errors

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# конфигурация мониторинга 
# =============================================================================

class KafkaMonitorConfig:
    """Конфигурация мониторинга Kafka метрик"""
    
    def __init__(self):
        """Загружает конфигурацию из Airflow Variables"""
        logger.info("🚀 INIT: Начинаем загрузку конфигурации KafkaMonitorConfig")
        try:
            # Kafka подключение
            self.kafka_bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
            self.kafka_consumer_group = Variable.get("KAFKA_CONSUMER_GROUP")
            self.kafka_input_topic = Variable.get("KAFKA_INPUT_TOPIC")
            self.kafka_output_topic = Variable.get("KAFKA_OUTPUT_TOPIC")
            self.kafka_test_topic = Variable.get("KAFKA_TEST_TOPIC")
            
            # Пороговые значения для алертов
            self.max_lag_threshold = int(Variable.get("PERF_TEST_MAX_LAG_THRESHOLD", "1000"))
            self.tps_tolerance = float(Variable.get("PERF_TEST_TPS_TOLERANCE", "0.9"))
            
            # Интервалы мониторинга
            self.lag_check_interval = int(Variable.get("MONITORING_KAFKA_LAG_CHECK_INTERVAL", "30"))
            self.stats_interval = int(Variable.get("MONITORING_STATS_INTERVAL", "10"))
            
            # Настройки алертов
            self.alert_email = Variable.get("MONITORING_ALERT_EMAIL", "admin@company.com")
            
            # Debug режим
            self.debug_mode = Variable.get("DEBUG_MODE", "False").lower() == "True"
            self.environment = Variable.get("ENVIRONMENT", "development")
            
            logger.info("✅ Конфигурация мониторинга загружена успешно")
            logger.info(f"🔍 DEBUG: KAFKA_BOOTSTRAP_SERVERS = {self.kafka_bootstrap_servers}")
            logger.info(f"🔍 DEBUG: KAFKA_CONSUMER_GROUP = {self.kafka_consumer_group}")
            logger.info(f"🔍 DEBUG: KAFKA_INPUT_TOPIC = {self.kafka_input_topic}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфигурации: {e}")
            raise


# =============================================================================
# kafka мониторинг класс
# =============================================================================

class KafkaLagMonitor:
    """Мониторинг Consumer Lag и производительности Kafka"""
    
    def __init__(self, config: KafkaMonitorConfig):
        self.config = config
        self.admin_client = None
        self.consumer = None
        self.metrics_history: List[Dict] = []
        
    def _init_kafka_connections(self):
        """инициализация подключений к kafka"""
        try:
            # kafka admin client для метрик
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                client_id='fraud-lag-monitor'
            )
            
            # consumer для проверки lag
            self.consumer = KafkaConsumer(
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id=f"{self.config.kafka_consumer_group}-monitor",
            enable_auto_commit=False,
            auto_offset_reset='latest'
        )
            
            logger.info(f"✅ kafka подключения инициализированы: {self.config.kafka_bootstrap_servers}")
            
        except Exception as e:
            logger.error(f"❌ ошибка инициализации kafka: {e}")
            raise
    
    def get_consumer_lag_info(self) -> Dict:
        """получение информации о consumer lag"""
        try:
            if not self.consumer:
                self._init_kafka_connections()
            
            # получаем информацию по топикам
            topics_to_monitor = [
                self.config.kafka_input_topic,
                self.config.kafka_output_topic,
                self.config.kafka_test_topic
            ]
            
            lag_info = {
                'timestamp': datetime.now().isoformat(),
                'consumer_group': self.config.kafka_consumer_group,
                'topics': {}
            }
            
            for topic in topics_to_monitor:
                try:
                    # получаем партиции топика
                    partitions = self.consumer.partitions_for_topic(topic)
                    if not partitions:
                        logger.warning(f"⚠️ топик {topic} не найден или пустой")
                        lag_info['topics'][topic] = {
                            'error': 'topic_not_found',
                            'status': 'missing'
                        }
                        continue
                    
                    topic_info = {
                        'partitions': {},
                        'total_lag': 0,
                        'total_current_offset': 0,
                        'total_log_end_offset': 0,
                        'consumer_group_exists': False
                    }
                    
                    # 🔧 ИСПРАВЛЕНО v8.1: Используем правильный подход с end_offsets()
                    # Создаем временный consumer для получения high water marks
                    end_offsets = {}
                    committed_offsets = {}
                    
                    try:
                        # Создаем топик партиции для всех партиций
                        topic_partitions = [TopicPartition(topic, p) for p in partitions]
                        
                        # Создаем временный consumer без группы для получения end offsets
                        temp_consumer = KafkaConsumer(
                            bootstrap_servers=self.config.kafka_bootstrap_servers,
                            auto_offset_reset='latest',
                            enable_auto_commit=False,
                            value_deserializer=lambda x: x,  # Простой deserializer
                            group_id=None  # Без group_id
                        )
                        
                        # Получаем end offsets (high water marks) - количество сообщений в топике
                        logger.debug(f"🔍 Получаем end_offsets для {len(topic_partitions)} партиций...")
                        end_offsets = temp_consumer.end_offsets(topic_partitions)
                        logger.debug(f"✅ end_offsets получены: {end_offsets}")
                        
                        temp_consumer.close()
                        
                        # Получаем committed offsets через основной consumer (если group существует) 
                        try:
                            committed_offsets = self.consumer.committed({tp: None for tp in topic_partitions})
                            logger.debug(f"✅ committed_offsets получены: {committed_offsets}")
                        except Exception as e:
                            logger.debug(f"🔍 Consumer group '{self.config.kafka_consumer_group}' не найдена: {e}")
                            committed_offsets = {tp: None for tp in topic_partitions}
                        
                    except Exception as e:
                        logger.error(f"❌ Ошибка получения offsets: {e}")
                        # Fallback to empty
                        end_offsets = {TopicPartition(topic, p): 0 for p in partitions}
                        committed_offsets = {TopicPartition(topic, p): None for p in partitions}
                    
                    for partition in partitions:
                        tp = TopicPartition(topic, partition)
                        
                        try:
                            # Получаем high water mark (количество сообщений в топике)
                            log_end_offset = end_offsets.get(tp, 0)
                            
                            # Получаем committed offset (позиция consumer группы)
                            committed = committed_offsets.get(tp)
                            current_offset = committed if committed is not None else 0
                            consumer_group_exists = committed is not None
                            
                            # Вычисляем lag
                            lag = log_end_offset - current_offset if consumer_group_exists else 0
                            
                            topic_info['partitions'][partition] = {
                                'current_offset': current_offset,
                                'log_end_offset': log_end_offset,
                                'lag': lag,
                                'consumer_group_exists': consumer_group_exists
                            }
                            
                            topic_info['total_lag'] += lag
                            topic_info['total_current_offset'] += current_offset
                            topic_info['total_log_end_offset'] += log_end_offset
                            topic_info['consumer_group_exists'] = consumer_group_exists or topic_info['consumer_group_exists']
                            
                            logger.debug(f"✅ Partition {partition}: end_offset={log_end_offset}, committed={current_offset}, lag={lag}")
                            
                        except Exception as partition_error:
                            logger.warning(f"⚠️ Ошибка обработки partition {partition}: {partition_error}")
                            topic_info['partitions'][partition] = {
                                'error': str(partition_error),
                                'consumer_group_exists': False,
                                'current_offset': 0,
                                'log_end_offset': 0,
                                'lag': 0
                            }
                    
                    # статус топика
                    if not topic_info['consumer_group_exists']:
                        topic_info['status'] = 'no_active_consumers'
                        logger.info(f"📊 {topic}: нет активных consumers, messages={topic_info['total_log_end_offset']}, partitions={len(partitions)}")
                    else:
                        topic_info['status'] = 'active_consumers'
                        logger.info(f"📊 {topic}: lag={topic_info['total_lag']}, "
                                  f"current={topic_info['total_current_offset']}, "
                                  f"end={topic_info['total_log_end_offset']}")
                    
                    lag_info['topics'][topic] = topic_info
                    
                except Exception as e:
                    logger.error(f"❌ ошибка мониторинга топика {topic}: {e}")
                    lag_info['topics'][topic] = {'error': str(e)}
            
            return lag_info
            
        except Exception as e:
            logger.error(f"❌ ошибка получения consumer lag: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def calculate_throughput_metrics(self, lag_info: dict) -> Dict:
        """вычисление метрик производительности и throughput"""
        try:
            current_time = datetime.now()
            
            # базовые метрики
            throughput_metrics = {
                'timestamp': current_time.isoformat(),
                'topics': {},
                'alerts': [],
                'overall_health': 'healthy'
            }
            
            for topic, topic_info in lag_info.get('topics', {}).items():
                if 'error' in topic_info and topic_info.get('status') != 'no_active_consumers':
                    continue
                
                total_lag = topic_info.get('total_lag', 0)
                log_end_offset = topic_info.get('total_log_end_offset', 0)
                consumer_group_exists = topic_info.get('consumer_group_exists', False)
                topic_status = topic_info.get('status', 'unknown')
                
                # определяем статус здоровья топика
                if topic_status == 'no_active_consumers':
                    # если нет активных consumer'ов, но топик содержит сообщения - это нормально для начальной стадии
                    health_status = 'waiting_consumers'
                    if log_end_offset > 0:
                        # есть сообщения, но нет потребителей - может быть предупреждением в продакшене
                        health_status = 'pending_consumption'
                        
                elif total_lag > self.config.max_lag_threshold:
                    health_status = 'critical'
                    throughput_metrics['overall_health'] = 'critical'
                    
                    alert = {
                        'type': 'consumer_lag_critical',
                        'topic': topic,
                        'lag': total_lag,
                        'threshold': self.config.max_lag_threshold,
                        'message': f"🚨 consumer lag критический для {topic}: {total_lag} > {self.config.max_lag_threshold}"
                    }
                    throughput_metrics['alerts'].append(alert)
                    
                elif total_lag > (self.config.max_lag_threshold * 0.7):
                    health_status = 'warning'
                    if throughput_metrics['overall_health'] == 'healthy':
                        throughput_metrics['overall_health'] = 'warning'
                else:
                    health_status = 'healthy'
                
                # расчет tps (если есть история)
                estimated_tps = 0
                if len(self.metrics_history) > 1:
                    prev_metrics = self.metrics_history[-1]
                    time_diff = (current_time - datetime.fromisoformat(prev_metrics['timestamp'])).total_seconds()
                    
                    if time_diff > 0 and topic in prev_metrics.get('topics', {}):
                        prev_offset = prev_metrics['topics'][topic].get('total_log_end_offset', 0)
                        offset_diff = log_end_offset - prev_offset
                        estimated_tps = offset_diff / time_diff
                
                throughput_metrics['topics'][topic] = {
                    'total_lag': total_lag,
                    'log_end_offset': log_end_offset,
                    'health_status': health_status,
                    'estimated_tps': round(estimated_tps, 2),
                    'partitions_count': len(topic_info.get('partitions', {}))
                }
                
                logger.info(f"📈 {topic}: lag={total_lag}, tps~={estimated_tps:.1f}, status={health_status}")
            
            # сохраняем в историю (последние 100 записей)
            self.metrics_history.append({
                'timestamp': current_time.isoformat(),
                'topics': {k: {'total_log_end_offset': v.get('total_log_end_offset', 0)} 
                          for k, v in lag_info.get('topics', {}).items()}
            })
            
            if len(self.metrics_history) > 100:
                self.metrics_history = self.metrics_history[-100:]
            
            return throughput_metrics
            
        except Exception as e:
            logger.error(f"❌ ошибка расчета throughput метрик: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def generate_performance_report(self) -> Dict:
        """генерация отчета о производительности"""
        try:
            logger.info("📊 генерация отчета о производительности системы...")
            
            # получаем текущие метрики lag
            lag_info = self.get_consumer_lag_info()
            
            # вычисляем throughput метрики
            throughput_metrics = self.calculate_throughput_metrics(lag_info)
            
            # формируем итоговый отчет
            report = {
                'report_timestamp': datetime.now().isoformat(),
                'monitoring_config': {
                    'consumer_group': self.config.kafka_consumer_group,
                    'max_lag_threshold': self.config.max_lag_threshold,
                    'environment': self.config.environment
                },
                'consumer_lag_info': lag_info,
                'throughput_metrics': throughput_metrics,
                'recommendations': []
            }
            
            # добавляем рекомендации на основе метрик
            overall_health = throughput_metrics.get('overall_health', 'unknown')
            
            # проверяем статус consumer groups
            topics_without_consumers = []
            topics_with_messages = []
            
            for topic, topic_info in lag_info.get('topics', {}).items():
                if topic_info.get('status') == 'no_active_consumers':
                    topics_without_consumers.append(topic)
                    if topic_info.get('total_log_end_offset', 0) > 0:
                        topics_with_messages.append((topic, topic_info.get('total_log_end_offset', 0)))
            
            # рекомендации для разных сценариев
            if topics_without_consumers:
                report['recommendations'].append({
                    'type': 'consumer_setup',
                    'priority': 'high',
                    'message': f'🎯 запустите spark streaming consumer для топиков: {", ".join(topics_without_consumers)}'
                })
                
            if topics_with_messages:
                messages_info = ", ".join([f"{topic}({count})" for topic, count in topics_with_messages])
                report['recommendations'].append({
                    'type': 'pending_processing',
                    'priority': 'medium',
                    'message': f'⏳ есть необработанные сообщения: {messages_info} - запустите потребителей'
                })
            
            if overall_health == 'critical':
                report['recommendations'].append({
                    'type': 'scaling',
                    'priority': 'high',
                    'message': '🔄 рекомендуется увеличить количество spark executors или партиций'
                })
                
            elif overall_health == 'warning':
                report['recommendations'].append({
                    'type': 'monitoring',
                    'priority': 'medium', 
                    'message': '👀 необходимо усиленное наблюдение за производительностью'
                })
                
            elif not topics_without_consumers:
                report['recommendations'].append({
                    'type': 'optimization',
                    'priority': 'low',
                    'message': '✅ система работает стабильно, можно оптимизировать настройки'
                })
            
            logger.info(f"✅ отчет сгенерирован. общий статус: {overall_health}")
            
            return report
            
        except Exception as e:
            logger.error(f"❌ ошибка генерации отчета: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def close_connections(self):
        """закрытие подключений к kafka"""
        try:
            if self.consumer:
                self.consumer.close()
                logger.info("✅ kafka consumer закрыт")
                
            if self.admin_client:
                self.admin_client.close()
                logger.info("✅ kafka admin client закрыт")
                
        except Exception as e:
            logger.error(f"⚠️ ошибка закрытия подключений: {e}")


# =============================================================================
# airflow task functions
# =============================================================================

def run_lag_monitoring_check(**context):
    """
    задача мониторинга consumer lag
    выполняет одиночную проверку состояния всех consumer groups
    """
    logger.info("🎯 TASK START: run_lag_monitoring_check начинает выполнение")
    logger.info("🔍 запуск проверки consumer lag...")
    
    try:
        # инициализация
        logger.info("📋 STEP: Создаем KafkaMonitorConfig()")
        config = KafkaMonitorConfig()
        monitor = KafkaLagMonitor(config)
        
        # выполнение мониторинга
        report = monitor.generate_performance_report()
        
        # логирование результатов
        logger.info("=" * 80)
        logger.info("📊 отчет о мониторинге consumer lag")
        logger.info("=" * 80)
        
        # общий статус
        overall_health = report.get('throughput_metrics', {}).get('overall_health', 'unknown')
        logger.info(f"🌡️  общий статус системы: {overall_health.upper()}")
        
        # детали по топикам
        for topic, metrics in report.get('throughput_metrics', {}).get('topics', {}).items():
            lag = metrics.get('total_lag', 0)
            tps = metrics.get('estimated_tps', 0)
            health = metrics.get('health_status', 'unknown')
            partitions = metrics.get('partitions_count', 0)
            
            # получаем дополнительную информацию из lag_info
            lag_details = report.get('consumer_lag_info', {}).get('topics', {}).get(topic, {})
            consumer_group_exists = lag_details.get('consumer_group_exists', False)
            topic_status = lag_details.get('status', 'unknown')
            messages_count = lag_details.get('total_log_end_offset', 0)
            
            logger.info(f"📈 {topic}:")
            
            if topic_status == 'no_active_consumers':
                logger.info(f"   ├── сообщений в топике: {messages_count:,}")
                logger.info(f"   ├── consumer group: не активен")
                logger.info(f"   ├── статус: {health} (ожидает потребителей)")
                logger.info(f"   └── партиции: {partitions}")
            else:
                logger.info(f"   ├── lag: {lag:,} сообщений")
                logger.info(f"   ├── tps: ~{tps} транзакций/сек")
                logger.info(f"   ├── статус: {health}")
                logger.info(f"   └── партиции: {partitions}")
        
        # алерты
        alerts = report.get('throughput_metrics', {}).get('alerts', [])
        if alerts:
            logger.info("🚨 активные алерты:")
            for alert in alerts:
                logger.info(f"   ⚠️  {alert.get('message', 'неизвестный алерт')}")
        
        # рекомендации
        recommendations = report.get('recommendations', [])
        if recommendations:
            logger.info("💡 рекомендации:")
            for rec in recommendations:
                logger.info(f"   🔧 [{rec.get('priority', 'unknown')}] {rec.get('message', 'нет описания')}")
        
        logger.info("=" * 80)
        
        # сохраняем отчет в xcom для других задач
        context['task_instance'].xcom_push(key='lag_monitoring_report', value=report)
        
        # определяем успешность на основе здоровья системы
        if overall_health == 'critical':
            logger.error("❌ критические проблемы с производительностью!")
            return "critical_issues_detected"
        elif overall_health == 'warning':
            logger.warning("⚠️ обнаружены предупреждения о производительности")
            return "warnings_detected"
        else:
            logger.info("✅ мониторинг завершен успешно")
            return "monitoring_successful"
            
    except Exception as e:
        logger.error(f"❌ ошибка выполнения мониторинга: {e}")
        raise
    
    finally:
        try:
            monitor.close_connections()
        except:
            pass


def run_continuous_lag_monitoring(**context):
    """
    задача непрерывного мониторинга consumer lag
    выполняет мониторинг в течение заданного времени с интервалами
    """
    logger.info("🔄 запуск непрерывного мониторинга consumer lag...")
    
    try:
        # инициализация
        config = KafkaMonitorConfig()
        monitor = KafkaLagMonitor(config)
        
        # параметры непрерывного мониторинга
        monitoring_duration = 300  # 5 минут
        check_interval = config.lag_check_interval  # из конфигурации
        
        start_time = time.time()
        reports = []
        
        logger.info(f"⏰ непрерывный мониторинг: {monitoring_duration}с с интервалом {check_interval}с")
        
        while (time.time() - start_time) < monitoring_duration:
            try:
                # генерируем отчет
                report = monitor.generate_performance_report()
                reports.append(report)
                
                # краткое логирование
                overall_health = report.get('throughput_metrics', {}).get('overall_health', 'unknown')
                total_lag = sum([
                    metrics.get('total_lag', 0) 
                    for metrics in report.get('throughput_metrics', {}).get('topics', {}).values()
                ])
                
                elapsed = int(time.time() - start_time)
                logger.info(f"[{elapsed:03d}s] 📊 статус: {overall_health}, общий lag: {total_lag:,}")
                
                # проверяем критические алерты
                alerts = report.get('throughput_metrics', {}).get('alerts', [])
                for alert in alerts:
                    if alert.get('type') == 'consumer_lag_critical':
                        logger.error(f"🚨 критический алерт: {alert.get('message')}")
                
            except Exception as e:
                logger.error(f"❌ ошибка в итерации мониторинга: {e}")
            
            # ждем до следующей проверки
            time.sleep(check_interval)
        
        # итоговый анализ
        logger.info("📈 анализ результатов непрерывного мониторинга...")
        
        if reports:
            # статистика по здоровью системы
            health_stats = {}
            for report in reports:
                health = report.get('throughput_metrics', {}).get('overall_health', 'unknown')
                health_stats[health] = health_stats.get(health, 0) + 1
            
            logger.info("📊 статистика здоровья системы:")
            for health, count in health_stats.items():
                percentage = (count / len(reports)) * 100
                logger.info(f"   {health}: {count} раз ({percentage:.1f}%)")
            
            # сохраняем все отчеты в xcom
            context['task_instance'].xcom_push(key='continuous_monitoring_reports', value=reports)
            
            # финальная оценка
            critical_count = health_stats.get('critical', 0)
            if critical_count > len(reports) * 0.1:  # более 10% критических
                logger.error(f"❌ слишком много критических состояний: {critical_count}/{len(reports)}")
                return "frequent_critical_issues"
            else:
                logger.info("✅ непрерывный мониторинг завершен успешно")
                return "continuous_monitoring_successful"
        else:
            logger.warning("⚠️ нет данных мониторинга")
            return "no_monitoring_data"
            
    except Exception as e:
        logger.error(f"❌ ошибка непрерывного мониторинга: {e}")
        raise
    
    finally:
        try:
            monitor.close_connections()
        except:
            pass


# =============================================================================
# dag определение
# =============================================================================

# параметры dag
default_args = {
    'owner': 'mlops-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

# Создание DAG
dag = DAG(
    'tasks8_etap3_kafka_lag_monitor_v8_1',
    default_args=default_args,
    description='🔧 v8.1: мониторинг consumer lag (ИСПРАВЛЕНО - правильные end_offsets)',
    schedule=timedelta(minutes=10),  # каждые 10 минут (новый синтаксис)
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'kafka', 'monitoring', 'fraud-detection', 'tasks8']
)

# =============================================================================
# задачи dag
# =============================================================================

# задача 1: одиночная проверка consumer lag
single_lag_check = PythonOperator(
    task_id='single_lag_check',
    python_callable=run_lag_monitoring_check,
    dag=dag,
    doc_md="""
    ## одиночная проверка consumer lag
    
    выполняет разовую проверку состояния consumer lag для всех отслеживаемых топиков:
    - transactions-input (входящие транзакции)
    - fraud-predictions (результаты инференса) 
    - transactions-input-test (тестовые данные)
    
    **метрики:**
    - consumer lag по партициям
    - throughput estimation
    - health status оценка
    - генерация алертов при превышении порогов
    """
)

# задача 2: непрерывный мониторинг (для нагрузочных тестов)
continuous_lag_monitor = PythonOperator(
    task_id='continuous_lag_monitor',
    python_callable=run_continuous_lag_monitoring,
    dag=dag,
    doc_md="""
    ## непрерывный мониторинг consumer lag
    
    выполняет непрерывный мониторинг в течение 5 минут с интервалом 30 секунд.
    используется для отслеживания производительности во время нагрузочных тестов.
    
    **анализ:**
    - тренды изменения lag
    - статистика стабильности системы  
    - выявление узких мест производительности
    - рекомендации по оптимизации
    """
)

# задача 3: анализ производительности топиков
def analyze_topic_performance(**context):
    """анализ производительности всех топиков"""
    logger.info("📊 анализ производительности топиков...")
    
    try:
        # получаем отчеты из предыдущих задач
        single_report = context['task_instance'].xcom_pull(
            task_ids='single_lag_check', 
            key='lag_monitoring_report'
        )
        
        continuous_reports = context['task_instance'].xcom_pull(
            task_ids='continuous_lag_monitor',
            key='continuous_monitoring_reports'
        )
        
        if not single_report and not continuous_reports:
            logger.warning("⚠️ нет данных для анализа")
            return "no_data_for_analysis"
        
        # анализируем производительность
        analysis = {
            'analysis_timestamp': datetime.now().isoformat(),
            'single_check_result': 'available' if single_report else 'missing',
            'continuous_monitoring_result': 'available' if continuous_reports else 'missing',
            'performance_summary': {}
        }
        
        if single_report:
            topics = single_report.get('throughput_metrics', {}).get('topics', {})
            for topic, metrics in topics.items():
                analysis['performance_summary'][topic] = {
                    'lag': metrics.get('total_lag', 0),
                    'health': metrics.get('health_status', 'unknown'),
                    'tps': metrics.get('estimated_tps', 0)
                }
        
        logger.info("=" * 60)
        logger.info("📈 итоговый анализ производительности")
        logger.info("=" * 60)
        
        for topic, summary in analysis.get('performance_summary', {}).items():
            logger.info(f"🎯 {topic}:")
            logger.info(f"   ├── lag: {summary['lag']:,}")  
            logger.info(f"   ├── health: {summary['health']}")
            logger.info(f"   └── tps: ~{summary['tps']}")
        
        # сохраняем итоговый анализ
        context['task_instance'].xcom_push(key='performance_analysis', value=analysis)
        
        logger.info("✅ анализ производительности завершен")
        return "analysis_completed"
        
    except Exception as e:
        logger.error(f"❌ ошибка анализа производительности: {e}")
        raise

topic_performance_analysis = PythonOperator(
    task_id='topic_performance_analysis',
    python_callable=analyze_topic_performance,
    dag=dag,
    doc_md="""
    ## анализ производительности топиков
    
    объединяет результаты одиночной проверки и непрерывного мониторинга
    для создания итогового отчета о производительности всех топиков.
    """
)

# =============================================================================
# определение зависимостей
# =============================================================================

# зависимости задач
single_lag_check >> topic_performance_analysis
continuous_lag_monitor >> topic_performance_analysis

# документация dag
dag.doc_md = """
# шаг 3: мониторинг consumer lag

этот dag реализует комплексный мониторинг производительности kafka consumer'ов 
для системы потокового инференса мошенничества.

## основные функции:

1. **одиночная проверка consumer lag** - разовая оценка текущего состояния
2. **непрерывный мониторинг** - отслеживание в течение 5 минут для нагрузочных тестов  
3. **анализ производительности** - итоговый отчет и рекомендации

## отслеживаемые топики:

- `transactions-input` - входящие транзакции от producer
- `fraud-predictions` - результаты spark streaming инференса
- `transactions-input-test` - тестовые данные для отладки

## метрики мониторинга:

- **consumer lag** по партициям и общий
- **throughput** (tps) оценка
- **health status** (healthy/warning/critical)
- **алерты** при превышении пороговых значений

## настройки алертов:

- максимальный lag: 1000 сообщений (perf_test_max_lag_threshold)
- интервал проверки: 30 секунд (monitoring_kafka_lag_check_interval)
- consumer group: fraud-detection-group

## использование:

этот dag автоматически запускается каждые 10 минут для базового мониторинга.
для нагрузочных тестов может запускаться вручную или из координирующего dag.
"""

if __name__ == "__main__":
    logger.info("dag шаг 3: kafka lag monitor загружен успешно")