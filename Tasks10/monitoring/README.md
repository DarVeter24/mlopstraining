# Tasks10 Monitoring Setup

Конфигурация мониторинга для Tasks10 ML API с автоматическим масштабированием.

## 📈 Метрики и пороговые значения

### ML Model Metrics ✅
| Метрика | Описание | Пороговое значение | Статус |
|---------|----------|-------------------|---------|
| `ml_model_predictions_total` | Общее количество предсказаний (counter) | - | ✅ Работает |
| `ml_model_prediction_duration_seconds` | Время выполнения предсказаний (histogram) | > 1s (warning) | ✅ Работает |
| `ml_model_fraud_probability` | Распределение вероятностей мошенничества (histogram) | > 20% fraud rate (info) | ✅ Работает |
| `ml_model_confidence_score` | Уровень уверенности модели (histogram) | - | ✅ Работает |
| `ml_model_active_requests` | Количество активных запросов (gauge) | > 50 (info) | ✅ Работает |

### HTTP Performance Metrics ✅
| Метрика | Описание | Пороговое значение | Статус |
|---------|----------|-------------------|---------|
| `http_requests_total` | Общее количество HTTP запросов (counter) | - | ✅ Работает |
| `http_request_duration_seconds` | Время обработки HTTP запросов (histogram) | 95p > 2s (warning) | ✅ Работает |
| `http_requests_in_progress` | Активные HTTP запросы (gauge) | > 20 (warning) | ✅ Работает |

### System Resources ✅
| Метрика | Описание | Пороговое значение | Статус |
|---------|----------|-------------------|---------|
| `ml_model_cpu_usage_percent` | Использование CPU в процентах (gauge) | > 80% (warning) | ✅ Работает |
| `ml_model_memory_usage_bytes` | Использование памяти в байтах (gauge) | > 70% (warning) | ✅ Работает |
| `ml_model_active_requests` | Активные запросы к модели (gauge) | > 50 (info) | ✅ Работает |

### Kubernetes HPA Metrics ✅
| Метрика | Описание | Пороговое значение | Статус |
|---------|----------|-------------------|---------|
| `kube_deployment_status_replicas` | Текущее количество реплик | - | ✅ Работает |
| `kube_horizontalpodautoscaler_spec_min_replicas` | Минимум реплик HPA | - | ✅ Работает |
| `kube_horizontalpodautoscaler_spec_max_replicas` | Максимум реплик HPA | - | ✅ Работает |

## 🚨 Алерты и уведомления

### Critical Alerts (требуют немедленного внимания)
- **🚨 AdminNotification_MaxScaleHighCPU**: **6 экземпляров + CPU > 80% за 5 минут** → Уведомление администратора
- **PodCrashLooping**: Pod перезапускается
- **ServiceDown**: Сервис недоступен  
- **HighMemoryUsage**: Memory > 85% за 5 минут
- **NoMLPredictions**: Нет предсказаний модели 10 минут

### Warning Alerts (требуют внимания)
- **HighCPUUsage**: CPU > 80% за 5 минут → HPA scaling
- **SlowResponseTime**: Response time > 2s (95p)
- **HighMemoryUsage**: Memory > 70% за 10 минут
- **HighActiveRequests**: Активных запросов > 50

### Info Alerts (информационные)
- **ModelScaledToMax**: Достигнут максимум реплик (6)
- **HPAScalingEvent**: Событие масштабирования HPA
- **HighFraudDetectionRate**: Высокий процент мошеннических транзакций
- **ModelConfidenceLow**: Низкая уверенность модели в предсказаниях

## 📊 Структура дашборда и методы анализа

### 🎯 Блок "ML Model Performance"

**Панели:**
1. **Predictions per Minute** (Time Series)
   - **Метрика**: `rate(ml_model_predictions_total[5m])`
   - **Показывает**: Количество предсказаний в минуту по типам результата
   - **Анализ**: 
     - Отслеживайте тренды в объеме предсказаний
     - Сравнивайте соотношение legitimate vs fraud
     - Резкие падения могут указывать на проблемы с моделью
   - **Нормальные значения**: 10-100 предсказаний/мин

2. **Average Prediction Time** (Gauge)
   - **Метрика**: `rate(ml_model_prediction_duration_seconds_sum[5m]) / rate(ml_model_prediction_duration_seconds_count[5m]) * 1000`
   - **Показывает**: Среднее время выполнения предсказаний в мс
   - **Анализ**:
     - Зеленый < 1000ms - отлично
     - Желтый 1000-2000ms - внимание
     - Красный > 2000ms - проблема производительности
   - **Действия**: При превышении 1500ms проверить нагрузку на модель

### 🌐 Блок "HTTP Performance"

**Панели:**
3. **HTTP Request Rate** (Time Series)
   - **Метрика**: `rate(http_requests_total[5m])`
   - **Показывает**: HTTP запросы в секунду по методам и статус-кодам
   - **Анализ**:
     - Мониторинг общей нагрузки на API
     - Выявление аномальных пиков трафика
     - Анализ распределения по эндпоинтам
   - **Нормальные значения**: 5-50 запросов/сек

4. **HTTP Error Rate** (Gauge)
   - **Метрика**: `(rate(http_requests_total{status_code=~"4..|5.."}[5m]) / rate(http_requests_total[5m])) * 100`
   - **Показывает**: Процент HTTP ошибок от общего количества запросов
   - **Анализ**:
     - Зеленый < 2% - нормально
     - Желтый 2-5% - требует внимания
     - Красный > 5% - критическая ситуация
   - **Действия**: При > 3% проверить логи приложения

### 🚀 Блок "System Resources & Scaling"

**Панели:**
5. **CPU Usage** (Time Series)
   - **Метрики**: 
     - `sum(rate(container_cpu_usage_seconds_total{namespace="mlops-tasks10",container="tasks10-ml-service-api"}[5m])) * 100` - суммарное CPU всех подов
     - `avg(rate(container_cpu_usage_seconds_total{namespace="mlops-tasks10",container="tasks10-ml-service-api"}[5m])) * 100` - среднее CPU на под
   - **Показывает**: Использование CPU подами ML API в процентах (агрегированное по всем подам)
   - **Анализ**:
     - **Total CPU Usage**: суммарное потребление всех подов (растет с количеством подов)
     - **Average CPU per Pod**: среднее потребление на под (должно быть стабильным)
     - Зеленый < 70% - нормальная нагрузка
     - Желтый 70-80% - повышенная нагрузка  
     - Красный > 80% - критическая нагрузка, триггер HPA
   - **Действия**: При среднем CPU > 80% проверить масштабирование HPA

6. **Memory Usage** (Time Series)
   - **Метрики**:
     - `sum(container_memory_usage_bytes{namespace="mlops-tasks10",container="tasks10-ml-service-api"})` - суммарная память всех подов
     - `avg(container_memory_usage_bytes{namespace="mlops-tasks10",container="tasks10-ml-service-api"})` - средняя память на под
   - **Показывает**: Использование памяти подами ML API (агрегированное по всем подам)
   - **Анализ**:
     - **Total Memory Usage**: суммарное потребление всех подов
     - **Average Memory per Pod**: среднее потребление на под (~130-140MB нормально)
     - Следите за трендами роста памяти на под (признак memory leak)
   - **Действия**: При росте памяти на под > 200MB проверить memory leaks

7. **Pod Scaling (HPA)** (Time Series)
   - **Метрики**:
     - `kube_deployment_status_replicas` - текущие реплики
     - `kube_horizontalpodautoscaler_spec_min_replicas` - минимум
     - `kube_horizontalpodautoscaler_spec_max_replicas` - максимум
   - **Показывает**: Динамику автоматического масштабирования
   - **Анализ**:
     - Частые изменения = нестабильная нагрузка
     - Достижение максимума = нужно увеличить лимиты
     - Долгое время на максимуме = постоянная высокая нагрузка

### 📊 Блок "Additional ML Metrics"

**Панели:**
8. **ML Model Active Requests** (Time Series)
   - **Метрика**: `ml_model_active_requests`
   - **Показывает**: Количество запросов в обработке
   - **Анализ**:
     - Зеленый < 30 - нормально
     - Желтый 30-50 - высокая нагрузка
     - Красный > 50 - возможна перегрузка
   - **Корреляция**: С CPU usage и response time

9. **HTTP Requests in Progress** (Gauge)
   - **Метрика**: `http_requests_in_progress`
   - **Показывает**: HTTP запросы в процессе обработки
   - **Анализ**:
     - Зеленый < 10 - нормально
     - Желтый 10-20 - внимание
     - Красный > 20 - перегрузка сервера

## 🔍 Методы комплексного анализа

### 1. **Анализ производительности модели**
```
1. Проверьте Predictions per Minute - стабильный ли объем?
2. Сопоставьте с Average Prediction Time - не растет ли время?
3. Посмотрите на Active Requests - нет ли накопления?
4. Проверьте CPU Usage - не превышает ли 80%?
5. Проверьте Memory Usage - нет ли утечек памяти?
```

### 2. **Диагностика проблем производительности**
```
Симптомы высокого времени ответа:
→ Average Prediction Time > 1500ms
→ HTTP Request Rate падает
→ HTTP Error Rate растет
→ CPU Usage > 80%
→ Active Requests > 30

Действия:
1. Проверить HPA - масштабируется ли?
2. Увеличить ресурсы (CPU/Memory limits)
3. Оптимизировать модель ML
```

### 3. **Мониторинг масштабирования**
```
Правильное масштабирование:
→ CPU Usage > 80% → Replicas увеличиваются
→ Нагрузка падает → CPU Usage < 50% → Replicas уменьшаются
→ Memory Usage стабильна при масштабировании
→ Response time остается < 1000ms

Проблемы:
→ CPU Usage > 80%, но Replicas не растут = проблема HPA
→ Memory Usage растет при масштабировании = memory leak
→ Replicas на максимуме долго = увеличить max_replicas
→ Частые колебания = настроить stabilization
```

### 4. **Корреляционный анализ**
```
Нормальные корреляции:
→ ↑ HTTP Request Rate → ↑ Active Requests → ↑ CPU Usage
→ ↑ CPU Usage → ↑ Replicas (через HPA)
→ ↑ Replicas → ↓ CPU Usage per pod
→ ↑ Active Requests → ↑ Memory Usage (временно)

Аномалии:
→ ↑ Request Rate, но ↓ Predictions = проблема с моделью
→ ↑ CPU Usage, но Replicas не растут = проблема с HPA
→ ↑ Memory Usage без роста нагрузки = memory leak
→ ↑ Error Rate без роста нагрузки = проблема приложения
→ CPU Usage норма, но Memory Usage растет = inefficient code
```

## 🚨 Алертинг и пороговые значения

### Критические алерты (немедленные действия)
- **Average Prediction Time > 3000ms** - модель перегружена
- **HTTP Error Rate > 10%** - серьезные проблемы с API  
- **Active Requests > 100** - система не справляется
- **All replicas down** - полный отказ сервиса

### Warning алерты (требуют внимания)
- **Average Prediction Time > 1500ms** - производительность падает
- **HTTP Error Rate > 3%** - начинающиеся проблемы
- **CPU Usage > 80%** - нужно масштабирование
- **Active Requests > 50** - высокая нагрузка

## 📋 Практические сценарии использования дашборда

### 🔥 Сценарий 1: "Высокое время ответа модели"
**Симптомы в дашборде:**
- Average Prediction Time > 2000ms (красная зона)
- ML Model Active Requests растет
- CPU Usage приближается к 80%
- Memory Usage может расти

**Пошаговый анализ:**
1. **Проверьте нагрузку**: HTTP Request Rate - есть ли пик трафика?
2. **Оцените масштабирование**: Pod Scaling (HPA) - увеличиваются ли реплики?
3. **Проверьте CPU**: CPU Usage - превышает ли 80%?
4. **Проверьте Memory**: Memory Usage - нет ли утечек памяти?
5. **Корреляция**: Active Requests vs CPU/Memory Usage - пропорциональный рост?

**Действия:**
```bash
# Проверить статус HPA
kubectl get hpa -n mlops-tasks10

# Увеличить ресурсы если нужно
kubectl patch deployment tasks10-ml-service-api -n mlops-tasks10 -p '{"spec":{"template":{"spec":{"containers":[{"name":"ml-api","resources":{"limits":{"cpu":"2000m","memory":"2Gi"}}}]}}}}'

# Увеличить max replicas в HPA
kubectl patch hpa tasks10-ml-service-hpa -n mlops-tasks10 -p '{"spec":{"maxReplicas":10}}'
```

### ⚠️ Сценарий 2: "Высокий процент ошибок"
**Симптомы в дашборде:**
- HTTP Error Rate > 5% (красная зона)
- HTTP Request Rate может быть нормальным
- Predictions per Minute падает

**Пошаговый анализ:**
1. **Тип ошибок**: В HTTP Request Rate посмотрите на status_code - 4xx или 5xx?
2. **Корреляция с нагрузкой**: Ошибки растут с нагрузкой или независимо?
3. **Модель работает**: Predictions per Minute - генерируются ли предсказания?
4. **Ресурсы**: Resource Usage - нет ли нехватки памяти?

**Действия по типу ошибок:**
```bash
# 4xx ошибки - проблемы с запросами
kubectl logs -n mlops-tasks10 -l app=tasks10-ml-service-api --tail=100 | grep "400\|404\|422"

# 5xx ошибки - проблемы сервера
kubectl logs -n mlops-tasks10 -l app=tasks10-ml-service-api --tail=100 | grep "500\|502\|503"

# Проверить статус подов
kubectl get pods -n mlops-tasks10 -l app=tasks10-ml-service-api
```

### 📈 Сценарий 3: "Неэффективное масштабирование"
**Симптомы в дашборде:**
- CPU Usage > 80% длительное время
- Pod Scaling (HPA) не увеличивает replicas
- Average Prediction Time растет

**Пошаговый анализ:**
1. **Статус HPA**: Replicas достигли максимума?
2. **Метрики HPA**: Какие метрики использует HPA для принятия решений?
3. **Ресурсы узлов**: Достаточно ли ресурсов в кластере?
4. **Стабилизация**: Не слишком ли агрессивные настройки стабилизации?

**Диагностика и исправление:**
```bash
# Проверить статус HPA
kubectl describe hpa tasks10-ml-service-hpa -n mlops-tasks10

# Проверить доступные ресурсы узлов
kubectl top nodes

# Проверить события HPA
kubectl get events -n mlops-tasks10 --field-selector involvedObject.name=tasks10-ml-service-hpa

# Настроить более быстрое масштабирование
kubectl patch hpa tasks10-ml-service-hpa -n mlops-tasks10 -p '{"spec":{"behavior":{"scaleUp":{"stabilizationWindowSeconds":60}}}}'
```

### 🎯 Сценарий 4: "Аномалии в предсказаниях модели"
**Симптомы в дашборде:**
- Predictions per Minute: странное соотношение legitimate vs fraud
- HTTP Request Rate нормальный
- Average Prediction Time в норме

**Анализ качества модели:**
1. **Соотношение результатов**: Обычно legitimate >> fraud
2. **Тренды**: Резкие изменения в распределении
3. **Корреляция с нагрузкой**: Меняется ли качество при высокой нагрузке?
4. **Версия модели**: Не было ли недавних обновлений?

**Проверка модели:**
```bash
# Проверить версию модели в метриках
curl -s http://tasks10-ml-api.darveter.com/metrics | grep ml_model_predictions_total | grep model_version

# Проверить логи модели
kubectl logs -n mlops-tasks10 -l app=tasks10-ml-service-api --tail=200 | grep -i "prediction\|model\|fraud"

# Тестовый запрос с известными данными
curl -X POST http://tasks10-ml-api.darveter.com/predict \
  -H "Content-Type: application/json" \
  -d '{"transaction_id": "test", "customer_id": 123, "terminal_id": 456, "tx_amount": 100.0, "tx_time_seconds": 1693737600, "tx_time_days": 19723, "tx_fraud_scenario": 0}'
```

### 💾 Сценарий 5: "Утечка памяти"
**Симптомы в дашборде:**
- Memory Usage постоянно растет без снижения
- CPU Usage может быть нормальным
- Pod Scaling (HPA) увеличивает реплики, но проблема остается
- Average Prediction Time может расти

**Пошаговый анализ:**
1. **Тренд памяти**: Memory Usage - постоянный рост или пики?
2. **Корреляция с нагрузкой**: Растет ли память пропорционально запросам?
3. **Поведение после рестарта**: Помогает ли перезапуск подов?
4. **Garbage Collection**: Проверить логи GC (если Python/Java)

**Диагностика и исправление:**
```bash
# Проверить потребление памяти подами
kubectl top pods -n mlops-tasks10 -l app=tasks10-ml-service-api

# Рестарт подов для проверки
kubectl rollout restart deployment tasks10-ml-service-api -n mlops-tasks10

# Проверить логи на ошибки памяти
kubectl logs -n mlops-tasks10 -l app=tasks10-ml-service-api --tail=500 | grep -i "memory\|oom\|killed"

# Увеличить memory limits временно
kubectl patch deployment tasks10-ml-service-api -n mlops-tasks10 -p '{"spec":{"template":{"spec":{"containers":[{"name":"ml-api","resources":{"limits":{"memory":"3Gi"}}}]}}}}'

# Настроить memory requests для лучшего планирования
kubectl patch deployment tasks10-ml-service-api -n mlops-tasks10 -p '{"spec":{"template":{"spec":{"containers":[{"name":"ml-api","resources":{"requests":{"memory":"1Gi"}}}]}}}}'
```

## 🎓 Обучение работе с дашбордом

### Ежедневная проверка (5 минут)
1. **Общее состояние**: Все панели зеленые?
2. **Тренды**: Predictions per Minute стабильны?
3. **Производительность**: Average Prediction Time < 1000ms?
4. **Ошибки**: HTTP Error Rate < 2%?
5. **CPU**: CPU Usage < 80%?
6. **Memory**: Memory Usage стабильна, нет постоянного роста?
7. **Масштабирование**: HPA работает корректно?

### Еженедельный анализ (15 минут)
1. **Тренды нагрузки**: Изменения в HTTP Request Rate за неделю
2. **Эффективность масштабирования**: Корреляция CPU Usage и Replicas
3. **Анализ памяти**: Тренды Memory Usage, нет ли утечек?
4. **Качество модели**: Стабильность соотношения legitimate/fraud
5. **Производительность**: Тренды Average Prediction Time
6. **Планирование ресурсов**: Нужно ли увеличить CPU/Memory лимиты?

### Месячный отчет (30 минут)
1. **SLA метрики**: % времени с Response Time < 1000ms
2. **Availability**: % времени без критических ошибок
3. **Ресурсы**: Статистика CPU/Memory utilization
4. **Масштабирование**: Статистика использования реплик
5. **Оптимизация**: Рекомендации по настройке CPU/Memory лимитов
6. **Планирование**: Прогноз роста нагрузки и потребности в ресурсах

## 🧪 Тестирование системы мониторинга

### 1. Тест метрик
```bash
# Генерация запросов для проверки метрик
for i in {1..10}; do
  curl -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{
      "transaction_id": "test-'$i'",
      "customer_id": 123,
      "terminal_id": 456,
      "tx_amount": 100.0,
      "tx_time_seconds": 1693737600,
      "tx_time_days": 19723,
      "tx_fraud_scenario": 0
    }'
  sleep 1
done

# Проверка метрик
curl http://tasks10-ml-api.darveter.com/metrics | grep ml_model_predictions_total
```

### 2. 🚨 Тест алерта администратора (MaxScale + HighCPU)
```bash
# КРИТИЧЕСКИЙ ТЕСТ: Проверка алерта при 6 экземплярах + CPU > 80%
# ⚠️ ВНИМАНИЕ: Этот тест создает высокую нагрузку и может вызвать реальные алерты!

# Шаг 1: Проверить текущее количество реплик
kubectl get deployment tasks10-ml-service-api -n mlops-tasks10

# Шаг 2: Создать интенсивную нагрузку для масштабирования до 6 реплик
# Запустить в нескольких терминалах одновременно:
while true; do
  curl -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{
      "transaction_id": "load-test",
      "customer_id": 123,
      "terminal_id": 456,
      "tx_amount": 100.0,
      "tx_time_seconds": 1693737600,
      "tx_time_days": 19723,
      "tx_fraud_scenario": 0
    }' > /dev/null 2>&1
done

# Шаг 3: Мониторинг масштабирования
watch kubectl get deployment tasks10-ml-service-api -n mlops-tasks10

# Шаг 4: Проверка CPU нагрузки (должна быть > 80%)
kubectl top pods -n mlops-tasks10 | grep tasks10-ml-service-api

# Шаг 5: Ожидание срабатывания алерта (через 5 минут после достижения 6 реплик + CPU > 80%)
# Алерт должен сработать с уведомлением администратора!

# Шаг 6: Остановка нагрузки (Ctrl+C во всех терминалах)
```

### 3. Тест нагрузки и метрик производительности
```bash
# Генерация нагрузки для тестирования HPA
for i in {1..100}; do
  curl -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{
      "transaction_id": "load-test-'$i'",
      "customer_id": 123,
      "terminal_id": 456,
      "tx_amount": 100.0,
      "tx_time_seconds": 1693737600,
      "tx_time_days": 19723,
      "tx_fraud_scenario": 0
    }' &
  if (( i % 10 == 0 )); then wait; fi
done
```

### 3. Тест алертов ошибок
```bash
# Генерация ошибок 4xx
for i in {1..20}; do
  curl -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{"invalid": "data"}'
done

# Проверка метрик ошибок
curl http://tasks10-ml-api.darveter.com/metrics | grep http_requests_total
```

### 4. Проверка HPA
```bash
# Мониторинг масштабирования
watch kubectl get hpa,pods -n mlops-tasks10
```

