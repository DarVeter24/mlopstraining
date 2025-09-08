# Tasks10 Monitoring Setup

Конфигурация мониторинга для Tasks10 ML API с автоматическим масштабированием.

## 📊 Компоненты

### 1. Grafana Dashboard
- **Файл**: `dashboards/ml-model-dashboard.json`
- **Назначение**: Визуализация метрик ML модели, HTTP производительности, ресурсов и Kafka
- **UID**: `tasks10-ml-dashboard`

### 2. AlertManager Rules
- **Файл**: `alerts/ml-alerts.yaml`
- **Назначение**: Правила алертинга для критических событий
- **Группы**: ML модель, Kafka, инфраструктура

## 🚀 Установка и настройка

### Шаг 1: Импорт Grafana Dashboard

#### Через Web UI:
1. Откройте Grafana: http://grafana.darveter.com
2. Перейдите в **Dashboards** → **Import**
3. Загрузите файл `dashboards/ml-model-dashboard.json`
4. Или скопируйте содержимое файла в поле JSON
5. Настройте datasource: выберите ваш Prometheus
6. Нажмите **Import**

#### Через API:
```bash
# Импорт дашборда через Grafana API
curl -X POST \
  http://grafana.darveter.com/api/dashboards/db \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer YOUR_API_KEY' \
  -d @dashboards/ml-model-dashboard.json
```

### Шаг 2: Настройка AlertManager Rules

#### Вариант 1: Через ConfigMap (рекомендуется)
```bash
# Создать ConfigMap с правилами
kubectl create configmap tasks10-alerts \
  --from-file=alerts/ml-alerts.yaml \
  -n monitoring

# Добавить в Prometheus конфигурацию
kubectl patch configmap prometheus-config -n monitoring \
  --patch '{"data":{"tasks10-alerts.yaml":"'$(cat alerts/ml-alerts.yaml | base64 -w 0)'"}}'
```

#### Вариант 2: Ручная настройка
1. Откройте Prometheus: http://prometheus.darveter.com
2. Перейдите в **Status** → **Configuration**
3. Добавьте правила из `alerts/ml-alerts.yaml` в секцию `rule_files`
4. Перезагрузите конфигурацию

### Шаг 3: Проверка интеграции

```bash
# Проверить что метрики доступны
curl http://tasks10-ml-api.darveter.com/metrics | grep ml_model

# Проверить targets в Prometheus
curl http://prometheus.darveter.com/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job=="tasks10-ml-service")'

# Проверить правила алертинга
curl http://prometheus.darveter.com/api/v1/rules | jq '.data.groups[] | select(.name=="tasks10-ml-model-alerts")'
```

## 📈 Метрики и пороговые значения

### ML Model Metrics
| Метрика | Описание | Пороговое значение |
|---------|----------|-------------------|
| `ml_model_predictions_total` | Общее количество предсказаний | - |
| `ml_model_prediction_duration_seconds` | Время выполнения предсказаний | > 1s (warning) |
| `ml_model_fraud_probability` | Распределение вероятностей мошенничества | > 20% fraud rate (info) |
| `ml_model_confidence_score` | Уровень уверенности модели | - |

### HTTP Performance Metrics
| Метрика | Описание | Пороговое значение |
|---------|----------|-------------------|
| `http_requests_total` | Общее количество HTTP запросов | - |
| `http_request_duration_seconds` | Время обработки HTTP запросов | 95p > 2s (warning) |
| `http_errors_total` | Количество HTTP ошибок | > 5% (critical) |

### System Resources
| Метрика | Описание | Пороговое значение |
|---------|----------|-------------------|
| `ml_model_cpu_usage` | Использование CPU | > 80% (warning) |
| `ml_model_memory_usage` | Использование памяти | > 70% (warning) |
| `ml_model_active_requests` | Активные запросы | - |

### Kafka Metrics
| Метрика | Описание | Пороговое значение |
|---------|----------|-------------------|
| `kafka_queue_length` | Длина очереди Kafka | > 1000 (critical) |
| `kafka_consumer_lag` | Лаг потребителя | > 500 (warning) |
| `kafka_messages_consumed_total` | Обработанные сообщения | - |

## 🚨 Алерты и уведомления

### Critical Alerts (требуют немедленного внимания)
- **HighErrorRate**: Error rate > 5% за 5 минут
- **KafkaQueueHigh**: Kafka queue > 1000 сообщений
- **PodCrashLooping**: Pod перезапускается
- **ServiceDown**: Сервис недоступен

### Warning Alerts (требуют внимания)
- **HighCPUUsage**: CPU > 80% за 5 минут → HPA scaling
- **SlowResponseTime**: Response time > 2s (95p)
- **HighMemoryUsage**: Memory > 70% за 10 минут
- **NoPredictions**: Нет предсказаний 10 минут

### Info Alerts (информационные)
- **ModelScaledToMax**: Достигнут максимум реплик (6)
- **HPAScalingEvent**: Событие масштабирования
- **HighFraudDetectionRate**: Высокий уровень мошенничества

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

### 2. Тест алертов CPU
```bash
# Имитация высокой нагрузки (осторожно!)
kubectl run cpu-stress --image=containerstack/cpustress \
  --rm -it --restart=Never \
  -- --cpu 4 --timeout 60s
```

### 3. Тест алертов ошибок
```bash
# Генерация ошибок 4xx
for i in {1..20}; do
  curl -X POST http://tasks10-ml-api.darveter.com/predict \
    -H "Content-Type: application/json" \
    -d '{"invalid": "data"}'
done
```

### 4. Проверка HPA
```bash
# Мониторинг масштабирования
watch kubectl get hpa,pods -n mlops-tasks10
```

## 🔧 Troubleshooting

### Метрики не отображаются в Grafana
1. Проверьте что Prometheus scraping настроен:
   ```bash
   kubectl get servicemonitor -n mlops-tasks10
   ```
2. Проверьте аннотации на Service:
   ```bash
   kubectl get svc tasks10-ml-service-service -n mlops-tasks10 -o yaml | grep prometheus
   ```

### Алерты не срабатывают
1. Проверьте правила в Prometheus:
   ```bash
   curl http://prometheus.darveter.com/api/v1/rules | jq '.data.groups[].rules[] | select(.alert=="HighCPUUsage")'
   ```
2. Проверьте статус алертов:
   ```bash
   curl http://prometheus.darveter.com/api/v1/alerts
   ```

### Dashboard показывает "No data"
1. Проверьте datasource в Grafana
2. Убедитесь что метрики доступны в Prometheus
3. Проверьте временной диапазон в дашборде

## 📚 Полезные ссылки

- **Grafana Dashboard**: http://grafana.darveter.com/d/tasks10-ml-dashboard
- **Prometheus Targets**: http://prometheus.darveter.com/targets
- **AlertManager**: http://alertmanager.darveter.com
- **ML API Metrics**: http://tasks10-ml-api.darveter.com/metrics
- **ML API Health**: http://tasks10-ml-api.darveter.com/health
- **ML API Docs**: http://tasks10-ml-api.darveter.com/docs
