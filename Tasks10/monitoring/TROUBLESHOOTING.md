# 🔧 Troubleshooting Tasks10 Dashboard

## 🚨 Проблема: "No data" и "Datasource prometheus was not found"

### ✅ **Шаг 1: Настройка Datasource в Grafana**

1. **Откройте Grafana**: http://grafana.darveter.com
2. **Перейдите в Configuration → Data Sources**
3. **Добавьте Prometheus datasource:**
   - **Name**: `Prometheus` (важно - именно это имя!)
   - **Type**: `Prometheus`
   - **URL**: `http://prometheus.darveter.com` или внутренний URL
   - **Access**: `Server (default)`
4. **Нажмите "Save & test"**

### ✅ **Шаг 2: Добавление scrape конфигурации в Prometheus**

Добавьте в ваш `prometheus-helm.yaml`:

```yaml
extraScrapeConfigs: |
  - job_name: 'disk-usage-exporter'
    static_configs:
      - targets: ['192.168.31.101:9995']
    scrape_interval: 60s
  - job_name: 'node-exporter'
    static_configs:
      - targets: ['192.168.31.101:9100']
    scrape_interval: 60s
  
  # 🆕 Tasks10 ML API метрики
  - job_name: 'tasks10-ml-service'
    static_configs:
      - targets: ['tasks10-ml-api.darveter.com:80']
    metrics_path: /metrics
    scrape_interval: 15s
    scrape_timeout: 10s
    params:
      format: ['prometheus']
    relabel_configs:
      - target_label: namespace
        replacement: mlops-tasks10
      - target_label: service
        replacement: tasks10-ml-service
```

### ✅ **Шаг 3: Перезагрузка Prometheus**

```bash
# Если Prometheus в Kubernetes
kubectl rollout restart deployment prometheus-server -n monitoring

# Или через ArgoCD
argocd app sync prometheus
```

### ✅ **Шаг 4: Проверка targets в Prometheus**

1. **Откройте Prometheus**: http://prometheus.darveter.com
2. **Перейдите в Status → Targets**
3. **Найдите job `tasks10-ml-service`**
4. **Убедитесь что статус UP**

### ✅ **Шаг 5: Тестирование метрик**

```bash
# Проверить что метрики доступны в Prometheus
curl "http://prometheus.darveter.com/api/v1/query?query=ml_model_predictions_total"

# Генерация тестовых запросов для метрик
for i in {1..5}; do
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
  sleep 2
done

# Проверить что метрики обновились
curl http://tasks10-ml-api.darveter.com/metrics | grep ml_model_predictions_total
```

### ✅ **Шаг 6: Повторный импорт дашборда**

1. **Удалите старый дашборд** в Grafana
2. **Импортируйте обновленную версию** `ml-model-dashboard.json`
3. **При импорте выберите правильный Prometheus datasource**

---

## 🧪 **Быстрая диагностика**

### Проверка 1: ML API доступен
```bash
curl http://tasks10-ml-api.darveter.com/health
# Ожидаем: {"status": "healthy"}
```

### Проверка 2: Метрики доступны
```bash
curl http://tasks10-ml-api.darveter.com/metrics | grep ml_model
# Ожидаем: ml_model_predictions_total, ml_model_prediction_duration_seconds
```

### Проверка 3: Prometheus scraping
```bash
curl "http://prometheus.darveter.com/api/v1/targets" | jq '.data.activeTargets[] | select(.job=="tasks10-ml-service")'
# Ожидаем: health: "up"
```

### Проверка 4: Метрики в Prometheus
```bash
curl "http://prometheus.darveter.com/api/v1/query?query=up{job=\"tasks10-ml-service\"}"
# Ожидаем: value: ["timestamp", "1"]
```

---

## 🎯 **Ожидаемый результат**

После выполнения всех шагов:

1. ✅ **Datasource**: Prometheus подключен в Grafana
2. ✅ **Targets**: `tasks10-ml-service` UP в Prometheus  
3. ✅ **Metrics**: ML метрики доступны в Prometheus
4. ✅ **Dashboard**: Все панели показывают данные

**Дашборд должен отображать:**
- 📊 Predictions per Minute (после генерации запросов)
- ⏱️ Average Prediction Time 
- 🌐 HTTP Request Rate
- 📈 HTTP Error Rate
- 💻 Resource Usage (если настроены системные метрики)
- 🔄 Pod Scaling (если настроен kube-state-metrics)

---

## 🆘 **Если проблемы остаются**

### Альтернативное решение: Прямое тестирование

```bash
# 1. Port-forward к ML API
kubectl port-forward svc/tasks10-ml-service-service 8080:80 -n mlops-tasks10 &

# 2. Генерация метрик
for i in {1..10}; do
  curl -X POST http://localhost:8080/predict \
    -H "Content-Type: application/json" \
    -d '{"transaction_id": "test-'$i'", "customer_id": 123, "terminal_id": 456, "tx_amount": 100.0, "tx_time_seconds": 1693737600, "tx_time_days": 19723, "tx_fraud_scenario": 0}'
done

# 3. Проверка метрик
curl http://localhost:8080/metrics | grep -E "(ml_model|http_requests)"
```

### Упрощенный дашборд для тестирования

Создайте простую панель в Grafana:
- **Query**: `up{job="tasks10-ml-service"}`
- **Visualization**: Stat
- **Ожидаем**: 1 (если сервис доступен)

Если эта панель работает, значит проблема в сложных запросах дашборда.
