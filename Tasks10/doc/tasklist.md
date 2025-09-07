# План разработки расширения MLOps системы с автоматическим масштабированием и мониторингом

## Прогресс выполнения

- **Итерация 1:** ✅ Завершена
- **Итерация 2:** ✅ Завершена  
- **Итерация 3:** ⬜ Не начата
- **Итерация 4:** ⬜ Не начата
- **Итерация 5:** ⬜ Не начата

---

## Итерация 1: Копирование базовых компонентов из Tasks9
- [x] Копирование структуры проекта из Tasks9
- [x] Копирование папки `src/` с ML API (api.py, config.py, model_loader.py)
- [x] Копирование папки `argocd/` с Kubernetes манифестами
- [x] Копирование Dockerfile и requirements.txt
- [x] Копирование .env-example и создание .env для Tasks10
- [x] Адаптация argocd/application.yaml под Tasks10 (изменение имени приложения)
- [x] Тестирование работы скопированного API локально

**Критерии тестирования:**
- Скопированный API успешно запускается локально
- Существующая модель из Tasks9 корректно работает (POST /predict возвращает результат)
- Эндпоинты /predict и /health работают корректно (статус 200)
- Docker образ собирается без ошибок (`docker build` успешен)
- Kubernetes манифесты проходят валидацию (`kubectl apply --dry-run=client`)

---

## Итерация 2: Добавление метрик Prometheus
- [x] Создание модуля src/metrics.py с определениями метрик
- [x] Расширение src/api.py эндпоинтом /metrics
- [x] Добавление метрик в эндпоинт /predict (predictions_total, prediction_duration)
- [x] Добавление HTTP метрик (requests_total, request_duration, errors_total)
- [x] Добавление метрик ошибок и производительности
- [x] Обновление requirements.txt (добавление prometheus_client==0.19.0)
- [x] Тестирование сбора метрик

**Критерии тестирования (локально):**
- Docker образ собирается с метриками: `docker build -t tasks10-ml-api:metrics .`
- Docker контейнер запускается: `docker run -p 8000:8000 tasks10-ml-api:metrics`
- Эндпоинт метрик работает: `curl http://localhost:8000/metrics` возвращает метрики в формате Prometheus
- После запроса к /predict метрики `ml_model_predictions_total` увеличиваются
- HTTP метрики `http_requests_total` корректно отслеживают запросы по статус-кодам
- Метрики ошибок `ml_model_errors_total` инкрементируются при ошибках
- `requirements.txt` содержит `prometheus_client==0.19.0`

---

## Итерация 3: Настройка автоматического масштабирования и развертывание
- [ ] Корректировка argocd/manifests/hpa.yaml для 4-6 реплик
- [ ] Настройка триггеров HPA на CPU > 80% и Memory > 70%
- [ ] Создание argocd/manifests/servicemonitor.yaml для интеграции с Prometheus
- [ ] Обновление argocd/README.md под Tasks10
- [ ] Развертывание через ArgoCD в готовый кластер
- [ ] Применение HPA к существующему Deployment
- [ ] Тестирование автоматического масштабирования

**Критерии тестирования:**
- ArgoCD развертывание успешно:
  - `kubectl apply -f argocd/project.yaml` создает ArgoCD Project
  - `kubectl apply -f argocd/application.yaml` создает ArgoCD Application
  - `argocd app sync tasks10-ml-service` успешно синхронизирует
  - `kubectl get pods,svc,deployment -n mlops-tasks10` показывает готовые ресурсы
  - Поды переходят в статус Running
- `kubectl get hpa -n mlops-tasks10` показывает HPA с min=4, max=6 реплик
- `kubectl get servicemonitor -n mlops-tasks10` показывает активный ServiceMonitor
- Метрики в Kubernetes работают:
  - `kubectl port-forward svc/tasks10-ml-service 8000:80 -n mlops-tasks10`
  - `curl http://localhost:8000/metrics` возвращает метрики
  - `curl http://tasks10-ml-api.darveter.com/metrics` через Ingress
- В Prometheus UI (через port-forward) метрики `ml_model_*` доступны и обновляются
- ServiceMonitor создает target в Prometheus с статусом UP
- При нагрузке `kubectl get pods -n mlops-tasks10` показывает увеличение реплик
- HPA не превышает лимит в 6 реплик при высокой нагрузке

---

## Итерация 4: Создание системы мониторинга
- [ ] Создание monitoring/dashboards/ml-model-dashboard.json для Grafana
- [ ] Создание monitoring/alerts/ml-alerts.yaml с правилами AlertManager
- [ ] Настройка алертов: CPU>80%, ErrorRate>5%, ResponseTime>2s, KafkaQueue>1000
- [ ] Импорт дашборда в существующий Grafana (вручную)
- [ ] Настройка правил алертинга в существующем AlertManager (вручную)
- [ ] Тестирование алертов и дашбордов

**Критерии тестирования:**
- Grafana дашборд отображает метрики ML модели
- Алерты срабатывают при превышении пороговых значений
- AlertManager отправляет уведомления при критических событиях
- Дашборд показывает производительность, ошибки и нагрузку
- Система мониторинга работает в реальном времени

---

## Итерация 5: Интеграция с Kafka и нагрузочное тестирование
- [ ] Адаптация airflow-dags/retrain_with_monitoring.py из Tasks8
- [ ] Добавление метрик Kafka в Airflow DAG (queue_length, consumer_lag)
- [ ] Создание load-testing/kafka-load-test.py для генерации нагрузки
- [ ] Настройка Kafka метрик для мониторинга очереди
- [ ] Проведение нагрузочного тестирования HPA
- [ ] Тестирование полной интеграции системы

**Критерии тестирования:**
- Airflow DAG корректно отслеживает метрики переобучения
- Kafka load test генерирует нагрузку для тестирования HPA
- Метрики Kafka (queue length, consumer lag) доступны в Prometheus
- HPA корректно масштабируется под нагрузкой от Kafka
- Система работает стабильно при высокой нагрузке
- Алерты срабатывают при проблемах с производительностью

---

## Дополнительные требования

**Интеграция с существующей инфраструктурой:**
- Использование готового Kubernetes кластера
- Подключение к существующему Prometheus/Grafana/AlertManager
- Интеграция с готовым Apache Airflow
- Использование готовой ML модели из Tasks9

**Качество и надежность:**
- Все компоненты должны работать с готовой инфраструктурой
- Минимальные изменения существующего кода
- Автоматическое восстановление при сбоях
- Полное покрытие метриками для observability

**Производительность:**
- Время ответа API < 2 секунд при нормальной нагрузке
- Успешная обработка нагрузки до 1000 запросов в минуту
- Автоматическое масштабирование в пределах 4-6 реплик
- Эффективное использование ресурсов Kubernetes
