# ArgoCD Deployment Guide для Tasks9 Fraud Detection API

## 📋 Обзор

Этот документ описывает процесс развертывания MLOps сервиса для детекции мошенничества в Kubernetes кластере с использованием ArgoCD для GitOps автоматизации.

## 🏗️ Архитектура решения

### Компоненты системы:
- **Namespace**: `tasks9-mlops` - изолированное пространство имен
- **Deployment**: `tasks9-fraud-detection-api` - основное приложение (2 реплики)
- **Service**: `tasks9-fraud-detection-service` - внутренний сервис
- **Ingress**: `tasks9-fraud-detection-ingress` - внешний доступ
- **ConfigMap**: `tasks9-fraud-detection-config` - конфигурация
- **HPA**: `tasks9-fraud-detection-hpa` - автоматическое масштабирование
- **ArgoCD Application**: автоматический деплой через GitOps

## 🚀 Быстрый старт

### 1. Предварительные требования

```bash
# Проверка доступности kubectl
kubectl version --client

# Проверка подключения к кластеру
kubectl cluster-info

# Проверка наличия ArgoCD (опционально)
kubectl get pods -n argocd
```

### 2. Ручное развертывание (для тестирования)

```bash
# Переход в папку с манифестами
cd Tasks9/argocd/manifests/

# Применение всех манифестов
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml
kubectl apply -f hpa.yaml

# Проверка статуса
kubectl get all -n tasks9-mlops
```

### 3. Автоматическое развертывание через ArgoCD

```bash
# Применение ArgoCD Application
kubectl apply -f application.yaml

# Проверка статуса приложения в ArgoCD
kubectl get applications -n argocd
```

## 🔧 Конфигурация

### Переменные окружения (ConfigMap)

| Переменная | Значение | Описание |
|------------|----------|----------|
| `API_HOST` | `0.0.0.0` | Хост API сервера |
| `API_PORT` | `8000` | Порт API сервера |
| `ENVIRONMENT` | `production` | Окружение |
| `DEBUG` | `false` | Режим отладки |
| `MLFLOW_TRACKING_URI` | `http://mlflow.darveter.com` | MLflow сервер |
| `MLFLOW_MODEL_NAME` | `fraud_model_production` | Название модели |
| `MLFLOW_MODEL_STAGE` | `Production` | Стадия модели |
| `AWS_ACCESS_KEY_ID` | `minio` | S3 ключ доступа |
| `AWS_SECRET_ACCESS_KEY` | `minio123` | S3 секретный ключ |
| `MLFLOW_S3_ENDPOINT_URL` | `http://minio.darveter.com:9000` | S3 эндпоинт |

### Ресурсы контейнера

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

### Автомасштабирование (HPA)

- **Минимум реплик**: 2
- **Максимум реплик**: 10
- **Метрики**: CPU (70%), Memory (80%)

## 🌐 Доступ к API

### Локальный доступ через port-forward

```bash
# Перенаправление порта
kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops

# Тестирование API
curl http://localhost:8000/health
curl http://localhost:8000/docs
```

### Внешний доступ через Ingress

**Хосты:**
- `tasks9-fraud-api.local` (для локального тестирования)
- `fraud-detection.darveter.com` (продакшен)

**Endpoints:**
- `GET /health` - проверка состояния сервиса
- `POST /predict` - предсказание мошенничества
- `GET /docs` - Swagger UI документация

## 🧪 Тестирование

### Автоматическое тестирование

```bash
# Запуск полного теста развертывания
./test-k8s-deployment.sh
```

### Ручное тестирование

```bash
# Проверка подов
kubectl get pods -n tasks9-mlops

# Проверка логов
kubectl logs -l app=fraud-detection-api -n tasks9-mlops

# Проверка сервисов
kubectl get svc -n tasks9-mlops

# Проверка HPA
kubectl get hpa -n tasks9-mlops

# Тестирование API
kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops &
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 100.50,
    "hour": 14,
    "day_of_week": 2,
    "user_age": 35,
    "account_balance": 1500.75
  }'
```

## 🏥 Мониторинг и Health Checks

### Kubernetes Probes

- **Liveness Probe**: `/health` каждые 10 сек (после 30 сек)
- **Readiness Probe**: `/health` каждые 5 сек (после 10 сек)  
- **Startup Probe**: `/health` каждые 10 сек (максимум 5 минут)

### Мониторинг ресурсов

```bash
# Использование ресурсов
kubectl top pods -n tasks9-mlops

# События
kubectl get events -n tasks9-mlops --sort-by='.lastTimestamp'

# Статус HPA
kubectl describe hpa tasks9-fraud-detection-hpa -n tasks9-mlops
```

## 🔄 ArgoCD GitOps

### Конфигурация Application

```yaml
spec:
  project: default
  source:
    repoURL: https://github.com/DarVeter24/mlopstraining.git
    targetRevision: HEAD
    path: Tasks9/argocd/manifests
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

### Управление через ArgoCD

```bash
# Синхронизация приложения
argocd app sync tasks9-fraud-detection

# Проверка статуса
argocd app get tasks9-fraud-detection

# Просмотр различий
argocd app diff tasks9-fraud-detection
```

## 🔧 Обслуживание

### Обновление образа

1. **Автоматически** (через CI/CD):
   - Push в `main` ветку → GitHub Actions → новый образ в GHCR
   - ArgoCD автоматически синхронизирует изменения

2. **Вручную**:
   ```bash
   # Обновление тега образа в deployment.yaml
   kubectl set image deployment/tasks9-fraud-detection-api \
     fraud-detection-api=ghcr.io/darveter24/mlopstraining/tasks9-fraud-detection-api:new-tag \
     -n tasks9-mlops
   ```

### Rolling Update

```bash
# Проверка статуса обновления
kubectl rollout status deployment/tasks9-fraud-detection-api -n tasks9-mlops

# Откат к предыдущей версии
kubectl rollout undo deployment/tasks9-fraud-detection-api -n tasks9-mlops

# История обновлений
kubectl rollout history deployment/tasks9-fraud-detection-api -n tasks9-mlops
```

### Масштабирование

```bash
# Ручное масштабирование
kubectl scale deployment tasks9-fraud-detection-api --replicas=5 -n tasks9-mlops

# Проверка автомасштабирования
kubectl get hpa -n tasks9-mlops -w
```

## 🧹 Очистка

### Удаление ресурсов

```bash
# Автоматическая очистка
./cleanup-k8s.sh

# Или вручную
kubectl delete -f manifests/
kubectl delete namespace tasks9-mlops
```

### Удаление ArgoCD Application

```bash
kubectl delete -f application.yaml
```

## 🚨 Troubleshooting

### Проблемы с запуском подов

```bash
# Проверка событий
kubectl describe pod -l app=fraud-detection-api -n tasks9-mlops

# Проверка логов
kubectl logs -l app=fraud-detection-api -n tasks9-mlops --previous

# Проверка ресурсов
kubectl top pods -n tasks9-mlops
```

### Проблемы с доступом к MLflow

```bash
# Тестирование подключения изнутри пода
kubectl exec -it deployment/tasks9-fraud-detection-api -n tasks9-mlops -- \
  curl -v http://mlflow.darveter.com

# Проверка DNS
kubectl exec -it deployment/tasks9-fraud-detection-api -n tasks9-mlops -- \
  nslookup mlflow.darveter.com
```

### Проблемы с Ingress

```bash
# Проверка Ingress контроллера
kubectl get pods -n ingress-nginx

# Проверка правил Ingress
kubectl describe ingress tasks9-fraud-detection-ingress -n tasks9-mlops

# Тестирование через NodePort (если Ingress не работает)
kubectl patch svc tasks9-fraud-detection-service -n tasks9-mlops -p '{"spec":{"type":"NodePort"}}'
```

## 📊 Метрики и логи

### Prometheus метрики

API автоматически экспортирует метрики через аннотации:
```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8000"
  prometheus.io/path: "/health"
```

### Централизованные логи

```bash
# Агрегация логов всех подов
kubectl logs -l app=fraud-detection-api -n tasks9-mlops --tail=100 -f
```

## 🔐 Безопасность

### Рекомендации:
1. Использовать Secrets вместо ConfigMap для чувствительных данных
2. Настроить Network Policies для изоляции трафика
3. Включить Pod Security Standards
4. Регулярно обновлять образы

### Пример Secret для чувствительных данных:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: tasks9-fraud-detection-secrets
  namespace: tasks9-mlops
type: Opaque
stringData:
  AWS_SECRET_ACCESS_KEY: "your-secret-key"
  MLFLOW_TRACKING_TOKEN: "your-mlflow-token"
```

---

## 📚 Дополнительные ресурсы

- [ArgoCD Documentation](https://argo-cd.readthedocs.io/)
- [Kubernetes Documentation](https://kubernetes.io/docs/)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

**Автор**: DarVeter24  
**Версия**: 1.0.0  
**Дата**: 2024
