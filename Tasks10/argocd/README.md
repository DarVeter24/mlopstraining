# 🚀 Tasks10 ML API with Monitoring - Kubernetes & ArgoCD

MLOps сервис с автоматическим масштабированием, мониторингом Prometheus и развертыванием через ArgoCD.

## 📁 Структура файлов

```
argocd/
├── README.md                    # Этот файл
├── ArgoCD.md                   # Подробная документация по ArgoCD
├── application.yaml            # ArgoCD Application манифест
├── test-k8s-deployment.sh      # Скрипт тестирования развертывания
├── cleanup-k8s.sh             # Скрипт очистки ресурсов
└── manifests/                  # Kubernetes манифесты
    ├── namespace.yaml          # Namespace для приложения
    ├── configmap.yaml          # Конфигурация переменных окружения
    ├── deployment.yaml         # Основное развертывание приложения
    ├── service.yaml            # Kubernetes Service
    ├── ingress.yaml            # Ingress для внешнего доступа
    ├── hpa.yaml                # Horizontal Pod Autoscaler (4-6 реплик)
    ├── servicemonitor.yaml     # ServiceMonitor для Prometheus
    └── kustomization.yaml      # Kustomize конфигурация
```

## ⚡ Быстрый старт

### 1. Ручное развертывание

```bash
# Применить все манифесты
kubectl apply -f manifests/

# Проверить статус
kubectl get all -n mlops-tasks10

# Тестировать API
kubectl port-forward svc/tasks10-ml-service 8000:80 -n mlops-tasks10
curl http://localhost:8000/health
```

### 2. ArgoCD развертывание

```bash
# Применить ArgoCD Application
kubectl apply -f application.yaml

# Проверить в ArgoCD UI
# https://argocd.your-cluster.com/applications/tasks10-ml-service
```

### 3. Автоматическое тестирование

```bash
# Запустить полный тест
./test-k8s-deployment.sh

# Очистить ресурсы
./cleanup-k8s.sh
```

## 🔧 Конфигурация

### Основные параметры

- **Namespace**: `mlops-tasks10`
- **Replicas**: 4 (min) - 6 (max с HPA)
- **Image**: `ghcr.io/darveter24/mlopstraining/tasks10-ml-api:latest`
- **Port**: 8000
- **Resources**: 512Mi-2Gi RAM, 250m-1000m CPU
- **Monitoring**: Prometheus метрики на `/metrics`

### Доступ к API

- **Internal**: `tasks10-ml-service.mlops-tasks10.svc.cluster.local:80`
- **External**: `tasks10-ml-api.local` или `tasks10-ml-api.darveter.com`
- **Port-forward**: `kubectl port-forward svc/tasks10-ml-service 8000:80 -n mlops-tasks10`

## 📊 API Endpoints

- `GET /health` - Health check
- `POST /predict` - ML prediction
- `GET /metrics` - Prometheus metrics
- `GET /docs` - Swagger UI
- `GET /openapi.json` - OpenAPI spec

## 🔍 Мониторинг

```bash
# Проверка подов
kubectl get pods -n mlops-tasks10

# Логи
kubectl logs -l app=tasks10-ml-service -n mlops-tasks10 -f

# Метрики HPA
kubectl get hpa -n mlops-tasks10

# ServiceMonitor для Prometheus
kubectl get servicemonitor -n mlops-tasks10

# Использование ресурсов
kubectl top pods -n mlops-tasks10

# Prometheus метрики
curl http://tasks10-ml-api.darveter.com/metrics
```

## 🛠️ Обслуживание

### Обновление образа

```bash
# Через ArgoCD (автоматически при push в main)
git push origin main

# Вручную
kubectl set image deployment/tasks10-ml-service \
  tasks10-ml-api=ghcr.io/darveter24/mlopstraining/tasks10-ml-api:new-tag \
  -n mlops-tasks10
```

### Масштабирование

```bash
# Ручное (в пределах HPA лимитов 4-6)
kubectl scale deployment tasks10-ml-service --replicas=5 -n mlops-tasks10

# Автоматическое (через HPA)
# Настроено на CPU 80%, Memory 70%
```

## 🚨 Troubleshooting

### Проблемы с подами

```bash
kubectl describe pod -l app=tasks10-ml-service -n mlops-tasks10
kubectl logs -l app=tasks10-ml-service -n mlops-tasks10 --previous
```

### Проблемы с MLflow

```bash
kubectl exec -it deployment/tasks10-ml-service -n mlops-tasks10 -- \
  curl -v http://mlflow.mlflow.svc.cluster.local:5000
```

### Проблемы с Ingress

```bash
kubectl describe ingress tasks10-ml-api-ingress -n mlops-tasks10
kubectl get pods -n ingress-nginx

# Проверка Prometheus интеграции
kubectl get servicemonitor tasks10-ml-service-monitor -n mlops-tasks10 -o yaml
```

## 📚 Документация

Подробная документация: [ArgoCD.md](./ArgoCD.md)

---

**🎯 Цель**: ML сервис с автоматическим масштабированием и мониторингом  
**🔧 Инструменты**: Kubernetes, ArgoCD, Prometheus, HPA, FastAPI, MLflow  
**📊 Особенности**: 4-6 реплик, CPU>80% триггеры, Prometheus метрики  
**👨‍💻 Автор**: DarVeter24
