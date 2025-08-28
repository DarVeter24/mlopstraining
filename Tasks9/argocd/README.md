# 🚀 Tasks9 Fraud Detection API - Kubernetes & ArgoCD

MLOps сервис для детекции мошенничества с автоматическим развертыванием через ArgoCD.

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
    ├── hpa.yaml                # Horizontal Pod Autoscaler
    └── kustomization.yaml      # Kustomize конфигурация
```

## ⚡ Быстрый старт

### 1. Ручное развертывание

```bash
# Применить все манифесты
kubectl apply -f manifests/

# Проверить статус
kubectl get all -n tasks9-mlops

# Тестировать API
kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops
curl http://localhost:8000/health
```

### 2. ArgoCD развертывание

```bash
# Применить ArgoCD Application
kubectl apply -f application.yaml

# Проверить в ArgoCD UI
# https://argocd.your-cluster.com/applications/tasks9-fraud-detection
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

- **Namespace**: `tasks9-mlops`
- **Replicas**: 2 (min) - 10 (max с HPA)
- **Image**: `ghcr.io/darveter24/mlopstraining/tasks9-fraud-detection-api:latest`
- **Port**: 8000
- **Resources**: 512Mi-2Gi RAM, 250m-1000m CPU

### Доступ к API

- **Internal**: `tasks9-fraud-detection-service.tasks9-mlops.svc.cluster.local:8000`
- **External**: `tasks9-fraud-api.local` или `fraud-detection.darveter.com`
- **Port-forward**: `kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops`

## 📊 API Endpoints

- `GET /health` - Health check
- `POST /predict` - Fraud prediction
- `GET /docs` - Swagger UI
- `GET /openapi.json` - OpenAPI spec

## 🔍 Мониторинг

```bash
# Проверка подов
kubectl get pods -n tasks9-mlops

# Логи
kubectl logs -l app=fraud-detection-api -n tasks9-mlops -f

# Метрики HPA
kubectl get hpa -n tasks9-mlops

# Использование ресурсов
kubectl top pods -n tasks9-mlops
```

## 🛠️ Обслуживание

### Обновление образа

```bash
# Через ArgoCD (автоматически при push в main)
git push origin main

# Вручную
kubectl set image deployment/tasks9-fraud-detection-api \
  fraud-detection-api=ghcr.io/darveter24/mlopstraining/tasks9-fraud-detection-api:new-tag \
  -n tasks9-mlops
```

### Масштабирование

```bash
# Ручное
kubectl scale deployment tasks9-fraud-detection-api --replicas=5 -n tasks9-mlops

# Автоматическое (через HPA)
# Настроено на CPU 70%, Memory 80%
```

## 🚨 Troubleshooting

### Проблемы с подами

```bash
kubectl describe pod -l app=fraud-detection-api -n tasks9-mlops
kubectl logs -l app=fraud-detection-api -n tasks9-mlops --previous
```

### Проблемы с MLflow

```bash
kubectl exec -it deployment/tasks9-fraud-detection-api -n tasks9-mlops -- \
  curl -v http://mlflow.darveter.com
```

### Проблемы с Ingress

```bash
kubectl describe ingress tasks9-fraud-detection-ingress -n tasks9-mlops
kubectl get pods -n ingress-nginx
```

## 📚 Документация

Подробная документация: [ArgoCD.md](./ArgoCD.md)

---

**🎯 Цель**: Автоматизированное развертывание ML сервиса в Kubernetes  
**🔧 Инструменты**: Kubernetes, ArgoCD, Docker, FastAPI, MLflow  
**👨‍💻 Автор**: DarVeter24
