#!/bin/bash

# Скрипт для тестирования развертывания в Kubernetes
set -e

echo "🚀 Тестирование развертывания Tasks9 Fraud Detection API в Kubernetes..."

# Цвета для вывода
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для проверки статуса
check_status() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ $1${NC}"
    else
        echo -e "${RED}❌ $1${NC}"
        exit 1
    fi
}

# Переход в папку с манифестами
cd "$(dirname "$0")/manifests"

# Проверка доступности kubectl
echo -e "${BLUE}📋 Проверка доступности kubectl...${NC}"
kubectl version --client > /dev/null 2>&1
check_status "kubectl доступен"

# Проверка подключения к кластеру
echo -e "${BLUE}🔗 Проверка подключения к кластеру...${NC}"
kubectl cluster-info > /dev/null 2>&1
check_status "Подключение к кластеру установлено"

# Применение манифестов
echo -e "${BLUE}📦 Применение Kubernetes манифестов...${NC}"
kubectl apply -f namespace.yaml
check_status "Namespace создан"

kubectl apply -f configmap.yaml
check_status "ConfigMap применен"

kubectl apply -f deployment.yaml
check_status "Deployment применен"

kubectl apply -f service.yaml
check_status "Service применен"

kubectl apply -f ingress.yaml
check_status "Ingress применен"

kubectl apply -f hpa.yaml
check_status "HPA применен"

# Ожидание готовности подов
echo -e "${BLUE}⏳ Ожидание готовности подов (максимум 5 минут)...${NC}"
kubectl wait --for=condition=ready pod -l app=fraud-detection-api -n tasks9-mlops --timeout=300s
check_status "Поды готовы"

# Проверка статуса развертывания
echo -e "${BLUE}📊 Проверка статуса ресурсов...${NC}"
kubectl get all -n tasks9-mlops

# Проверка логов
echo -e "${BLUE}📝 Проверка логов приложения...${NC}"
kubectl logs -l app=fraud-detection-api -n tasks9-mlops --tail=10

# Тестирование API через порт-форвардинг
echo -e "${BLUE}🌐 Тестирование API через port-forward...${NC}"
kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops &
PORT_FORWARD_PID=$!

# Ждем запуска port-forward
sleep 5

# Тестирование health endpoint
echo -e "${BLUE}🏥 Тестирование /health endpoint...${NC}"
curl -s http://localhost:8000/health | jq . || curl -s http://localhost:8000/health
check_status "Health endpoint отвечает"

# Тестирование predict endpoint
echo -e "${BLUE}🔮 Тестирование /predict endpoint...${NC}"
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 100.50,
    "hour": 14,
    "day_of_week": 2,
    "user_age": 35,
    "account_balance": 1500.75
  }' | jq . || curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "amount": 100.50,
    "hour": 14,
    "day_of_week": 2,
    "user_age": 35,
    "account_balance": 1500.75
  }'
check_status "Predict endpoint отвечает"

# Остановка port-forward
kill $PORT_FORWARD_PID 2>/dev/null || true

# Проверка HPA
echo -e "${BLUE}📈 Проверка HPA статуса...${NC}"
kubectl get hpa -n tasks9-mlops

echo -e "${GREEN}🎉 Все тесты прошли успешно!${NC}"
echo -e "${YELLOW}📋 Для доступа к API используйте:${NC}"
echo -e "   kubectl port-forward svc/tasks9-fraud-detection-service 8000:8000 -n tasks9-mlops"
echo -e "   curl http://localhost:8000/health"
echo -e "   curl http://localhost:8000/docs"
