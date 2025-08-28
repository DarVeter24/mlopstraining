#!/bin/bash

# Скрипт для очистки ресурсов Kubernetes
set -e

echo "🧹 Очистка ресурсов Tasks9 Fraud Detection API из Kubernetes..."

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
        echo -e "${YELLOW}⚠️  $1${NC}"
    fi
}

# Переход в папку с манифестами
cd "$(dirname "$0")/manifests"

# Удаление ресурсов
echo -e "${BLUE}🗑️  Удаление Kubernetes ресурсов...${NC}"

kubectl delete -f hpa.yaml --ignore-not-found=true
check_status "HPA удален"

kubectl delete -f ingress.yaml --ignore-not-found=true
check_status "Ingress удален"

kubectl delete -f service.yaml --ignore-not-found=true
check_status "Service удален"

kubectl delete -f deployment.yaml --ignore-not-found=true
check_status "Deployment удален"

kubectl delete -f configmap.yaml --ignore-not-found=true
check_status "ConfigMap удален"

# Ожидание удаления подов
echo -e "${BLUE}⏳ Ожидание удаления подов...${NC}"
kubectl wait --for=delete pod -l app=fraud-detection-api -n tasks9-mlops --timeout=120s 2>/dev/null || true
check_status "Поды удалены"

kubectl delete -f namespace.yaml --ignore-not-found=true
check_status "Namespace удален"

echo -e "${GREEN}🎉 Очистка завершена успешно!${NC}"
