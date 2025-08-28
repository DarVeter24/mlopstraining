#!/bin/bash

# Скрипт для тестирования fallback версии API
set -e

echo "🧪 Тестирование Fallback версии Fraud Detection API..."

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

# Остановка предыдущих контейнеров
echo -e "${BLUE}🛑 Остановка предыдущих контейнеров...${NC}"
docker-compose -f docker-compose.fallback.yml down 2>/dev/null || true

# Сборка и запуск fallback версии
echo -e "${BLUE}🔨 Сборка fallback Docker образа...${NC}"
docker build -f Dockerfile.fallback -t tasks9-fraud-detection-fallback .
check_status "Docker образ собран"

echo -e "${BLUE}🚀 Запуск fallback сервиса...${NC}"
docker-compose -f docker-compose.fallback.yml up -d
check_status "Fallback сервис запущен"

# Ожидание запуска сервиса
echo -e "${BLUE}⏳ Ожидание запуска сервиса...${NC}"
sleep 15

# Проверка health endpoint
echo -e "${BLUE}🏥 Тестирование /health endpoint...${NC}"
response=$(curl -s http://localhost:8000/health)
echo "Response: $response"

if echo "$response" | grep -q '"status":"healthy"'; then
    echo -e "${GREEN}✅ Health endpoint работает${NC}"
else
    echo -e "${RED}❌ Health endpoint не работает${NC}"
    docker-compose -f docker-compose.fallback.yml logs
    exit 1
fi

# Проверка типа модели
if echo "$response" | grep -q '"model_type":"mock"'; then
    echo -e "${GREEN}✅ Mock модель загружена${NC}"
else
    echo -e "${YELLOW}⚠️  Неожиданный тип модели${NC}"
fi

# Тестирование predict endpoint
echo -e "${BLUE}🔮 Тестирование /predict endpoint...${NC}"
predict_response=$(curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "test_tx_123",
    "customer_id": 1001,
    "terminal_id": 2001,
    "tx_amount": 150.50,
    "tx_time_seconds": 1705312200,
    "tx_time_days": 19723,
    "tx_fraud_scenario": 0
  }')

echo "Predict Response: $predict_response"

if echo "$predict_response" | grep -q '"transaction_id":"test_tx_123"'; then
    echo -e "${GREEN}✅ Predict endpoint работает${NC}"
else
    echo -e "${RED}❌ Predict endpoint не работает${NC}"
    docker-compose -f docker-compose.fallback.yml logs
    exit 1
fi

# Проверка типа модели в ответе
if echo "$predict_response" | grep -q '"model_type":"mock"'; then
    echo -e "${GREEN}✅ Mock предсказание выполнено${NC}"
else
    echo -e "${YELLOW}⚠️  Неожиданный тип модели в предсказании${NC}"
fi

# Тестирование с большой суммой (должно быть более подозрительно)
echo -e "${BLUE}💰 Тестирование с большой суммой...${NC}"
high_amount_response=$(curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "test_tx_456",
    "customer_id": 1002,
    "terminal_id": 2002,
    "tx_amount": 5000.00,
    "tx_time_seconds": 1705316800,
    "tx_time_days": 19723,
    "tx_fraud_scenario": 0
  }')

echo "High Amount Response: $high_amount_response"

# Проверка документации
echo -e "${BLUE}📚 Проверка Swagger UI...${NC}"
docs_response=$(curl -s http://localhost:8000/docs)
if echo "$docs_response" | grep -q "swagger"; then
    echo -e "${GREEN}✅ Swagger UI доступен${NC}"
else
    echo -e "${YELLOW}⚠️  Swagger UI может быть недоступен${NC}"
fi

# Показать логи
echo -e "${BLUE}📝 Последние логи сервиса:${NC}"
docker-compose -f docker-compose.fallback.yml logs --tail=20

echo -e "${GREEN}🎉 Fallback тестирование завершено успешно!${NC}"
echo -e "${YELLOW}📋 Сервис доступен по адресам:${NC}"
echo -e "   Health: http://localhost:8000/health"
echo -e "   Predict: http://localhost:8000/predict"
echo -e "   Docs: http://localhost:8000/docs"
echo -e "${YELLOW}🛑 Для остановки используйте:${NC}"
echo -e "   docker-compose -f docker-compose.fallback.yml down"
