#!/bin/bash

# Скрипт для тестирования Docker образа

echo "🐳 ТЕСТИРОВАНИЕ DOCKER ОБРАЗА FRAUD DETECTION API"
echo "================================================"

# Проверяем Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен"
    exit 1
fi

if ! docker info &> /dev/null; then
    echo "❌ Docker daemon не запущен"
    echo "💡 На NixOS попробуйте: sudo systemctl start docker"
    exit 1
fi

# Сборка образа
echo "🔨 Собираем Docker образ..."
if docker build -t fraud-detection-api:latest .; then
    echo "✅ Docker образ собран успешно"
else
    echo "❌ Ошибка при сборке Docker образа"
    exit 1
fi

# Проверяем размер образа
echo ""
echo "📊 Информация об образе:"
docker images fraud-detection-api:latest

# Запускаем контейнер для тестирования
echo ""
echo "🚀 Запускаем контейнер для тестирования..."
CONTAINER_ID=$(docker run -d -p 8000:8000 --name fraud-api-test fraud-detection-api:latest)

if [ $? -eq 0 ]; then
    echo "✅ Контейнер запущен: $CONTAINER_ID"
    
    # Ждем запуска
    echo "⏳ Ждем запуска приложения..."
    sleep 30
    
    # Тестируем health check
    echo "🔍 Тестируем health check..."
    if curl -s http://localhost:8000/health > /dev/null; then
        echo "✅ Health check прошел успешно"
        
        # Показываем ответ health check
        echo "📋 Ответ health check:"
        curl -s http://localhost:8000/health | jq '.' 2>/dev/null || curl -s http://localhost:8000/health
        
        # Тестируем predict endpoint
        echo ""
        echo "🔍 Тестируем predict endpoint..."
        PREDICT_RESPONSE=$(curl -s -X POST http://localhost:8000/predict \
          -H "Content-Type: application/json" \
          -d '{
            "transaction_id": "docker_test_001",
            "customer_id": 1001,
            "terminal_id": 2001,
            "tx_amount": 100.50,
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0
          }')
        
        if [ $? -eq 0 ]; then
            echo "✅ Predict endpoint работает"
            echo "📋 Ответ predict:"
            echo "$PREDICT_RESPONSE" | jq '.' 2>/dev/null || echo "$PREDICT_RESPONSE"
        else
            echo "❌ Predict endpoint не отвечает"
        fi
        
    else
        echo "❌ Health check не прошел"
        echo "📋 Логи контейнера:"
        docker logs $CONTAINER_ID | tail -20
    fi
    
    # Останавливаем и удаляем контейнер
    echo ""
    echo "🧹 Очищаем тестовые ресурсы..."
    docker stop $CONTAINER_ID > /dev/null
    docker rm $CONTAINER_ID > /dev/null
    echo "✅ Контейнер остановлен и удален"
    
else
    echo "❌ Не удалось запустить контейнер"
    exit 1
fi

echo ""
echo "🎉 ТЕСТИРОВАНИЕ DOCKER ОБРАЗА ЗАВЕРШЕНО!"
echo "📋 Образ готов для развертывания"

