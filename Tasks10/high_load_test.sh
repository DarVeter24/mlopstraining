#!/bin/bash

echo "🚀 Запуск МОЩНОЙ HTTP атаки на ML API"

# ML API URL
API_URL="http://tasks10-ml-api.darveter.com/predict"

# Тестовый payload
PAYLOAD='{
  "transaction_id": "test_high_load",
  "customer_id": 12345,
  "terminal_id": 67890,
  "tx_amount": 100.50,
  "tx_time_seconds": 3600,
  "tx_time_days": 1,
  "tx_fraud_scenario": 0
}'

echo "🎯 Цель: $API_URL"
echo "📊 Запуск 10 параллельных процессов по 100 запросов каждый"
echo "⚡ Общая нагрузка: ~1000 HTTP запросов"

# Функция для одного процесса нагрузки
run_load_process() {
    local process_id=$1
    local requests_count=100
    
    echo "🔥 Процесс $process_id: Запуск $requests_count запросов"
    
    for i in $(seq 1 $requests_count); do
        curl -s -X POST "$API_URL" \
             -H "Content-Type: application/json" \
             -d "$PAYLOAD" > /dev/null &
        
        # Небольшая задержка для контроля TPS
        sleep 0.1
    done
    
    echo "✅ Процесс $process_id: Завершен"
}

# Запуск параллельных процессов
for i in $(seq 1 10); do
    run_load_process $i &
done

echo "🚀 Все процессы запущены!"
echo "📈 Следите за метриками: kubectl top pods -n mlops-tasks10"
echo "📊 Следите за HPA: kubectl get hpa -n mlops-tasks10 -w"

wait
echo "✅ Нагрузочное тестирование завершено!"
