#!/bin/bash

echo "🧪 Tasks10 ML API - Полный тест мониторинга"
echo "============================================="

# Цвета для вывода
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Функция для проверки
check_result() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        return 1
    fi
}

echo ""
echo -e "${BLUE}1️⃣ Проверка базовой доступности API${NC}"
echo "-----------------------------------"

# Проверка health endpoint
echo "🔍 Тестируем /health endpoint..."
health_response=$(curl -s -w "%{http_code}" http://localhost:8000/health -o /tmp/health.json)
check_result $? "Health endpoint доступен"

if [ "$health_response" = "200" ]; then
    echo "📊 Health response:"
    cat /tmp/health.json | jq '.'
else
    echo -e "${RED}❌ Health endpoint вернул код: $health_response${NC}"
fi

echo ""
echo -e "${BLUE}2️⃣ Генерация тестовых данных${NC}"
echo "-----------------------------"

echo "🚀 Генерируем 20 тестовых запросов..."
for i in {1..20}; do
    response=$(curl -s -X POST http://localhost:8000/predict \
        -H "Content-Type: application/json" \
        -d '{
            "transaction_id": "test-'$i'",
            "customer_id": '$((RANDOM % 1000))',
            "terminal_id": '$((RANDOM % 100))',
            "tx_amount": '$((RANDOM % 1000 + 10))',
            "tx_time_seconds": 1693737600,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0
        }')
    
    if [ $((i % 5)) -eq 0 ]; then
        echo "✅ Выполнено $i/20 запросов"
    fi
    sleep 0.5
done

echo -e "${GREEN}✅ Сгенерировано 20 тестовых предсказаний${NC}"

echo ""
echo -e "${BLUE}3️⃣ Проверка метрик в API${NC}"
echo "-------------------------"

echo "📊 Проверяем метрики предсказаний..."
predictions_metric=$(curl -s http://localhost:8000/metrics | grep "ml_model_predictions_total{" | head -1)
echo "Метрика предсказаний: $predictions_metric"

echo "📊 Проверяем HTTP метрики..."
http_metric=$(curl -s http://localhost:8000/metrics | grep "http_requests_total{" | grep "predict" | head -1)
echo "HTTP метрика: $http_metric"

echo "📊 Проверяем системные метрики..."
memory_metric=$(curl -s http://localhost:8000/metrics | grep "ml_model_memory_usage_bytes" | head -1)
echo "Memory метрика: $memory_metric"

echo ""
echo -e "${BLUE}4️⃣ Тест доступности Prometheus${NC}"
echo "-------------------------------"

echo "🔍 Проверяем доступность Prometheus..."
kubectl port-forward svc/prometheus-server 9090:80 -n prometheus &
PROMETHEUS_PID=$!
sleep 5

echo "📊 Тестируем Prometheus API..."
prom_response=$(curl -s -w "%{http_code}" "http://localhost:9090/api/v1/targets" -o /tmp/targets.json)
if [ "$prom_response" = "200" ]; then
    echo -e "${GREEN}✅ Prometheus API доступен${NC}"
    
    # Проверяем наш target
    echo "🎯 Проверяем target tasks10-ml-service..."
    tasks10_target=$(cat /tmp/targets.json | jq -r '.data.activeTargets[] | select(.job=="tasks10-ml-service") | .health')
    if [ "$tasks10_target" = "up" ]; then
        echo -e "${GREEN}✅ Tasks10 ML Service target UP в Prometheus${NC}"
    else
        echo -e "${YELLOW}⚠️ Tasks10 ML Service target: $tasks10_target${NC}"
    fi
else
    echo -e "${RED}❌ Prometheus API недоступен (код: $prom_response)${NC}"
fi

# Останавливаем port-forward
kill $PROMETHEUS_PID 2>/dev/null

echo ""
echo -e "${BLUE}5️⃣ Тест запросов к Prometheus метрикам${NC}"
echo "-------------------------------------"

kubectl port-forward svc/prometheus-server 9090:80 -n prometheus &
PROMETHEUS_PID=$!
sleep 5

echo "📊 Тестируем запрос метрик ML модели..."
ml_query="ml_model_predictions_total"
ml_response=$(curl -s "http://localhost:9090/api/v1/query?query=$ml_query" | jq -r '.data.result | length')

if [ "$ml_response" -gt "0" ]; then
    echo -e "${GREEN}✅ Метрики ML модели найдены в Prometheus ($ml_response результатов)${NC}"
else
    echo -e "${YELLOW}⚠️ Метрики ML модели пока не найдены в Prometheus${NC}"
fi

echo "📊 Тестируем HTTP метрики..."
http_query="http_requests_total"
http_response=$(curl -s "http://localhost:9090/api/v1/query?query=$http_query" | jq -r '.data.result | length')

if [ "$http_response" -gt "0" ]; then
    echo -e "${GREEN}✅ HTTP метрики найдены в Prometheus ($http_response результатов)${NC}"
else
    echo -e "${YELLOW}⚠️ HTTP метрики пока не найдены в Prometheus${NC}"
fi

# Останавливаем port-forward
kill $PROMETHEUS_PID 2>/dev/null

echo ""
echo -e "${BLUE}6️⃣ Тест доступности Grafana${NC}"
echo "----------------------------"

echo "🔍 Проверяем доступность Grafana..."
kubectl port-forward svc/grafana 3000:80 -n grafana &
GRAFANA_PID=$!
sleep 5

grafana_response=$(curl -s -w "%{http_code}" "http://localhost:3000/api/health" -o /tmp/grafana.json)
if [ "$grafana_response" = "200" ]; then
    echo -e "${GREEN}✅ Grafana доступна${NC}"
else
    echo -e "${RED}❌ Grafana недоступна (код: $grafana_response)${NC}"
fi

# Останавливаем port-forward
kill $GRAFANA_PID 2>/dev/null

echo ""
echo -e "${BLUE}7️⃣ Проверка HPA и масштабирования${NC}"
echo "--------------------------------"

echo "📊 Текущий статус HPA..."
kubectl get hpa -n mlops-tasks10

echo "📊 Текущие поды..."
kubectl get pods -n mlops-tasks10 -l app=tasks10-ml-api

echo ""
echo -e "${BLUE}8️⃣ Нагрузочный тест для HPA${NC}"
echo "-----------------------------"

echo "🚀 Запускаем нагрузочный тест (50 запросов)..."
for i in {1..50}; do
    curl -s -X POST http://localhost:8000/predict \
        -H "Content-Type: application/json" \
        -d '{
            "transaction_id": "load-'$i'",
            "customer_id": '$((RANDOM % 1000))',
            "terminal_id": '$((RANDOM % 100))',
            "tx_amount": '$((RANDOM % 2000 + 100))',
            "tx_time_seconds": 1693737600,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0
        }' > /dev/null &
    
    if [ $((i % 10)) -eq 0 ]; then
        echo "🔥 Запущено $i/50 параллельных запросов"
    fi
done

echo "⏳ Ждем завершения запросов..."
wait

echo ""
echo "📊 Статус HPA после нагрузки:"
kubectl get hpa -n mlops-tasks10

echo "📊 Поды после нагрузки:"
kubectl get pods -n mlops-tasks10 -l app=tasks10-ml-api

echo ""
echo -e "${BLUE}9️⃣ Финальная проверка метрик${NC}"
echo "-----------------------------"

echo "📊 Финальные метрики API:"
echo "Predictions total:"
curl -s http://localhost:8000/metrics | grep "ml_model_predictions_total{" || echo "Нет данных"

echo "HTTP requests total:"
curl -s http://localhost:8000/metrics | grep "http_requests_total{.*predict" || echo "Нет данных"

echo "Memory usage:"
curl -s http://localhost:8000/metrics | grep "ml_model_memory_usage_bytes" || echo "Нет данных"

echo ""
echo "============================================="
echo -e "${GREEN}✅ Полный тест мониторинга завершен!${NC}"
echo "============================================="

echo ""
echo -e "${YELLOW}📋 Следующие шаги:${NC}"
echo "1. Проверьте Grafana дашборд: http://localhost:3000"
echo "2. Проверьте Prometheus: http://localhost:9090"
echo "3. Запустите Escalating Attack для тестирования алертов"
echo ""
echo -e "${BLUE}🚀 Для запуска Escalating Attack:${NC}"
echo "cd dag && python -c 'from kafka_attack_producer import run_escalating_attack; run_escalating_attack()'"
