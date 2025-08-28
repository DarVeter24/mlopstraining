#!/bin/bash
echo "🔍 Мониторинг CI/CD Pipeline для Tasks9"
echo "=================================="

while true; do
    echo -e "\n⏰ $(date)"
    
    # Получаем статус последнего workflow run
    STATUS=$(curl -s "https://api.github.com/repos/DarVeter24/mlopstraining/actions/runs?per_page=1" | \
             grep -E '"status"|"conclusion"' | head -2)
    
    echo "📊 Статус pipeline:"
    echo "$STATUS"
    
    # Проверяем завершился ли workflow
    if echo "$STATUS" | grep -q '"status": "completed"'; then
        echo "✅ Pipeline завершен!"
        CONCLUSION=$(echo "$STATUS" | grep '"conclusion"' | cut -d'"' -f4)
        if [ "$CONCLUSION" = "success" ]; then
            echo "🎉 Результат: SUCCESS!"
        else
            echo "❌ Результат: $CONCLUSION"
        fi
        break
    fi
    
    echo "⏳ Pipeline все еще выполняется..."
    sleep 30
done

echo -e "\n🔗 Ссылка на результаты: https://github.com/DarVeter24/mlopstraining/actions"
