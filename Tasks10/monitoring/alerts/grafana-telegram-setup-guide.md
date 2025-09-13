# 📱 Руководство по настройке Telegram алертов в Grafana UI

## 📋 Цель
Настроить алерт **AdminNotification_MaxScaleHighCPU** в Grafana UI с отправкой уведомлений в **Telegram** при достижении критических условий:
- **6+ подов** (максимальное масштабирование HPA)
- **CPU > 80%** в течение 5+ минут

**Преимущества Telegram:**
- ✅ Мгновенные уведомления (1-2 секунды)
- ✅ Не требует SMTP настройки
- ✅ Настройка за 5 минут
- ✅ Работает везде (мобильный, десктоп, веб)
- ✅ Бесплатно и надежно

---

## 🎯 ЭТАП 1: Создание Telegram бота

### Шаг 1.1: Создаем бота через @BotFather
1. Откройте Telegram
2. Найдите и напишите **@BotFather**
3. Отправьте команду: `/newbot`
4. Выберите имя бота: `Tasks10_ML_Alerts_Bot`
5. Выберите username: `tasks10_ml_alerts_bot`
6. **Сохраните Bot Token**: `8447985728:AAHbCJ99aIekD2lrtjxxX2JMcXWL1nqlLx8`

### Шаг 1.2: Получаем Chat ID
1. Напишите вашему боту любое сообщение (например: "Привет")
2. Откройте в браузере:
   ```
   https://api.telegram.org/bot<BOT_TOKEN>/getUpdates
   ```
   Замените `<BOT_TOKEN>` на ваш токен
3. Найдите в JSON ответе: `"chat":{"id":290326354}`
4. **Сохраните Chat ID**: `290326354`

**Пример ответа API:**
```json
{
  "ok": true,
  "result": [
    {
      "update_id": 123456,
      "message": {
        "message_id": 1,
        "from": {...},
        "chat": {
          "id": 123456789,  <-- Это ваш Chat ID
          "first_name": "Admin",
          "type": "private"
        },
        "date": 1234567890,
        "text": "Привет"
      }
    }
  ]
}
```

---

## 🎯 ЭТАП 2: Настройка Telegram Contact Point в Grafana

### Шаг 2.1: Открываем Grafana Alerting
1. Откройте Grafana: `http://grafana.darveter.com`
2. Войдите под admin аккаунтом
3. В левом меню выберите **Alerting** (🔔)
4. Перейдите в раздел **Contact points**

### Шаг 2.2: Создаем Telegram Contact Point
1. Нажмите **"New contact point"**
2. Заполните форму:
   ```
   Name: telegram-admin-alerts
   Integration: Telegram
   ```

### Шаг 2.3: Настраиваем Telegram параметры
```yaml
BOT API Token: 8447985728:AAHbCJ99aIekD2lrtjxxX2JMcXWL1nqlLx8
Chat ID: 290326354
Message: |
  🚨 КРИТИЧЕСКИЙ АЛЕРТ: Tasks10 ML API
  
  Система достигла критических условий:
  ✅ Подов: {{ $values.A.Value }} (максимум = 6)
  ✅ CPU > 80% в течение 5+ минут

  🔧 Требуемые действия:
  1. Проверить ресурсы кластера
  2. Проанализировать паттерны нагрузки
  3. Рассмотреть увеличение лимитов ресурсов
  4. Исследовать возможную DDoS атаку
  
  ⏰ Время: {{ $labels.__alert_rule_timestamp__ }}
  🏷️ Сервис: {{ $labels.service }}

Parse Mode: HTML
Disable Web Page Preview: true
Protect Content: false
```

curl -s -X POST "https://api.telegram.org/bot8447985728:AAHbCJ99aIekD2lrtjxxX2JMcXWL1nqlLx8/sendMessage" \
  -d chat_id="290326354" \
  -d text="Тест от Grafana"

### Шаг 2.4: Тестируем уведомление
1. Нажмите **"Test"** для проверки отправки
2. В Telegram должно прийти тестовое сообщение от вашего бота
3. Если сообщение пришло - нажмите **"Save contact point"**
4. Если не пришло - проверьте Bot Token и Chat ID

---

## 🎯 ЭТАП 3: Создание Alert Rule

### Шаг 3.1: Создаем новое правило
1. В разделе **Alerting** выберите **Alert rules**
2. Нажмите **"New rule"**
3. Выберите **"Grafana managed alert"**

### Шаг 3.2: Настраиваем Query
```yaml
Rule name: AdminNotification_MaxScaleHighCPU
Folder: Tasks10 ML Alerts

# Query A - Проверяем количество подов И CPU
Data source: Prometheus
Query: kube_deployment_status_replicas{deployment="tasks10-ml-service-api", namespace="mlops-tasks10"} >= 4

# РАБОЧИЙ CPU запрос (отдельно):
avg(rate(container_cpu_usage_seconds_total{namespace="mlops-tasks10", pod=~"tasks10-ml-service-api-.*"}[1m])) * 100 > 0.1



Legend: Max Scale + High CPU
```

### Шаг 3.3: Настраиваем условие
```yaml
# Condition
Condition: A
IS ABOVE: 0

# Evaluation
Evaluate every: 1m
For: 5m
```

### Шаг 3.4: Добавляем аннотации и лейблы
**Annotations:**
```yaml
Summary: 🚨 ADMIN ALERT: Maximum scale reached with high CPU load
Description: Tasks10 ML API has scaled to maximum replicas (6) AND average CPU usage >80% for 5+ minutes. Immediate admin intervention required.
Runbook URL: https://wiki.company.com/runbooks/admin-max-scale-high-cpu  
Dashboard URL: http://grafana.darveter.com/d/tasks10-ml-dashboard
```

**Labels:**
```yaml
severity: critical
service: tasks10-ml-api
alert_type: admin_notification
notify: admin
team: devops
```

### Шаг 3.5: Сохраняем правило
1. Нажмите **"Save rule and exit"**
2. Убедитесь, что правило появилось в списке **Alert rules**

---

## 🎯 ЭТАП 4: Настройка Notification Policy

### Шаг 4.1: Создаем политику уведомлений
1. Перейдите в **Alerting** → **Notification policies**
2. Нажмите **"New specific policy"**

### Шаг 4.2: Настраиваем матчинг
```yaml
Matchers:
- alert_type = admin_notification
- severity = critical

Contact point: telegram-admin-alerts
```

### Шаг 4.3: Настраиваем тайминги
```yaml
Group wait: 10s
Group interval: 5m
Repeat interval: 30m
```

### Шаг 4.4: Сохраняем политику
Нажмите **"Save policy"**

---

## 🎯 ЭТАП 5: Тестирование алерта

### Шаг 5.1: Проверяем статус правила
1. Откройте **Alert rules**
2. Найдите **AdminNotification_MaxScaleHighCPU**
3. Статус должен быть **"Normal"** (зеленый)

### Шаг 5.2: Мониторим условия
Отслеживайте в дашборде:
- **Pod Scaling (HPA)**: количество подов
- **CPU Usage**: средняя нагрузка CPU

### Шаг 5.3: Запускаем нагрузочное тестирование


### Шаг 5.4: Ожидаем срабатывания
При достижении критических условий:
1. **Grafana**: Алерт изменит статус на **"Alerting"** (красный)
2. **Telegram**: Придет уведомление от вашего бота
3. **Dashboard**: Покажет 6 подов + CPU > 80%

---

## 📊 ЭТАП 6: Мониторинг и верификация

### Шаг 6.1: Проверяем алерты в реальном времени
```bash
# Мониторим поды
watch kubectl get pods -n mlops-tasks10

# Мониторим HPA
watch kubectl get hpa -n mlops-tasks10  

# Мониторим CPU
watch kubectl top pods -n mlops-tasks10
```

### Шаг 6.2: Проверяем Grafana Alert History
1. **Alerting** → **Alert rules** → **AdminNotification_MaxScaleHighCPU**
2. Вкладка **"History"** - история срабатываний
3. Вкладка **"State history"** - изменения состояния

### Шаг 6.3: Проверяем Telegram уведомления
Ожидаемое сообщение в Telegram:
```
🚨 КРИТИЧЕСКИЙ АЛЕРТ: Tasks10 ML API

Система достигла критических условий:
✅ Подов: 6 (максимум = 6)
✅ CPU > 80% в течение 5+ минут

📊 Dashboard: http://grafana.darveter.com/d/tasks10-ml-dashboard
📋 Runbook: https://wiki.company.com/runbooks/admin-max-scale-high-cpu

🔧 Требуемые действия:
1. Проверить ресурсы кластера
2. Проанализировать паттерны нагрузки
3. Рассмотреть увеличение лимитов ресурсов
4. Исследовать возможную DDoS атаку

⏰ Время: 2024-12-20 15:30:45
🏷️ Сервис: tasks10-ml-api
```

---

## 🚨 TROUBLESHOOTING

### Проблема: Telegram бот не отправляет сообщения
**Решение:**
1. Проверьте Bot Token: он должен быть в формате `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`
2. Проверьте Chat ID: это число, например `123456789`
3. Убедитесь, что вы написали боту хотя бы одно сообщение
4. Тестируйте Contact Point: **Test** кнопка в Grafana

### Проблема: Алерт не срабатывает
**Решение:**
1. Проверьте метрики в Prometheus: `http://prometheus.darveter.com`
2. Выполните query вручную в Grafana → Explore:
   ```
   kube_deployment_status_replicas{deployment="tasks10-ml-service-api", namespace="mlops-tasks10"}
   
   avg(rate(container_cpu_usage_seconds_total{namespace="mlops-tasks10",container="tasks10-ml-service-api"}[5m])) * 100
   ```
3. Убедитесь, что поды действительно масштабируются до 6

### Проблема: Query возвращает пустой результат
**Решение:**
1. Упростите query - проверьте каждую часть отдельно
2. Убедитесь, что метрики доступны в Prometheus
3. Проверьте правильность namespace и deployment имен

---

## ✅ КРИТЕРИИ УСПЕШНОСТИ

- [ ] Telegram бот создан и протестирован
- [ ] Contact Point создан и отправляет тестовые сообщения
- [ ] Alert Rule создано с корректным query  
- [ ] Notification Policy настроена
- [ ] При 6 подах + CPU>80% алерт срабатывает
- [ ] Telegram уведомления приходят в течение 1-2 секунд
- [ ] Алерт отображается в Grafana UI как "Alerting"

---

## 📋 ПОЛЕЗНЫЕ ССЫЛКИ

- **Grafana Dashboard**: http://grafana.darveter.com/d/tasks10-ml-dashboard
- **Prometheus**: http://prometheus.darveter.com
- **Airflow**: http://localhost:8080 (через port-forward)
- **Alert Rules**: http://grafana.darveter.com/alerting/list
- **Contact Points**: http://grafana.darveter.com/alerting/notifications
- **@BotFather**: https://t.me/BotFather

---

## 🔧 КОМАНДЫ ДЛЯ БЫСТРОГО ДОСТУПА

```bash
# Port-forward для Grafana (если нужно)
kubectl port-forward svc/grafana 3000:80 -n grafana

# Port-forward для Prometheus  
kubectl port-forward svc/prometheus-server 9090:80 -n prometheus

# Port-forward для Airflow
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow

# Мониторинг системы
kubectl get pods,hpa -n mlops-tasks10
kubectl top pods -n mlops-tasks10

# Получение Chat ID через API
curl "https://api.telegram.org/bot<BOT_TOKEN>/getUpdates"
```

## 🚀 БЫСТРЫЙ СТАРТ: Telegram за 5 минут

1. **@BotFather** → `/newbot` → получите Bot Token
2. Напишите боту → откройте `https://api.telegram.org/bot<TOKEN>/getUpdates` → получите Chat ID
3. **Grafana** → **Alerting** → **Contact points** → **New contact point**
4. **Integration: Telegram** → введите Token и Chat ID
5. **Test** → должно прийти тестовое сообщение
6. **Save contact point**

**Готово! Теперь все алерты будут приходить в Telegram мгновенно!** 🚀
