# 🐳 Docker Deployment Guide

Руководство по контейнеризации и развертыванию Fraud Detection API.

## 📋 Требования

- Docker 20.10+
- Docker Compose 2.0+
- 4GB RAM (рекомендуется)
- 2 CPU cores (рекомендуется)

## 🚀 Быстрый старт

### 1. Сборка и запуск через Docker Compose

```bash
# Запуск основного сервиса
docker-compose up -d

# Проверка статуса
docker-compose ps

# Просмотр логов
docker-compose logs -f fraud-detection-api
```

### 2. Сборка и запуск через Docker

```bash
# Сборка образа
docker build -t fraud-detection-api:latest .

# Запуск контейнера
docker run -d \
  -p 8000:8000 \
  --name fraud-api \
  --env-file .env \
  fraud-detection-api:latest
```

### 3. Использование Makefile

```bash
# Показать все доступные команды
make help

# Собрать и протестировать
make docker-build
make docker-test

# Запустить через docker-compose
make docker-compose-up
```

## 🔧 Конфигурация

### Переменные окружения

Основные переменные конфигурации:

```bash
# MLflow
MLFLOW_TRACKING_URI=http://mlflow.darveter.com
MLFLOW_MODEL_RUN_ID=eb1baf3686bd452981925a0fa4725ab8
MLFLOW_MODEL_ARTIFACT=fraud_model_production
MLFLOW_USERNAME=admin
MLFLOW_PASSWORD=password1312

# S3/MinIO
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_ENDPOINT_URL=http://192.168.31.201:9000
S3_BUCKET_NAME=mlflow-artifacts

# API
API_HOST=0.0.0.0
API_PORT=8000
ENVIRONMENT=production
DEBUG=false
LOG_LEVEL=INFO
```

### Docker Compose конфигурация

```yaml
services:
  fraud-detection-api:
    build: .
    ports:
      - "8000:8000"
    environment:
      - MLFLOW_TRACKING_URI=http://mlflow.darveter.com
      # ... другие переменные
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '0.5'
          memory: 1G
```

## 🧪 Тестирование

### Автоматическое тестирование

```bash
# Полное тестирование Docker образа
./docker-test.sh

# Или через Makefile
make docker-test
```

### Ручное тестирование

```bash
# Health check
curl http://localhost:8000/health

# Тест предсказания
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "test_001",
    "customer_id": 1001,
    "terminal_id": 2001,
    "tx_amount": 100.50,
    "tx_time_seconds": 1705312200,
    "tx_time_days": 19723,
    "tx_fraud_scenario": 0
  }'
```

## 📊 Мониторинг

### Health Check

Контейнер включает встроенный health check:

```bash
# Проверка статуса контейнера
docker ps

# Детальная информация о health check
docker inspect fraud-api | grep -A 10 Health
```

### Логи

```bash
# Просмотр логов
docker logs fraud-api

# Следить за логами в реальном времени
docker logs -f fraud-api

# Логи через docker-compose
docker-compose logs -f fraud-detection-api
```

## 🔍 Отладка

### Подключение к контейнеру

```bash
# Подключиться к запущенному контейнеру
docker exec -it fraud-api /bin/bash

# Через Makefile
make docker-shell
```

### Проверка ресурсов

```bash
# Использование ресурсов
docker stats fraud-api

# Информация о контейнере
docker inspect fraud-api
```

## 📈 Оптимизация

### Размер образа

- Используется multi-stage build для уменьшения размера
- Базовый образ: `python:3.13-slim`
- Исключены dev-зависимости в production образе

### Производительность

- Непривилегированный пользователь для безопасности
- Оптимизированные настройки Python
- Resource limits в docker-compose

### Кэширование слоев

```bash
# Сборка с кэшированием
docker build --cache-from fraud-detection-api:latest -t fraud-detection-api:latest .
```

## 🚨 Решение проблем

### Проблемы с подключением к MLflow

```bash
# Проверить сетевое подключение
docker exec fraud-api curl -v http://mlflow.darveter.com

# Проверить переменные окружения
docker exec fraud-api env | grep MLFLOW
```

### Проблемы с памятью

```bash
# Увеличить лимиты памяти в docker-compose.yml
deploy:
  resources:
    limits:
      memory: 6G
```

### Проблемы с загрузкой модели

```bash
# Проверить логи при старте
docker logs fraud-api | grep -i model

# Проверить доступность S3
docker exec fraud-api curl -v http://192.168.31.201:9000
```

## 📋 Чек-лист развертывания

- [ ] Docker и Docker Compose установлены
- [ ] Переменные окружения настроены
- [ ] MLflow сервер доступен
- [ ] S3/MinIO доступен
- [ ] Порт 8000 свободен
- [ ] Достаточно ресурсов (RAM/CPU)
- [ ] Health check проходит
- [ ] API эндпоинты отвечают

## 🔗 Полезные команды

```bash
# Очистка Docker ресурсов
make docker-clean

# Перезапуск сервиса
docker-compose restart fraud-detection-api

# Обновление образа
docker-compose pull
docker-compose up -d

# Бэкап конфигурации
cp docker-compose.yml docker-compose.yml.backup
```

