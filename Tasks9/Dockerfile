# Multi-stage build для оптимизации размера образа
FROM python:3.13-slim as builder

# Устанавливаем системные зависимости для сборки
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Создаем рабочую директорию
WORKDIR /app

# Копируем файлы зависимостей
COPY requirements.txt .

# Устанавливаем Python зависимости
RUN pip install --no-cache-dir --user -r requirements.txt

# Финальный образ
FROM python:3.13-slim

# Создаем непривилегированного пользователя
RUN groupadd -r mlops && useradd -r -g mlops mlops

# Устанавливаем системные зависимости времени выполнения
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Копируем установленные пакеты из builder stage
COPY --from=builder /root/.local /home/mlops/.local

# Создаем рабочую директорию
WORKDIR /app

# Копируем код приложения
COPY src/ ./src/
COPY env-example .env

# Устанавливаем права доступа
RUN chown -R mlops:mlops /app

# Переключаемся на непривилегированного пользователя
USER mlops

# Добавляем локальные пакеты в PATH
ENV PATH=/home/mlops/.local/bin:$PATH
ENV PYTHONPATH=/app

# Настройки для оптимизации Python
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Открываем порт
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Запускаем приложение
# Переменные для fallback
ENV USE_MOCK_MODEL=false
ENV MODEL_LOAD_TIMEOUT=60

# Команда запуска с улучшенной версией API
CMD ["python", "-m", "src.api_improved"]

