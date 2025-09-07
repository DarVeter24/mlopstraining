# GitHub Actions для Tasks10

## 🎯 Обзор

Данная папка содержит CI/CD pipeline для Tasks10 ML API с интеграцией Prometheus метрик и автоматическим развертыванием.

## 📁 Структура

```
.github/
├── workflows/
│   ├── ci-cd.yml      # Основной CI/CD pipeline
│   └── security.yml   # Сканирование безопасности
└── README.md          # Данная документация
```

## 🔄 Workflow: ci-cd.yml

### Триггеры
- **Push**: `main`, `develop` ветки (только изменения в `Tasks10/`)
- **Pull Request**: в `main` ветку (только изменения в `Tasks10/`)
- **Manual**: через GitHub UI (`workflow_dispatch`)

### Этапы

1. **Test** - Тестирование на Python 3.11, 3.12, 3.13
   - Установка зависимостей
   - Unit тесты с coverage
   - Тестирование API endpoints
   - Проверка Prometheus метрик

2. **Lint** - Проверка качества кода
   - Black (форматирование)
   - isort (импорты)
   - flake8 (линтинг)
   - mypy (типизация)

3. **Docker Test** - Тестирование контейнера
   - Сборка Docker образа
   - Запуск контейнера
   - Тестирование endpoints (`/health`, `/metrics`, `/predict`)
   - Проверка наличия ML метрик

4. **Build and Push** - Публикация образа (только `main`)
   - Логин в GitHub Container Registry
   - Сборка и публикация образа
   - Генерация deployment артефактов

5. **Notify** - Уведомления о результате

### Образ в реестре
```
ghcr.io/darveter24/mlopstraining/tasks10-ml-service-api:latest
```

## 🔒 Workflow: security.yml

### Триггеры
- **Push/PR**: при изменениях в `Tasks10/`
- **Schedule**: еженедельно по понедельникам в 02:00 UTC

### Проверки
- **Safety**: сканирование зависимостей на уязвимости
- **Bandit**: анализ безопасности Python кода
- **Trivy**: сканирование Docker образа
- **CodeQL**: статический анализ кода

## 🚀 Path Filters

Важная особенность: все workflow'ы используют **path filters**:

```yaml
on:
  push:
    paths: 
      - 'Tasks10/**'  # Только изменения в Tasks10/
```

Это означает:
- ✅ Tasks10 workflows запускаются только при изменениях в `Tasks10/`
- ✅ Tasks9 workflows запускаются только при изменениях в `Tasks9/`
- ✅ Нет конфликтов между проектами
- ✅ Оптимальное использование ресурсов GitHub Actions

## 📊 Интеграция с ArgoCD

После успешной сборки образа:

1. **Docker образ** публикуется в GHCR
2. **ArgoCD** автоматически обнаружит новый образ
3. **Kubernetes** развернет обновленный сервис
4. **Prometheus** начнет собирать метрики с endpoint `/metrics`

## 🧪 Локальное тестирование

Перед push'ом можно протестировать локально:

```bash
# В директории Tasks10/
make docker-build
make docker-run
curl http://localhost:8000/metrics
```

## 🔧 Настройка

### Переменные окружения
- `GITHUB_TOKEN` - автоматически предоставляется GitHub
- Дополнительные секреты настраиваются в Settings → Secrets

### Permissions
Workflow имеет необходимые разрешения:
- `contents: read` - чтение репозитория
- `packages: write` - публикация в GHCR
