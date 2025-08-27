# Makefile для управления MLOps проектом

.PHONY: help install test clean docker-build docker-run docker-test docker-compose-up docker-compose-down

# Переменные
IMAGE_NAME := fraud-detection-api
IMAGE_TAG := latest
CONTAINER_NAME := fraud-api-test

help: ## Показать справку по командам
	@echo "Доступные команды:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Установить зависимости в виртуальное окружение
	python -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt

test: ## Запустить тесты приложения
	python -m pytest tests/ -v

test-unit: ## Запустить только unit тесты
	python -m pytest tests/ -v -m "not integration"

test-integration: ## Запустить только integration тесты
	python -m pytest tests/ -v -m integration

test-coverage: ## Запустить тесты с покрытием кода
	python -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

test-ci: ## Запустить тесты как в CI
	python -m pytest tests/ -v --cov=src --cov-report=xml --cov-report=term-missing

run: ## Запустить API локально
	python -m src.api

run-simple: ## Запустить упрощенную версию API
	python -m src.api_simple

clean: ## Очистить временные файлы
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/

docker-build: ## Собрать Docker образ
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

docker-run: ## Запустить контейнер
	docker run -d -p 8000:8000 --name $(CONTAINER_NAME) $(IMAGE_NAME):$(IMAGE_TAG)

docker-stop: ## Остановить контейнер
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true

docker-test: ## Полное тестирование Docker образа
	./docker-test.sh

docker-logs: ## Показать логи контейнера
	docker logs $(CONTAINER_NAME)

docker-shell: ## Подключиться к контейнеру
	docker exec -it $(CONTAINER_NAME) /bin/bash

docker-compose-up: ## Запустить через docker-compose
	docker-compose up -d

docker-compose-down: ## Остановить docker-compose
	docker-compose down

docker-compose-test: ## Запустить тестовый сервис через docker-compose
	docker-compose --profile test up -d fraud-detection-test

docker-clean: ## Очистить Docker ресурсы
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true
	docker system prune -f

health-check: ## Проверить здоровье API
	curl -f http://localhost:8000/health

api-test: ## Протестировать API эндпоинты
	@echo "Testing health endpoint..."
	curl -s http://localhost:8000/health | jq '.' || curl -s http://localhost:8000/health
	@echo "\nTesting predict endpoint..."
	curl -s -X POST http://localhost:8000/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id":"makefile_test","customer_id":1001,"terminal_id":2001,"tx_amount":100.50,"tx_time_seconds":1705312200,"tx_time_days":19723,"tx_fraud_scenario":0}' \
		| jq '.' || curl -s -X POST http://localhost:8000/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id":"makefile_test","customer_id":1001,"terminal_id":2001,"tx_amount":100.50,"tx_time_seconds":1705312200,"tx_time_days":19723,"tx_fraud_scenario":0}'

lint: ## Проверить код с помощью линтеров
	black --check src/ tests/
	isort --check-only src/ tests/
	flake8 src/ tests/
	mypy src/ --ignore-missing-imports

format: ## Отформатировать код
	black src/ tests/
	isort src/ tests/

security: ## Проверить безопасность кода
	bandit -r src/ -f json || true
	safety check --json || true

ci-install: ## Установить зависимости для CI
	python -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install pytest pytest-cov pytest-asyncio httpx black isort flake8 mypy bandit safety

ci-test: ci-install lint test-ci security ## Полный цикл CI тестирования
	@echo "CI тестирование завершено!"

requirements: ## Обновить requirements.txt
	pip freeze > requirements_frozen.txt

dev-setup: install ## Настроить среду разработки
	@echo "Среда разработки настроена!"
	@echo "Запустите: make run или make docker-compose-up"

full-test: clean docker-build docker-test ## Полный цикл тестирования
	@echo "Полное тестирование завершено!"

