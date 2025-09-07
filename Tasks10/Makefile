# Makefile для управления Tasks10 MLOps проектом

.PHONY: help install test clean docker-build docker-run docker-test k8s-deploy k8s-cleanup

# Переменные
IMAGE_NAME := tasks10-ml-api
IMAGE_TAG := latest
CONTAINER_NAME := tasks10-ml-test
NAMESPACE := mlops-tasks10

help: ## Показать справку по командам
	@echo "Доступные команды Tasks10:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Установить зависимости в виртуальное окружение
	python -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt

test: ## Запустить тесты приложения
	python -m pytest tests/ -v

run: ## Запустить API локально
	python -m src.api

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

docker-clean: ## Очистить Docker ресурсы
	docker stop $(CONTAINER_NAME) || true
	docker rm $(CONTAINER_NAME) || true
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true

# Kubernetes команды
k8s-validate: ## Валидировать Kubernetes манифесты
	kubectl apply --dry-run=client -f argocd/manifests/

k8s-deploy-argocd: ## Развернуть через ArgoCD
	kubectl apply -f argocd/project.yaml
	kubectl apply -f argocd/application.yaml

k8s-deploy-direct: ## Прямое развертывание манифестов
	kubectl apply -f argocd/manifests/

k8s-status: ## Показать статус ресурсов
	kubectl get pods,svc,deployment,hpa,ingress -n $(NAMESPACE)

k8s-logs: ## Показать логи подов
	kubectl logs -l app=tasks10-ml-api -n $(NAMESPACE) --tail=100 -f

k8s-cleanup: ## Удалить ресурсы из Kubernetes
	kubectl delete -f argocd/manifests/ || true
	kubectl delete application tasks10-ml-service -n argocd || true

# API тестирование
health-check: ## Проверить здоровье API
	curl -f http://localhost:8000/health

metrics-check: ## Проверить метрики Prometheus
	curl -f http://localhost:8000/metrics

api-test: ## Протестировать API эндпоинты
	@echo "Testing health endpoint..."
	curl -s http://localhost:8000/health | jq '.' || curl -s http://localhost:8000/health
	@echo "\nTesting predict endpoint..."
	curl -s -X POST http://localhost:8000/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id":"tasks10_test","customer_id":1001,"terminal_id":2001,"tx_amount":100.50,"tx_time_seconds":1705312200,"tx_time_days":19723,"tx_fraud_scenario":0}' \
		| jq '.' || curl -s -X POST http://localhost:8000/predict \
		-H "Content-Type: application/json" \
		-d '{"transaction_id":"tasks10_test","customer_id":1001,"terminal_id":2001,"tx_amount":100.50,"tx_time_seconds":1705312200,"tx_time_days":19723,"tx_fraud_scenario":0}'

# Port forwarding для тестирования в K8s
port-forward-api: ## Port forward к API в K8s
	kubectl port-forward svc/tasks10-ml-service 8000:8000 -n $(NAMESPACE)

port-forward-prometheus: ## Port forward к Prometheus
	kubectl port-forward svc/prometheus-server 9090:80 -n monitoring

# Мониторинг команды
argocd-status: ## Показать статус ArgoCD приложения
	argocd app get tasks10-ml-service

argocd-sync: ## Принудительно синхронизировать ArgoCD
	argocd app sync tasks10-ml-service

# Полный цикл развертывания
deploy-full: k8s-validate k8s-deploy-argocd ## Полное развертывание через ArgoCD
	@echo "Tasks10 развертывание через ArgoCD завершено!"
	@echo "Проверьте статус: make k8s-status"
	@echo "Для тестирования: make port-forward-api (в отдельном терминале) && make api-test"
