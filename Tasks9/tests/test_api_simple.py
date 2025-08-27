"""
Тесты для упрощенного API (без загрузки модели)
"""

import pytest
from fastapi.testclient import TestClient

from src.api_simple import app


class TestAPISimple:
    """Тесты для упрощенного API"""

    @pytest.fixture
    def client(self):
        """Фикстура для тестового клиента"""
        return TestClient(app)

    def test_root_endpoint(self, client):
        """Тест корневого эндпоинта"""
        response = client.get("/")
        assert response.status_code == 200

        data = response.json()
        assert "message" in data
        assert "version" in data
        assert "docs" in data
        assert "health" in data
        assert data["mode"] == "test_without_model"

    def test_health_endpoint(self, client):
        """Тест health check эндпоинта"""
        response = client.get("/health")
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["model_loaded"] is False
        assert data["model_version"] == "test_mode"
        assert data["environment"] == "test"

    def test_predict_endpoint_normal_transaction(self, client):
        """Тест предсказания для нормальной транзакции"""
        transaction_data = {
            "transaction_id": "test_001",
            "customer_id": 1001,
            "terminal_id": 2001,
            "tx_amount": 50.0,
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0,
        }

        response = client.post("/predict", json=transaction_data)
        assert response.status_code == 200

        data = response.json()
        assert data["transaction_id"] == "test_001"
        assert data["is_fraud"] is False
        assert 0.0 <= data["fraud_probability"] <= 1.0
        assert 0.0 <= data["confidence"] <= 1.0

    def test_predict_endpoint_suspicious_transaction(self, client):
        """Тест предсказания для подозрительной транзакции"""
        transaction_data = {
            "transaction_id": "test_002",
            "customer_id": 1002,
            "terminal_id": 2002,
            "tx_amount": 5000.0,  # Большая сумма
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 1,  # Подозрительный сценарий
        }

        response = client.post("/predict", json=transaction_data)
        assert response.status_code == 200

        data = response.json()
        assert data["transaction_id"] == "test_002"
        assert data["is_fraud"] is True
        assert 0.0 <= data["fraud_probability"] <= 1.0
        assert 0.0 <= data["confidence"] <= 1.0

    def test_predict_endpoint_invalid_data(self, client):
        """Тест предсказания с невалидными данными"""
        # Отсутствует обязательное поле
        invalid_data = {
            "transaction_id": "test_003",
            "customer_id": 1003,
            # Отсутствует terminal_id
            "tx_amount": 100.0,
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0,
        }

        response = client.post("/predict", json=invalid_data)
        assert response.status_code == 422  # Validation error

    def test_predict_endpoint_negative_amount(self, client):
        """Тест предсказания с отрицательной суммой"""
        transaction_data = {
            "transaction_id": "test_004",
            "customer_id": 1004,
            "terminal_id": 2004,
            "tx_amount": -100.0,  # Отрицательная сумма
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0,
        }

        response = client.post("/predict", json=transaction_data)
        assert response.status_code == 422  # Validation error

    def test_predict_endpoint_zero_amount(self, client):
        """Тест предсказания с нулевой суммой"""
        transaction_data = {
            "transaction_id": "test_005",
            "customer_id": 1005,
            "terminal_id": 2005,
            "tx_amount": 0.0,  # Нулевая сумма
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0,
        }

        response = client.post("/predict", json=transaction_data)
        assert response.status_code == 422  # Validation error

    def test_docs_endpoint(self, client):
        """Тест эндпоинта документации"""
        response = client.get("/docs")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    def test_openapi_endpoint(self, client):
        """Тест OpenAPI схемы"""
        response = client.get("/openapi.json")
        assert response.status_code == 200

        data = response.json()
        assert "openapi" in data
        assert "info" in data
        assert "paths" in data

        # Проверяем наличие наших эндпоинтов
        assert "/" in data["paths"]
        assert "/health" in data["paths"]
        assert "/predict" in data["paths"]
