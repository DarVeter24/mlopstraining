#!/usr/bin/env python3
"""
Ручной тест REST API для fraud detection
"""
import time
import requests
import json
from datetime import datetime

print("🧪 ТЕСТИРОВАНИЕ FRAUD DETECTION REST API")
print("=" * 45)

# Запускаем API сервер в отдельном процессе
print("📋 Инструкция:")
print("1. В отдельном терминале запустите: cd /data/Projects_backup/MLOps-traning/task-for-exam/mlopstraining/Tasks9")
print("2. Активируйте venv: source venv/bin/activate")  
print("3. Запустите API: python -m src.api")
print("4. Дождитесь сообщения '🎯 Fraud Detection API is ready!'")
print("5. Нажмите Enter здесь для продолжения тестирования...")
input()

API_BASE_URL = "http://localhost:8000"

def test_health_endpoint():
    """Тест health check эндпоинта"""
    print("\n🔍 Тестируем /health эндпоинт...")
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Health Check Response:")
            print(json.dumps(data, indent=2))
            return True
        else:
            print(f"❌ Health check failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Health check error: {e}")
        return False

def test_predict_endpoint():
    """Тест predict эндпоинта"""
    print("\n🔍 Тестируем /predict эндпоинт...")
    
    # Тестовые данные (нормальная транзакция)
    test_transaction_normal = {
        "transaction_id": "test_tx_001",
        "customer_id": 1001,
        "terminal_id": 2001,
        "tx_amount": 100.50,
        "tx_time_seconds": int(time.time()),
        "tx_time_days": (datetime.utcnow() - datetime(1970, 1, 1)).days,
        "tx_fraud_scenario": 0
    }
    
    # Тестовые данные (подозрительная транзакция)
    test_transaction_fraud = {
        "transaction_id": "test_tx_002",
        "customer_id": 1002,
        "terminal_id": 2002,
        "tx_amount": 2500.00,
        "tx_time_seconds": int(time.time()),
        "tx_time_days": (datetime.utcnow() - datetime(1970, 1, 1)).days,
        "tx_fraud_scenario": 1
    }
    
    # Тест 1: Нормальная транзакция
    print("\n📊 Тест 1: Нормальная транзакция")
    print(f"Input: {test_transaction_normal}")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/predict",
            json=test_transaction_normal,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Prediction Response:")
            print(json.dumps(data, indent=2))
            
            # Проверяем результат
            if data["is_fraud"]:
                print("🚨 Результат: FRAUD")
            else:
                print("✅ Результат: NORMAL")
        else:
            print(f"❌ Prediction failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Prediction error: {e}")
        return False
    
    # Тест 2: Подозрительная транзакция
    print("\n📊 Тест 2: Подозрительная транзакция")
    print(f"Input: {test_transaction_fraud}")
    
    try:
        response = requests.post(
            f"{API_BASE_URL}/predict",
            json=test_transaction_fraud,
            headers={"Content-Type": "application/json"}
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Prediction Response:")
            print(json.dumps(data, indent=2))
            
            # Проверяем результат
            if data["is_fraud"]:
                print("🚨 Результат: FRAUD")
            else:
                print("✅ Результат: NORMAL")
        else:
            print(f"❌ Prediction failed: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Prediction error: {e}")
        return False
    
    return True

def test_root_endpoint():
    """Тест root эндпоинта"""
    print("\n🔍 Тестируем / (root) эндпоинт...")
    try:
        response = requests.get(f"{API_BASE_URL}/")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✅ Root Response:")
            print(json.dumps(data, indent=2))
            return True
        else:
            print(f"❌ Root endpoint failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Root endpoint error: {e}")
        return False

def test_docs_endpoint():
    """Тест docs эндпоинта"""
    print("\n🔍 Тестируем /docs эндпоинт...")
    try:
        response = requests.get(f"{API_BASE_URL}/docs")
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("✅ OpenAPI docs доступны")
            print(f"📖 Swagger UI: {API_BASE_URL}/docs")
            return True
        else:
            print(f"❌ Docs endpoint failed: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Docs endpoint error: {e}")
        return False

# Запуск тестов
if __name__ == "__main__":
    success_count = 0
    total_tests = 4
    
    print("\n🚀 НАЧИНАЕМ ТЕСТИРОВАНИЕ API...")
    
    # Тестируем все эндпоинты
    if test_health_endpoint():
        success_count += 1
    
    if test_root_endpoint():
        success_count += 1
    
    if test_docs_endpoint():
        success_count += 1
    
    if test_predict_endpoint():
        success_count += 1
    
    # Итоги
    print("\n" + "=" * 45)
    print(f"🎯 РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")
        print("🎉 REST API ПОЛНОСТЬЮ РАБОТАЕТ!")
        print("\n📋 Доступные эндпоинты:")
        print(f"   • GET  {API_BASE_URL}/        - Информация об API")
        print(f"   • GET  {API_BASE_URL}/health  - Health check")
        print(f"   • POST {API_BASE_URL}/predict - Предсказание fraud")
        print(f"   • GET  {API_BASE_URL}/docs    - OpenAPI документация")
        print("\n🚀 ИТЕРАЦИЯ 2 УСПЕШНО ЗАВЕРШЕНА!")
    else:
        print(f"❌ {total_tests - success_count} тестов не прошли")
        print("🔧 Требуется дополнительная отладка")

