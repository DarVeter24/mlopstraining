#!/usr/bin/env python3
"""
Простой тест API endpoints через HTTP requests
"""
import requests
import time
import subprocess
import signal
import os
from multiprocessing import Process

def start_api_server():
    """Запускает API сервер в отдельном процессе"""
    os.system("python -m src.api_simple &")

def test_api_endpoints():
    """Тестирует API endpoints"""
    print("🚀 Starting API server...")
    
    # Запускаем сервер в фоне
    server_process = subprocess.Popen([
        "python", "-m", "src.api_simple"
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    try:
        # Ждем запуска сервера
        print("⏳ Waiting for server to start...")
        time.sleep(5)
        
        base_url = "http://localhost:8000"
        
        # Тест 1: Root endpoint
        print("🔍 Testing root endpoint...")
        response = requests.get(f"{base_url}/")
        assert response.status_code == 200
        print("✅ Root endpoint OK")
        
        # Тест 2: Health endpoint
        print("🔍 Testing health endpoint...")
        response = requests.get(f"{base_url}/health")
        assert response.status_code == 200
        print("✅ Health endpoint OK")
        
        # Тест 3: Predict endpoint
        print("🔍 Testing predict endpoint...")
        test_data = {
            "transaction_id": "test_001",
            "customer_id": 1001,
            "terminal_id": 2001,
            "tx_amount": 100.50,
            "tx_time_seconds": 1705312200,
            "tx_time_days": 19723,
            "tx_fraud_scenario": 0
        }
        response = requests.post(f"{base_url}/predict", json=test_data)
        assert response.status_code == 200
        print("✅ Predict endpoint OK")
        
        print("🎉 All API tests passed!")
        
    finally:
        # Останавливаем сервер
        print("🛑 Stopping server...")
        server_process.terminate()
        server_process.wait()

if __name__ == "__main__":
    test_api_endpoints()

