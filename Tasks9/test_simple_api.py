#!/usr/bin/env python3
"""
Простой тест API без загрузки модели
"""
from fastapi import FastAPI
from pydantic import BaseModel, Field
import uvicorn

# Простое FastAPI приложение для тестирования
app = FastAPI(title="Simple Test API", version="1.0.0")

class SimpleRequest(BaseModel):
    message: str = Field(..., description="Test message")

class SimpleResponse(BaseModel):
    response: str = Field(..., description="Response message")
    status: str = Field(..., description="Status")

@app.get("/")
async def root():
    return {"message": "Simple Test API", "status": "working"}

@app.get("/health")
async def health():
    return {"status": "healthy", "api": "working"}

@app.post("/test", response_model=SimpleResponse)
async def test_endpoint(request: SimpleRequest):
    return SimpleResponse(
        response=f"Received: {request.message}",
        status="success"
    )

if __name__ == "__main__":
    print("🚀 Starting Simple Test API...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")

