"""Упрощенная версия FastAPI для тестирования без загрузки модели."""

from datetime import datetime

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# Простая конфигурация
API_TITLE = "Fraud Detection API (Test Mode)"
API_VERSION = "1.0.0"
API_DESCRIPTION = "MLOps REST API for fraud detection model - Test Mode"

# Create FastAPI app
app = FastAPI(
    title=API_TITLE, version=API_VERSION, description=API_DESCRIPTION, debug=True
)


# Pydantic models
class TransactionData(BaseModel):
    """Input data model for fraud detection based on Spark ML model schema."""

    transaction_id: str = Field(
        ..., description="Unique transaction identifier for tracking"
    )
    customer_id: int = Field(..., gt=0, description="Unique customer identifier")
    terminal_id: int = Field(..., gt=0, description="Terminal/merchant identifier")
    tx_amount: float = Field(..., gt=0, description="Transaction amount")
    tx_time_seconds: int = Field(
        ..., ge=0, description="Transaction timestamp in seconds since epoch"
    )
    tx_time_days: int = Field(
        ..., ge=0, description="Transaction day number since reference date"
    )
    tx_fraud_scenario: int = Field(
        0, ge=0, le=1, description="Fraud scenario indicator (0=normal, 1=fraud test)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_12345",
                "customer_id": 1001,
                "terminal_id": 2001,
                "tx_amount": 150.50,
                "tx_time_seconds": 1705312200,
                "tx_time_days": 19723,
                "tx_fraud_scenario": 0,
            }
        }


class FraudPredictionResponse(BaseModel):
    """Response model for fraud detection."""

    transaction_id: str = Field(..., description="Transaction identifier")
    is_fraud: bool = Field(..., description="Whether transaction is predicted as fraud")
    fraud_probability: float = Field(
        ..., ge=0, le=1, description="Probability of fraud (0-1)"
    )
    confidence: float = Field(..., ge=0, le=1, description="Model confidence score")
    model_version: str = Field(..., description="Model version used for prediction")
    prediction_time: str = Field(..., description="ISO timestamp of prediction")


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service status")
    timestamp: str = Field(..., description="Current timestamp")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    model_version: str = Field(..., description="Loaded model version")
    environment: str = Field(..., description="Environment (dev/prod)")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        model_loaded=False,  # В тестовом режиме модель не загружена
        model_version="test_mode",
        environment="test",
    )


@app.post("/predict", response_model=FraudPredictionResponse)
async def predict_fraud(transaction: TransactionData):
    """Predict fraud for a transaction (TEST MODE - mock predictions)."""
    try:
        # MOCK PREDICTION LOGIC для тестирования
        # В реальном режиме здесь будет вызов модели

        # Простая логика для демонстрации:
        # Если сумма > 1000 или tx_fraud_scenario = 1, то считаем fraud
        is_fraud = transaction.tx_amount > 1000.0 or transaction.tx_fraud_scenario == 1
        fraud_probability = 0.8 if is_fraud else 0.2
        confidence = 0.9  # Высокая уверенность для демо

        return FraudPredictionResponse(
            transaction_id=transaction.transaction_id,
            is_fraud=is_fraud,
            fraud_probability=fraud_probability,
            confidence=confidence,
            model_version="mock_v1.0.0",
            prediction_time=datetime.utcnow().isoformat() + "Z",
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}",
        )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Fraud Detection API - Test Mode",
        "version": API_VERSION,
        "docs": "/docs",
        "health": "/health",
        "mode": "test_without_model",
    }


if __name__ == "__main__":
    import uvicorn

    print("🚀 Starting Fraud Detection API in TEST MODE...")
    print("📋 This version uses mock predictions without real model")
    uvicorn.run(
        "src.api_simple:app", host="0.0.0.0", port=8000, log_level="info", reload=True
    )
