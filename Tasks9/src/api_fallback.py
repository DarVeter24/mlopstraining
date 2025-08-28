"""
Fallback API with mock model for cases when MLflow is unavailable.
This ensures the service starts even if MLflow is down.
"""

import logging
from contextlib import asynccontextmanager
from typing import Any, Dict

import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from .config import config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionData(BaseModel):
    """Input data model for fraud prediction."""

    amount: float
    hour: int
    day_of_week: int
    user_age: int
    account_balance: float

    class Config:
        json_schema_extra = {
            "example": {
                "amount": 100.50,
                "hour": 14,
                "day_of_week": 2,
                "user_age": 35,
                "account_balance": 1500.75,
            }
        }


class PredictionResponse(BaseModel):
    """Response model for fraud prediction."""

    is_fraud: bool
    fraud_probability: float
    model_version: str
    prediction_time: str
    status: str

    class Config:
        json_schema_extra = {
            "example": {
                "is_fraud": False,
                "fraud_probability": 0.15,
                "model_version": "mock-v1.0",
                "prediction_time": "2024-01-15T10:30:45Z",
                "status": "success",
            }
        }


class MockModel:
    """Mock model that provides realistic predictions without MLflow."""

    def __init__(self):
        self.version = "mock-v1.0"
        self.loaded = True
        logger.info("🎭 Mock model initialized")

    def predict(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Generate mock prediction based on simple rules."""
        try:
            # Simple rule-based mock prediction
            amount = data["amount"].iloc[0]
            hour = data["hour"].iloc[0]
            account_balance = data["account_balance"].iloc[0]

            # Mock logic: suspicious if large amount at odd hours with low balance
            fraud_score = 0.0

            if amount > 1000:
                fraud_score += 0.3
            if hour < 6 or hour > 22:
                fraud_score += 0.2
            if account_balance < amount * 2:
                fraud_score += 0.4
            if amount > account_balance:
                fraud_score += 0.5

            fraud_score = min(fraud_score, 0.95)  # Cap at 95%
            is_fraud = fraud_score > 0.5

            return {
                "prediction": [1 if is_fraud else 0],
                "probability": [[1 - fraud_score, fraud_score]],
            }

        except Exception as e:
            logger.error(f"Mock prediction error: {e}")
            # Return safe default
            return {"prediction": [0], "probability": [[0.8, 0.2]]}


# Global model instance
model = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    # Startup
    global model
    logger.info("🚀 Starting Fraud Detection API (Fallback Mode)...")

    try:
        logger.info("✅ Configuration validated")
        logger.info("🎭 Initializing mock model...")

        model = MockModel()

        logger.info("✅ Mock model loaded successfully")
        logger.info("⚠️  Running in FALLBACK MODE - using mock predictions")
        logger.info(f"🌐 API will be available on {config.API_HOST}:{config.API_PORT}")

    except Exception as e:
        logger.error(f"❌ Failed to initialize mock model: {e}")
        raise

    yield

    # Shutdown
    logger.info("🛑 Shutting down Fraud Detection API...")


# Create FastAPI app with lifespan
app = FastAPI(
    title="Tasks9 Fraud Detection API",
    description="MLOps service for fraud detection (Fallback Mode)",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health_check():
    """Health check endpoint."""

    model_status = (
        "loaded"
        if model and hasattr(model, "loaded") and model.loaded
        else "not_loaded"
    )

    return {
        "status": "healthy",
        "service": "fraud-detection-api",
        "version": "1.0.0",
        "model_status": model_status,
        "mode": "fallback",
        "timestamp": pd.Timestamp.now().isoformat(),
    }


@app.post("/predict", response_model=PredictionResponse)
async def predict_fraud(transaction: TransactionData):
    """Predict fraud probability for a transaction."""

    if not model:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        # Convert to DataFrame
        input_data = pd.DataFrame(
            [
                {
                    "amount": transaction.amount,
                    "hour": transaction.hour,
                    "day_of_week": transaction.day_of_week,
                    "user_age": transaction.user_age,
                    "account_balance": transaction.account_balance,
                }
            ]
        )

        # Get prediction
        result = model.predict(input_data)

        # Extract results
        prediction = result["prediction"][0]
        probabilities = result["probability"][0]
        fraud_probability = probabilities[1]  # Probability of fraud

        return PredictionResponse(
            is_fraud=bool(prediction),
            fraud_probability=round(fraud_probability, 4),
            model_version=model.version,
            prediction_time=pd.Timestamp.now().isoformat(),
            status="success",
        )

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=f"Prediction failed: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Tasks9 Fraud Detection API",
        "version": "1.0.0",
        "mode": "fallback",
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.API_HOST, port=config.API_PORT, log_level="info")
