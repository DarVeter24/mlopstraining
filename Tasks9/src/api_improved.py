"""Improved FastAPI REST service for fraud detection with fallback support."""

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

from .config import config
from .model_loader import model_loader

# Configure logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)


class MockModel:
    """Mock model for fallback when MLflow is unavailable."""

    def __init__(self):
        self.version = "mock-v1.0"
        self.loaded = True
        logger.info("🎭 Mock model initialized for fallback")

    def predict(self, data: pd.DataFrame) -> list:
        """Generate mock prediction based on simple rules."""
        try:
            row = data.iloc[0]
            tx_amount = row.get("tx_amount", 0)
            tx_time_seconds = row.get("tx_time_seconds", 0)
            customer_id = row.get("customer_id", 0)

            # Simple rule-based mock logic
            fraud_score = 0.0

            # Large amounts are more suspicious
            if tx_amount > 1000:
                fraud_score += 0.3
            elif tx_amount > 500:
                fraud_score += 0.1

            # Odd hours (night time) are more suspicious
            hour = (tx_time_seconds // 3600) % 24
            if hour < 6 or hour > 22:
                fraud_score += 0.2

            # Customer ID patterns (simple check)
            if customer_id % 100 < 5:  # 5% of customers flagged as high-risk
                fraud_score += 0.4

            # Cap the score and add some randomness
            fraud_score = min(fraud_score + (hash(str(customer_id)) % 10) / 100, 0.95)

            return [fraud_score]

        except Exception as e:
            logger.error(f"Mock prediction error: {e}")
            return [0.2]  # Safe default


# Global model instance
model: Optional[Any] = None
model_type: str = "unknown"


async def load_model_with_fallback():
    """Load model with timeout and fallback to mock model."""
    global model, model_type

    timeout = config.MODEL_LOAD_TIMEOUT
    use_mock = config.USE_MOCK_MODEL

    if use_mock:
        logger.info("🎭 Using mock model (USE_MOCK_MODEL=true)")
        model = MockModel()
        model_type = "mock"
        return

    try:
        logger.info(f"📦 Loading ML model (timeout: {timeout}s)...")

        # Try to load real model with timeout
        model = await asyncio.wait_for(
            asyncio.to_thread(model_loader.load_model), timeout=timeout
        )

        if model is not None:
            logger.info("✅ Real MLflow model loaded successfully")
            model_type = "mlflow"
        else:
            raise Exception("Model loader returned None")

    except asyncio.TimeoutError:
        logger.warning(f"⏰ Model loading timed out after {timeout}s")
        logger.info("🎭 Falling back to mock model")
        model = MockModel()
        model_type = "mock"

    except Exception as e:
        logger.error(f"❌ Failed to load real model: {e}")
        logger.info("🎭 Falling back to mock model")
        model = MockModel()
        model_type = "mock"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events."""
    # Startup
    global model, model_type
    logger.info("🚀 Starting Fraud Detection API...")

    try:
        # Validate configuration
        config.validate_config()
        logger.info("✅ Configuration validated")

        # Load model (with fallback)
        await load_model_with_fallback()

        logger.info(f"🎯 Fraud Detection API is ready! (Model: {model_type})")

    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        # Don't raise - let the app start with mock model
        model = MockModel()
        model_type = "mock"
        logger.info("🎭 Started with emergency mock model")

    yield

    # Shutdown
    logger.info("🛑 Shutting down Fraud Detection API...")


# Create FastAPI app with lifespan
app = FastAPI(
    title=config.API_TITLE,
    version=config.API_VERSION,
    description=config.API_DESCRIPTION + " (with fallback support)",
    debug=config.DEBUG,
    lifespan=lifespan,
)


# Pydantic models (same as original)
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
    model_type: str = Field(..., description="Model type (mlflow/mock)")
    prediction_time: str = Field(..., description="ISO timestamp of prediction")

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_12345",
                "is_fraud": False,
                "fraud_probability": 0.15,
                "confidence": 0.85,
                "model_version": "v1.2.3",
                "model_type": "mlflow",
                "prediction_time": "2024-01-15T10:30:00Z",
            }
        }


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service status")
    timestamp: str = Field(..., description="Current timestamp")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    model_version: str = Field(..., description="Loaded model version")
    model_type: str = Field(..., description="Model type (mlflow/mock)")
    environment: str = Field(..., description="Environment (dev/prod)")


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint."""
    global model, model_type

    model_loaded = model is not None
    model_version = "unknown"

    if model_type == "mock" and model_loaded:
        model_version = getattr(model, "version", "mock-v1.0")
    elif model_type == "mlflow" and model_loaded:
        try:
            model_info = model_loader.get_model_info()
            model_version = model_info.get("model_version", "unknown")
        except:
            model_version = "mlflow-unknown"

    return HealthResponse(
        status="healthy" if model_loaded else "unhealthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        model_loaded=model_loaded,
        model_version=model_version,
        model_type=model_type,
        environment=config.ENVIRONMENT,
    )


@app.post("/predict", response_model=FraudPredictionResponse)
async def predict_fraud(transaction: TransactionData):
    """Predict fraud for a transaction."""
    global model, model_type

    if model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Service unavailable.",
        )

    try:
        start_time = time.time()

        # Prepare input data for model as DataFrame
        input_data = pd.DataFrame(
            [
                {
                    "customer_id": transaction.customer_id,
                    "terminal_id": transaction.terminal_id,
                    "tx_amount": transaction.tx_amount,
                    "tx_time_seconds": transaction.tx_time_seconds,
                    "tx_time_days": transaction.tx_time_days,
                    "tx_fraud_scenario": transaction.tx_fraud_scenario,
                }
            ]
        )

        logger.debug(f"Input data for prediction: {input_data.to_dict('records')[0]}")

        # Make prediction
        if model_type == "mock":
            prediction_result = model.predict(input_data)
            fraud_probability = float(prediction_result[0])
        else:
            # MLflow model
            prediction_result = model.predict(input_data)
            fraud_probability = (
                float(prediction_result[0])
                if isinstance(prediction_result, (list, tuple))
                else float(prediction_result)
            )

        # Ensure probability is in [0, 1] range
        fraud_probability = max(0.0, min(1.0, fraud_probability))

        # Determine if transaction is fraud
        is_fraud = fraud_probability >= 0.5

        # Calculate confidence
        confidence = abs(fraud_probability - 0.5) * 2

        # Get model version
        model_version = "unknown"
        if model_type == "mock":
            model_version = getattr(model, "version", "mock-v1.0")
        else:
            try:
                model_info = model_loader.get_model_info()
                model_version = model_info.get("model_version", "unknown")
            except:
                model_version = "mlflow-unknown"

        prediction_time = time.time() - start_time

        if getattr(config, "ENABLE_PERFORMANCE_LOGGING", True):
            logger.info(
                f"Prediction completed in {prediction_time:.3f}s "
                f"for transaction {transaction.transaction_id} "
                f"(model: {model_type})"
            )

        return FraudPredictionResponse(
            transaction_id=transaction.transaction_id,
            is_fraud=is_fraud,
            fraud_probability=fraud_probability,
            confidence=confidence,
            model_version=model_version,
            model_type=model_type,
            prediction_time=datetime.utcnow().isoformat() + "Z",
        )

    except Exception as e:
        logger.error(
            f"❌ Prediction failed for transaction "
            f"{transaction.transaction_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}",
        )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "Fraud Detection API",
        "version": config.API_VERSION,
        "model_type": model_type,
        "docs": "/docs",
        "health": "/health",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api_improved:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level=config.LOG_LEVEL.lower(),
        reload=config.DEBUG,
    )
