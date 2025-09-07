"""FastAPI REST service for fraud detection with fallback logic."""

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional, Any
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI, HTTPException, status, Request, Response
from pydantic import BaseModel, Field

from .config import config
from .model_loader import model_loader
from .metrics import (
    get_metrics,
    track_prediction_metrics,
    track_http_metrics,
    ml_model_errors_total,
    update_system_metrics
)

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
    """Load model with fallback to mock model."""
    global model, model_type
    
    # Check if we should use mock model directly
    if config.USE_MOCK_MODEL:
        logger.info("🎭 USE_MOCK_MODEL=true, using mock model")
        model = MockModel()
        model_type = "mock"
        return

    try:
        timeout = config.MODEL_LOAD_TIMEOUT
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
    logger.info("�� Starting Tasks10 ML API with Monitoring...")

    try:
        # Validate configuration
        config.validate_config()
        logger.info("✅ Configuration validated")

        # Load model (with fallback)
        await load_model_with_fallback()

        logger.info(f"🎯 Tasks10 ML API is ready! (Model: {model_type})")

    except Exception as e:
        logger.error(f"❌ Failed to initialize: {e}")
        # Even if config validation fails, we can still start with mock model
        logger.info("🎭 Starting with mock model as last resort")
        model = MockModel()
        model_type = "mock"

    yield

    # Shutdown
    logger.info("🛑 Shutting down Tasks10 ML API...")


# Create FastAPI app with lifespan
app = FastAPI(
    title=config.API_TITLE,
    version=config.API_VERSION,
    description=config.API_DESCRIPTION,
    debug=config.DEBUG,
    lifespan=lifespan,
)


# Pydantic models for request/response
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
        ..., ge=0, description="Fraud scenario identifier (0=normal)"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_001",
                "customer_id": 123,
                "terminal_id": 456,
                "tx_amount": 150.0,
                "tx_time_seconds": 1705312200,
                "tx_time_days": 19723,
                "tx_fraud_scenario": 0,
            }
        }


class FraudPredictionResponse(BaseModel):
    """Response model for fraud prediction."""

    transaction_id: str = Field(..., description="Transaction identifier")
    fraud_probability: float = Field(
        ..., ge=0.0, le=1.0, description="Probability of fraud (0-1)"
    )
    risk_level: str = Field(..., description="Risk level: low, medium, high")
    model_version: str = Field(..., description="Model version used")
    model_type: str = Field(..., description="Model type: mlflow or mock")
    prediction_time: str = Field(..., description="Prediction timestamp")
    confidence_score: float = Field(
        ..., ge=0.0, le=1.0, description="Model confidence score"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_001",
                "fraud_probability": 0.15,
                "risk_level": "low",
                "model_version": "v1.2.3",
                "model_type": "mlflow",
                "prediction_time": "2024-01-15T10:30:00Z",
                "confidence_score": 0.85,
            }
        }


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service status")
    model_loaded: bool = Field(..., description="Model loading status")
    model_type: str = Field(..., description="Type of loaded model")
    version: str = Field(..., description="API version")
    timestamp: str = Field(..., description="Health check timestamp")
    environment: str = Field(..., description="Environment (dev/prod)")


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    metrics_data = get_metrics()
    return Response(content=metrics_data, media_type="text/plain")


@app.get("/health", response_model=HealthResponse)
@track_http_metrics("health")
async def health_check(request: Request):
    """Health check endpoint with detailed status information."""
    global model, model_type
    
    try:
        # Update system metrics
        update_system_metrics()
        
        model_loaded = model is not None
        current_model_type = model_type if model_loaded else "none"
        
        return HealthResponse(
            status="healthy" if model_loaded else "degraded",
            model_loaded=model_loaded,
            model_type=current_model_type,
            version=config.API_VERSION,
            timestamp=datetime.utcnow().isoformat() + "Z",
            environment=config.ENVIRONMENT,
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Health check failed: {str(e)}",
        )


@app.get("/", response_model=dict)
@track_http_metrics("root")
async def root(request: Request):
    """Root endpoint with service information."""
    global model_type
    
    return {
        "service": "Tasks10 ML API with Monitoring",
        "version": config.API_VERSION,
        "model_type": model_type,
        "status": "running",
        "endpoints": {
            "health": "/health",
            "predict": "/predict",
            "metrics": "/metrics",
            "docs": "/docs",
        },
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }


@app.post("/predict", response_model=FraudPredictionResponse)
@track_http_metrics("predict")
@track_prediction_metrics("mock")
async def predict_fraud(transaction: TransactionData, request: Request):
    """Predict fraud probability for a transaction."""
    global model, model_type
    
    if model is None:
        logger.error("Model not loaded")
        ml_model_errors_total.labels(error_type="ModelNotLoaded").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not available",
        )

    try:
        start_time = time.time()

        # Convert to DataFrame for model input
        input_data = pd.DataFrame([transaction.dict()])

        # Make prediction
        prediction = model.predict(input_data)
        fraud_probability = float(prediction[0]) if prediction else 0.0

        # Determine risk level
        if fraud_probability >= 0.7:
            risk_level = "high"
        elif fraud_probability >= 0.3:
            risk_level = "medium"
        else:
            risk_level = "low"

        # Calculate confidence score (simplified)
        confidence_score = min(0.95, 0.7 + abs(fraud_probability - 0.5))

        # Get model version
        model_version = getattr(model, "version", "unknown")

        prediction_time = time.time() - start_time
        logger.info(
            f"✅ Prediction completed for {transaction.transaction_id}: "
            f"fraud_prob={fraud_probability:.3f}, risk={risk_level}, "
            f"time={prediction_time:.3f}s, model={model_type}"
        )

        return FraudPredictionResponse(
            transaction_id=transaction.transaction_id,
            fraud_probability=fraud_probability,
            risk_level=risk_level,
            model_version=model_version,
            model_type=model_type,
            prediction_time=datetime.utcnow().isoformat() + "Z",
            confidence_score=confidence_score,
        )

    except Exception as e:
        # Record error metric
        ml_model_errors_total.labels(error_type=e.__class__.__name__).inc()
        logger.error(f"❌ Prediction failed for transaction {transaction.transaction_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}",
        )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level=config.LOG_LEVEL.lower(),
        reload=config.DEBUG,
    )
