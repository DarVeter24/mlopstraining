"""FastAPI REST service for fraud detection."""

import logging
import time
from datetime import datetime

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

# Create FastAPI app
app = FastAPI(
    title=config.API_TITLE,
    version=config.API_VERSION,
    description=config.API_DESCRIPTION,
    debug=config.DEBUG,
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

    class Config:
        json_schema_extra = {
            "example": {
                "transaction_id": "tx_12345",
                "is_fraud": False,
                "fraud_probability": 0.15,
                "confidence": 0.85,
                "model_version": "v1.2.3",
                "prediction_time": "2024-01-15T10:30:00Z",
            }
        }


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str = Field(..., description="Service status")
    timestamp: str = Field(..., description="Current timestamp")
    model_loaded: bool = Field(..., description="Whether model is loaded")
    model_version: str = Field(..., description="Loaded model version")
    environment: str = Field(..., description="Environment (dev/prod)")


# Global model instance
model = None


@app.on_event("startup")
async def startup_event():
    """Initialize the application on startup."""
    global model

    try:
        logger.info("🚀 Starting Tasks10 ML API with Monitoring...")

        # Validate configuration
        config.validate_config()
        logger.info("✅ Configuration validated")

        # Load the model
        logger.info("📦 Loading ML model...")
        model = model_loader.load_model()
        logger.info("✅ ML model loaded successfully")

        logger.info("🎯 Tasks10 ML API is ready!")

    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        raise RuntimeError(f"Application startup failed: {e}")


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    metrics_data = get_metrics()
    return Response(content=metrics_data, media_type="text/plain")


@app.get("/health", response_model=HealthResponse)
@track_http_metrics("health")
async def health_check(request: Request):
    """Health check endpoint."""
    model_info = model_loader.get_model_info()

    return HealthResponse(
        status="healthy" if model_info["model_loaded"] else "unhealthy",
        timestamp=datetime.utcnow().isoformat() + "Z",
        model_loaded=model_info["model_loaded"],
        model_version=model_info["model_version"],
        environment=config.ENVIRONMENT,
    )


@app.post("/predict", response_model=FraudPredictionResponse)
@track_http_metrics("predict")
async def predict_fraud(request: Request, transaction: TransactionData):
    """Predict fraud for a transaction."""

    if model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Service unavailable.",
        )

    try:
        start_time = time.time()

        # Prepare input data for Spark ML model as DataFrame
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

        # Get model version for metrics
        model_info = model_loader.get_model_info()
        model_version = model_info["model_version"]
        
        # Use prediction metrics decorator function
        @track_prediction_metrics(model_version)
        def make_prediction():
            # Make prediction using MLflow PyFunc model (Spark ML Pipeline)
            prediction = model.predict(input_data)[0]

            # For Spark ML binary classification, prediction is typically 0.0 or 1.0
            fraud_probability = float(prediction)

            # Determine if transaction is fraud
            is_fraud = fraud_probability >= 0.5

            # For binary predictions, confidence is absolute difference from 0.5
            confidence = abs(fraud_probability - 0.5) * 2  # Scale to 0-1 range
            
            return {
                "is_fraud": is_fraud,
                "fraud_probability": fraud_probability,
                "confidence": confidence
            }
        
        # Execute prediction with metrics tracking
        result = make_prediction()
        
        prediction_time = time.time() - start_time

        if config.ENABLE_PERFORMANCE_LOGGING:
            logger.info(
                f"Prediction completed in {prediction_time:.3f}s "
                f"for transaction {transaction.transaction_id}"
            )

        return FraudPredictionResponse(
            transaction_id=transaction.transaction_id,
            is_fraud=result["is_fraud"],
            fraud_probability=result["fraud_probability"],
            confidence=result["confidence"],
            model_version=model_version,
            prediction_time=datetime.utcnow().isoformat() + "Z",
        )

    except Exception as e:
        # Record error metrics
        error_type = type(e).__name__
        ml_model_errors_total.labels(error_type=error_type).inc()
        
        logger.error(
            f"❌ Prediction failed for transaction {transaction.transaction_id}: {e}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Prediction failed: {str(e)}",
        )


@app.get("/")
@track_http_metrics("root")
async def root(request: Request):
    """Root endpoint with API information."""
    return {
        "message": "Tasks10 ML API with Monitoring",
        "version": config.API_VERSION,
        "docs": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.api:app",
        host=config.API_HOST,
        port=config.API_PORT,
        log_level=config.LOG_LEVEL.lower(),
        reload=config.DEBUG,
    )
