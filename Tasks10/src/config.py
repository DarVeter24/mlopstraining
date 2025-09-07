"""Configuration management for the fraud detection API."""

import os
from typing import Optional

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class for the fraud detection API."""

    # MLflow Configuration
    MLFLOW_TRACKING_URI: str = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    MLFLOW_MODEL_NAME: str = os.getenv("MLFLOW_MODEL_NAME", "fraud_detection_model")
    MLFLOW_MODEL_STAGE: str = os.getenv("MLFLOW_MODEL_STAGE", "Production")
    MLFLOW_MODEL_RUN_ID: Optional[str] = os.getenv("MLFLOW_MODEL_RUN_ID")
    MLFLOW_MODEL_ARTIFACT: str = os.getenv(
        "MLFLOW_MODEL_ARTIFACT", "fraud_model_production"
    )
    MLFLOW_USERNAME: Optional[str] = os.getenv("MLFLOW_USERNAME")
    MLFLOW_PASSWORD: Optional[str] = os.getenv("MLFLOW_PASSWORD")

    # S3/MinIO Configuration
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_ENDPOINT_URL: Optional[str] = os.getenv("AWS_ENDPOINT_URL")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "mlflow-artifacts")

    # API Configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    API_TITLE: str = os.getenv("API_TITLE", "Tasks10 ML API with Monitoring")
    API_VERSION: str = os.getenv("API_VERSION", "1.0.0")
    API_DESCRIPTION: str = os.getenv(
        "API_DESCRIPTION", "MLOps REST API for fraud detection with Prometheus metrics"
    )

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

    # Model Configuration (in-memory only)
    MODEL_CACHE_TTL: int = int(os.getenv("MODEL_CACHE_TTL", "3600"))
    MODEL_TIMEOUT: int = int(os.getenv("MODEL_TIMEOUT", "30"))
    USE_MOCK_MODEL: bool = os.getenv("USE_MOCK_MODEL", "false").lower() == "true"
    MODEL_LOAD_TIMEOUT: int = int(os.getenv("MODEL_LOAD_TIMEOUT", "30"))

    # Health Check Configuration
    HEALTH_CHECK_INTERVAL: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))

    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"
    ENABLE_PERFORMANCE_LOGGING: bool = (
        os.getenv("ENABLE_PERFORMANCE_LOGGING", "false").lower() == "true"
    )
    CLEANUP_TEMP_FILES: bool = os.getenv("CLEANUP_TEMP_FILES", "true").lower() == "true"

    @classmethod
    def validate_config(cls) -> None:
        """Validate required configuration parameters."""
        required_vars = [
            ("MLFLOW_TRACKING_URI", cls.MLFLOW_TRACKING_URI),
            ("MLFLOW_MODEL_NAME", cls.MLFLOW_MODEL_NAME),
        ]

        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)

        if missing_vars:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing_vars)}"
            )

    @classmethod
    def is_development(cls) -> bool:
        """Check if running in development environment."""
        return cls.ENVIRONMENT.lower() == "development"

    @classmethod
    def is_production(cls) -> bool:
        """Check if running in production environment."""
        return cls.ENVIRONMENT.lower() == "production"


# Global config instance
config = Config()
