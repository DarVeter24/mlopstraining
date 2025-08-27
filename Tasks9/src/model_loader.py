"""Model loading and management for fraud detection.

Loads Spark ML models from MLflow directly into memory for fast inference.
Models are cached in memory without any local file dependencies.
"""

import logging
import os
from typing import Any, Optional

import mlflow.pyfunc

from .config import config

# Configure logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)


class ModelLoader:
    """Handles loading and caching of the fraud detection model.

    Loads Spark ML models from MLflow directly into memory for fast inference.
    Models are cached in memory to avoid repeated MLflow calls.
    No local file operations - pure in-memory caching only.
    """

    def __init__(self):
        self.model: Optional[Any] = None
        self.model_loaded: bool = False
        self.model_version: Optional[str] = None

    def setup_mlflow_environment(self) -> None:
        """Setup MLflow environment variables."""
        # Set MLflow tracking URI
        mlflow.set_tracking_uri(config.MLFLOW_TRACKING_URI)
        logger.info(f"MLflow tracking URI set to: {config.MLFLOW_TRACKING_URI}")

        # Set MLflow authentication if provided
        if config.MLFLOW_USERNAME and config.MLFLOW_PASSWORD:
            os.environ["MLFLOW_TRACKING_USERNAME"] = config.MLFLOW_USERNAME
            os.environ["MLFLOW_TRACKING_PASSWORD"] = config.MLFLOW_PASSWORD
            logger.info("MLflow authentication configured")

        # Set S3/MinIO credentials for artifact access
        if config.AWS_ACCESS_KEY_ID and config.AWS_SECRET_ACCESS_KEY:
            os.environ["AWS_ACCESS_KEY_ID"] = config.AWS_ACCESS_KEY_ID
            os.environ["AWS_SECRET_ACCESS_KEY"] = config.AWS_SECRET_ACCESS_KEY
            if config.AWS_ENDPOINT_URL:
                os.environ["AWS_ENDPOINT_URL"] = config.AWS_ENDPOINT_URL
            logger.info("S3/MinIO credentials configured for MLflow")

    def load_model_from_mlflow(self) -> Any:
        """Load model from MLflow Model Registry."""
        try:
            logger.info("Loading model from MLflow...")
            self.setup_mlflow_environment()

            # Construct model URI
            if config.MLFLOW_MODEL_RUN_ID and config.MLFLOW_MODEL_ARTIFACT:
                # Load from specific run and artifact
                model_uri = (
                    f"runs:/{config.MLFLOW_MODEL_RUN_ID}/{config.MLFLOW_MODEL_ARTIFACT}"
                )
                logger.info(f"Loading model from run: {model_uri}")
            else:
                # Load from model registry with stage
                model_uri = (
                    f"models:/{config.MLFLOW_MODEL_NAME}/{config.MLFLOW_MODEL_STAGE}"
                )
                logger.info(f"Loading model from registry: {model_uri}")

            # Load model using PyFunc interface
            model = mlflow.pyfunc.load_model(model_uri)
            logger.info(f"✅ Model loaded successfully from MLflow: {model_uri}")

            # Try to get model version info
            try:
                client = mlflow.tracking.MlflowClient()
                if config.MLFLOW_MODEL_RUN_ID:
                    run = client.get_run(config.MLFLOW_MODEL_RUN_ID)
                    self.model_version = f"run_{config.MLFLOW_MODEL_RUN_ID[:8]}"
                else:
                    versions = client.get_latest_versions(
                        config.MLFLOW_MODEL_NAME, stages=[config.MLFLOW_MODEL_STAGE]
                    )
                    if versions:
                        self.model_version = f"v{versions[0].version}"
                logger.info(f"Model version: {self.model_version}")
            except Exception as e:
                logger.warning(f"Could not get model version info: {e}")
                self.model_version = "unknown"

            return model

        except Exception as e:
            logger.error(f"❌ Failed to load model from MLflow: {e}")
            raise RuntimeError(f"Failed to load model from MLflow: {e}")

    def load_model(self) -> Any:
        """Load model directly from MLflow into memory."""
        if self.model_loaded and self.model is not None:
            logger.info("Model already loaded, returning cached instance")
            return self.model

        try:
            # Load model directly from MLflow
            logger.info("Loading model from MLflow into memory")
            model = self.load_model_from_mlflow()

            # Cache the model in memory
            self.model = model
            self.model_loaded = True

            logger.info("🎯 Model ready for inference")
            return model

        except Exception as e:
            logger.error(f"❌ CRITICAL: Failed to load model: {e}")
            raise RuntimeError(f"Failed to load model: {e}")

    def get_model_info(self) -> dict:
        """Get information about the loaded model."""
        return {
            "model_loaded": self.model_loaded,
            "model_version": self.model_version or "unknown",
            "loading_strategy": "in_memory_only",
            "mlflow_uri": config.MLFLOW_TRACKING_URI,
            "model_name": config.MLFLOW_MODEL_NAME,
            "model_stage": config.MLFLOW_MODEL_STAGE,
        }


# Global model loader instance
model_loader = ModelLoader()
