"""
Тесты для модуля конфигурации
"""


class TestConfig:
    """Тесты для класса Config"""

    def test_config_has_required_attributes(self):
        """Тест наличия обязательных атрибутов конфигурации"""
        # Импортируем модуль заново для тестирования
        from src.config import Config

        config = Config()

        # Проверяем наличие основных атрибутов
        assert hasattr(config, "MLFLOW_TRACKING_URI")
        assert hasattr(config, "API_HOST")
        assert hasattr(config, "API_PORT")
        assert hasattr(config, "ENVIRONMENT")
        assert hasattr(config, "DEBUG")
        assert hasattr(config, "LOG_LEVEL")

    def test_config_default_values(self):
        """Тест значений по умолчанию"""
        from src.config import Config

        config = Config()

        # Проверяем типы данных
        assert isinstance(config.API_HOST, str)
        assert isinstance(config.API_PORT, int)
        assert isinstance(config.DEBUG, bool)
        assert isinstance(config.LOG_LEVEL, str)

        # Проверяем разумные значения
        assert config.API_PORT > 0
        assert config.API_PORT < 65536
        assert config.LOG_LEVEL in ["DEBUG", "INFO", "WARNING", "ERROR"]

    def test_config_validation_method_exists(self):
        """Тест существования метода валидации"""
        from src.config import Config

        # Проверяем, что метод существует
        assert hasattr(Config, "validate_config")
        assert callable(getattr(Config, "validate_config"))

        # Проверяем, что метод можно вызвать
        try:
            Config.validate_config()
        except Exception as e:
            # Если возникает исключение, проверяем, что это ValueError
            assert isinstance(e, ValueError)

    def test_config_environment_methods(self):
        """Тест методов проверки окружения"""
        from src.config import Config

        # Проверяем существование методов
        assert hasattr(Config, "is_development")
        assert hasattr(Config, "is_production")
        assert callable(getattr(Config, "is_development"))
        assert callable(getattr(Config, "is_production"))

        config = Config()

        # Проверяем, что методы возвращают булевы значения
        assert isinstance(config.is_development(), bool)
        assert isinstance(config.is_production(), bool)

        # Проверяем взаимоисключающность
        if config.is_development():
            assert not config.is_production()
        elif config.is_production():
            assert not config.is_development()

    def test_config_mlflow_attributes(self):
        """Тест MLflow атрибутов"""
        from src.config import Config

        config = Config()

        # Проверяем наличие MLflow атрибутов
        assert hasattr(config, "MLFLOW_TRACKING_URI")
        assert hasattr(config, "MLFLOW_MODEL_NAME")
        assert hasattr(config, "MLFLOW_MODEL_STAGE")
        assert hasattr(config, "MLFLOW_MODEL_ARTIFACT")

        # Проверяем, что URI является строкой и содержит протокол
        assert isinstance(config.MLFLOW_TRACKING_URI, str)
        assert config.MLFLOW_TRACKING_URI.startswith(("http://", "https://"))

    def test_config_api_attributes(self):
        """Тест API атрибутов"""
        from src.config import Config

        config = Config()

        # Проверяем API атрибуты
        assert hasattr(config, "API_TITLE")
        assert hasattr(config, "API_VERSION")
        assert hasattr(config, "API_DESCRIPTION")

        # Проверяем типы
        assert isinstance(config.API_TITLE, str)
        assert isinstance(config.API_VERSION, str)
        assert isinstance(config.API_DESCRIPTION, str)

        # Проверяем, что версия имеет правильный формат
        version_parts = config.API_VERSION.split(".")
        assert len(version_parts) >= 2  # Минимум major.minor

    def test_config_s3_attributes(self):
        """Тест S3/MinIO атрибутов"""
        from src.config import Config

        config = Config()

        # Проверяем наличие S3 атрибутов
        assert hasattr(config, "S3_BUCKET_NAME")
        assert hasattr(config, "AWS_ACCESS_KEY_ID")
        assert hasattr(config, "AWS_SECRET_ACCESS_KEY")
        assert hasattr(config, "AWS_ENDPOINT_URL")

        # Проверяем, что bucket name является строкой
        assert isinstance(config.S3_BUCKET_NAME, str)
        assert len(config.S3_BUCKET_NAME) > 0
