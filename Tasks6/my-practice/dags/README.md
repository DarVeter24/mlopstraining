"""
DAG: simple_fraud_detection_training
Description: Простой DAG для обучения модели мошенничества с использованием Python оператора
Версия: 2.0 (обновлено с MLflow 2.15.1 и Airflow Variables)
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Все переменные теперь берутся из Airflow Variables (airflow_variables.json)
# Это обеспечивает централизованное управление конфигурацией

# Настройки DAG
default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def train_model_python():
    """
    Функция для обучения модели с использованием PySpark
    Обновлено: MLflow 2.15.1, Airflow Variables, обработка ошибок
    """
    import mlflow
    import mlflow.spark
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    
    print("Starting fraud detection model training...")
    print(f"MLflow version: {mlflow.__version__}")  # Проверка версии MLflow
    
    # Получаем все переменные из Airflow Variables
    S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
    S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
    S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
    CLEAN_DATA_PATH = Variable.get("CLEAN_DATA_PATH")
    MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
    MLFLOW_EXPERIMENT_NAME = Variable.get("MLFLOW_EXPERIMENT_NAME")
    MLFLOW_TRACKING_USERNAME = Variable.get("MLFLOW_TRACKING_USERNAME")
    MLFLOW_TRACKING_PASSWORD = Variable.get("MLFLOW_TRACKING_PASSWORD")
    
    # Настройка переменных окружения для MLflow аутентификации
    os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD
    
    # Настройка MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    # Создание Spark сессии с необходимыми JAR файлами и настройками для MinIO
    spark = SparkSession.builder \
        .appName("FraudDetectionModel") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT_URL) \
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "200000") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("Spark session created successfully")
    
    # Загрузка готовых очищенных данных в формате Parquet
    print(f"Loading cleaned data from: {CLEAN_DATA_PATH}")
    
    try:
        df = spark.read.parquet(CLEAN_DATA_PATH)
        print(f"Data loaded successfully. Total records: {df.count()}")
        print("Data schema:")
        df.printSchema()
        
        # Показать первые несколько строк
        print("First 5 rows:")
        df.show(5, truncate=False)
        
        # Проверим, что у нас есть целевая переменная
        if 'tx_fraud' not in df.columns:
            print("Warning: 'tx_fraud' column not found. Available columns:", df.columns)
            # Если нет tx_fraud, попробуем найти похожий столбец
            fraud_columns = [col for col in df.columns if 'fraud' in col.lower()]
            if fraud_columns:
                print(f"Found potential fraud columns: {fraud_columns}")
            else:
                raise ValueError("No fraud-related column found in the dataset")
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise
    
    # Разделение на обучающую и тестовую выборки
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set size: {train_df.count()}")
    print(f"Test set size: {test_df.count()}")
    
    # Подготовка признаков (используем tx_fraud как целевую переменную)
    target_col = 'tx_fraud'  # Целевая переменная из вашего dataset
    feature_cols = [col for col in train_df.columns 
                   if col != target_col and 
                   col != 'transaction_id' and  # Исключаем ID
                   col != 'tx_datetime' and     # Исключаем дату/время
                   train_df.schema[col].dataType.typeName() != 'string']
    print(f"Target column: {target_col}")
    print(f"Feature columns: {feature_cols}")
    
    # Создание пайплайна
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
    classifier = RandomForestClassifier(labelCol=target_col, featuresCol="features", numTrees=10, maxDepth=5)
    
    pipeline = Pipeline(stages=[assembler, scaler, classifier])
    
    # Обучение модели
    run_name = f"training_{datetime.now().strftime('%Y%m%d_%H%M')}"
    print(f"Starting MLflow run: {run_name}")
    
    with mlflow.start_run(run_name=run_name):
        print("Training model...")
        model = pipeline.fit(train_df)
        print("Model training completed")
        
        # Предсказания
        predictions = model.transform(test_df)
        print("Predictions generated")
        
        # Оценка модели
        evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="prediction")
        auc = evaluator.evaluate(predictions)
        print(f"Model AUC: {auc}")
        
        # Логирование метрик
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("training_samples", train_df.count())
        mlflow.log_metric("test_samples", test_df.count())
        mlflow.log_metric("feature_count", len(feature_cols))
        
        # Логирование параметров модели
        mlflow.log_param("num_trees", 10)
        mlflow.log_param("max_depth", 5)
        mlflow.log_param("feature_columns", str(feature_cols))
        mlflow.log_param("mlflow_version", mlflow.__version__)
        
        # Логирование модели с обработкой ошибок
        try:
            mlflow.spark.log_model(model, "fraud_detection_model")
            print("Model logged to MLflow successfully")
        except Exception as model_log_error:
            print(f"Failed to log model (continuing without model artifacts): {model_log_error}")
            mlflow.log_param("model_logging_error", str(model_log_error))
            # Продолжаем выполнение без падения
        
        print(f"Training completed successfully! AUC: {auc}")
    
    spark.stop()
    print("Spark session stopped")

# DAG определение
with DAG(
    dag_id="simple_fraud_detection_training",
    default_args=default_args,
    description="Простое обучение модели мошенничества с PySpark и MLflow v2.0",
    schedule=timedelta(hours=1),  # Запуск каждый час
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mlops', 'fraud-detection', 'v2.0'],
) as dag:
    
    # Простой Python оператор для обучения модели
    train_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model_python,
        dag=dag,
    )
    
    # Определение последовательности выполнения задач
    train_task
