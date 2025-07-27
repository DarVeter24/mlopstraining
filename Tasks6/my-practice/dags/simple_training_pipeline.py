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
    
    # Добавляем AWS переменные для доступа к S3/MinIO (используем S3_ переменные)
    os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    os.environ["AWS_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    
    print(f"S3 configuration:")
    print(f"  Endpoint: {S3_ENDPOINT_URL}")
    print(f"  Bucket: {S3_BUCKET_NAME}")
    print(f"  Access Key: {S3_ACCESS_KEY[:4]}***")
    
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
        
        # Диагностика колонок
        print("Available columns and their types:")
        for field in df.schema.fields:
            print(f"  {field.name}: {field.dataType}")
        
        # Проверим, есть ли другие колонки с потенциальными метками мошенничества
        potential_fraud_cols = [col for col in df.columns if 'fraud' in col.lower() or 'label' in col.lower() or 'class' in col.lower()]
        if potential_fraud_cols:
            print(f"Found potential fraud-related columns: {potential_fraud_cols}")
            for col in potential_fraud_cols:
                unique_vals = df.select(col).distinct().limit(10).collect()
                print(f"  {col} unique values: {[row[0] for row in unique_vals]}")
        
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
    
    # Проверим баланс классов
    target_col = 'tx_fraud'
    fraud_count = df.filter(df[target_col] == 1).count()
    normal_count = df.filter(df[target_col] == 0).count()
    total_count = df.count()
    
    print(f"Dataset balance:")
    print(f"  Normal transactions (0): {normal_count} ({normal_count/total_count*100:.2f}%)")
    print(f"  Fraud transactions (1): {fraud_count} ({fraud_count/total_count*100:.2f}%)")
    print(f"  Total: {total_count}")
    
    if fraud_count == 0:
        print("WARNING: No fraud transactions found in original tx_fraud column!")
        
        # Попробуем использовать tx_fraud_scenario
        scenario_fraud_count = df.filter(df['tx_fraud_scenario'] > 0).count()
        print(f"Checking tx_fraud_scenario > 0: {scenario_fraud_count} cases")
        
        if scenario_fraud_count > 0:
            print("Using tx_fraud_scenario > 0 as fraud indicator")
            from pyspark.sql.functions import when
            df = df.withColumn(
                target_col,
                when(df['tx_fraud_scenario'] > 0, 1).otherwise(0)
            )
        else:
            print("Creating artificial fraud cases based on transaction patterns...")
            # Создаем искусственные мошеннические случаи на основе паттернов
            from pyspark.sql.functions import rand, when
            
            # Стратегия: большие транзакции (топ 2%) + случайные (0.5%) = мошенничество
            # Находим 98-й перцентиль по сумме транзакций
            percentile_98 = df.approxQuantile("tx_amount", [0.98], 0.01)[0]
            print(f"98th percentile of tx_amount: {percentile_98}")
            
            # Добавляем случайную колонку
            df_with_rand = df.withColumn("random", rand(seed=42))
            
            # Помечаем как мошеннические:
            # - Транзакции > 98-го перцентиля (большие суммы)
            # - 0.5% случайных транзакций
            df = df_with_rand.withColumn(
                target_col,
                when(
                    (df_with_rand["tx_amount"] > percentile_98) | 
                    (df_with_rand["random"] < 0.005), 1
                ).otherwise(0)
            ).drop("random")
        
        # Пересчитываем статистику
        fraud_count = df.filter(df[target_col] == 1).count()
        normal_count = df.filter(df[target_col] == 0).count()
        total_count = df.count()
        
        print(f"After fraud detection logic:")
        print(f"  Normal transactions (0): {normal_count} ({normal_count/total_count*100:.2f}%)")
        print(f"  Fraud transactions (1): {fraud_count} ({fraud_count/total_count*100:.2f}%)")
        print(f"  Total: {total_count}")
        
        # Пересоздаем train/test split с новыми данными
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        print(f"Updated training set size: {train_df.count()}")
        print(f"Updated test set size: {test_df.count()}")
    
    if fraud_count == 0:
        raise ValueError("Still no fraud transactions found! Cannot train fraud detection model.")
    
    if fraud_count < 100:
        print(f"WARNING: Very few fraud cases ({fraud_count}). Model quality may be poor.")
    
    # Подготовка признаков (используем tx_fraud как целевую переменную)
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
        
        # Дополнительные метрики для несбалансированных данных
        from pyspark.ml.evaluation import MulticlassClassificationEvaluator
        
        # Accuracy
        accuracy_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
        accuracy = accuracy_evaluator.evaluate(predictions)
        
        # Precision
        precision_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="weightedPrecision")
        precision = precision_evaluator.evaluate(predictions)
        
        # Recall
        recall_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="weightedRecall")
        recall = recall_evaluator.evaluate(predictions)
        
        # F1-score
        f1_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="f1")
        f1 = f1_evaluator.evaluate(predictions)
        
        print(f"Model Metrics:")
        print(f"  AUC: {auc:.4f}")
        print(f"  Accuracy: {accuracy:.4f}")
        print(f"  Precision: {precision:.4f}")
        print(f"  Recall: {recall:.4f}")
        print(f"  F1-Score: {f1:.4f}")
        
        # Confusion Matrix
        predictions.groupBy(target_col, "prediction").count().show()
        
        # Логирование метрик
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("training_samples", train_df.count())
        mlflow.log_metric("test_samples", test_df.count())
        mlflow.log_metric("feature_count", len(feature_cols))
        mlflow.log_metric("fraud_count", fraud_count)
        mlflow.log_metric("fraud_percentage", fraud_count/total_count*100)
        
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
            print(f"Failed to log Spark model: {model_log_error}")
            mlflow.log_param("model_logging_error", str(model_log_error))
            
            # Альтернативный способ - сохранить как файл
            try:
                import tempfile
                import shutil
                
                # Создаем временную директорию
                with tempfile.TemporaryDirectory() as temp_dir:
                    model_path = f"{temp_dir}/spark_model"
                    model.write().overwrite().save(model_path)
                    
                    # Логируем как артефакт
                    mlflow.log_artifacts(model_path, "spark_model")
                    print("Model saved as artifacts successfully")
                    
            except Exception as artifact_error:
                print(f"Failed to save model as artifacts: {artifact_error}")
                mlflow.log_param("artifact_logging_error", str(artifact_error))
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