"""
dag: ab_testing_fraud_detection
description: a/b тестирование модели мошенничества с валидацией стратегий
версия: 1.0 (адаптировано из simple_training_pipeline.py)
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
try:
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable
from scipy.stats import ttest_ind

# Настройки DAG
default_args = {
    'owner': 'mlops',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def bootstrap_metrics_spark(y_test, y_pred, n_iterations=100):
    """
    bootstrap анализ метрик для pyspark результатов
    
    parameters
    ----------
    y_test : array-like
        истинные метки
    y_pred : array-like  
        предсказания модели
    n_iterations : int, default=100
        количество итераций bootstrap
        
    returns
    -------
    scores : pandas.dataframe
        dataframe с метриками для каждой итерации
    """
    from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
    
    np.random.seed(42)
    
    # конвертируем в numpy arrays если нужно
    y_test = np.array(y_test) if not isinstance(y_test, np.ndarray) else y_test
    y_pred = np.array(y_pred) if not isinstance(y_pred, np.ndarray) else y_pred
    
    df_bootstrap = pd.DataFrame({
        "y_test": y_test,
        "y_pred": y_pred,
    })
    
    scores = []
    
    print(f"выполняется bootstrap анализ ({n_iterations} итераций)...")
    for i in range(n_iterations):
        sample = df_bootstrap.sample(frac=1.0, replace=True)
        
        try:
            metrics = {
                "F1": f1_score(sample["y_test"], sample["y_pred"]),
                "Precision": precision_score(sample["y_test"], sample["y_pred"]),
                "Recall": recall_score(sample["y_test"], sample["y_pred"]),
                "AUC": roc_auc_score(sample["y_test"], sample["y_pred"])
            }
            scores.append(metrics)
        except Exception as e:
            print(f"Ошибка в итерации {i}: {e}")
            continue
    
    print(f"Bootstrap завершен. Получено {len(scores)} валидных итераций")
    return pd.DataFrame(scores)


def statistical_comparison_fraud(scores_base, scores_candidate, alpha=0.01):
    """
    статистическое сравнение двух моделей fraud detection
    
    parameters
    ----------
    scores_base : pandas.dataframe
        метрики базовой модели
    scores_candidate : pandas.dataframe
        метрики модели-кандидата
    alpha : float, default=0.01
        уровень значимости
        
    returns
    -------
    results : dict
        словарь с результатами статистического сравнения
    """
    results = {}
    
    # используем F1 и Precision как основные метрики
    for metric in ['F1', 'Precision']:
        if metric not in scores_base.columns or metric not in scores_candidate.columns:
            print(f"метрика {metric} не найдена, пропускаем")
            continue
            
        t_stat, pvalue = ttest_ind(scores_base[metric], scores_candidate[metric])
        
        # размер эффекта (cohen's d) с защитой от деления на ноль
        pooled_std = np.sqrt((scores_base[metric].var() + scores_candidate[metric].var()) / 2)
        improvement = scores_candidate[metric].mean() - scores_base[metric].mean()
        
        if pooled_std == 0 or np.isnan(pooled_std):
            effect_size = 0.0  # Нет эффекта если нет вариации
        else:
            effect_size = abs(improvement) / pooled_std
        
        # Защита от NaN в статистических тестах
        if np.isnan(t_stat):
            t_stat = 0.0
        if np.isnan(pvalue):
            pvalue = 1.0  # Консервативно: нет значимости
            
        is_significant = pvalue < alpha
        
        results[metric] = {
            't_statistic': t_stat,
            'p_value': pvalue,
            'effect_size': effect_size,
            'is_significant': is_significant,
            'base_mean': scores_base[metric].mean(),
            'candidate_mean': scores_candidate[metric].mean(),
            'improvement': improvement
        }
        
        print(f"\n{metric} статистика:")
        print(f"  базовая модель: {results[metric]['base_mean']:.4f}")
        print(f"  модель-кандидат: {results[metric]['candidate_mean']:.4f}")
        print(f"  улучшение: {improvement:+.4f}")
        print(f"  p-value: {pvalue:.6f}")
        print(f"  значимо (α={alpha}): {is_significant}")
    
    return results


def train_production_model():
    """
    обучение базовой (production) модели fraud detection
    """
    import mlflow
    import mlflow.spark
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    
    print("=== обучение базовой (production) модели ===")
    
    # получаем переменные из airflow
    S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
    S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
    CLEAN_DATA_PATH = Variable.get("CLEAN_DATA_PATH")
    MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
    MLFLOW_EXPERIMENT_NAME = Variable.get("MLFLOW_EXPERIMENT_NAME")
    MLFLOW_TRACKING_USERNAME = Variable.get("MLFLOW_TRACKING_USERNAME")
    MLFLOW_TRACKING_PASSWORD = Variable.get("MLFLOW_TRACKING_PASSWORD")
    
    # настройка окружения
    os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD
    os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    os.environ["AWS_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    
    # настройка mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    # создание spark сессии
    spark = SparkSession.builder \
        .appName("FraudDetection_Production") \
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
    
    try:
        # Загрузка данных
        print(f"Загрузка данных из: {CLEAN_DATA_PATH}")
        df = spark.read.parquet(CLEAN_DATA_PATH)
        print(f"Загружено записей: {df.count()}")
        
        # обработка целевой переменной (логика из simple_training_pipeline.py)
        target_col = 'tx_fraud'
        
        # проверяем наличие мошеннических транзакций
        fraud_count = df.filter(df[target_col] == 1).count()
        if fraud_count == 0:
            print("создание искусственных мошеннических случаев...")
            from pyspark.sql.functions import when, rand
            
            percentile_98 = df.approxQuantile("tx_amount", [0.98], 0.01)[0]
            df_with_rand = df.withColumn("random", rand(seed=42))
            
            df = df_with_rand.withColumn(
                target_col,
                when(
                    (df_with_rand["tx_amount"] > percentile_98) | 
                    (df_with_rand["random"] < 0.005), 1
                ).otherwise(0)
            ).drop("random")
            
            fraud_count = df.filter(df[target_col] == 1).count()
            print(f"создано мошеннических случаев: {fraud_count}")
        
        # разделение данных
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # подготовка признаков
        feature_cols = [col for col in train_df.columns 
                       if col != target_col and 
                       col != 'transaction_id' and
                       col != 'tx_datetime' and
                       train_df.schema[col].dataType.typeName() != 'string']
        
        print(f"признаки для обучения: {len(feature_cols)}")
        
        # создание пайплайна (базовые параметры)
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
        classifier = RandomForestClassifier(
            labelCol=target_col, 
            featuresCol="features",
            numTrees=10,  # Базовые параметры
            maxDepth=5,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, classifier])
        
        # обучение
        with mlflow.start_run(run_name=f"production_model_{datetime.now().strftime('%y%m%d_%H%M')}"):
            print("обучение базовой модели...")
            model = pipeline.fit(train_df)
            
            # предсказания
            predictions = model.transform(test_df)
            
            # оценка модели
            evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="rawPrediction")
            auc = evaluator.evaluate(predictions)
            
            accuracy_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
            accuracy = accuracy_evaluator.evaluate(predictions)
            
            f1_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="f1")
            f1 = f1_evaluator.evaluate(predictions)
            
            print(f"базовая модель - auc: {auc:.4f}, f1: {f1:.4f}, accuracy: {accuracy:.4f}")
            
            # логирование метрик
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_param("model_type", "production_baseline")
            mlflow.log_param("num_trees", 10)
            mlflow.log_param("max_depth", 5)
            
            # сохранение модели
            try:
                mlflow.spark.log_model(model, "fraud_model_production")
                print("базовая модель сохранена в mlflow")
            except Exception as e:
                print(f"ошибка сохранения модели: {e}")
            
            # сохраняем предсказания для а/в теста
            y_test_prod = [row[target_col] for row in predictions.select(target_col).collect()]
            y_pred_prod = [row["prediction"] for row in predictions.select("prediction").collect()]
            
            # сохраняем в контексте airflow (через xcom)
            return {
                "model_metrics": {"auc": auc, "f1_score": f1, "accuracy": accuracy},
                "y_test": y_test_prod,
                "y_pred": y_pred_prod,
                "model_type": "production"
            }
    
    finally:
        spark.stop()


def train_candidate_model():
    """
    обучение модели-кандидата с оптимизированными гиперпараметрами
    """
    import mlflow
    import mlflow.spark
    from pyspark.sql import SparkSession
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.classification import RandomForestClassifier
    from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
    
    print("=== обучение модели-кандидата ===")
    
    # получаем переменные из airflow
    S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
    S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
    CLEAN_DATA_PATH = Variable.get("CLEAN_DATA_PATH")
    MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
    MLFLOW_EXPERIMENT_NAME = Variable.get("MLFLOW_EXPERIMENT_NAME")
    MLFLOW_TRACKING_USERNAME = Variable.get("MLFLOW_TRACKING_USERNAME")
    MLFLOW_TRACKING_PASSWORD = Variable.get("MLFLOW_TRACKING_PASSWORD")
    
    # настройка окружения
    os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD
    os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    os.environ["AWS_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    
    # настройка mlflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    # создание spark сессии
    spark = SparkSession.builder \
        .appName("FraudDetection_Candidate") \
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
    
    try:
        # загрузка данных (та же логика)
        df = spark.read.parquet(CLEAN_DATA_PATH)
        
        # обработка целевой переменной
        target_col = 'tx_fraud'
        fraud_count = df.filter(df[target_col] == 1).count()
        if fraud_count == 0:
            from pyspark.sql.functions import when, rand
            percentile_98 = df.approxQuantile("tx_amount", [0.98], 0.01)[0]
            df_with_rand = df.withColumn("random", rand(seed=42))
            df = df_with_rand.withColumn(
                target_col,
                when(
                    (df_with_rand["tx_amount"] > percentile_98) | 
                    (df_with_rand["random"] < 0.005), 1
                ).otherwise(0)
            ).drop("random")
        
        # разделение данных (тот же seed для честного сравнения)
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # подготовка признаков
        feature_cols = [col for col in train_df.columns 
                       if col != target_col and 
                       col != 'transaction_id' and
                       col != 'tx_datetime' and
                       train_df.schema[col].dataType.typeName() != 'string']
        
        # создание пайплайна с улучшенными параметрами
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", withStd=True, withMean=True)
        classifier = RandomForestClassifier(
            labelCol=target_col, 
            featuresCol="features",
            numTrees=20,  # Больше деревьев
            maxDepth=8,   # Больше глубина
            minInstancesPerNode=2,  # Дополнительная регуляризация
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, classifier])
        
        # обучение
        with mlflow.start_run(run_name=f"candidate_model_{datetime.now().strftime('%y%m%d_%H%M')}"):
            print("обучение модели-кандидата...")
            model = pipeline.fit(train_df)
            
            # предсказания
            predictions = model.transform(test_df)
            
            # оценка модели
            evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="rawPrediction")
            auc = evaluator.evaluate(predictions)
            
            accuracy_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="accuracy")
            accuracy = accuracy_evaluator.evaluate(predictions)
            
            f1_evaluator = MulticlassClassificationEvaluator(labelCol=target_col, predictionCol="prediction", metricName="f1")
            f1 = f1_evaluator.evaluate(predictions)
            
            print(f"модель-кандидат - auc: {auc:.4f}, f1: {f1:.4f}, accuracy: {accuracy:.4f}")
            
            # логирование метрик
            mlflow.log_metric("auc", auc)
            mlflow.log_metric("f1_score", f1)
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_param("model_type", "candidate_optimized")
            mlflow.log_param("num_trees", 20)
            mlflow.log_param("max_depth", 8)
            
            # сохранение модели
            try:
                mlflow.spark.log_model(model, "fraud_model_candidate")
                print("модель-кандидат сохранена в mlflow")
            except Exception as e:
                print(f"ошибка сохранения модели: {e}")
            
            # сохраняем предсказания для а/в теста
            y_test_cand = [row[target_col] for row in predictions.select(target_col).collect()]
            y_pred_cand = [row["prediction"] for row in predictions.select("prediction").collect()]
            
            return {
                "model_metrics": {"auc": auc, "f1_score": f1, "accuracy": accuracy},
                "y_test": y_test_cand,
                "y_pred": y_pred_cand,
                "model_type": "candidate"
            }
    
    finally:
        spark.stop()


def ab_test_fraud_models(**context):
    """
    a/b тестирование моделей fraud detection
    """
    print("=== a/b тестирование моделей fraud detection ===")
    
    # получаем результаты предыдущих тасков
    ti = context['ti']
    production_results = ti.xcom_pull(task_ids='train_production_model')
    candidate_results = ti.xcom_pull(task_ids='train_candidate_model')
    
    if not production_results or not candidate_results:
        raise ValueError("Не удалось получить результаты обучения моделей")
    
    print("результаты базовой модели:", production_results['model_metrics'])
    print("результаты модели-кандидата:", candidate_results['model_metrics'])
    
    # получаем предсказания
    y_test_prod = production_results['y_test']
    y_pred_prod = production_results['y_pred']
    y_test_cand = candidate_results['y_test']
    y_pred_cand = candidate_results['y_pred']
    
    # проверяем, что тестовые данные одинаковые
    if len(y_test_prod) != len(y_test_cand):
        print("внимание: разные размеры тестовых выборок!")
    
    # bootstrap анализ
    print("\nвыполнение bootstrap анализа...")
    prod_bootstrap = bootstrap_metrics_spark(y_test_prod, y_pred_prod, n_iterations=100)
    cand_bootstrap = bootstrap_metrics_spark(y_test_cand, y_pred_cand, n_iterations=100)
    
    print("bootstrap базовой модели:")
    print(prod_bootstrap.describe())
    
    print("\nbootstrap модели-кандидата:")
    print(cand_bootstrap.describe())
    
    # статистическое сравнение
    print("\n=== статистическое сравнение ===")
    comparison_results = statistical_comparison_fraud(prod_bootstrap, cand_bootstrap, alpha=0.01)
    
    # принятие решения по 2 критериям
    f1_results = comparison_results.get('F1', {})
    f1_significant = f1_results.get('is_significant', False)
    f1_improvement = f1_results.get('improvement', 0) > 0
    
    # ✅ 2 критерия: статистическая значимость + положительное улучшение
    should_deploy = f1_significant and f1_improvement
    
    print(f"\n{'='*60}")
    print("итоговое решение по a/b тесту:")
    print(f"{'='*60}")
    print(f"✅ критерий 1 - статистическая значимость (α=0.01): {f1_significant}")
    print(f"✅ критерий 2 - положительное улучшение f1: {f1_improvement}")
    print(f"")
    if should_deploy:
        print("🚀 решение: развернуть новую модель в production")
        print("   модель-кандидат показала статистически значимое улучшение")
    else:
        print("⛔ решение: оставить текущую модель в production")
        if not f1_improvement:
            print("   причина: модель-кандидат не показала улучшения f1")
        elif not f1_significant:
            print("   причина: улучшение статистически незначимо")
    print(f"{'='*60}")
    
    # логируем результаты в mlflow
    import mlflow
    
    # Получаем переменные для MLflow аутентификации
    MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
    MLFLOW_EXPERIMENT_NAME = Variable.get("MLFLOW_EXPERIMENT_NAME")
    MLFLOW_TRACKING_USERNAME = Variable.get("MLFLOW_TRACKING_USERNAME")  
    MLFLOW_TRACKING_PASSWORD = Variable.get("MLFLOW_TRACKING_PASSWORD")
    S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
    S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
    
    # ВАЖНО: Настройка переменных окружения ДО вызовов MLflow
    os.environ["MLFLOW_TRACKING_USERNAME"] = MLFLOW_TRACKING_USERNAME
    os.environ["MLFLOW_TRACKING_PASSWORD"] = MLFLOW_TRACKING_PASSWORD
    os.environ["AWS_ACCESS_KEY_ID"] = S3_ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = S3_SECRET_KEY
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    os.environ["AWS_S3_ENDPOINT_URL"] = S3_ENDPOINT_URL
    
    # Теперь безопасно настраиваем MLflow с обработкой ошибок
    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
        
        with mlflow.start_run(run_name=f"ab_test_{datetime.now().strftime('%y%m%d_%H%M')}"):
            # логируем результаты сравнения (с проверкой на NaN)
            for metric, results in comparison_results.items():
                # Функция для безопасного логирования (заменяет NaN на 0.0)
                def safe_log_metric(key, value):
                    if np.isnan(value) or np.isinf(value):
                        mlflow.log_metric(key, 0.0)
                        print(f"Предупреждение: {key} содержит NaN/Inf, заменено на 0.0")
                    else:
                        mlflow.log_metric(key, float(value))
                
                safe_log_metric(f"{metric.lower()}_base_mean", results['base_mean'])
                safe_log_metric(f"{metric.lower()}_candidate_mean", results['candidate_mean'])
                safe_log_metric(f"{metric.lower()}_improvement", results['improvement'])
                safe_log_metric(f"{metric.lower()}_p_value", results['p_value'])
                safe_log_metric(f"{metric.lower()}_effect_size", results['effect_size'])
            
            # логируем решение
            mlflow.log_param("f1_significant", f1_significant)
            mlflow.log_param("f1_improvement_positive", f1_improvement)
            mlflow.log_param("should_deploy", should_deploy)
            mlflow.log_param("decision_criteria", "statistical_significance + positive_improvement")
            mlflow.log_param("alpha", 0.01)
            
        print("результаты a/b теста успешно сохранены в mlflow")
    
    except Exception as mlflow_error:
        print(f"предупреждение: не удалось сохранить в mlflow: {mlflow_error}")
        print("a/b тест завершен успешно, но логирование в mlflow пропущено")
    
    # Упрощаем возвращаемые данные для XCom (убираем большие объекты)
    simplified_results = {
        "should_deploy": should_deploy,
        "f1_significant": f1_significant,
        "f1_improvement": f1_improvement,
        "f1_base_mean": f1_results.get('base_mean', 0),
        "f1_candidate_mean": f1_results.get('candidate_mean', 0),
        "f1_p_value": f1_results.get('p_value', 1.0),
        "production_f1": production_results['model_metrics']['f1_score'],
        "candidate_f1": candidate_results['model_metrics']['f1_score'],
        "production_auc": production_results['model_metrics']['auc'],
        "candidate_auc": candidate_results['model_metrics']['auc'],
        "decision_reason": "2_criteria_validation"
    }
    
    return simplified_results


# dag определение
with DAG(
    dag_id="ab_testing_fraud_detection",
    default_args=default_args,
    description="a/b тестирование модели мошенничества с валидацией стратегий",
    schedule=timedelta(hours=6),  # запуск каждые 6 часов
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mlops', 'fraud-detection', 'ab-testing', 'validation'],
) as dag:
    
    # task 1: обучение базовой (production) модели
    train_production_task = PythonOperator(
        task_id="train_production_model",
        python_callable=train_production_model,
        dag=dag,
    )
    
    # Task 2: Обучение модели-кандидата
    train_candidate_task = PythonOperator(
        task_id="train_candidate_model",
        python_callable=train_candidate_model,
        dag=dag,
    )
    
    # Task 3: A/B тестирование моделей
    ab_test_task = PythonOperator(
        task_id="ab_test_models",
        python_callable=ab_test_fraud_models,
        dag=dag,
    )
    
    # определение последовательности выполнения задач
    [train_production_task, train_candidate_task] >> ab_test_task