# ───────────────────────────────────────────────
# 0. Стартуем Spark с доступом к MinIO
# ───────────────────────────────────────────────
from argparse import ArgumentParser
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

def main():
    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Bucket name is not provided")

    spark = (SparkSession
        .builder
        .appName("sum-txt-files")
        .enableHiveSupport()
        .getOrCreate()
    )

    # ───────────────────────────────────────────────
    # 1. Читаем ВСЕ .txt  (sep=',', header=False) + явная схема
    # ───────────────────────────────────────────────
    input_path = f"s3a://{bucket_name}/input_data/*.txt"

    schema = StructType([
        StructField("transaction_id",    IntegerType(), True),
        StructField("tx_datetime",       StringType(),  True),
        StructField("customer_id",       IntegerType(), True),
        StructField("terminal_id",       IntegerType(), True),
        StructField("tx_amount",         DoubleType(),  True),
        StructField("tx_time_seconds",   IntegerType(), True),
        StructField("tx_time_days",      IntegerType(), True),
        StructField("tx_fraud",          IntegerType(), True),
        StructField("tx_fraud_scenario", IntegerType(), True),
    ])

    raw_df = (spark.read
               .csv(input_path,
                    header=False,       # шапка «плохая» – игнорируем
                    sep=",",
                    schema=schema,
                    mode="DROPMALFORMED")  # пропускаем совсем битые строки
    )

    # Убираем первую строку‑шапку (transaction_id == null)
    df = raw_df.filter(raw_df.transaction_id.isNotNull())

    print("Схема после чтения:")
    df.printSchema()
    df.show(5)

    # ───────────────────────────────────────────────
    # 2. Drop NA Удаляем строки с пропущенными значениями
    # ───────────────────────────────────────────────
    rows_before = df.count()
    df_clean    = df.dropna()
    print(f"DropNA: {rows_before} → {df_clean.count()}")

    # ───────────────────────────────────────────────
    # 3. Удаляем выбросы (3 σ) по всем числовым колонкам
    # ───────────────────────────────────────────────
    from pyspark.sql.types import NumericType
    from functools import reduce

    numeric_cols = [f.name for f in df_clean.schema.fields
                    if isinstance(f.dataType, NumericType)]

    stats_row = (df_clean
                 .select(*[F.mean(c).alias(f"{c}_mean") for c in numeric_cols],
                         *[F.stddev(c).alias(f"{c}_std") for c in numeric_cols])
                 .collect()[0])

    conds = []
    for c in numeric_cols:
        mu = stats_row[f"{c}_mean"]; sigma = stats_row[f"{c}_std"]
        if sigma:  # если σ не None
            low, hi = mu - 3*sigma, mu + 3*sigma
            conds.append((F.col(c) >= low) & (F.col(c) <= hi))

    df_no_outliers = df_clean.filter(reduce(lambda a, b: a & b, conds)
                                     if conds else F.lit(True))
    print(f"После выбросов: {df_no_outliers.count()} строк")

    # ───────────────────────────────────────────────
    # 4. Логические фильтры
    # ───────────────────────────────────────────────
    df_valid = df_no_outliers
    if "tx_amount" in df_valid.columns:
        df_valid = df_valid.filter(F.col("tx_amount") >= 0)
    if "tx_time_seconds" in df_valid.columns:
        df_valid = df_valid.filter(
            (F.col("tx_time_seconds") >= 0) & (F.col("tx_time_seconds") <= 86400)
        )
    if "customer_id" in df_valid.columns:
        df_valid = df_valid.filter(F.col("customer_id").isNotNull())
    if "terminal_id" in df_valid.columns:
        df_valid = df_valid.filter(F.col("terminal_id").isNotNull())

    print(f"Логические фильтры: {df_no_outliers.count()} → {df_valid.count()}")

    # ───────────────────────────────────────────────
    # 5. Сохраняем корректный Parquet
    # ───────────────────────────────────────────────
    output_path = f"s3a://{bucket_name}/output_data/sum_data.parquet"

    (df_valid
       .write
       .mode("overwrite")
       .parquet(output_path))

    print(f"✅ Новый Parquet сохранён: {output_path}")

if __name__ == "__main__":
    main()