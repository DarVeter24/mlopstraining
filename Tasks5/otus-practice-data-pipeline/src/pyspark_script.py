"""
Script: pyspark_script.py
Description: PySpark script for testing.
"""

from argparse import ArgumentParser
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


def sum_csv_files(source_path: str, output_path: str) -> None:
    """
    Reads CSV files from source bucket, calculates sum of 'field' column
    and saves result to output bucket as parquet file

    Parameters
    ----------
    source_path : str
        Path to the source bucket with CSV files
    output_path : str
        Path to the output bucket to save the result
    """

    spark = (SparkSession
        .builder
        .appName("sum-csv-files")
        .enableHiveSupport()
        .getOrCreate()
    )

    df = (spark
        .read
        .option('comment', '#')
        .schema("field INT")
        .format('csv')
        .load(source_path)
    )

    sum_value = df.agg(spark_sum('field').alias('sum')).collect()[0]['sum']

    result_df = spark.createDataFrame(
        [(sum_value,)],
        ["result"]
    )

    result_df.write.mode("overwrite").parquet(output_path)
    print("Successfully saved the result to the output bucket!")


def main():
    """Main function to execute the PySpark job"""
    parser = ArgumentParser()
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()
    bucket_name = args.bucket

    if not bucket_name:
        raise ValueError("Environment variable S3_BUCKET_NAME is not set")

    input_path = f"s3a://{bucket_name}/input_data/*.csv"
    output_path = f"s3a://{bucket_name}/output_data/sum_data.parquet"
    sum_csv_files(input_path, output_path)

if __name__ == "__main__":
    main()
