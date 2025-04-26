# /opt/airflow/jobs/load_bronze.py
from dotenv import load_dotenv
import os
import re
from collections import defaultdict
import boto3
from botocore.config import Config
from pyspark.sql import SparkSession

def main():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")

    print(f"🔐 Endpoint: {s3_endpoint} | Access Key: {s3_access_key}")

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    bucket_name = "ingestion"
    response = s3.list_objects_v2(Bucket=bucket_name)
    csv_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

    prefix_groups = defaultdict(list)
    for file in csv_files:
        match = re.match(r"(.+?)_\d+\.csv", file)
        prefix = match.group(1) if match else file.replace(".csv", "")
        prefix_groups[prefix].append(file)

    spark = SparkSession.builder \
        .appName("IcebergMinIOIngestion") \
        .config("spark.jars", "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "s3a://datalake/iceberg") \
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", s3_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    for prefix, files in prefix_groups.items():
        print(f"\n🔧 Processando prefixo: {prefix}")
        file_paths = [f"s3a://{bucket_name}/{file}" for file in files]
        df = spark.read.option("header", "true").csv(file_paths)
        df.printSchema()

        cols = ", ".join([f"{field.name} STRING" for field in df.schema.fields])

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.bronze.{prefix} (
                {cols}
            )
            USING iceberg
        """)

        df.writeTo(f"local.bronze.{prefix}").append()
        print(f"✅ Tabela 'local.bronze.{prefix}' criada/populada com sucesso!")

    print("\n🚀 Todas as tabelas foram processadas com sucesso.")

if __name__ == "__main__":
    main()
