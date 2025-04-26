import os
import re
from collections import defaultdict
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from datetime import datetime
import logging

# -------------------------
# Configuração do Logging
# -------------------------
def setup_logger():
    logger = logging.getLogger("minio_upload")
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt='{"level": "%(levelname)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

logger = setup_logger()

# Carrega variáveis de ambiente do .env
load_dotenv()
s3_endpoint = os.getenv("S3_ENDPOINT")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

logger.info(f"🔐 Endpoint: {s3_endpoint} | Access Key: {s3_access_key[:4]}***")

# Conexão com MinIO via boto3
s3 = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Lista arquivos no bucket "ingestion"
bucket_name = "ingestion"
response = s3.list_objects_v2(Bucket=bucket_name)
csv_files = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

# Validação: existem arquivos CSV?
if not csv_files:
    logger.info("Nenhum arquivo .csv encontrado no bucket 'ingestion'. Abortando script.", level="WARNING")
    exit(0)


logger.info(f"{len(csv_files)} arquivos .csv encontrados no bucket.")

# Agrupa arquivos por prefixo
prefix_groups = defaultdict(list)
for file in csv_files:
    match = re.match(r"(.+?)_\d+\.csv", file)
    prefix = match.group(1) if match else file.replace(".csv", "")
    prefix_groups[prefix].append(file)

# Cria sessão Spark com suporte a Iceberg + MinIO (S3A)
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

# Processa os arquivos agrupados por prefixo
for prefix, files in prefix_groups.items():
    logger.info(f"🔧 Processando prefixo: {prefix}")

    file_paths = [f"s3a://{bucket_name}/{file}" for file in files]
    df = spark.read.option("header", "true").csv(file_paths)

    # Adiciona a coluna created_at com a data/hora atual
    from pyspark.sql.functions import current_timestamp
    df = df.withColumn("created_at", current_timestamp())

    # Exibe o schema inferido
    df.printSchema()

    # Prepara os campos para criação da tabela Iceberg
    cols = ", ".join([f"{field.name} STRING" for field in df.schema.fields])

    # Cria tabela Iceberg se não existir
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.bronze.{prefix} (
            {cols}
        )
        USING iceberg
    """)

    # Inserção com append
    #df.writeTo(f"local.bronze.{prefix}").append()
    #Mais performatico para grandes volumes
    df.writeTo(f"local.bronze.{prefix}").overwritePartitions()
    logger.info(f"✅ Tabela 'local.bronze.{prefix}' criada/populada com sucesso!")

    # Remove arquivos processados do bucket
    for file in files:
        s3.delete_object(Bucket=bucket_name, Key=file)
        logger.info(f"🗑️ Arquivo deletado do bucket: {file}")

logger.info("🚀 Todas as tabelas foram processadas e os arquivos .csv foram excluídos com sucesso.")
