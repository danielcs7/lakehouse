import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import logging

# -------------------------
# Configuração do Logging
# -------------------------
def setup_logger():
    # Evita múltiplos handlers
    logger = logging.getLogger("minio_silver")
    if logger.handlers:  # Remove handlers existentes
        logger.handlers = []
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt='{"level": "%(levelname)s", "message": "%(message)s"}')
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

# Cria sessão Spark com suporte a Iceberg + MinIO (S3A)
spark = SparkSession.builder \
    .appName("IcebergMinIOSilver") \
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
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.sql.catalog.local.default-namespace", "default") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lista de tabelas bronze a processar (pode ser configurada dinamicamente ou via argumento)
bronze_tables = ["clientes"]  # Substitua por uma lista dinâmica se necessário

# Processa cada tabela bronze
for prefix in bronze_tables:
    logger.info(f"🔧 Processando prefixo para camada silver: {prefix}")

    # --------------------------------
    # Camada Silver
    # --------------------------------
    # Carrega os dados da camada bronze (apenas do dia atual para otimizar)
    try:
        silver_source_df = spark.table(f"local.bronze.{prefix}") \
            .filter(f"date(created_at) = '{datetime.now().strftime('%Y-%m-%d')}'")
    except Exception as e:
        logger.error(f"❌ Falha ao carregar tabela 'local.bronze.{prefix}': {str(e)}")
        continue

    # Validação: Remove duplicatas e nulos na chave primária (id)
    silver_source_df = silver_source_df.dropDuplicates(["id"]).filter("id IS NOT NULL")

    # Verifica se há dados a processar
    if silver_source_df.count() == 0:
        logger.info(f"ℹ️ Nenhum dado encontrado em 'local.bronze.{prefix}' para o dia atual. Pulando processamento.")
        continue

    # Exibe o schema da camada silver
    #logger.info("Schema da camada silver:")
    #silver_source_df.printSchema()

    # Prepara os campos para criação da tabela Iceberg
    cols = ", ".join([f"{field.name} STRING" for field in silver_source_df.schema.fields if field.name != "created_at"] + ["created_at TIMESTAMP"])

    # Cria tabela Iceberg na camada silver se não existir
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.silver.{prefix} (
            {cols}
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
        TBLPROPERTIES (
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='snappy',
            'write.target-file-size-bytes'='134217728',
            'commit.retry.num-retries'='10'
        )
    """)

    # Carrega a tabela silver existente
    silver_df = spark.table(f"local.silver.{prefix}")

    # Separa registros para atualização (existem na tabela silver) e inserção (novos)
    silver_existing_ids = silver_df.select("id").distinct()
    silver_update_df = silver_source_df.join(silver_existing_ids, "id", "inner")  # Registros que já existem
    silver_insert_df = silver_source_df.join(silver_existing_ids, "id", "left_anti")  # Registros novos

    # Escreve novos registros (inserção)
    if not silver_insert_df.isEmpty():
        silver_insert_df.writeTo(f"local.silver.{prefix}").append()
        logger.info(f"✅ Inseridos novos registros na tabela 'local.silver.{prefix}'.")

    # Escreve atualizações (sobrescreve partições afetadas)
    if not silver_update_df.isEmpty():
        silver_update_df.writeTo(f"local.silver.{prefix}").overwritePartitions()
        logger.info(f"✅ Atualizados registros existentes na tabela 'local.silver.{prefix}'.")

    # Manutenção na camada silver
    spark.sql(f"CALL local.system.rewrite_data_files(table => 'local.silver.{prefix}')")
    logger.info(f"🧹 Compactação de arquivos executada na tabela 'local.silver.{prefix}'.")

logger.info("🚀 Todas as tabelas silver foram processadas com sucesso.")

# Fecha a sessão Spark
spark.stop()