import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max

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

# Carrega as variáveis de ambiente
load_dotenv()
s3_endpoint = os.getenv("S3_ENDPOINT")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

logger.info("Variáveis de ambiente carregadas.")

# Inicializa SparkSession com suporte a Iceberg + MinIO
logger.info("Inicializando SparkSession...")
spark = SparkSession.builder \
    .appName("IcebergSilverToGold") \
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
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
logger.info("SparkSession inicializado com sucesso.")

# Carrega as tabelas 'pedido' e 'cliente' da camada Silver
logger.info("Lendo tabelas da camada Silver...")
pedido_df = spark.read.table("local.silver.pedido")
cliente_df = spark.read.table("local.silver.clientes")
logger.info("Tabelas 'pedido' e 'clientes' carregadas.")

# Realizar Join entre 'pedido' e 'cliente' (supondo que a chave seja 'cliente_id')
logger.info("Realizando join entre as tabelas...")
joined_df = pedido_df.join(cliente_df, pedido_df.cliente_id == cliente_df.id)

# Agregações e Transformações para a camada Gold
logger.info("Aplicando agregações para gerar tabela Gold...")
gold_df = joined_df.groupBy(pedido_df.cliente_id, cliente_df.nome) \
    .agg(
        count(pedido_df.id).alias("total_pedidos"),
        sum(pedido_df.valor_total).alias("valor_total_pedidos"),
        avg(pedido_df.valor_total).alias("media_valor_pedido"),
        max(pedido_df.data_pedido).alias("ultimo_pedido")
    )

# Criar a tabela Gold (se não existir)
logger.info("Escrevendo tabela Gold 'pedido_cliente'...")
gold_df.writeTo("local.gold.pedido_cliente").createOrReplace()

logger.info("🚀 Tabela Gold 'pedido_cliente' criada com sucesso!")
