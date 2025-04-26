import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Carrega as variáveis de ambiente
load_dotenv()
s3_endpoint = os.getenv("S3_ENDPOINT")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

# Inicializa SparkSession com suporte a Iceberg + MinIO
spark = SparkSession.builder \
    .appName("IcebergBronzeToSilver") \
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

# Lista tabelas da camada bronze
bronze_tables = [row.tableName for row in spark.sql("SHOW TABLES IN local.bronze").collect()]

for table in bronze_tables:
    print(f"\n🔁 Processando tabela: {table}")

    # Lê a tabela da camada bronze
    df = spark.read.format("iceberg").load(f"local.bronze.{table}")

    # Exemplo de transformação: remove duplicados
    df_transformed = df.dropDuplicates()

    # Cria tabela silver (sobrescreve se necessário)
    df_transformed.writeTo(f"local.silver.{table}").createOrReplace()

    # melhor perfoemance para grandes volumas 
    df_transformed.writeTo(f"local.silver.{table}").overwritePartitions()


    print(f"✅ Tabela 'local.silver.{table}' criada com dados tratados.")

print("\n🚀 Todas as tabelas foram processadas para a camada silver.")
