
-- DROP TABLE public.clientes;
CREATE TABLE public.auditoria (
	id serial4 NOT NULL,
	id_registro int4 NULL,
	acao varchar(10) NULL,
	nome_tabela varchar(255) NULL,
	"data" date DEFAULT CURRENT_DATE NULL,
	CONSTRAINT auditoria_pkey PRIMARY KEY (id)
);


CREATE OR REPLACE FUNCTION log_auditoria() 
RETURNS TRIGGER AS $$
BEGIN
    -- Inserção ou atualização
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
        INSERT INTO auditoria(id_registro, acao, nome_tabela) 
        VALUES (NEW.id, TG_OP, TG_TABLE_NAME);
    -- Exclusão
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO auditoria(id_registro, acao, nome_tabela) 
        VALUES (OLD.id, 'exclusao', TG_TABLE_NAME);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TABLE public.clientes (
	id serial4 NOT NULL,
	nome varchar(100) NOT NULL,
	email varchar(100) NOT NULL,
	data_cadastro date DEFAULT CURRENT_DATE NULL,
	status varchar(20) NULL,
	CONSTRAINT clientes_email_key UNIQUE (email),
	CONSTRAINT clientes_pkey PRIMARY KEY (id),
	CONSTRAINT clientes_status_check CHECK (((status)::text = ANY ((ARRAY['ativo'::character varying, 'inativo'::character varying, 'pendente'::character varying])::text[])))
);

-- Table Triggers
create trigger auditoria_trigger_tabela_1 after
insert or delete  or update  on  public.clientes for each row execute function log_auditoria();


INSERT INTO clientes (nome, email, status) VALUES 
('João Silva', 'joao@empresa.com', 'ativo'),
('Maria Santos', 'maria@empresa.com', 'ativo'),
('Carlos Oliveira', 'carlos@empresa.com', 'pendente');

------SCRIPTS PARA DADOS FAKES PYTHON
from faker import Faker
import psycopg2
from tqdm import tqdm
import random

# Inicializa o Faker
fake = Faker('pt_BR')  # Locale brasileiro

# Conexão com o banco
conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="000.000.000.000",
    port="5432"
)
cursor = conn.cursor()

# Configurações
total_registros = 100_000
batch_size = 10_000  # Inserções em lote para performance
status_opcoes = ['ativo', 'inativo', 'pendente']

def gerar_dados_fake(inicio, qtd):
    dados = []
    for i in range(inicio, inicio + qtd):
        nome = fake.name()
        email = f"{nome.lower().replace(' ', '.')}.{i}@exemplo.com"
        data_cadastro = fake.date_between(start_date='-2y', end_date='today')
        status = random.choice(status_opcoes)
        dados.append((nome, email, data_cadastro, status))
    return dados

# Inserção em lotes
for inicio in tqdm(range(0, total_registros, batch_size)):
    dados = gerar_dados_fake(inicio, batch_size)
    args_str = ','.join(cursor.mogrify("(%s,%s,%s,%s)", x).decode("utf-8") for x in dados)
    cursor.execute(f"INSERT INTO clientes (nome, email, data_cadastro, status) VALUES {args_str}")
    conn.commit()

cursor.close()
conn.close()

-----CRIAÇÃO DO SCRIPT DE EXTRACAO DOS DADOS COM PYTHON
import pandas as pd
import yaml
import os
from sqlalchemy import create_engine

# Load configuração do YAML
with open('/opt/notebook/Engineering/tables.yml', 'r') as file:
    config = yaml.safe_load(file)

# Cria pasta 'data' se não existir
os.makedirs("/opt/notebook/Engineering/data", exist_ok=True)

# Configurações de conexão com o PostgreSQL
USER = "postgres"
PASSWORD = "postgres"
HOST = "000.000.000.000"
PORT = "5432"
DB = "postgres"

# Cria engine SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

def extrair_tabela_pequena(source, target):
    print(f"Extraindo tabela pequena: {source}")
    df = pd.read_sql_query(f"SELECT * FROM {source}", engine)
    output_path = f"/opt/notebook/Engineering/data/{target}.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Salvou: {output_path} ({len(df)} linhas)")

def extrair_tabela_grande(source, target, partition_config):
    col = partition_config["column"]
    lower = partition_config["lower_bound"]
    upper = partition_config["upper_bound"]
    num_parts = partition_config["num_partitions"]

    step = (upper - lower + 1) // num_parts

    for i in range(num_parts):
        start = lower + i * step
        end = start + step - 1 if i < num_parts - 1 else upper

        print(f"Extraindo {target}_{i+1:02d}: {col} de {start} até {end}")

        query = f"""
            SELECT * FROM {source}
            WHERE {col} BETWEEN %s AND %s
        """
        df = pd.read_sql_query(query, engine, params=(start, end))
        output_path = f"/opt/notebook/Engineering/data/{target}_{i+1:02d}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Salvou: {output_path} ({len(df)} linhas)")

# Loop pelas tabelas do YAML
for table in config['folders']:
    source = table['source']
    target = table['target']
    large = table.get('large_table', False)

    if large:
        extrair_tabela_grande(source, target, table['partition_config'])
    else:
        extrair_tabela_pequena(source, target)

print("✅ Extração finalizada.")

-->> aqui esta o tables.yml
folders:
  - source: public.clientes
    target: clientes
    large_table: true
    table_name: clientes
    partition_config:
      column: id
      lower_bound: 1
      upper_bound: 1000
      num_partitions: 20
      
  #- source: "pedido"
  #  target: "pedido"
  #  large_table: false  # ← Adicione esta linha para tabelas grandes
  #  partition_column: "data_pedido"
    
    

  #- source: "stgPedidos"
  #  target: "stgPedidos"  
  #  large_table: true  # ← Adicione esta linha para tabelas grandes
  #  partition_column: "scompetencia"
  #  partition_config:
  #    column: "chave"          # Coluna numérica para particionamento
  #    lower_bound: 1           # Valor mínimo estimado
  #    upper_bound: 10000000    # Valor máximo estimado
  #    num_partitions: 10       # Número de partições


----- AGORA A PARTE DE UPLOAD DOS ARQUIVOS .CSV NA INGESTION
import boto3
from botocore.client import Config
import os
import logging
import json
from dotenv import load_dotenv

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

# -------------------------
# Configurações
# -------------------------
MINIO_BUCKET = "ingestion"
DATA_FOLDER = "/opt/notebook/Engineering/data"

# Carregar variáveis do arquivo .env
load_dotenv()

s3_endpoint = os.getenv("S3_ENDPOINT")
s3_access_key = os.getenv("S3_ACCESS_KEY")
s3_secret_key = os.getenv("S3_SECRET_KEY")

# Cria cliente do MinIO com boto3
s3 = boto3.client(
    "s3",
    endpoint_url=s3_endpoint,
    aws_access_key_id=s3_access_key,
    aws_secret_access_key=s3_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

# Verifica se o bucket existe
buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
if MINIO_BUCKET not in buckets:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    logger.info(f"Bucket '{MINIO_BUCKET}' criado.")
else:
    logger.info(f"Bucket '{MINIO_BUCKET}' já existe.")

# Lista arquivos .csv
csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith(".csv")]

# Se não houver arquivos, encerra
if not csv_files:
    logger.info("Nenhum arquivo .csv encontrado. Processo encerrado.")
    exit(0)

# Envia os arquivos e remove após envio
for filename in csv_files:
    filepath = os.path.join(DATA_FOLDER, filename)
    logger.info(f"Iniciando upload do arquivo: {filename}")

    try:
        with open(filepath, "rb") as f:
            s3.upload_fileobj(f, MINIO_BUCKET, filename)
        logger.info(f"Arquivo enviado com sucesso: {filename}")

        os.remove(filepath)
        logger.info(f"Arquivo removido após upload: {filename}")
    except Exception as e:
        logger.error(f"Erro ao processar {filename}: {str(e)}")

logger.info("Todos os arquivos CSV foram processados com sucesso.")


----- AGORA PEGAR OS ARQUIVOS .CSV DA INGESTION E LEVAR PARA BRONZE COM TABELAS ICEBERG
import re
from collections import defaultdict
from dotenv import load_dotenv
import boto3
from botocore.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import logging
import os  # Adicionado para importar os.getenv

# -------------------------
# Configuração do Logging
# -------------------------
def setup_logger():
    logger = logging.getLogger("minio_upload")
    if logger.handlers:
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
    logger.info("Nenhum arquivo .csv encontrado no bucket 'ingestion'. Abortando script.")
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
    .config("spark.hadoop.fs.s3a.fast.upload", "true") \
    .config("spark.sql.catalog.local.default-namespace", "default") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Lista para armazenar arquivos a serem deletados
files_to_delete = []

# Processa os arquivos agrupados por prefixo
for prefix, files in prefix_groups.items():
    logger.info(f"🔧 Processando prefixo: {prefix}")

    # --------------------------------
    # Camada Bronze
    # --------------------------------
    file_paths = [f"s3a://{bucket_name}/{file}" for file in files]
    df = spark.read.option("header", "true").csv(file_paths)

    # Adiciona a coluna created_at com a data/hora atual
    df = df.withColumn("created_at", current_timestamp())

    # Validação: Remove duplicatas e nulos na chave primária (id)
    df = df.dropDuplicates(["id"]).filter("id IS NOT NULL")

    # Prepara os campos para criação da tabela Iceberg
    cols = ", ".join([f"{field.name} STRING" for field in df.schema.fields if field.name != "created_at"] + ["created_at TIMESTAMP"])

    # Cria tabela Iceberg na camada bronze se não existir
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.bronze.{prefix} (
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

    # Carrega a tabela bronze existente
    bronze_df = spark.table(f"local.bronze.{prefix}")

    # Separa registros para atualização (existem na tabela bronze) e inserção (novos)
    existing_ids = bronze_df.select("id").distinct()
    update_df = df.join(existing_ids, "id", "inner")  # Registros que já existem
    insert_df = df.join(existing_ids, "id", "left_anti")  # Registros novos

    # Escreve novos registros (inserção)
    if not insert_df.isEmpty():
        insert_df.writeTo(f"local.bronze.{prefix}").append()
        logger.info(f"✅ Inseridos novos registros na tabela 'local.bronze.{prefix}'.")

    # Escreve atualizações (sobrescreve partições afetadas)
    if not update_df.isEmpty():
        update_df.writeTo(f"local.bronze.{prefix}").overwritePartitions()
        logger.info(f"✅ Atualizados registros existentes na tabela 'local.bronze.{prefix}'.")

    # Manutenção na camada bronze
    spark.sql(f"CALL local.system.rewrite_data_files(table => 'local.bronze.{prefix}')")
    logger.info(f"🧹 Compactação de arquivos executada na tabela 'local.bronze.{prefix}'.")

    # Adiciona arquivos processados à lista de exclusão
    files_to_delete.extend(files)

# Deleta arquivos processados do bucket após todas as operações Spark
for file in files_to_delete:
    s3.delete_object(Bucket=bucket_name, Key=file)
    logger.info(f"🗑️ Arquivo deletado do bucket: {file}")

logger.info("🚀 Todas as tabelas foram processadas e os arquivos .csv foram excluídos com sucesso.")

# Fecha a sessão Spark
spark.stop()

-----AGORA PEGAR OS ARQUIVOS DA BRONZE E LEVAR PARA SILVER
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

-- NÃO FOI CRIADO A GOLD POIS SÓ TEMOS UMA TABELA

Na pasta Notebook eu tenho as todos esses SCRIPTS acima e as pastas SCRIPTS o .env e a pasta data 
na pasta scripts fica os arquvos .py que declarou acima.

Agora irei passar configurações do docker

--Dockercompose
services:
  airflow-init:
    image: apache/airflow:2.8.1-python3.10
    container_name: airflow-init
    depends_on:
      postgres:
        condition: service_healthy
    env_file:
      - .env
    command: >
      bash -c "
      airflow db migrate &&
      airflow users create --username airflow --firstname Air --lastname Flow --role Admin --email airflow@example.com --password airflow
      "
    volumes:
      - /Volumes/MACBACKUP/workspaceSparkHive/spark-iceberg-hive/notebook/Engineering/data:/opt/airflow/data    
      - ./src/airflow/dags:/opt/airflow/dags
      - ./src/airflow/logs:/opt/airflow/logs
      - ./src/airflow/plugins:/opt/airflow/plugins
      - ./src/airflow/scripts:/opt/airflow/scripts
      - ./src/airflow/data:/opt/airflow/data
      - ./src/airflow/.env:/opt/airflow/.env
    networks:
      - iceber-net

  airflow-webserver:
    image: apache/airflow:2.8.1-python3.10
    container_name: airflow-webserver
    restart: always
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    env_file:
      - .env
    ports:
      - "8080:8080"
    command: webserver
    volumes:
      - ./notebook:/opt/notebook
      - /Volumes/MACBACKUP/workspaceSparkHive/spark-iceberg-hive/notebook/Engineering/data:/opt/airflow/data
      - ./src/airflow/dags:/opt/airflow/dags
      - ./src/airflow/logs:/opt/airflow/logs
      - ./src/airflow/plugins:/opt/airflow/plugins
      - ./src/airflow/.env:/opt/airflow/.env
      - ./src/airflow/scripts:/opt/airflow/scripts
      - ./src/airflow/data:/opt/airflow/data
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - iceber-net

  airflow-scheduler:
    image: apache/airflow:2.8.1-python3.10
    container_name: airflow-scheduler
    restart: always
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    env_file:
      - .env
    command: scheduler
    volumes:
      - ./notebook:/opt/notebook
      - /Volumes/MACBACKUP/workspaceSparkHive/spark-iceberg-hive/notebook/Engineering/data:/opt/airflow/data    
      - ./src/airflow/dags:/opt/airflow/dags
      - ./src/airflow/logs:/opt/airflow/logs
      - ./src/airflow/plugins:/opt/airflow/plugins
      - ./src/airflow/.env:/opt/airflow/.env
      - ./src/airflow/scripts:/opt/airflow/scripts
      - ./src/airflow/data:/opt/airflow/data
      - /var/run/docker.sock:/var/run/docker.sock
    networks:
      - iceber-net

  # Storages
  postgres:
    container_name: postgres
    hostname: postgres
    image: postgres:11
    ports:
      - "5434:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - ./src/postgres/init-database.sh:/docker-entrypoint-initdb.d/init-database.sh
      - ~/projects/postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: [ "CMD", "pg_isready", "-U", "postgres" ]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always
    networks:
      - iceber-net

  trino:
    container_name: trino
    hostname: trino
    image: "trinodb/trino:425"
    restart: always
    ports:
      - "8889:8889"
    volumes:
      - ./src/trino/etc-coordinator:/etc/trino
      - ./src/trino/catalog:/etc/trino/catalog
    depends_on:
      - hive-metastore
    networks:
      - iceber-net

  trino-worker:
    profiles: [ "trino-worker" ]
    container_name: trino-worker
    hostname: trino-worker
    image: "trinodb/trino:425"
    restart: always
    volumes:
      - ./src/trino/etc-worker:/etc/trino
      - ./src/trino/catalog:/etc/trino/catalog
    depends_on:
      - trino
    networks:
      - iceber-net

  minio:
    container_name: minio
    hostname: minio
    image: 'minio/minio'
    ports:
      - '9000:9000'
      - '9001:9001'
    volumes:
      - ./src/minio_data:/data
    environment:
      MINIO_ROOT_USER: ${S3_ACCESS_KEY}
      MINIO_ROOT_PASSWORD: ${S3_SECRET_KEY}
      MINIO_DOMAIN: ${MINIO_DOMAIN}
    command: server /data --console-address ":9001"
    networks:
      - iceber-net

  minio-job:
    image: 'minio/mc'
    container_name: minio-job
    hostname: minio-job
    env_file:
      - .env
    entrypoint: |
      /bin/bash -c "
      sleep 5;
      /usr/bin/mc config --quiet host add myminio http://minio:9000 \$S3_ACCESS_KEY \$S3_SECRET_KEY || true;
      /usr/bin/mc mb --quiet myminio/datalake || true;
      "
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
      AWS_REGION: ${AWS_REGION}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      S3_ENDPOINT: ${S3_ENDPOINT}
      S3_PATH_STYLE_ACCESS: "true"
    depends_on:
      - minio
    networks:
      - iceber-net

  hive-metastore:
    container_name: hive-metastore
    hostname: hive-metastore
    build:
      dockerfile: ./src/hive-metastore/Dockerfile
    image: dataincode/openlakehouse:hive-metastore-3.1.2
    env_file:
      - .env
    ports:
      - '9083:9083'
    environment:
      HIVE_METASTORE_DRIVER: org.postgresql.Driver
      HIVE_METASTORE_JDBC_URL: ${HIVE_METASTORE_JDBC_URL}
      HIVE_METASTORE_USER: hive
      HIVE_METASTORE_PASSWORD: hive
      HIVE_METASTORE_WAREHOUSE_DIR: ${HIVE_METASTORE_WAREHOUSE_DIR}
      S3_ENDPOINT: ${S3_ENDPOINT}
      S3_ACCESS_KEY: ${S3_ACCESS_KEY}
      S3_SECRET_KEY: ${S3_SECRET_KEY}
      S3_PATH_STYLE_ACCESS: "true"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - iceber-net

  spark-iceberg:
    build:
      dockerfile: ./src/spark/Dockerfile-spark3.5
    image: dataincode/openlakehouse:spark-3.5
    container_name: spark-iceberg
    hostname: spark-iceberg
    entrypoint: |
      /bin/bash -c "
      /opt/spark/sbin/start-master.sh &&
      jupyter lab --notebook-dir=/opt/notebook --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root
      "
    ports:
      - "4040:4040"
      - "8900:8888"
      - "7077:7077"  # Adicionado para o mestre Spark
      - "8082:8080"  # Porta da UI do mestre Spark
    depends_on:
      - minio
      - hive-metastore
    environment:
      SPARK_MODE: master
      SPARK_MASTER_MEMORY: 4g
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
      AWS_REGION: ${AWS_REGION}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      S3_ENDPOINT: ${S3_ENDPOINT}
      S3_PATH_STYLE_ACCESS: "true"
    volumes:
      - ./notebook:/opt/notebook
      - ./src/jupyter/jupyter_server_config.py:/root/.jupyter/jupyter_server_config.py
      - ./src/jupyter/themes.jupyterlab-settings:/root/.jupyter/lab/user-settings/@jupyterlab/apputils-extension/themes.jupyterlab-settings
      - ./src/spark/spark-defaults-iceberg.conf:/opt/spark/conf/spark-defaults.conf
      - ./src/spark/spark-env.sh:/opt/spark/conf/spark-env.sh
    networks:
      - iceber-net

  spark-worker:
    build:
      dockerfile: ./src/spark/Dockerfile-spark3.5
    image: dataincode/openlakehouse:spark-3.5
    deploy:
      replicas: 1
    hostname: spark-worker-{{.Task.Slot}}
    entrypoint: |
      /bin/bash -c "
      /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-iceberg:7077
      "
    depends_on:
      - spark-iceberg
      - minio
      - hive-metastore
    environment:
      SPARK_MODE: worker
      SPARK_WORKER_MEMORY: 2g
      SPARK_WORKER_CORES: 2
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID}
      AWS_SECRET_ACCESS_KEY: ${S3_SECRET_KEY}
      AWS_REGION: ${AWS_REGION}
      AWS_DEFAULT_REGION: ${AWS_DEFAULT_REGION}
      S3_ENDPOINT: ${S3_ENDPOINT}
      S3_PATH_STYLE_ACCESS: "true"
    volumes:
      - ./notebook:/opt/notebook
      - ./src/spark/spark-defaults-iceberg.conf:/opt/spark/conf/spark-defaults.conf
      - ./src/spark/spark-env.sh:/opt/spark/conf/spark-env.sh
    networks:
      - iceber-net

# Configure Network
networks:
  iceber-net:
    name: iceber-net

------------------------ Na pasta src fica as configuraçoes do airflow,hive-metastore,jupyter
--minio_data,postgres,spark,trino e volume

--Dockerfiler do spark com algumas alterações
FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

WORKDIR ${SPARK_HOME}
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

ENV SPARK_VERSION_SHORT=3.5
ENV SPARK_VERSION=3.5.1
ENV AWS_SDK_VERSION=1.12.773
ENV HADOOP_AWS_VERSION=3.3.4
ENV SPARK_HADOOP_CLOUD=2.12

# Configure SPARK
RUN apt-get update -y && apt-get install -y curl wget
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_${SPARK_HADOOP_CLOUD}/${SPARK_VERSION}/spark-hadoop-cloud_${SPARK_HADOOP_CLOUD}-${SPARK_VERSION}.jar -o ${SPARK_HOME}/jars/spark-hadoop-cloud_${SPARK_HADOOP_CLOUD}-${SPARK_VERSION}.jar


# Configure ICEBERG
ENV ICEBERG_VERSION=1.6.0
ENV AWS_SDK_BUNDLE_VERSION=2.20.18

RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_2.12-${ICEBERG_VERSION}.jar -o ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_VERSION_SHORT}_2.12-${ICEBERG_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_BUNDLE_VERSION}/bundle-${AWS_SDK_BUNDLE_VERSION}.jar -Lo /opt/spark/jars/aws-bundle-${AWS_SDK_BUNDLE_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/${AWS_SDK_BUNDLE_VERSION}/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar -Lo /opt/spark/jars/url-connection-client-${AWS_SDK_BUNDLE_VERSION}.jar


# Configure ZSH
RUN apt-get install -y zsh git
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true
ENV TERM xterm
ENV ZSH_THEME robbyrussell
RUN chsh -s /usr/bin/zsh

# Configure PYTHON
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH
COPY ./src/jupyter/requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt
COPY ./src/jupyter/jupyter_server_config.py /root/.jupyter/
COPY ./src/jupyter/themes.jupyterlab-settings /root/.jupyter/lab/user-settings/@jupyterlab/apputils-extension/
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

ENTRYPOINT ["tail", "-f", "/dev/null"]

---------- ainda no spark configuração do spark-env.sh
# Configurações básicas do Spark
SPARK_HOME=/opt/spark
SPARK_CONF_DIR=/opt/spark/conf

# Configuração do mestre Spark
SPARK_MASTER_HOST=spark-iceberg
SPARK_MASTER_PORT=7077

# Configurações para integração com MinIO (opcional, já definidas no script)
export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${S3_SECRET_KEY}
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

# Configuração do Hadoop (ajuste conforme necessário)
HADOOP_CONF_DIR=/opt/spark/conf

# Opcional: Configurações de logging e desempenho
SPARK_WORKER_MEMORY=2g
SPARK_WORKER_CORES=2
SPARK_LOG_DIR=/opt/spark/logs

-------------- spark-defaults-iceberg.conf
## iceberg-spark-runtime-3.4_2.12:1.3.1
spark.sql.extensions              org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.catalogImplementation hive
spark.hive.metastore.uris thrift://hive-metastore:9083

spark.hadoop.fs.s3a.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.access.key minio
spark.hadoop.fs.s3a.secret.key minio123
spark.hadoop.fs.s3a.endpoint http://minio:9000
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.path.style.access true

spark.hadoop.fs.s3.impl org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3.access.key minio
spark.hadoop.fs.s3.secret.key minio123
spark.hadoop.fs.s3.endpoint http://minio:9000
spark.hadoop.fs.s3.connection.ssl.enabled false
spark.hadoop.fs.s3.path.style.access true

spark.sql.catalog.iceberg                 org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type            hive
spark.sql.catalog.iceberg.uri             thrift://hive-metastore:9083
spark.sql.catalog.iceberg.io-impl         org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.s3.endpoint     http://minio:9000
spark.sql.catalog.iceberg.warehouse       s3a://datalake/iceberg
spark.sql.catalog.iceberg.s3.path-style-access true

spark.sql.catalog.spark_catalog           org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type      hive

spark.hadoop.hive.cli.print.header true

-------hive-metastore
--Dockerfile
#FROM starburstdata/hive:3.1.2-e.18
FROM starburstdata/hive:3.1.2-e.18
ENV REGION=""
ENV GOOGLE_CLOUD_KEY_FILE_PATH=""
ENV AZURE_ADL_CLIENT_ID=""
ENV AZURE_ADL_CREDENTIAL=""
ENV AZURE_ADL_REFRESH_URL=""
ENV AZURE_ABFS_STORAGE_ACCOUNT=""
ENV AZURE_ABFS_ACCESS_KEY=""
ENV AZURE_WASB_STORAGE_ACCOUNT=""
ENV AZURE_ABFS_OAUTH=""
ENV AZURE_ABFS_OAUTH_TOKEN_PROVIDER=""
ENV AZURE_ABFS_OAUTH_CLIENT_ID=""
ENV AZURE_ABFS_OAUTH_SECRET=""
ENV AZURE_ABFS_OAUTH_ENDPOINT=""
ENV AZURE_WASB_ACCESS_KEY=""
ENV HIVE_METASTORE_USERS_IN_ADMIN_ROLE="admin"


# Remove JAR slf4j-log4j12-1.7.30.jar from hadoop
# to avoid SLF4J: Class path contains multiple SLF4J bindings message
RUN rm -f /opt/hadoop-3.3.1/share/hadoop/common/lib/slf4j-log4j12-1.7.30.jar


-----------airflow
--arquivo .env 
#Airflow
AIRFLOW_WWW_USER_USERNAME=airflow
AIRFLOW_WWW_USER_PASSWORD=airflow
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow
AIRFLOW_UID=50000

#Postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=airflow

# Configurações e key Minio S3
AWS_ACCESS_KEY_ID=minio
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123
S3_ENDPOINT=http://minio:9000
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
MINIO_DOMAIN=minio

# Configurações Hive-Metastorage
HIVE_METASTORE_JDBC_URL=jdbc:postgresql://postgres:5432/metastore
HIVE_METASTORE_USER=hive
HIVE_METASTORE_PASSWORD=hive
HIVE_METASTORE_WAREHOUSE_DIR=s3://datalake/

# Spark

-- a DAG para executar o pipeline
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator

class RobustBashOperator(BashOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disable_template_rendering = True  # Desativa template Jinja2
    
    def execute(self, context):
        try:
            # Força a execução como comando puro
            self.bash_command = f"""
            set -e
            {self.bash_command}
            """
            return super().execute(context)
        except Exception as e:
            self.log.error(f"Command failed with exception: {str(e)}")
            raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 14),  # Data anterior
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Temporariamente desabilita retries para debug
    'retry_delay': timedelta(minutes=5),
    'do_xcom_push': False,
    'execution_timeout': timedelta(minutes=30),
    'pool': 'default_pool',
    'priority_weight': 3,
}

dag = DAG(
    'spark_lakehouse',
    default_args=default_args,
    description='Executa todos os scripts Spark em sequência',
    schedule_interval=None,
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Tarefas com task_ids únicos
etl_process = RobustBashOperator(
    task_id='extract_transform_load',
    bash_command="""
    /opt/airflow/scripts/start_dlake_etl.sh
    """,
    dag=dag,
)

upload_data = RobustBashOperator(
    task_id='upload_raw_data',
    bash_command="""
    /opt/airflow/scripts/start_dlake_upload.sh
    """,
    dag=dag,
)

bronze_layer = RobustBashOperator(
    task_id='process_bronze_layer',
    bash_command="""
    /opt/airflow/scripts/start_dlake_bronze.sh
    """,
    dag=dag,
)

silver_layer = RobustBashOperator(
    task_id='process_silver_layer',
    bash_command="""
    /opt/airflow/scripts/start_dlake_silver.sh
    """,
    dag=dag,
)

# Define a ordem de execução
start >> etl_process >> upload_data >> bronze_layer >> silver_layer >> end
----------------
Existe tbem a pasta scripts onde existe os arquivos .sh para serem executados
--etl
#!/bin/bash
set -euo pipefail
echo "#########################################################################"
echo "#   _____  _               _  ________            ______ _______ _      #"
echo "#  |  __ \| |        /\   | |/ /  ____|          |  ____|__   __| |     #"
echo "#  | |  | | |       /  \  | ' /| |__     ______  | |__     | |  | |     #"
echo "#  | |  | | |      / /\ \ |  < |  __|   |______| |  __|    | |  | |     #"
echo "#  | |__| | |____ / ____ \| . \| |____           | |____   | |  | |____ #"
echo "#  |_____/|______/_/    \_\_|\_\______|          |______|  |_|  |______|#"
echo "#########################################################################"

log_file="/opt/airflow/logs/etl_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== Iniciando ETL em $(date) ==="
    
    # Executa o comando Spark e captura o código de saída
    docker exec spark-iceberg spark-submit /opt/notebook/Engineering/0101_etl.py 
    exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        echo "ERRO: Falha na execução do Spark (Código: $exit_code)"
        exit $exit_code
    fi
    
    echo "=== ETL concluído com sucesso em $(date) ==="
} > "$log_file" 2>&1

--upload
#!/bin/bash
set -euo pipefail
echo "################################################################################"
echo "#      ____  __    ___    __ __ ______     __  ______  __    ____  ___    ____ #"
echo "#     / __ \/ /   /   |  / //_// ____/    / / / / __ \/ /   / __ \/   |  / __ \#"
echo "#    / / / / /   / /| | / ,<  / __/______/ / / / /_/ / /   / / / / /| | / / / /#"
echo "#   / /_/ / /___/ ___ |/ /| |/ /__/_____/ /_/ / ____/ /___/ /_/ / ___ |/ /_/ / #"
echo "#   /_____/_____/_/  |_/_/ |_/_____/     \____/_/   /_____/\____/_/  |_/_____/ #"
echo "################################################################################"

log_file="/opt/airflow/logs/upload_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== Iniciando ETL em $(date) ==="
    
    # Executa o comando Spark e captura o código de saída
    docker exec spark-iceberg spark-submit /opt/notebook/Engineering/0202_upload.py
    exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        echo "ERRO: Falha na execução do Spark (Código: $exit_code)"
        exit $exit_code
    fi
    
    echo "=== ETL concluído com sucesso em $(date) ==="
} > "$log_file" 2>&1

-----bronze
#!/bin/bash
set -euo pipefail
echo "##################################################################################"
echo "#      ____  __    ___    __ __ ______     ____  ____  ____  _   _______   ______#"
echo "#     / __ \/ /   /   |  / //_// ____/    / __ )/ __ \/ __ \/ | / /__  /  / ____/#"
echo "#    / / / / /   / /| | / ,<  / __/______/ __  / /_/ / / / /  |/ /  / /  / __/   #"
echo "#   / /_/ / /___/ ___ |/ /| |/ /__/_____/ /_/ / _, _/ /_/ / /|  /  / /__/ /___   #"
echo "#   /_____/_____/_/  |_/_/ |_/_____/    /_____/_/ |_|\____/_/ |_/  /____/_____/  #"
echo "##################################################################################"

log_file="/opt/airflow/logs/bronze_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== Iniciando BRONZE em $(date) ==="
    
    # Executa o comando Spark e captura o código de saída
    docker exec spark-iceberg spark-submit /opt/notebook/Engineering/0303Bronze.py 
    exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        echo "ERRO: Falha na execução do Spark (Código: $exit_code)"
        exit $exit_code
    fi
    
    echo "=== BRONZE concluído com sucesso em $(date) ==="
} > "$log_file" 2>&1

-----silver
#!/bin/bash
set -euo pipefail
echo "##########################################################################"
echo "#      ____  __    ___    __ __ ______    _____ ______ _    ____________ #"
echo "#     / __ \/ /   /   |  / //_// ____/   / ___//  _/ /| |  / / ____/ __ \#"
echo "#    / / / / /   / /| | / ,<  / __/______\__ \ / // / | | / / __/ / /_/ /#"
echo "#   / /_/ / /___/ ___ |/ /| |/ /__/_____/__/ // // /__| |/ / /___/ _, _/ #"
echo "#   /_____/_____/_/  |_/_/ |_/_____/    /____/___/_____/___/_____/_/ |_| #"
echo "##########################################################################"

log_file="/opt/airflow/logs/silver_$(date +%Y%m%d_%H%M%S).log"

{
    echo "=== Iniciando SILVER em $(date) ==="
    
    # Executa o comando Spark e captura o código de saída
    docker exec spark-iceberg spark-submit /opt/notebook/Engineering/0404Silver.py
    exit_code=$?
    
    if [ $exit_code -ne 0 ]; then
        echo "ERRO: Falha na execução do Spark (Código: $exit_code)"
        exit $exit_code
    fi
    
    echo "=== SILVER concluído com sucesso em $(date) ==="
} > "$log_file" 2>&1


---pasta jupyter
--jupyter_server_config.py
c = get_config()
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8899
c.ServerApp.token = ''
c.ServerApp.password = ''
c.ServerApp.terminado_settings = {'shell_command': ['/usr/bin/zsh']}

--requirements.txt
jupyterlab==4.2.5
pyiceberg[pyarrow,s3fs,duckdb,pandas,hive]==0.6.0
jupysql==0.10.14
findspark==2.0.1

#aws minio
boto3
minio

#BANCO DE DADOS
SQLAlchemy
psycopg2-binary

#ANALITICS
tqdm
python-dotenv
python-json-logger 
requests
pyyaml
--themes.jupyterlab-settings
{
    "theme": "JupyterLab Dark",
    "theme-scrollbars": false,
    "adaptive-theme": false,
    "preferred-light-theme": "JupyterLab Light",
    "preferred-dark-theme": "JupyterLab Dark",
    "overrides": {
        "code-font-family": null,
        "code-font-size": null,
        "content-font-family": null,
        "content-font-size1": null,
        "ui-font-family": null,
        "ui-font-size1": null
    }
}
-----pasta minio_data

---pasta postgres
--init-database.sh
#!/bin/bash

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE metastore;
    CREATE USER hive WITH ENCRYPTED PASSWORD 'hive';
    GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

    CREATE DATABASE airflow;
    CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE metastore TO airflow;
EOSQL

-------pasta trino
----catalog
--hive.properties
connector.name=hive

hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123

hive.metastore-cache-ttl=0s
hive.metastore-refresh-interval=5s
hive.metastore-timeout=10s

--postgresql.properties
connector.name=postgresql
connection-url=jdbc:postgresql://metastore_db:5432/hive
connection-user=hive
connection-password=hive

--tcph.properties
connector.name=tpch

----etc-coordinator



