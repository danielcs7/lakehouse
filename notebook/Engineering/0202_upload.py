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
exec