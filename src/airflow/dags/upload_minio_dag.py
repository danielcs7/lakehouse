from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Para start e end
from datetime import datetime
import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv

def upload_to_minio():
    load_dotenv(dotenv_path="/opt/airflow/.env")

    MINIO_BUCKET = "ingestion"
    DATA_FOLDER = "/opt/airflow/data"

    s3_endpoint = os.getenv("S3_ENDPOINT")
    s3_access_key = os.getenv("S3_ACCESS_KEY")
    s3_secret_key = os.getenv("S3_SECRET_KEY")

    s3 = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

    buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
    if MINIO_BUCKET not in buckets:
        s3.create_bucket(Bucket=MINIO_BUCKET)
        print(f"🪣 Bucket '{MINIO_BUCKET}' criado.")
    else:
        print(f"🪣 Bucket '{MINIO_BUCKET}' já existe.")

    for filename in os.listdir(DATA_FOLDER):
        if filename.endswith(".csv"):
            filepath = os.path.join(DATA_FOLDER, filename)
            print(f"📤 Enviando: {filename}")
            with open(filepath, "rb") as f:
                s3.upload_fileobj(f, MINIO_BUCKET, filename)
            print(f"✅ Enviado: {filename}")

    print("🎉 Todos os arquivos CSV foram enviados com sucesso para o MinIO!")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='upload_to_minio_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['minio', 'upload'],
    description='DAG para enviar arquivos CSV para o MinIO',
) as dag:

    start = EmptyOperator(task_id='start')
    upload = PythonOperator(task_id='upload_to_minio', python_callable=upload_to_minio)
    end = EmptyOperator(task_id='end')

    start >> upload >> end
