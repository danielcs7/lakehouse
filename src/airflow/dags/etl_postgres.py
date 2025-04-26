from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
import yaml
import os
from sqlalchemy import create_engine

# Configurações do PostgreSQL
USER = "postgres"
PASSWORD = "admin"
HOST = "192.168.0.202"
PORT = "5432"
DB = "postgres"

engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}")

def carregar_config():
    with open('/opt/airflow/dags/tables.yml', 'r') as file:
        return yaml.safe_load(file)

def extrair_tabela_pequena(source, target):
    os.makedirs("data", exist_ok=True)
    df = pd.read_sql_query(f"SELECT * FROM {source}", engine)
    output_path = f"data/{target}.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Salvou: {output_path} ({len(df)} linhas)")

def extrair_tabela_grande(source, target, partition_config):
    os.makedirs("data", exist_ok=True)
    col = partition_config["column"]
    lower = partition_config["lower_bound"]
    upper = partition_config["upper_bound"]
    num_parts = partition_config["num_partitions"]

    step = (upper - lower + 1) // num_parts

    for i in range(num_parts):
        start = lower + i * step
        end = start + step - 1 if i < num_parts - 1 else upper

        query = f"""
            SELECT * FROM {source}
            WHERE {col} BETWEEN %s AND %s
        """
        df = pd.read_sql_query(query, engine, params=(start, end))
        output_path = f"data/{target}_{i+1:02d}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Salvou: {output_path} ({len(df)} linhas)")

def executar_etl():
    config = carregar_config()

    for table in config['folders']:
        source = table['source']
        target = table['target']
        large = table.get('large_table', False)

        if large:
            extrair_tabela_grande(source, target, table['partition_config'])
        else:
            extrair_tabela_pequena(source, target)

with DAG(
    dag_id="etl_postgres_para_csv",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "postgres", "csv"]
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    executar_etl_task = PythonOperator(
        task_id="executar_etl_postgres",
        python_callable=executar_etl
    )

    start >> executar_etl_task >> end
