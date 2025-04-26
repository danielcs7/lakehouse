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
PASSWORD = "admin"
HOST = "192.168.0.202"
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
