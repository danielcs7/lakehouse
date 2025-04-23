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
