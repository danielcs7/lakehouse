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

