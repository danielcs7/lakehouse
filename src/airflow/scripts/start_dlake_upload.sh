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