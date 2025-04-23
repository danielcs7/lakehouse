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
