#!/bin/bash

# Nome do container Spark
CONTAINER_NAME="spark-iceberg"

# Diretório onde os scripts Python estão localizados
SCRIPT_DIR="/opt/notebook"  # Local no container

# Verifica se o container está rodando
if [ "$(docker ps -q -f name=$CONTAINER_NAME)" ]; then
    echo "✅ O container $CONTAINER_NAME já está rodando."
else
    echo "🚀 Iniciando container $CONTAINER_NAME..."
    docker start $CONTAINER_NAME
    sleep 5
fi

# Lista os arquivos Python no diretório do container e ordena por nome (garantindo a ordem correta)
SCRIPT_FILES=$(docker exec $CONTAINER_NAME ls $SCRIPT_DIR/*.py | sort)

# Itera sobre todos os scripts Python ordenados
for SCRIPT_PATH in $SCRIPT_FILES; do
    SCRIPT_NAME=$(basename "$SCRIPT_PATH")
    
    # Verifica se o arquivo é de fato um arquivo .py
    if [[ "$SCRIPT_PATH" == *.py ]]; then
        echo "🚀 Executando $SCRIPT_NAME no Spark..."
        
        # Executa o spark-submit para cada script Python encontrado dentro do container
        docker exec -it $CONTAINER_NAME spark-submit $SCRIPT_PATH
        sleep 2  # Adiciona um pequeno delay entre os scripts (opcional)
    fi
done
