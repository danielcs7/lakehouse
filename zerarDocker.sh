# Remove todos os containers
if [ "$(docker ps -aq)" ]; then
  docker rm -f $(docker ps -aq)
else
  echo "Nenhum container para remover."
fi

# Remove todas as imagens
if [ "$(docker images -q)" ]; then
  docker rmi -f $(docker images -q)
else
  echo "Nenhuma imagem para remover."
fi

# Remove todos os volumes
if [ "$(docker volume ls -q)" ]; then
  docker volume rm -f $(docker volume ls -q)
else
  echo "Nenhum volume para remover."
fi

# Remove todas as networks (exceto as padrão)
if [ "$(docker network ls | grep -v 'bridge\|host\|none' | awk 'NR>1 {print $1}')" ]; then
  docker network rm $(docker network ls | grep -v 'bridge\|host\|none' | awk 'NR>1 {print $1}')
else
  echo "Nenhuma network personalizada para remover."
fi

# Limpa o cache de build
docker builder prune -af
