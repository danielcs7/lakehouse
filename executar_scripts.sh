#!/bin/bash

docker exec -it spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0101_etl.py"
docker exec -it spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0202_upload.py"
docker exec -it spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0303Bronze.py"
docker exec -it spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0404Silver.py"
docker exec -it spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0505Gold.py"

echo "Todos os scripts foram executados!"ß