from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'spark_iceberg_pipeline',
    default_args=default_args,
    schedule_interval=None,
    tags=['datalake'],
) as dag:

    etl_task = BashOperator(
        task_id='etl',
        bash_command='docker exec spark-iceberg /bin/bash -c "spark-submit /opt/notebook/Engineering/0101_etl.py 2>&1"',
    )

    upload_task = BashOperator(
        task_id='upload',
        bash_command='docker run --network=net-spark-iceberg -v $(pwd)/notebook:/opt/notebook spark-iceberg:latest /bin/bash -c "spark-submit /opt/notebook/Engineering/0202_upload.py 2>&1"',
    )

    bronze_task = BashOperator(
        task_id='bronze',
        bash_command='docker run --network=net-spark-iceberg -v $(pwd)/notebook:/opt/notebook spark-iceberg:latest /bin/bash -c "spark-submit /opt/notebook/Engineering/0303Bronze.py 2>&1"',
    )

    silver_task = BashOperator(
        task_id='silver',
        bash_command='docker run --network=net-spark-iceberg -v $(pwd)/notebook:/opt/notebook spark-iceberg:latest /bin/bash -c "spark-submit /opt/notebook/Engineering/0404Silver.py 2>&1"',
    )

    gold_task = BashOperator(
        task_id='gold',
        bash_command='docker run --network=net-spark-iceberg -v $(pwd)/notebook:/opt/notebook spark-iceberg:latest /bin/bash -c "spark-submit /opt/notebook/Engineering/0505Gold.py 2>&1"',
    )

    # Define a ordem das tarefas
    etl_task >> upload_task >> bronze_task >> silver_task >> gold_task