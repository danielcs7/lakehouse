# /opt/airflow/dags/bronze_loader_with_spark_submit.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="bronze_loader_with_spark_submit",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "iceberg", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --jars /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.0.jar \
        /opt/airflow/jobs/load_bronze.py
        """
    )

    end = EmptyOperator(task_id="end")

    start >> run_spark >> end
