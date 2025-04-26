from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.dummy import DummyOperator

class RobustBashOperator(BashOperator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.disable_template_rendering = True  # Desativa template Jinja2
    
    def execute(self, context):
        try:
            # Força a execução como comando puro
            self.bash_command = f"""
            set -e
            {self.bash_command}
            """
            return super().execute(context)
        except Exception as e:
            self.log.error(f"Command failed with exception: {str(e)}")
            raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 14),  # Data anterior
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Temporariamente desabilita retries para debug
    'retry_delay': timedelta(minutes=5),
    'do_xcom_push': False,
    'execution_timeout': timedelta(minutes=30),
    'pool': 'default_pool',
    'priority_weight': 3,
}

dag = DAG(
    'spark_lakehouse',
    default_args=default_args,
    description='Executa todos os scripts Spark em sequência',
    schedule_interval=None,
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Tarefas com task_ids únicos
etl_process = RobustBashOperator(
    task_id='extract_transform_load',
    bash_command="""
    /opt/airflow/scripts/start_dlake_etl.sh
    """,
    dag=dag,
)

upload_data = RobustBashOperator(
    task_id='upload_raw_data',
    bash_command="""
    /opt/airflow/scripts/start_dlake_upload.sh
    """,
    dag=dag,
)

bronze_layer = RobustBashOperator(
    task_id='process_bronze_layer',
    bash_command="""
    /opt/airflow/scripts/start_dlake_bronze.sh
    """,
    dag=dag,
)

silver_layer = RobustBashOperator(
    task_id='process_silver_layer',
    bash_command="""
    /opt/airflow/scripts/start_dlake_silver.sh
    """,
    dag=dag,
)

# Define a ordem de execução
start >> etl_process >> upload_data >> bronze_layer >> silver_layer >> end