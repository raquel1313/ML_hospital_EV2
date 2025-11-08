"""
DAG simplificado para ejecutar el pipeline de Kedro
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Configuración por defecto
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir el DAG
dag = DAG(
    'hospital_mortality_simple',
    default_args=default_args,
    description='Pipeline simple de análisis de mortalidad hospitalaria',
    schedule_interval=None,  # Solo manual
    catchup=False,
    tags=['kedro', 'healthcare', 'simple']
)

# Task: Verificar que Kedro está disponible
check_kedro = BashOperator(
    task_id='check_kedro_installation',
    bash_command='kedro --version || echo "Kedro not found"',
    dag=dag
)

# Task: Listar archivos de datos
check_data = BashOperator(
    task_id='check_data_files',
    bash_command='ls -lah /opt/airflow/data/01_raw/ || echo "No data directory"',
    dag=dag
)

# Task: Verificar estructura del proyecto
check_project = BashOperator(
    task_id='check_project_structure',
    bash_command='ls -lah /opt/airflow/conf/base/ || echo "No conf directory"',
    dag=dag
)

# Definir orden de ejecución
check_kedro >> check_data >> check_project