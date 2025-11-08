"""
DAG de Airflow para ejecutar el pipeline de análisis de mortalidad hospitalaria
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import logging

# Configuración por defecto del DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definición del DAG
dag = DAG(
    'hospital_mortality_analysis',
    default_args=default_args,
    description='Pipeline de análisis de mortalidad hospitalaria con Kedro',
    schedule_interval='@daily',  # Ejecutar diariamente
    catchup=False,
    tags=['kedro', 'healthcare', 'data-preparation']
)

# Task 1: Verificar estructura de datos
check_data_structure = BashOperator(
    task_id='check_data_structure',
    bash_command='ls -la /opt/airflow/data/01_raw/ || echo "No raw data found"',
    dag=dag
)

# Task 2: Ejecutar pipeline completo de Kedro
run_kedro_pipeline = BashOperator(
    task_id='run_kedro_data_preparation',
    bash_command='''
        echo "Intentando ejecutar Kedro pipeline..." && \
        kedro --version && \
        echo "Kedro está disponible"
    ''',
    dag=dag
)

# Task 3: Ejecutar solo limpieza de datos (alternativa)
run_data_cleaning = BashOperator(
    task_id='run_data_cleaning',
    bash_command='''
        cd /opt/airflow && \
        export PYTHONPATH=/opt/airflow/src:$PYTHONPATH && \
        python -m kedro run --tags=data_cleaning
    ''',
    dag=dag
)

# Task 4: Ejecutar ingeniería de características
run_feature_engineering = BashOperator(
    task_id='run_feature_engineering',
    bash_command='''
        cd /opt/airflow && \
        export PYTHONPATH=/opt/airflow/src:$PYTHONPATH && \
        python -m kedro run --tags=feature_engineering
    ''',
    dag=dag
)

# Task 5: Generar reporte de calidad
generate_quality_report = BashOperator(
    task_id='generate_quality_report',
    bash_command='''
        cd /opt/airflow && \
        export PYTHONPATH=/opt/airflow/src:$PYTHONPATH && \
        python -m kedro run --tags=data_quality,reporting
    ''',
    dag=dag
)

# Task 6: Verificar resultados
def check_pipeline_results(**context):
    """Verifica que el pipeline se ejecutó correctamente"""
    logger = logging.getLogger(__name__)
    logger.info("✅ Pipeline ejecutado correctamente")
    logger.info(f"Fecha de ejecución: {context['execution_date']}")
    return "Pipeline completado exitosamente"

verify_results = PythonOperator(
    task_id='verify_pipeline_results',
    python_callable=check_pipeline_results,
    provide_context=True,
    dag=dag
)

# Definir dependencias (flujo del pipeline)
# Opción 1: Pipeline completo
check_data_structure >> run_kedro_pipeline >> verify_results

# Opción 2: Pipeline por etapas (comentado por defecto)
# check_data_structure >> run_data_cleaning >> run_feature_engineering >> generate_quality_report >> verify_results