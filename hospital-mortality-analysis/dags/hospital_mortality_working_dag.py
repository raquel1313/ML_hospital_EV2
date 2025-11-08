"""
DAG funcional para análisis de mortalidad hospitalaria
Este DAG NO importa módulos de Kedro directamente
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    'hospital_mortality_working',
    default_args=default_args,
    description='Pipeline funcional de análisis de mortalidad hospitalaria',
    schedule_interval='@daily',
    catchup=False,
    tags=['kedro', 'healthcare', 'production']
)

# ============================================================================
# FASE 1: VERIFICACIÓN DEL ENTORNO
# ============================================================================

verify_environment = BashOperator(
    task_id='verify_environment',
    bash_command='''
        echo "========================================"
        echo "Verificando entorno..."
        echo "========================================"
        echo "Python version:"
        python --version
        echo ""
        echo "Kedro version:"
        kedro --version || echo "Kedro no disponible"
        echo ""
        echo "Paquetes instalados:"
        pip list | grep -E "kedro|pandas|numpy" || echo "Paquetes no encontrados"
        echo "========================================"
    ''',
    dag=dag
)

check_data_exists = BashOperator(
    task_id='check_data_exists',
    bash_command='''
        echo "Verificando archivos de datos..."
        if [ -d "/opt/airflow/data/01_raw" ]; then
            echo "✅ Directorio data/01_raw existe"
            ls -lh /opt/airflow/data/01_raw/
            file_count=$(ls -1 /opt/airflow/data/01_raw/*.csv 2>/dev/null | wc -l)
            echo "Archivos CSV encontrados: $file_count"
            if [ $file_count -eq 0 ]; then
                echo "⚠️  ADVERTENCIA: No se encontraron archivos CSV"
            fi
        else
            echo "❌ ERROR: Directorio data/01_raw no existe"
            exit 1
        fi
    ''',
    dag=dag
)

# ============================================================================
# FASE 2: INSTALACIÓN DE DEPENDENCIAS (si es necesario)
# ============================================================================

install_dependencies = BashOperator(
    task_id='install_dependencies',
    bash_command='''
        echo "Verificando dependencias..."
        pip show kedro-datasets || pip install kedro-datasets[pandas]
        echo "✅ Dependencias verificadas"
    ''',
    dag=dag
)

# ============================================================================
# FASE 3: EJECUCIÓN DEL PIPELINE (Versión Segura)
# ============================================================================

run_data_preparation = BashOperator(
    task_id='run_data_preparation_pipeline',
    bash_command='''
        echo "========================================"
        echo "Ejecutando Pipeline de Preparación de Datos"
        echo "========================================"
        
        # Verificar que estamos en el directorio correcto
        cd /opt/airflow
        echo "Directorio actual: $(pwd)"
        
        # Verificar estructura del proyecto
        if [ ! -f "pyproject.toml" ]; then
            echo "❌ ERROR: No se encuentra pyproject.toml"
            echo "El proyecto Kedro no está montado correctamente"
            exit 1
        fi
        
        echo "✅ Proyecto Kedro encontrado"
        
        # Intentar ejecutar el pipeline
        echo "Ejecutando: kedro run --pipeline=data_preparation"
        kedro run --pipeline=data_preparation 2>&1 || {
            echo "❌ ERROR: El pipeline falló"
            echo "Verificando logs..."
            ls -lh logs/ 2>/dev/null || echo "No hay logs disponibles"
            exit 1
        }
        
        echo "✅ Pipeline ejecutado exitosamente"
    ''',
    dag=dag
)

# ============================================================================
# FASE 4: VERIFICACIÓN DE RESULTADOS
# ============================================================================

verify_outputs = BashOperator(
    task_id='verify_output_data',
    bash_command='''
        echo "Verificando datos procesados..."
        
        # Verificar datos intermedios
        if [ -d "/opt/airflow/data/02_intermediate" ]; then
            echo "✅ Datos intermedios:"
            ls -lh /opt/airflow/data/02_intermediate/
        else
            echo "⚠️  No se generaron datos intermedios"
        fi
        
        # Verificar datos primarios
        if [ -d "/opt/airflow/data/03_primary" ]; then
            echo "✅ Datos primarios:"
            ls -lh /opt/airflow/data/03_primary/
        else
            echo "⚠️  No se generaron datos primarios"
        fi
        
        echo "Verificación completada"
    ''',
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_execution_report',
    python_callable=lambda **context: print(f"""
    ========================================
    REPORTE DE EJECUCIÓN
    ========================================
    DAG: {context['dag'].dag_id}
    Execution Date: {context['execution_date']}
    ========================================
    ✅ Pipeline ejecutado exitosamente
    ========================================
    """),
    provide_context=True,
    dag=dag
)

# ============================================================================
# DEFINIR FLUJO DE EJECUCIÓN
# ============================================================================

verify_environment >> check_data_exists >> install_dependencies >> run_data_preparation >> verify_outputs >> generate_report