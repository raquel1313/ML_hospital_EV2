#!/bin/bash

# Script para configurar el entorno de Airflow + Kedro

echo "🚀 Configurando entorno de Airflow + Kedro..."

# Crear directorios necesarios
echo "📁 Creando estructura de directorios..."
mkdir -p dags logs plugins data/01_raw data/02_intermediate data/03_primary

# Crear archivo .env si no existe
if [ ! -f .env ]; then
    echo "📝 Creando archivo .env..."
    cat > .env << EOF
# Airflow
AIRFLOW_UID=$(id -u)
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow123

# Dependencias adicionales
_PIP_ADDITIONAL_REQUIREMENTS=kedro pandas numpy matplotlib seaborn scikit-learn

# Kedro
KEDRO_ENV=local
EOF
    echo "✅ Archivo .env creado"
else
    echo "ℹ️  Archivo .env ya existe"
fi

# Inicializar base de datos de Airflow
echo "🗄️  Inicializando Airflow..."
docker-compose up airflow-init

echo ""
echo "✅ Configuración completada!"
echo ""
echo "📋 Próximos pasos:"
echo "   1. Coloca tus datos CSV en: data/01_raw/"
echo "   2. Inicia los servicios: docker-compose up -d"
echo "   3. Accede a Airflow: http://localhost:8080"
echo "   4. Usuario: airflow / Contraseña: airflow123"
echo ""