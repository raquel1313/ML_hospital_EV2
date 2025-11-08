.PHONY: help setup start stop restart logs clean test kedro-run kedro-viz

help:
	@echo "🏥 Hospital Mortality Analysis - Comandos disponibles"
	@echo ""
	@echo "  make setup          - Configura el entorno inicial"
	@echo "  make start          - Inicia todos los servicios"
	@echo "  make stop           - Detiene todos los servicios"
	@echo "  make restart        - Reinicia todos los servicios"
	@echo "  make logs           - Muestra logs en tiempo real"
	@echo "  make logs-airflow   - Logs del scheduler de Airflow"
	@echo "  make clean          - Limpia contenedores y volúmenes"
	@echo "  make test           - Ejecuta tests del proyecto"
	@echo "  make kedro-run      - Ejecuta pipeline de Kedro localmente"
	@echo "  make kedro-viz      - Abre visualización de Kedro"
	@echo "  make airflow-ui     - Abre interfaz de Airflow"
	@echo ""

setup:
	@echo "🚀 Configurando entorno..."
	bash setup_airflow.sh

start:
	@echo "▶️  Iniciando servicios..."
	docker-compose up -d
	@echo "✅ Servicios iniciados!"
	@echo "🌐 Airflow UI: http://localhost:8080"
	@echo "👤 Usuario: airflow | Contraseña: airflow123"

stop:
	@echo "⏹️  Deteniendo servicios..."
	docker-compose down

restart:
	@echo "🔄 Reiniciando servicios..."
	docker-compose restart

logs:
	@echo "📋 Mostrando logs..."
	docker-compose logs -f

logs-airflow:
	@echo "📋 Logs del scheduler de Airflow..."
	docker-compose logs -f airflow-scheduler

clean:
	@echo "🧹 Limpiando contenedores y volúmenes..."
	docker-compose down -v
	rm -rf logs/* plugins/__pycache__

test:
	@echo "🧪 Ejecutando tests..."
	pytest

kedro-run:
	@echo "🏃 Ejecutando pipeline de Kedro..."
	kedro run --pipeline=data_preparation

kedro-viz:
	@echo "📊 Abriendo Kedro Viz..."
	kedro viz --host=0.0.0.0 --port=4141

airflow-ui:
	@echo "🌐 Abriendo Airflow UI..."
	open http://localhost:8080 || xdg-open http://localhost:8080 || start http://localhost:8080

build:
	@echo "🏗️  Construyendo imagen Docker..."
	docker-compose build

status:
	@echo "📊 Estado de los servicios:"
	docker-compose ps