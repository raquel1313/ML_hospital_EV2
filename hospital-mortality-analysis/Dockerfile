# Imagen base de Python
FROM python:3.10-slim

# Establecer directorio de trabajo
WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar archivos de requisitos
COPY requirements.txt .

# Instalar dependencias de Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto
COPY . .

# Instalar el proyecto en modo editable
RUN pip install -e .

RUN pip install kedro-datasets[pandas]


# Exponer puerto (si es necesario para Kedro Viz)
EXPOSE 4141

# Comando por defecto
CMD ["kedro", "run"]
RUN pip install kedro
