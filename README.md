# Proyecto Machine Learning – Datos Hospitalarios

Este proyecto corresponde a la **Evaluación Parcial 1** de la asignatura **Fundamentos de Machine Learning**. El objetivo es realizar un análisis completo de los datos hospitalarios siguiendo la metodología **CRISP-DM**, incluyendo análisis exploratorio, limpieza, preprocesamiento y preparación de los datos para modelos de Machine Learning.

---

## Contenido primera entrega:

- drive https://drive.google.com/drive/folders/1RpgAwfTtKmTIpI_AShD9NVGed9zHpbu_?usp=drive_link
- Video Notebook: https://drive.google.com/file/d/161VQnNmgvgxEceRi6aeWOpwESYDX30Hc/view?usp=sharing
- Video Kedro: https://drive.google.com/file/d/1KER7dLGDQB4TnOZGYtrHmLLGeBLYUX3r/view?usp=drive_link

---
## Contenido Segunda entrega:
- drive:
- Video :

---
## Requisitos

- Python 3.10 o superior.
- Librerías necesarias (instalables mediante `requirements.txt`):
- numpy
- pandas
- matplotlib
- seaborn
- scikit-learn
- missingno
- Jupyter Notebook para visualizar y ejecutar el análisis.

---

## Instalación

1. Clonar el repositorio:

2. Crear un entorno virtual (recomendado):

bash``` 
Copiar código
python -m venv venv```
3. Activar el entorno virtual:

Windows:

bash```
Copiar código
venv\Scripts\activate```
Linux / Mac:

bash```
Copiar código
source venv/bin/activate```
4. Instalar las dependencias:

bash```
Copiar código
pip install -r requirements.txt```

Uso
5. Abrir el notebook:

bash```
Copiar código
jupyter notebook notebook.ipynb```
6. Seguir el flujo del notebook, que incluye:

- Comprensión del negocio y los datos: análisis de las variables, tipos de datos y contexto del hospital.

- Análisis exploratorio de datos (EDA): estadísticas descriptivas, distribución de variables, correlaciones y visualizaciones.

- Detección y tratamiento de valores faltantes y atípicos.

- Transformaciones de datos: normalización, estandarización y codificación de variables.

Preparación de targets: para tareas de regresión y clasificación según el caso.

Justificación de decisiones: cada paso está documentado con markdown explicando los motivos y resultados.
---
## Metodología
El proyecto sigue CRISP-DM, con foco en las tres primeras fases:

Comprensión del negocio: definición del problema y objetivos.

Comprensión de los datos: revisión de variables, estadísticas, distribuciones y anomalías.

Preparación de los datos: limpieza, imputación, transformación y preparación de targets.

Recomendaciones
Revisar los markdown dentro del notebook para entender cada decisión tomada.

Mantener el entorno virtual activo para evitar conflictos con librerías del sistema.

Los datasets deben permanecer en la carpeta data/ para que el notebook funcione correctamente.


