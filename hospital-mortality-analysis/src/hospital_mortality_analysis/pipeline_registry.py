"""
Registro de todos los pipelines del proyecto
"""

from typing import Dict
from kedro.pipeline import Pipeline

from hospital_mortality_analysis.pipelines.data_preparation import pipeline as dp
from hospital_mortality_analysis.pipelines.classification import pipeline as classification
from hospital_mortality_analysis.pipelines import regression

def register_pipelines() -> Dict[str, Pipeline]:
    """
    Registra todos los pipelines del proyecto.
    
    Returns:
        Diccionario de pipelines del proyecto
    """
    data_preparation_pipeline = dp.create_pipeline()
    classification_pipeline = classification.create_pipeline()
    regression_pipeline = regression.create_pipeline()

    return {
        "__default__": data_preparation_pipeline + classification_pipeline + regression_pipeline,
        "data_preparation": data_preparation_pipeline,
        "classification": classification_pipeline,
        "regression": regression_pipeline,
    }
