"""
Pipeline de preparación de datos hospitalarios
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    clean_column_names,
    standardize_key_columns,
    merge_datasets,
    create_age_categories,
    engineer_comorbidity_features,
    generate_data_quality_report,
    create_exploratory_plots
)

def create_pipeline(**kwargs) -> Pipeline:
    """
    Crea el pipeline de preparación de datos
    
    Returns:
        Pipeline de Kedro con todos los nodos
    """
    return pipeline([
        # Fase 1: Limpieza de columnas
        node(
            func=clean_column_names,
            inputs=["df1_raw", "params:data_preparation"],
            outputs="df1_cleaned",
            name="clean_df1_columns",
            tags=["data_cleaning", "column_processing"]
        ),
        node(
            func=clean_column_names,
            inputs=["df2_raw", "params:data_preparation"],
            outputs="df2_cleaned",
            name="clean_df2_columns",
            tags=["data_cleaning", "column_processing"]
        ),
        node(
            func=clean_column_names,
            inputs=["df3_raw", "params:data_preparation"],
            outputs="df3_cleaned",
            name="clean_df3_columns",
            tags=["data_cleaning", "column_processing"]
        ),
        node(
            func=clean_column_names,
            inputs=["df4_raw", "params:data_preparation"],
            outputs="df4_cleaned",
            name="clean_df4_columns",
            tags=["data_cleaning", "column_processing"]
        ),
        
        # Fase 2: Estandarización de columnas clave
        node(
            func=standardize_key_columns,
            inputs=["df1_cleaned", "params:data_preparation.key_column_mappings"],
            outputs="df1_standardized",
            name="standardize_df1_keys",
            tags=["data_cleaning", "standardization"]
        ),
        node(
            func=standardize_key_columns,
            inputs=["df2_cleaned", "params:data_preparation.key_column_mappings"],
            outputs="df2_standardized",
            name="standardize_df2_keys",
            tags=["data_cleaning", "standardization"]
        ),
        node(
            func=standardize_key_columns,
            inputs=["df3_cleaned", "params:data_preparation.key_column_mappings"],
            outputs="df3_standardized",
            name="standardize_df3_keys",
            tags=["data_cleaning", "standardization"]
        ),
        node(
            func=standardize_key_columns,
            inputs=["df4_cleaned", "params:data_preparation.key_column_mappings"],
            outputs="df4_standardized",
            name="standardize_df4_keys",
            tags=["data_cleaning", "standardization"]
        ),
        
        # Fase 3: Fusión de datasets
        node(
            func=merge_datasets,
            inputs=[
                "df1_standardized",
                "df2_standardized", 
                "df3_standardized",
                "df4_standardized",
                "params:data_preparation.merge_key_column"
            ],
            outputs="merged_dataset",
            name="merge_hospital_datasets",
            tags=["data_integration", "merging"]
        ),
        
        # Fase 4: Ingeniería de características
        node(
            func=create_age_categories,
            inputs=["merged_dataset", "params:data_preparation.age_categories"],
            outputs="dataset_with_age_categories",
            name="create_age_categories",
            tags=["feature_engineering", "categorization"]
        ),
        
        node(
            func=engineer_comorbidity_features,
            inputs=["dataset_with_age_categories", "params:data_preparation.comorbidity_groups"],
            outputs="feature_engineered_dataset",
            name="engineer_comorbidity_features",
            tags=["feature_engineering", "comorbidities"]
        ),
        
        # Fase 5: Análisis de calidad y exploración
        node(
            func=generate_data_quality_report,
            inputs="feature_engineered_dataset",
            outputs="data_quality_report",
            name="generate_quality_report",
            tags=["data_quality", "reporting"]
        ),
        
        node(
            func=create_exploratory_plots,
            inputs=["feature_engineered_dataset", "params:exploratory_analysis"],
            outputs="exploratory_plots",
            name="create_exploratory_analysis",
            tags=["exploration", "visualization"]
        )
    ])