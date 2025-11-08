"""
Nodos para el pipeline de preparación de datos hospitalarios
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

def clean_column_names(df: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """
    Limpia los nombres de las columnas del DataFrame
    
    Args:
        df: DataFrame a limpiar
        params: Parámetros de configuración para limpieza
    
    Returns:
        DataFrame con columnas limpias
    """
    df_clean = df.copy()
    
    # Eliminar espacios
    df_clean.columns = df_clean.columns.str.strip()
    
    # Eliminar caracteres específicos
    for char in params['columns_to_remove_chars']:
        df_clean.columns = df_clean.columns.str.replace(char, "", regex=False)
    
    # Reemplazar caracteres
    for old_char, new_char in params['column_replacements'].items():
        df_clean.columns = df_clean.columns.str.replace(old_char, new_char, regex=False)
    
    # Convertir a mayúsculas
    df_clean.columns = df_clean.columns.str.upper()
    
    logger.info(f"Columnas limpiadas: {df_clean.columns.tolist()}")
    return df_clean

def standardize_key_columns(df: pd.DataFrame, mappings: Dict[str, str]) -> pd.DataFrame:
    """
    Estandariza los nombres de columnas clave para el merge
    
    Args:
        df: DataFrame a estandarizar
        mappings: Diccionario de mapeo de nombres de columnas
    
    Returns:
        DataFrame con columnas estandarizadas
    """
    df_standardized = df.copy()
    
    for old_name, new_name in mappings.items():
        if old_name in df_standardized.columns:
            df_standardized = df_standardized.rename(columns={old_name: new_name})
            logger.info(f"Columna renombrada: {old_name} -> {new_name}")
    
    return df_standardized

def merge_datasets(df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame, 
                  df4: pd.DataFrame, merge_key: str) -> pd.DataFrame:
    """
    Fusiona los datasets hospitalarios
    
    Args:
        df1, df2, df3, df4: DataFrames a fusionar
        merge_key: Nombre de la columna clave para el merge
    
    Returns:
        DataFrame fusionado
    """
    # Concatenar datasets 2, 3 y 4
    df_brought_dead = pd.concat([df2, df3, df4], ignore_index=True)
    logger.info(f"Datasets 2-4 concatenados: {df_brought_dead.shape}")
    
    # Fusionar con dataset 1
    df_merged = pd.merge(df1, df_brought_dead, on=merge_key, how="outer")
    logger.info(f"Dataset final fusionado: {df_merged.shape}")
    
    return df_merged

def create_age_categories(df: pd.DataFrame, age_thresholds: Dict[str, int]) -> pd.DataFrame:
    """
    Crea categorías de edad
    
    Args:
        df: DataFrame con columna de edad
        age_thresholds: Umbrales para categorización
    
    Returns:
        DataFrame con nueva columna de categorías de edad
    """
    df_with_categories = df.copy()
    
    def categorize_age(age):
        if pd.isna(age):
            return 'Unknown'
        elif age < age_thresholds['child']:
            return 'Niño'
        elif age < age_thresholds['young']:
            return 'Joven'
        elif age < age_thresholds['adult']:
            return 'Adulto'
        else:
            return 'Mayor'
    
    df_with_categories['AGE_CAT'] = df_with_categories['AGE_x'].apply(categorize_age)
    logger.info("Categorías de edad creadas")
    
    return df_with_categories

def engineer_comorbidity_features(df: pd.DataFrame, 
                                comorbidity_params: Dict[str, Any]) -> pd.DataFrame:
    """
    Crea features de comorbilidades
    
    Args:
        df: DataFrame base
        comorbidity_params: Parámetros de configuración de comorbilidades
    
    Returns:
        DataFrame con nuevas features de comorbilidades
    """
    df_features = df.copy()
    
    for condition_name, config in comorbidity_params.items():
        if 'columns' in config:
            # Múltiples columnas (ej: cardiopatía)
            columns = [col for col in config['columns'] if col in df_features.columns]
            if columns:
                df_features[condition_name.upper()] = (
                    df_features[columns]
                    .fillna(0)
                    .sum(axis=1)
                    .apply(lambda x: 1 if x > config['binary_threshold'] else 0)
                )
                logger.info(f"Feature creada: {condition_name.upper()} de columnas {columns}")
        
        elif 'source_column' in config:
            # Una sola columna
            source_col = config['source_column']
            if source_col in df_features.columns:
                df_features[condition_name.upper()] = (
                    df_features[source_col]
                    .fillna(0)
                    .apply(lambda x: 1 if x > config['binary_threshold'] else 0)
                )
                logger.info(f"Feature creada: {condition_name.upper()} de columna {source_col}")
    
    return df_features

def generate_data_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Genera reporte de calidad de datos
    
    Args:
        df: DataFrame a analizar
    
    Returns:
        Diccionario con métricas de calidad
    """
    report = {
        'shape': df.shape,
        'columns': df.columns.tolist(),
        'data_types': df.dtypes.to_dict(),
        'missing_values': df.isnull().sum().to_dict(),
        'missing_percentage': (df.isnull().sum() / len(df) * 100).to_dict(),
        'duplicate_rows': df.duplicated().sum(),
        'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2
    }
    
    logger.info(f"Reporte de calidad generado para dataset de {df.shape[0]} filas y {df.shape[1]} columnas")
    
    return report

def create_exploratory_plots(df: pd.DataFrame, plot_params: Dict[str, Any]) -> None:
    """
    Crea gráficos exploratorios básicos
    
    Args:
        df: DataFrame para análisis
        plot_params: Parámetros de configuración de plots
    """
    fig_size = plot_params['figure_size']
    
    # Configurar estilo
    plt.style.use('default')
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.suptitle('Análisis Exploratorio - Datos Hospitalarios', fontsize=16)
    
    # 1. Distribución de edad
    if 'AGE_x' in df.columns:
        sns.histplot(df['AGE_x'].dropna(), bins=20, kde=True, 
                    color='skyblue', ax=axes[0,0])
        axes[0,0].set_title('Distribución de Edad')
        axes[0,0].set_xlabel('Edad')
        axes[0,0].set_ylabel('Frecuencia')
    
    # 2. Distribución por género
    if 'GENDER_x' in df.columns:
        sns.countplot(x='GENDER_x', data=df, 
                     palette=plot_params['color_palettes']['default'], ax=axes[0,1])
        axes[0,1].set_title('Distribución por Género')
        axes[0,1].tick_params(axis='x', rotation=45)
    
    # 3. Outcome si existe
    if 'OUTCOME' in df.columns:
        sns.countplot(x='OUTCOME', data=df, 
                     palette=plot_params['color_palettes']['outcome'], ax=axes[1,0])
        axes[1,0].set_title('Distribución de Outcome')
        axes[1,0].tick_params(axis='x', rotation=45)
    
    # 4. Comorbilidades principales
    comorbidity_cols = [col for col in ['CARDIOPATIA', 'DIABETES', 'HIPERTENSION'] 
                       if col in df.columns]
    if comorbidity_cols:
        comorbidity_data = df[comorbidity_cols].sum()
        comorbidity_data.plot(kind='bar', ax=axes[1,1], color='coral')
        axes[1,1].set_title('Prevalencia de Comorbilidades')
        axes[1,1].set_xlabel('Comorbilidad')
        axes[1,1].set_ylabel('Número de Casos')
        axes[1,1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    return fig