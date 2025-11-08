import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import logging

logger = logging.getLogger(__name__)

def split_data(data: pd.DataFrame, params: dict):
    """Divide los datos en train y test, manteniendo solo columnas numéricas y eliminando NaNs"""
    target_col = params['target_column']
    test_size = params.get('test_size', 0.2)
    random_state = params.get('random_state', 42)
    
    # ✅ 1. Eliminar filas donde el target es NaN
    data_clean = data.dropna(subset=[target_col])
    logger.info(f"Filas eliminadas por NaN en target: {len(data) - len(data_clean)}")
    
    # ✅ 2. Separar el target
    y = data_clean[target_col]
    
    # ✅ 3. Eliminar el target de las features
    X = data_clean.drop(columns=[target_col])
    
    # ✅ 4. Seleccionar SOLO columnas numéricas
    numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()
    X_numeric = X[numeric_cols]
    
    # ✅ 5. Eliminar filas con NaN en features (opcional pero recomendado)
    mask = ~X_numeric.isna().any(axis=1)
    X_numeric = X_numeric[mask]
    y = y[mask]
    
    logger.info(f"Features numéricas seleccionadas: {len(numeric_cols)}")
    logger.info(f"Columnas eliminadas (no numéricas): {len(X.columns) - len(numeric_cols)}")
    logger.info(f"Filas finales después de limpieza: {len(X_numeric)}")
    
    # ✅ 6. Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X_numeric, y, test_size=test_size, random_state=random_state
    )
    
    logger.info(f"Datos divididos: Train={len(X_train)}, Test={len(X_test)}")
    return X_train, X_test, y_train, y_test

def train_model(X_train: pd.DataFrame, y_train: pd.Series, params: dict):
    """Entrena el modelo de regresión"""
    model = RandomForestRegressor(**params)
    model.fit(X_train, y_train)
    logger.info("Modelo de RandomForest entrenado exitosamente")
    return model

def evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series):
    """Evalúa el modelo y retorna métricas"""
    y_pred = model.predict(X_test)
    
    metrics = {
        'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
        'mae': float(mean_absolute_error(y_test, y_pred)),
        'r2': float(r2_score(y_test, y_pred)),
        'mse': float(mean_squared_error(y_test, y_pred))
    }
    
    logger.info(f"Métricas del modelo de regresión:")
    logger.info(f"  - RMSE: {metrics['rmse']:.4f}")
    logger.info(f"  - MAE: {metrics['mae']:.4f}")
    logger.info(f"  - R²: {metrics['r2']:.4f}")
    
    return metrics