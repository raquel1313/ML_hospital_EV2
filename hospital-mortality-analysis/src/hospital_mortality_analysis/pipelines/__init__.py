from .classification.pipeline import create_pipeline as create_classification_pipeline
from .regression.pipeline import create_pipeline as create_regression_pipeline

def register_pipelines():
    return {
        "classification": create_classification_pipeline(),
        "regression": create_regression_pipeline(),
        "__default__": create_classification_pipeline() + create_regression_pipeline()
    }
