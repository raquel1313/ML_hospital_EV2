from kedro.pipeline import Pipeline, node, pipeline
from .nodes import split_data, train_model, evaluate_model

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=split_data,
            inputs=["feature_engineered_dataset", "params:regression"],
            outputs=["X_train_reg", "X_test_reg", "y_train_reg", "y_test_reg"],
            name="split_regression_data",
        ),
        node(
            func=train_model,
            inputs=["X_train_reg", "y_train_reg", "params:regression.model_params"],
            outputs="regression_model",
            name="train_regression_model",
        ),
        node(
            func=evaluate_model,
            inputs=["regression_model", "X_test_reg", "y_test_reg"],
            outputs="regression_metrics",
            name="evaluate_regression_model",
        ),
    ])