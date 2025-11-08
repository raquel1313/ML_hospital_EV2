
# src/hospital_mortality_analysis/pipelines/classification/pipeline.py
from kedro.pipeline import Pipeline, node
from . import nodes

def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=nodes.train_random_forest,
                inputs=["X_train", "y_train"],
                outputs="rf_model",
                name="train_rf_node"
            ),
            node(
                func=nodes.predict_model,
                inputs=["rf_model", "X_test"],
                outputs="rf_predictions",
                name="predict_rf_node"
            ),
            node(
                func=nodes.evaluate_model,
                inputs=["y_test", "rf_predictions"],
                outputs="rf_metrics",
                name="evaluate_rf_node"
            ),
            node(
                func=nodes.save_model,
                inputs=["rf_model", "params:model_output_path"],
                outputs=None,
                name="save_rf_node"
            ),
        ]
    )
