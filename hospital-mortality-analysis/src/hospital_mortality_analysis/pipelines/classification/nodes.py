
# src/hospital_mortality_analysis/pipelines/classification/nodes.py
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
import joblib

def train_random_forest(X_train, y_train):
    rf = RandomForestClassifier(class_weight='balanced', random_state=42)
    rf.fit(X_train, y_train)
    return rf

def predict_model(model, X_test):
    return model.predict(X_test)

def evaluate_model(y_test, y_pred):
    report = classification_report(y_test, y_pred, output_dict=True)
    return report

def save_model(model, path):
    joblib.dump(model, path)
