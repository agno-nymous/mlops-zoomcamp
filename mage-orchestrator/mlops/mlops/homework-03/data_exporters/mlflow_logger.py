import mlflow
import pickle

mlflow.set_tracking_uri("http://mlflow:1234")
print("Tracking URI:", mlflow.get_tracking_uri())
mlflow.set_experiment("Linear Regression")

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(artifacts, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    dv, lr = artifacts

    with open("dict_vectorizer.pkl", 'wb') as f:
        pickle.dump(dv, f)
    
    with mlflow.start_run() as run:
        mlflow.sklearn.log_model(
            lr, "linear regression model"
        )

        mlflow.log_artifact("dict_vectorizer.pkl", "Dict Vectorizer")

