from utils import utils


def main(
    branch_name: str,
    model_name: str,
    model_version: str,
) -> None:
    """
    Loads and runs a linear regression model then saves the predictions as parquet files.

    This function loads and runs a linear regression model on features dataset.
    If the model is a care home model (which predicts the filled posts per bed ratio), it
    multiplies the prediction by the number of beds to get the filled post equivalent prediction.
    Predictions are capped at a minimum of one.
    The predictions are then saved as parquet files.

    Args:
        branch_name (str): The name of the branch currently being used.
        model_name (str): The name of the model to run.
        model_version (str): The version of the model to load (e.g. '1.0.0').
    """
    print(f"Running model: {model_name} version: {model_version}")


if __name__ == "__main__":
    (
        branch_name,
        model_name,
        model_version,
    ) = utils.collect_arguments(
        (
            "--branch_name",
            "The name of the branch currently being used",
        ),
        (
            "--model_name",
            "The name of the model to run",
        ),
        (
            "--model_version",
            "The version of the model to load (e.g. '1.0.0')",
        ),
    )
    main(
        branch_name,
        model_name,
        model_version,
    )
