from utils import utils


def main(
    branch_name: str,
    model_name: str,
    model_version: str,
) -> None:
    """
    Train and save a linear regression model on a given features dataset and model.

    A new version of the model will be saved including the metrics to measure ongoing performance.

    Args:
        branch_name (str): The name of the branch currently being used.
        model_name (str): The name of the model to train.
        model_version (str): The version of the model to use (e.g. '1.0.0').
    """
    print(f"Training model: {model_name} version: {model_version}")


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
            "The name of the model to train",
        ),
        (
            "--model_version",
            "The version of the model to use (e.g. '1.0.0')",
        ),
    )
    main(
        branch_name,
        model_name,
        model_version,
    )
