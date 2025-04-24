import sys

from utils.estimate_filled_posts.models import utils as mUtils
from utils import utils


def main(
    features_source: str,
    dependent_variable: str,
    model_source: str,
) -> None:
    print("Training linear regression model...")

    features_df = utils.read_from_parquet(features_source)

    trained_lr_model = mUtils.train_lasso_regression_model(
        features_df, dependent_variable
    )

    mUtils.save_model_to_s3(trained_lr_model, model_source)


if __name__ == "__main__":
    print("Spark job 'train_linear_regression_model' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        features_source,
        dependent_variable,
        model_source,
    ) = utils.collect_arguments(
        (
            "--features_source",
            "Source s3 directory for parquet CQC pir cleaned dataset",
        ),
        (
            "--dependent_variable",
            "Name of the column which is the dependent variable for the model to be trained",
        ),
        (
            "--model_source",
            "Source s3 directory for the model",
        ),
    )
    main(
        features_source,
        dependent_variable,
        model_source,
    )

    print("Spark job 'train_linear_regression_model' complete")
