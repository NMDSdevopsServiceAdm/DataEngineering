import sys

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts.models import utils as mUtils
from utils import utils


def main(
    features_source: str,
    model_source: str,
    dependent_variable: str = IndCQC.imputed_filled_post_model,
) -> None:
    """
    Train and save a linear regression model on a given features dataset and dependent variable.

    A new version of the model will be saved in the model_source directory with a new run number.

    Args:
        features_source (str): Source s3 directory for parquet features dataset.
        model_source (str): S3 path to models (e.g. 's3://pipeline-resources/models/prediction/1.0.0/').
        dependent_variable (str): Name of the column which is the dependent variable for the model to be trained.
    """
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
        model_source,
        dependent_variable,
    ) = utils.collect_arguments(
        (
            "--features_source",
            "Source s3 directory for parquet features dataset",
        ),
        (
            "--model_source",
            "S3 path to models (e.g. 's3://pipeline-resources/models/prediction/1.0.0/').",
        ),
        (
            "--dependent_variable",
            "Name of the column which is the dependent variable for the model to be trained",
        ),
    )
    main(
        features_source,
        model_source,
        dependent_variable,
    )

    print("Spark job 'train_linear_regression_model' complete")
