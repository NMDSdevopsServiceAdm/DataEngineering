import sys

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.estimate_filled_posts.models import utils as mUtils
from utils.estimate_filled_posts.ml_model_metrics import save_model_metrics
from utils import utils


def main(
    features_source: str,
    model_source: str,
    metrics_destination: str,
    is_care_home_model: bool = False,
) -> None:
    """
    Train and save a linear regression model on a given features dataset and dependent variable.

    A new version of the model will be saved in the model_source directory with a new run number.

    Args:
        features_source (str): Source s3 directory for parquet features dataset.
        model_source (str): S3 path to models (e.g. 's3://pipeline-resources/models/prediction/1.0.0/').
        metrics_destination (str): S3 path to save model metrics.
        is_care_home_model (bool): Flag indicating if the model is for care homes or not which determines the dependent variable.
    """
    if is_care_home_model:
        dependent_variable = IndCQC.imputed_filled_posts_per_bed_ratio_model
    else:
        dependent_variable = IndCQC.imputed_filled_post_model

    features_df = utils.read_from_parquet(features_source)

    features_df = utils.select_rows_with_non_null_value(dependent_variable)

    train_df, test_df = mUtils.create_test_and_train_datasets(
        features_df, test_ratio=0.2, seed=42
    )

    trained_lr_model = mUtils.train_lasso_regression_model(train_df, dependent_variable)
    saved_model_path = mUtils.save_model_to_s3(trained_lr_model, model_source)

    save_model_metrics(
        test_df,
        dependent_variable,
        saved_model_path,
        metrics_destination,
    )


if __name__ == "__main__":
    print("Spark job 'train_linear_regression_model' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        features_source,
        model_source,
        metrics_destination,
        is_care_home_model,
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
            "--metrics_destination",
            "S3 destination path to save model metrics",
        ),
        (
            "--is_care_home_model",
            "Flag indicating if the model is for care homes or not which determines the dependent variable",
        ),
    )
    main(
        features_source,
        model_source,
        metrics_destination,
        is_care_home_model,
    )

    print("Spark job 'train_linear_regression_model' complete")
