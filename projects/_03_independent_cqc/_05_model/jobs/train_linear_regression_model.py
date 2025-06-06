from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from projects._03_independent_cqc._06_estimate_filled_posts.utils.models import (
    utils as mUtils,
)
from projects._03_independent_cqc._05_model.utils.model_metrics import (
    save_model_metrics,
)
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

    care_home_identifier: str = "care_home"

    if care_home_identifier in model_name:
        dependent_variable = IndCQC.imputed_filled_posts_per_bed_ratio_model
    else:
        dependent_variable = IndCQC.imputed_filled_post_model

    features_source = mUtils.generate_model_features_s3_path(branch_name, model_name)
    model_s3_location = mUtils.generate_model_s3_path(
        branch_name, model_name, model_version
    )

    features_df = utils.read_from_parquet(features_source)

    features_df = utils.select_rows_with_non_null_value(dependent_variable)

    train_df, test_df = mUtils.create_test_and_train_datasets(
        features_df, test_ratio=0.2, seed=42
    )

    trained_lr_model = mUtils.train_lasso_regression_model(
        train_df, dependent_variable, model_name
    )

    model_run_number = mUtils.save_model_to_s3(trained_lr_model, model_s3_location)

    save_model_metrics(
        trained_lr_model,
        test_df,
        dependent_variable,
        branch_name,
        model_name,
        model_version,
        model_run_number,
    )


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
