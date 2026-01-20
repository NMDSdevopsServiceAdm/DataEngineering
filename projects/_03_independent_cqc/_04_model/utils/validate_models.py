import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


def get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy(
    df: pl.DataFrame,
) -> int:
    """
    Returns the expected row count for validation of the model_01_features_non_res_with_dormancy dataset.
    This function tries to replicate the feature creation process to get the row count.

    Args:
        df (pl.DataFrame): compare Dataframe to get expect row count from

    Returns:
        int: The expected row count after performing minimum set of feature creation steps.
    """
    df = df.filter(
        pl.col(IndCQC.care_home) == CareHome.care_home,
        pl.col(IndCQC.dormancy).is_not_null(),
    )
    row_count = df.height

    return row_count
