import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


def get_expected_row_count_for_validation_model_01_features_non_res_with_dormancy(
    df: pl.DataFrame, features_list: list[str]
) -> int:
    """
    Returns the expected row count for validation of the model_01_features_non_res_with_dormancy dataset.
    This function tries to replicate the feature creation process to get the row count.

    Args:
        df (pl.DataFrame): compare Dataframe to get expect row count from
        features_list (list[str]): a list of columns representing the model features

    Returns:
        int: The expected row count after performing minimum set of feature creation steps.
    """
    df = df.with_columns(pl.col(IndCQC.time_since_dormant).fill_null(999))
    df = df.filter(
        pl.col(IndCQC.care_home) == CareHome.care_home,
        pl.col(IndCQC.dormancy).is_not_null(),
        pl.col(IndCQC.regulated_activities_offered).is_not_null(),
        pl.col(IndCQC.cqc_location_import_date).is_not_null(),
        pl.col(IndCQC.posts_rolling_average_model).is_not_null(),
        pl.col(IndCQC.services_offered).is_not_null(),
        pl.col(IndCQC.specialisms_offered).is_not_null(),
        pl.col(IndCQC.current_rural_urban_indicator_2011).is_not_null(),
        pl.col(IndCQC.current_region).is_not_null(),
        pl.col(IndCQC.related_location).is_not_null(),
        pl.col(IndCQC.time_registered).is_not_null(),
        pl.col(IndCQC.time_since_dormant).is_not_null(),
        # pl.all_horizontal([pl.col(feature).is_not_null() for feature in features_list]),
    )
    row_count = df.height

    return row_count
