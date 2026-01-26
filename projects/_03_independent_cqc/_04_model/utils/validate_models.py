from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome

non_res_with_dormancy_cols_for_features = [
    IndCQC.care_home_status_count,
    IndCQC.dormancy,
    IndCQC.regulated_activities_offered,
    IndCQC.cqc_location_import_date,
    IndCQC.posts_rolling_average_model,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_region,
    IndCQC.related_location,
    IndCQC.time_registered,
    IndCQC.time_since_dormant,
]

non_res_without_dormancy_cols_for_features = [
    IndCQC.posts_rolling_average_model,
    IndCQC.care_home_status_count,
    IndCQC.cqc_location_import_date,
    IndCQC.time_registered,
    IndCQC.services_offered,
    IndCQC.regulated_activities_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_region,
    IndCQC.related_location,
]

care_home_cols_for_features = [
    IndCQC.care_home_status_count,
    IndCQC.care_home,
    IndCQC.regulated_activities_offered,
    IndCQC.cqc_location_import_date,
    IndCQC.posts_rolling_average_model,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_region,
]


def get_expected_row_count_for_model_features(df: pl.DataFrame, model: str) -> int:
    """
    Returns the expected row count for validation of the model features dataset.
    This function tries to replicate the feature creation process to get the row count.

    Args:
        df (pl.DataFrame): compare Dataframe to get expect row count from
        model (str): the model for which the features were created. Thiss affect which variables must be non-null.

    Returns:
        int: The expected row count after performing minimum set of feature creation steps.

    Raises:
        ValueError: Raises a value error if the model provided is not known to the function.
    """
    if model == "non_res_with_dormancy_model":
        df = df.with_columns(pl.col(IndCQC.time_since_dormant).fill_null(999))
        df = df.filter(
            pl.col(IndCQC.care_home) == CareHome.not_care_home,
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in non_res_with_dormancy_cols_for_features
                ]
            ),
        )
    elif model == "non_res_without_dormancy_model":
        df = df.filter(
            pl.col(IndCQC.care_home) == CareHome.not_care_home,
            pl.col(IndCQC.cqc_location_import_date) < date(2025, 1, 1),
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in non_res_without_dormancy_cols_for_features
                ]
            ),
        )
    elif model == "care_home_model":
        df = df.filter(
            pl.col(IndCQC.care_home) == CareHome.care_home,
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in care_home_cols_for_features
                ]
            ),
        )
    else:
        raise ValueError(f"{model} is not a recognised model in the pipeline.")
    row_count = df.height
    return row_count
