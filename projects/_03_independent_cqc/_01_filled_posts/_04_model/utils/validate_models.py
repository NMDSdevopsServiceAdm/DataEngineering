from datetime import date

import polars as pl

from polars_utils.expressions import is_care_home, is_not_care_home
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

non_res_with_dormancy_cols_for_features = [
    IndCQC.dormancy,
    IndCQC.regulated_activities_offered,
    IndCQC.cqc_location_import_date,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_region,
    IndCQC.related_location,
    IndCQC.time_registered,
    IndCQC.time_since_dormant,
]

non_res_without_dormancy_cols_for_features = [
    IndCQC.regulated_activities_offered,
    IndCQC.cqc_location_import_date,
    IndCQC.posts_rolling_average_model,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_region,
    IndCQC.related_location,
    IndCQC.time_registered,
]

care_home_cols_for_features = [
    IndCQC.care_home,
    IndCQC.regulated_activities_offered,
    IndCQC.cqc_location_import_date,
    IndCQC.number_of_beds,
    IndCQC.banded_bed_ratio_rolling_average_model,
    IndCQC.services_offered,
    IndCQC.specialisms_offered,
    IndCQC.current_rural_urban_indicator_2011,
    IndCQC.current_region,
]


def get_expected_row_count_for_model_features(lf: pl.LazyFrame, model: str) -> int:
    """
    Returns the expected row count for validation of the model features dataset.

    This function tries to replicate the feature creation process to get the row count,
    so each model's list of non-null columns should only include columns that model uses.

    The input is kept lazy so that only the columns used by the filters are read from the
    wide comparison dataset, and rows are streamed as only their count is collected.

    Args:
        lf (pl.LazyFrame): comparison LazyFrame to derive the expected row count from
        model (str): the model for which the features were created. This affects which columns must be non-null.

    Returns:
        int: The expected row count after performing minimum set of feature creation steps.

    Raises:
        ValueError: Raises a value error if the model provided is not known to the function.
    """
    if model == "non_res_with_dormancy_model":
        lf = lf.with_columns(pl.col(IndCQC.time_since_dormant).fill_null(999))
        lf = lf.filter(
            is_not_care_home(),
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in non_res_with_dormancy_cols_for_features
                ]
            ),
        )
    elif model == "non_res_without_dormancy_model":
        lf = lf.filter(
            is_not_care_home(),
            pl.col(IndCQC.cqc_location_import_date) < date(2025, 1, 1),
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in non_res_without_dormancy_cols_for_features
                ]
            ),
        )
    elif model == "care_home_model":
        lf = lf.filter(
            is_care_home(),
            pl.all_horizontal(
                [
                    pl.col(col_for_features).is_not_null()
                    for col_for_features in care_home_cols_for_features
                ]
            ),
        )
    else:
        raise ValueError(f"{model} is not a recognised model in the pipeline.")
    return lf.select(pl.len()).collect(engine="streaming").item()
