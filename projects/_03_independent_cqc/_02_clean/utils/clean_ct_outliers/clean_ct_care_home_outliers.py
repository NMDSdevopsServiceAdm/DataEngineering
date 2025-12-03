from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition import (
    clean_ct_values_after_consecutive_repetition,
)
from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.null_posts_per_bed_ratio_outliers import (
    null_posts_per_bed_outliers,
)
from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    add_filtering_rule_column,
    aggregate_values_to_provider_level,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTCareHomeFilteringRule


def clean_capacity_tracker_care_home_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of Capacity Tracker care home data including a filtering rule column.

    This function first duplicates the original data into a cleaned column and then runs
    various functions designed to clean outlier values.

    Args:
        df (DataFrame): A dataframe containing `ct_care_home_total_employed`.

    Returns:
        DataFrame: A dataframe containing `ct_care_home_total_employed` and `ct_care_home_total_employed_cleaned`.
    """
    print("Cleaning Capacity Tracker care home data...")

    df = df.withColumn(
        IndCQC.ct_care_home_total_employed_cleaned,
        F.col(IndCQC.ct_care_home_total_employed),
    )
    df = add_filtering_rule_column(
        df,
        IndCQC.ct_care_home_filtering_rule,
        IndCQC.ct_care_home_total_employed_cleaned,
        CTCareHomeFilteringRule.populated,
        CTCareHomeFilteringRule.missing_data,
    )

    df = aggregate_values_to_provider_level(df, IndCQC.ct_care_home_total_employed)

    df = null_posts_per_bed_outliers(df)

    df = clean_ct_values_after_consecutive_repetition(
        df,
        IndCQC.ct_care_home_total_employed_cleaned,
        IndCQC.ct_care_home_total_employed_cleaned,
        True,
        IndCQC.location_id,
    )

    return df
