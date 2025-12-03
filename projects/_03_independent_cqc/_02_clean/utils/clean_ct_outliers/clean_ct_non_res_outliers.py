from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_repetition import (
    clean_ct_values_after_consecutive_repetition,
)
from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    add_filtering_rule_column,
    aggregate_values_to_provider_level,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTNonResFilteringRule


def clean_capacity_tracker_non_res_outliers(df: DataFrame) -> DataFrame:
    """
    Creates a clean version of Capacity Tracker non residential data including a filtering rule column.

    This function first duplicates the original data into a cleaned column and then runs
    various functions designed to clean outlier values.

    Args:
        df (DataFrame): A dataframe containing `ct_non_res_care_workers_employed`.

    Returns:
        DataFrame: A dataframe containing `ct_non_res_care_workers_employed` and `ct_non_res_filtering_rule`.
    """
    print("Cleaning Capacity Tracker non-residential data...")

    df = df.withColumn(
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        F.col(IndCQC.ct_non_res_care_workers_employed),
    )
    df = add_filtering_rule_column(
        df,
        IndCQC.ct_non_res_filtering_rule,
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        CTNonResFilteringRule.populated,
        CTNonResFilteringRule.missing_data,
    )

    df = aggregate_values_to_provider_level(df, IndCQC.ct_non_res_care_workers_employed)

    # TODO - #1225 filter spikes

    df = clean_ct_values_after_consecutive_repetition(
        df,
        IndCQC.ct_non_res_care_workers_employed,
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        False,
        IndCQC.location_id,
    )

    return df
