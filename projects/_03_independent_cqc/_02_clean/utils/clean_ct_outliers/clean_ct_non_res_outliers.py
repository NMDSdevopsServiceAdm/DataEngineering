from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.clean_ct_outliers.clean_ct_longitudinal_outliers import (
    clean_longitudinal_outliers,
)
from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    add_filtering_rule_column,
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

    # TODO - #1226 filter repeated values

    df = clean_longitudinal_outliers(
        df=df,
        group_by_col=IndCQC.location_id,
        col_to_clean=IndCQC.ct_non_res_care_workers_employed,
        cleaned_column_name=IndCQC.ct_non_res_care_workers_employed_cleaned,
        proportion_to_filter=0.05,
        remove_whole_record=False,
        care_home=False,
    )

    return df
