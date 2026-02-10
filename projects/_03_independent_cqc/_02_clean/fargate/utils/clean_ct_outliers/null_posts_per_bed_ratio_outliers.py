from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from projects._03_independent_cqc._02_clean.utils.filtering_utils import (
    update_filtering_rule,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CTCareHomeFilteringRule

MINIMUM_RATIO_CUTOFF: float = 0.66
MAXIMUM_RATIO_CUTOFF: float = 6.0


def null_posts_per_bed_outliers(df: DataFrame) -> DataFrame:
    """
    Nulls capacity tracker values where posts to bed ratio is out of bounds.

    Analysis of Capacity Tracker posts to bed ratio between April 2024 and September 2025 showed
    95% of locations per month have a ratio between 0.66 and 6.00.

    The filtering rule column is updated to 'beds_ratio_outlier' where outliers have been nulled.

    Args:
        df(DataFrame): A dataframe with ct_care_home_posts_per_bed_ratio column.

    Returns:
        DataFrame: A dataframe with ct_care_home_total_employed_cleaned nulled if outside of ratio cutoffs.
    """

    df = df.withColumn(
        IndCQC.ct_care_home_total_employed_cleaned,
        F.when(
            (F.col(IndCQC.ct_care_home_posts_per_bed_ratio).isNull())
            | (
                (F.col(IndCQC.ct_care_home_posts_per_bed_ratio) > MINIMUM_RATIO_CUTOFF)
                & (
                    F.col(IndCQC.ct_care_home_posts_per_bed_ratio)
                    < MAXIMUM_RATIO_CUTOFF
                )
            ),
            F.col(IndCQC.ct_care_home_total_employed_cleaned),
        ).otherwise(None),
    )

    df = update_filtering_rule(
        df,
        IndCQC.ct_care_home_filtering_rule,
        IndCQC.ct_care_home_total_employed,
        IndCQC.ct_care_home_total_employed_cleaned,
        CTCareHomeFilteringRule.populated,
        CTCareHomeFilteringRule.beds_ratio_outlier,
    )

    return df
