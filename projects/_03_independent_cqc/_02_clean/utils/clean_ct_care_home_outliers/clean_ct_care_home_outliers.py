from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

MINIMUM_RATIO_CUTOFF: float = 0.66
MAXIMUM_RATIO_CUTOFF: float = 6.0


def null_ct_posts_to_beds_outliers(df: DataFrame) -> DataFrame:
    """
    Nulls capacity tracker values where posts to bed ratio is out of bounds.

    Analysis of Capacity Tracker posts to bed ratio between April 2024 and September 2025 showed
    95% of locations per month have a ratio between 0.66 and 6.00.

    This function copies ct_care_home_total_employed into ct_care_home_total_employed_cleaned
    when the locations posts to bed ratio is inside 0.66 to 6.00 or ratio is null.

    Args:
        df(DataFrame): A dataframe with ct_care_home_posts_per_bed_ratio column.

    Returns:
        DataFrame: The same dataframe with ct_care_home_total_employed_cleaned.
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
            F.col(IndCQC.ct_care_home_total_employed),
        ).otherwise(None),
    )

    return df
