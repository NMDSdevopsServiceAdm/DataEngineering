from pyspark.sql import DataFrame, functions as F, Window

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import AscwdsFilteringRule
from utils.ind_cqc_filled_posts_utils.null_ascwds_filled_post_outliers.ascwds_filtering_utils import (
    update_filtering_rule,
)


def null_longitudinal_outliers(
    df: DataFrame,
) -> DataFrame:
    """
    Null filled_posts_per_bed_ratio when the value is an outlier for that location.

    A value is defined as an outlier for that location when it is more than 3 standard deviations above or below the mean for that location.

    Args:
        df(DataFrame): A dataframe with the columns filled_posts_per_bed_ratio, ascwds_filled_posts_dedup_clean and location_id.

    Returns:
        DataFrame: A dataframe with longitudinal outliers removed.
    """
    permitted_number_of_standard_deviations_from_mean = 3
    w = Window.partitionBy(IndCQC.location_id).rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    df = df.withColumn(
        IndCQC.location_mean, F.mean(IndCQC.filled_posts_per_bed_ratio).over(w)
    )
    df = df.withColumn(
        IndCQC.standard_deviation,
        F.stddev_samp(IndCQC.filled_posts_per_bed_ratio).over(w),
    )
    df = df.withColumn(
        IndCQC.max_permitted_value,
        F.col(IndCQC.location_mean)
        + (
            F.col(IndCQC.standard_deviation)
            * permitted_number_of_standard_deviations_from_mean
        ),
    )
    df = df.withColumn(
        IndCQC.min_permitted_value,
        F.col(IndCQC.location_mean)
        - (
            F.col(IndCQC.standard_deviation)
            * permitted_number_of_standard_deviations_from_mean
        ),
    )
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean,
        F.when(
            (df[IndCQC.filled_posts_per_bed_ratio] > df[IndCQC.max_permitted_value])
            | (df[IndCQC.filled_posts_per_bed_ratio] < df[IndCQC.min_permitted_value]),
            None,
        ).otherwise(df[IndCQC.ascwds_filled_posts_dedup_clean]),
    )
    df = df.withColumn(
        IndCQC.filled_posts_per_bed_ratio,
        F.when(
            (df[IndCQC.filled_posts_per_bed_ratio] > df[IndCQC.max_permitted_value])
            | (df[IndCQC.filled_posts_per_bed_ratio] < df[IndCQC.min_permitted_value]),
            None,
        ).otherwise(df[IndCQC.filled_posts_per_bed_ratio]),
    )

    df = df.drop(
        *[
            IndCQC.location_mean,
            IndCQC.standard_deviation,
            IndCQC.max_permitted_value,
            IndCQC.min_permitted_value,
        ]
    )
    df = update_filtering_rule(df, AscwdsFilteringRule.longitudinal_outlier)
    return df
