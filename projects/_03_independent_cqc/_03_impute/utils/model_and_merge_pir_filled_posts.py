from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from projects._03_independent_cqc.utils.utils.utils import get_selected_value
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


@dataclass
class ThresholdValues:
    max_absolute_difference: int = 100
    max_percentage_difference: float = 0.5
    months_in_two_years: int = 24


posts_col = IndCQC.ascwds_filled_posts_dedup_clean
people_col = IndCQC.pir_people_directly_employed_dedup


# converted to polars -> projects\_03_independent_cqc\_03_impute\fargate\utils\convert_pir_people_to_filled_posts.py
def convert_pir_to_filled_posts(df: DataFrame) -> DataFrame:
    """
    Converts PIR people to filled posts using a global ratio.

    The ratio is calculated using a filtered subset of valid rows and then
    applied only to non-care home locations where PIR people is present.

    Args:
        df (DataFrame): A dataframe with PIR people and ASC-WDS filled posts.

    Returns:
        DataFrame: The input dataframe with estimated PIR filled posts.
    """

    ratio = compute_global_ratio(df)
    print(f"PIR people to filled posts ratio: {ratio:.4f}")

    return df.withColumn(
        IndCQC.pir_filled_posts_model,
        F.when(
            (F.col(IndCQC.care_home) == CareHome.not_care_home)
            & F.col(people_col).isNotNull()
            & (F.col(people_col) > 0),
            F.col(people_col) * F.lit(ratio),
        ).otherwise(None),
    )


# converted to polars -> projects\_03_independent_cqc\_03_impute\fargate\utils\convert_pir_people_to_filled_posts.py
def compute_global_ratio(
    df: DataFrame, ratio_cutoff: float = 2.0, abs_diff_cutoff: float = 50.0
) -> float:
    """
    Computes the global ratio of filled posts to PIR people using only valid rows.
    """
    ratio = F.col(posts_col) / F.col(people_col)
    abs_diff = F.abs(F.col(posts_col) - F.col(people_col))

    lower_ratio_cutoff = 1 / ratio_cutoff
    upper_ratio_cutoff = ratio_cutoff

    agg_df = df.filter(
        (F.col(IndCQC.care_home) == CareHome.not_care_home)
        & F.col(people_col).isNotNull()
        & (F.col(people_col) > 0)
        & F.col(posts_col).isNotNull()
        & (F.col(posts_col) > 0)
        & (
            ((ratio >= lower_ratio_cutoff) & (ratio <= upper_ratio_cutoff))
            | (abs_diff <= abs_diff_cutoff)
        )
    ).select(
        [
            F.sum(F.col(posts_col)).alias("posts_sum"),
            F.sum(F.col(people_col)).alias("people_sum"),
        ]
    )
    result = agg_df.collect()

    posts_sum = result[0]["posts_sum"]
    people_sum = result[0]["people_sum"]

    if people_sum == 0 or posts_sum is None:
        raise ValueError("No valid rows available to compute PIR ratio.")

    return posts_sum / people_sum


# TO DO - write polars code here - projects\_03_independent_cqc\_03_impute\fargate\utils\combine_ascwds_and_pir.py
def merge_ascwds_and_pir_filled_post_submissions(df: DataFrame) -> DataFrame:
    """
    Merges ASCWDS and PIR filled post estimates based on recently and similarity thresholds and stores in a new column.

    The ASCWDS dataset is the preferred source for workforce filled post figures.
    However, if a workplace has not submitted ASCWDS data for a prolonged period of time and the corresponding PIR
    submission differs significantly (both in absolute and percentage terms) then the PIR figure is used instead.
    This ensures that downstream imputation uses the most reliable and recent data.

    If the PIR and ASCWDS values are within acceptable difference thresholds, the older ASCWDS value is retained.
    This avoids introducing noise when the PIR and ASCWDS values are effectively aligned.

    Args:
        df (DataFrame): Input PySpark DataFrame containing filled posts from ASCWDS and PIR and their respective submission dates.

    Returns:
        DataFrame: A DataFrame with an additional column `ascwds_pir_merged` that contains either the ASCWDS or PIR filled posts value,
                   depending on submission recency and similarity thresholds.
    """
    df = create_repeated_ascwds_clean_column(df)
    df = create_last_submission_columns(df)
    df = create_ascwds_pir_merged_column(df)
    df = include_pir_if_never_submitted_ascwds(df)
    df = drop_temporary_columns(df)
    return df


def create_repeated_ascwds_clean_column(df: DataFrame) -> DataFrame:
    """
    Creates a column containing cleaned ascwds filled posts filled forwards.

    This column is needed to compare to people directly employed figures to see where they diverge.

    Args:
        df (DataFrame): A dataframe with cleaned ascwds data

    Returns:
        DataFrame: A dataframe with an extra column containing ascwds filled posts filled forwards.
    """
    w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_dedup_clean_repeated,
        F.last(IndCQC.ascwds_filled_posts_dedup_clean, ignorenulls=True).over(w),
    )
    return df


def create_last_submission_columns(df: DataFrame) -> DataFrame:
    """
    Creates columns containing the latest submission dates by location id for ascwds and pir data.

    This column is needed to identify whether there has been a gap of at least two years
    between an ascwds submission and a pir submission.

    Args:
        df (DataFrame): A dataframe with ascwds and pir data.

    Returns:
        DataFrame: A dataframe with two extra columns containing the latest submission dates.
    """
    w = (
        Window.partitionBy(IndCQC.location_id)
        .orderBy(IndCQC.cqc_location_import_date)
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = get_selected_value(
        df,
        w,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.cqc_location_import_date,
        IndCQC.last_ascwds_submission,
        "last",
    )
    df = get_selected_value(
        df,
        w,
        IndCQC.pir_filled_posts_model,
        IndCQC.cqc_location_import_date,
        IndCQC.last_pir_submission,
        "last",
    )

    return df


def create_ascwds_pir_merged_column(df: DataFrame) -> DataFrame:
    """
    Merges ASCWDS and PIR filled post estimates based on recently and similarity thresholds and stores in a new column.

    The ASCWDS dataset is the preferred source for workforce filled post figures.
    However, if a workplace has not submitted ASCWDS data for a prolonged period of time and the corresponding PIR
    submission differs significantly (both in absolute and percentage terms) then the PIR figure is used instead.

    If the PIR and ASCWDS values are within acceptable difference thresholds, the older ASCWDS value is retained.

    Args:
        df (DataFrame): Input PySpark DataFrame containing filled posts from ASCWDS and PIR and their respective submission dates.

    Returns:
        DataFrame: A DataFrame with an additional column `ascwds_pir_merged` that contains either the ASCWDS or PIR filled posts value,
                   depending on submission recency and similarity thresholds.
    """
    time_between_last_pir_and_ascwds_submissions = F.months_between(
        F.col(IndCQC.last_pir_submission),
        F.col(IndCQC.last_ascwds_submission),
    )
    absolute_difference_between_pir_and_ascwds = F.abs(
        F.col(IndCQC.pir_filled_posts_model)
        - F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    )
    average_of_pir_and_ascwds = (
        F.col(IndCQC.pir_filled_posts_model)
        + F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
    ) / 2

    df = df.withColumn(
        IndCQC.ascwds_pir_merged,
        F.when(
            (
                time_between_last_pir_and_ascwds_submissions
                > ThresholdValues.months_in_two_years
            )
            & (
                absolute_difference_between_pir_and_ascwds
                > ThresholdValues.max_absolute_difference
            )
            & (
                (absolute_difference_between_pir_and_ascwds / average_of_pir_and_ascwds)
                > ThresholdValues.max_percentage_difference
            ),
            F.col(IndCQC.pir_filled_posts_model),
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )
    return df


def include_pir_if_never_submitted_ascwds(df: DataFrame) -> DataFrame:
    """
    Updates the 'ascwds_pir_merged' column to use PIR filled posts model values for locations who have never submitted ASC-WDS data.

    Args:
        df (DataFrame): Input DataFrame with columns:
            - location_id (str)
            - ascwds_pir_merged (double)
            - pir_filled_posts_model (double)

    Returns:
        DataFrame: DataFrame with updated 'ascwds_pir_merged' values
                   where applicable.
    """
    w = Window.partitionBy(IndCQC.location_id)

    df = df.withColumn(
        IndCQC.submitted_ascwds_data,
        F.max(F.col(IndCQC.ascwds_pir_merged).isNotNull().cast(IntegerType())).over(w),
    )
    df = df.withColumn(
        IndCQC.ascwds_pir_merged,
        F.when(
            F.col(IndCQC.submitted_ascwds_data) == 0,
            F.col(IndCQC.pir_filled_posts_model),
        ).otherwise(F.col(IndCQC.ascwds_pir_merged)),
    ).drop(IndCQC.submitted_ascwds_data)

    return df


def drop_temporary_columns(df: DataFrame) -> DataFrame:
    """
    Drops temporary columns from the blend pir and ascwds function.

    Args:
        df (DataFrame): A dataframe which has just had the ascwds and pir data blended.

    Returns:
        DataFrame: A dataframe with temporary columns removed.
    """
    df = df.drop(
        IndCQC.last_ascwds_submission,
        IndCQC.last_pir_submission,
        IndCQC.ascwds_filled_posts_dedup_clean_repeated,
    )
    return df
