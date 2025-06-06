from dataclasses import dataclass

from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import IntegerType

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.utils import insert_predictions_into_pipeline
from projects._03_independent_cqc._04_feature_engineering.utils.helper import (
    vectorise_dataframe,
)
from utils.ind_cqc_filled_posts_utils.utils import get_selected_value


@dataclass
class ThresholdValues:
    max_absolute_difference: int = 100
    max_percentage_difference: float = 0.5
    months_in_two_years: int = 24


def model_pir_filled_posts(
    df: DataFrame, linear_regression_model_source: str
) -> DataFrame:
    """
    Uses a linear regression model to convert PIR values from 'people directly employed' to estimated 'filled posts'.

    Args:
        df (DataFrame): A dataframe with the column people directly employed deduplicated.
        linear_regression_model_source (str): The location of the linear regression model in s3.

    Returns:
        DataFrame: The input dataframe with an additional column containing the estimated PIR filled posts model predictions.
    """

    non_res_df = utils.select_rows_with_value(
        df, IndCQC.care_home, CareHome.not_care_home
    )
    features_df = utils.select_rows_with_non_null_value(
        non_res_df, IndCQC.pir_people_directly_employed_dedup
    )
    vectorised_features_df = vectorise_dataframe(
        features_df, [IndCQC.pir_people_directly_employed_dedup]
    )
    lr_trained_model = LinearRegressionModel.load(linear_regression_model_source)

    predictions = lr_trained_model.transform(vectorised_features_df)

    df = insert_predictions_into_pipeline(
        df,
        predictions,
        IndCQC.pir_filled_posts_model,
    )
    return df


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
