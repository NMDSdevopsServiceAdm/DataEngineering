from dataclasses import dataclass

from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import FloatType


from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.insert_predictions_into_pipeline import (
    insert_predictions_into_pipeline,
)
from utils.features.helper import vectorise_dataframe
from utils.ind_cqc_filled_posts_utils.utils import (
    get_selected_value,
)


@dataclass
class ThresholdValues:
    max_percentage_difference: float = 0.5
    max_absolute_difference: int = 100
    months_in_two_years: int = 24


def blend_pir_and_ascwds_when_ascwds_out_of_date(
    df: DataFrame, linear_regression_model_source: str
) -> DataFrame:
    """
    Merges people directly employed and ascwds filled posts cleaned when ascwds
    hasn't been updated recently and people directly employed has.

    This function handles the individual steps for this process.

    Args:
        df (DataFrame): A dataframe with cleaned ascwds data and deduplicated pir data.
        linear_regression_model_source (str): The location of the linear regression model in s3.

    Returns:
        DataFrame: A dataframe with people directly employed filled posts merged into ascwds values for estimatation.
    """
    df = create_repeated_ascwds_clean_column(df)
    df = create_pir_people_directly_employed_dedup_modelled_column(
        df, linear_regression_model_source
    )
    df = create_last_submission_columns(df)
    df = merge_pir_people_directly_employed_modelled_into_ascwds_clean_column(df)
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


def create_pir_people_directly_employed_dedup_modelled_column(
    df: DataFrame, linear_regression_model_source: str
) -> DataFrame:
    """
    Creates a column containing people directly employed deduplicated values converted to filled posts using the non-res pir model.

    This column is needed to compare to ascwds filled posts cleaned to see where they diverge.

    Args:
        df (DataFrame): A dataframe with the column people directly employed deduplicated.
        linear_regression_model_source (str): The location of the linear regression model in s3.

    Returns:
        DataFrame: A dataframe with an extra column containing people directly employed deduplicated values converted to filled posts.
    """

    non_res_df = utils.select_rows_with_value(
        df, IndCQC.care_home, CareHome.not_care_home
    )
    features_df = utils.select_rows_with_non_null_value(
        non_res_df, IndCQC.pir_people_directly_employed_dedup
    )
    vectorised_features_df = vectorise_dataframe(
        df=features_df,
        list_for_vectorisation=[IndCQC.pir_people_directly_employed_dedup],
    )
    lr_trained_model = LinearRegressionModel.load(linear_regression_model_source)

    predictions = lr_trained_model.transform(vectorised_features_df)
    df = insert_predictions_into_pipeline(
        df,
        predictions,
        IndCQC.pir_people_directly_employed_filled_posts,
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
        IndCQC.pir_people_directly_employed_dedup,
        IndCQC.cqc_location_import_date,
        IndCQC.last_pir_submission,
        "last",
    )

    return df


def merge_pir_people_directly_employed_modelled_into_ascwds_clean_column(
    df: DataFrame,
) -> DataFrame:
    """
    Merges filled posts estimate from people directly employed into ascwds clean
    data when ascwds hasn't been updated and the two data sources have very different values.

    Analysis of the datasets has shown that when the two dataset diverge and there is no recent ascwds data,
    then the PIR value is more likely to align with other data sources, so in this case, we should take that
    value into account rather than predicting no change from the ascwds data.

    Args:
        df (DataFrame): A dataframe with ascwds filled posts cleaned and people directly employed converted into a filled posts estimate.

    Returns:
        DataFrame: A dataframe with the people directly employed estimates merged into the ascwds cleaned column.
    """
    df = df.withColumn(
        IndCQC.ascwds_pir_merged,
        df[IndCQC.ascwds_filled_posts_dedup_clean].cast(FloatType()),
    )

    df = df.withColumn(
        IndCQC.ascwds_pir_merged,
        F.when(
            (
                F.months_between(
                    F.col(IndCQC.last_pir_submission),
                    F.col(IndCQC.last_ascwds_submission),
                )
                > ThresholdValues.months_in_two_years
            )
            & (
                F.abs(
                    F.col(IndCQC.pir_people_directly_employed_filled_posts)
                    - F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
                )
                > ThresholdValues.max_absolute_difference
            )
            & (
                F.abs(
                    F.col(IndCQC.pir_people_directly_employed_filled_posts)
                    - F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
                )
                / (
                    (
                        F.col(IndCQC.pir_people_directly_employed_filled_posts)
                        + F.col(IndCQC.ascwds_filled_posts_dedup_clean_repeated)
                    )
                    / 2
                )
                > ThresholdValues.max_percentage_difference
            ),
            F.col(IndCQC.pir_people_directly_employed_filled_posts),
        ).otherwise(F.col(IndCQC.ascwds_filled_posts_dedup_clean)),
    )
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
        IndCQC.pir_people_directly_employed_filled_posts,
    )
    return df
