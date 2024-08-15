import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import CareHome
from utils.estimate_filled_posts.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.estimate_filled_posts.models.extrapolation import model_extrapolation


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    NUMBER_OF_DAYS_IN_CARE_HOME_ROLLING_AVERAGE = (
        185  # Note: using 185 as a proxy for 6 months
    )
    NUMBER_OF_DAYS_IN_NON_RES_ROLLING_AVERAGE = (
        185  # Note: using 185 as a proxy for 6 months
    )


def main(
    cleaned_ind_cqc_source: str,
    estimated_missing_ascwds_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating missing ASCWDS independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

    print(cleaned_ind_cqc_df.rdd.getNumPartitions())

    estimate_missing_ascwds_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )
    print(estimate_missing_ascwds_df.rdd.getNumPartitions())
    estimate_missing_ascwds_df = model_care_home_posts_per_bed_rolling_average(
        estimate_missing_ascwds_df,
        NumericalValues.NUMBER_OF_DAYS_IN_CARE_HOME_ROLLING_AVERAGE,
        IndCQC.rolling_average_care_home_posts_per_bed_model,
    )
    print(estimate_missing_ascwds_df.rdd.getNumPartitions())
    estimate_missing_ascwds_df = model_non_res_filled_post_rolling_average(
        estimate_missing_ascwds_df,
        NumericalValues.NUMBER_OF_DAYS_IN_NON_RES_ROLLING_AVERAGE,
        IndCQC.rolling_average_non_res_model,
    )
    print(estimate_missing_ascwds_df.rdd.getNumPartitions())
    estimate_missing_ascwds_df = model_extrapolation(
        estimate_missing_ascwds_df, IndCQC.rolling_average_care_home_posts_per_bed_model
    )
    print(estimate_missing_ascwds_df.rdd.getNumPartitions())
    estimate_missing_ascwds_df = model_interpolation(estimate_missing_ascwds_df)
    print(estimate_missing_ascwds_df.rdd.getNumPartitions())
    print(f"Exporting as parquet to {estimated_missing_ascwds_ind_cqc_destination}")

    utils.write_to_parquet(
        estimate_missing_ascwds_df,
        estimated_missing_ascwds_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate missing ASCWDS independent CQC filled posts")


def model_care_home_posts_per_bed_rolling_average(
    df: DataFrame,
    number_of_days: int,
    model_column_name: str,
) -> DataFrame:
    """
    Estimate filled posts for care homes based on the rolling average of filled posts to bed ratio.

    This function uses the primary_service_rolling_average model to calculate the average filled posts per bed
    value over time. The rolling average ratios outputted from that model are multiplied by the number of beds
    to give the equivalent rolling filled posts. Values are only populated in the new column for care homes.

    Args:
        df (DataFrame): The input DataFrame containing filled posts per bed ratio.
        number_of_days (int): The number of days to include in the rolling average time period.
        model_column_name (str): The name of the new column to store the rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    df = model_primary_service_rolling_average(
        df,
        IndCQC.filled_posts_per_bed_ratio,
        number_of_days,
        model_column_name,
    )

    df = df.withColumn(
        model_column_name,
        F.when(
            F.col(IndCQC.care_home) == CareHome.care_home,
            F.col(model_column_name) * F.col(IndCQC.number_of_beds),
        ),
    )

    return df


def model_non_res_filled_post_rolling_average(
    df: DataFrame,
    number_of_days: int,
    model_column_name: str,
) -> DataFrame:
    """
    Estimate filled posts for non res locations based on the rolling average of filled posts.

    This function uses the primary_service_rolling_average model to calculate the average filled posts over
    time. Values are only populated in the new column for non res locations.

    Args:
        df (DataFrame): The input DataFrame containing filled posts per bed ratio.
        number_of_days (int): The number of days to include in the rolling average time period.
        model_column_name (str): The name of the new column to store the rolling average.

    Returns:
        DataFrame: The input DataFrame with the new column containing the rolling average.
    """
    df = model_primary_service_rolling_average(
        df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        number_of_days,
        model_column_name,
    )

    df = df.withColumn(
        model_column_name,
        F.when(
            F.col(IndCQC.care_home) == CareHome.not_care_home,
            F.col(model_column_name),
        ),
    )

    return df


if __name__ == "__main__":
    print("Spark job 'estimate_missing_ascwds_ind_cqc_filled_posts' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_ind_cqc_source",
            "Source s3 directory for cleaned_ind_cqc_filled_posts",
        ),
        (
            "--estimated_missing_ascwds_ind_cqc_destination",
            "Destination s3 directory for outputting estimate missing ASCWDS filled posts",
        ),
    )

    main(
        cleaned_ind_cqc_source,
        estimated_missing_ascwds_ind_cqc_destination,
    )
