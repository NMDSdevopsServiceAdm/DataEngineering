import sys
from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
    PartitionKeys as Keys,
)
from utils.column_values.categorical_column_values import PrimaryServiceType
from utils.estimate_filled_posts.models.primary_service_rolling_average import (
    model_primary_service_rolling_average,
)
from utils.estimate_filled_posts.models.interpolation import model_interpolation
from utils.estimate_filled_posts.models.extrapolation import model_extrapolation


PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    NUMBER_OF_DAYS_IN_ROLLING_AVERAGE = 185  # Note: using 185 as a proxy for 6 months


def main(
    cleaned_ind_cqc_source: str,
    estimated_missing_ascwds_ind_cqc_destination: str,
) -> DataFrame:
    print("Estimating missing ASCWDS independent CQC filled posts...")

    cleaned_ind_cqc_df = utils.read_from_parquet(cleaned_ind_cqc_source)

    estimate_missing_ascwds_df = utils.create_unix_timestamp_variable_from_date_column(
        cleaned_ind_cqc_df,
        date_col=IndCQC.cqc_location_import_date,
        date_format="yyyy-MM-dd",
        new_col_name=IndCQC.unix_time,
    )

    estimate_missing_ascwds_df = model_primary_service_rolling_average(
        estimate_missing_ascwds_df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_filled_posts_dedup_clean,
        NumericalValues.NUMBER_OF_DAYS_IN_ROLLING_AVERAGE,
        IndCQC.rolling_average_model,
    )
    estimate_missing_ascwds_df = model_extrapolation(
        estimate_missing_ascwds_df, IndCQC.rolling_average_model
    )

    estimate_missing_ascwds_df = model_interpolation(
        estimate_missing_ascwds_df,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.interpolation_model_ascwds_filled_posts_dedup_clean,
    )
    estimate_missing_ascwds_df = model_interpolation(
        estimate_missing_ascwds_df,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.interpolation_model_filled_posts_per_bed_ratio,
    )
    estimate_missing_ascwds_df = merge_imputed_columns(estimate_missing_ascwds_df)

    print(f"Exporting as parquet to {estimated_missing_ascwds_ind_cqc_destination}")

    utils.write_to_parquet(
        estimate_missing_ascwds_df,
        estimated_missing_ascwds_ind_cqc_destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )

    print("Completed estimate missing ASCWDS independent CQC filled posts")


def merge_interpolated_values_into_interpolated_filled_posts(
    df: DataFrame,
) -> DataFrame:
    """
    Use the interpolated value columns to create a single column with interpolated values for care homes and non-res.

    Args:
        df (DataFrame): A dataframe with interpolated values.

    Returns:
        DataFrame: A dataframe with a single column with interpolated filled posts values.
    """
    df = df.withColumn(
        IndCQC.interpolation_model,
        F.when(
            df[IndCQC.primary_service_type] == PrimaryServiceType.non_residential,
            F.col(IndCQC.interpolation_model_ascwds_filled_posts_dedup_clean),
        ).when(
            (df[IndCQC.primary_service_type] == PrimaryServiceType.care_home_only)
            | (
                df[IndCQC.primary_service_type]
                == PrimaryServiceType.care_home_with_nursing
            ),
            F.col(IndCQC.interpolation_model_filled_posts_per_bed_ratio)
            * F.col(IndCQC.number_of_beds),
        ),
    )
    return df


def merge_imputed_columns(df: DataFrame) -> DataFrame:
    """
    Merges the extrapolation and interpolation columns to create a new column.

    This function merges the extrapolation and interpolation columns to create a new column called ascwds_filled_posts_imputed.

    Args:
        df (DataFrame): A dataframe with the columns extrapolation_rolling_average and interpolation_model_ascwds_filled_posts_dedup_clean.

    Returns:
        Dataframe: A dataframe with a new merged column called ascwds_filled_posts_imputed.
    """
    df = df.withColumn(
        IndCQC.ascwds_filled_posts_imputed,
        F.when(
            df[IndCQC.interpolation_model_ascwds_filled_posts_dedup_clean].isNotNull(),
            F.col(IndCQC.interpolation_model_ascwds_filled_posts_dedup_clean),
        ).when(
            df[IndCQC.extrapolation_rolling_average_model].isNotNull(),
            F.col(IndCQC.extrapolation_rolling_average_model),
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
