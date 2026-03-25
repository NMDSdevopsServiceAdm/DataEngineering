from dataclasses import dataclass

import polars as pl

from polars_utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


@dataclass
class NumericalValues:
    number_of_days_in_window: str = "3mo5d"  # 3 months and 5 days


def main(
    cleaned_ind_cqc_source: str,
    destination: str,
) -> None:
    """
    Impute values into ASC-WDS, PIR and Capacity Tracker data.

    Args:
        cleaned_ind_cqc_source (str): s3 path to the cleaned ind cqc data
        destination (str): s3 path to save the output data
    """
    lf = utils.scan_parquet(cleaned_ind_cqc_source)
    print("Cleaned IND CQC LazyFrame read in")

    # create_unix_timestamp_variable_from_date_column

    # combine_care_home_and_non_res_values_into_single_column - combined_ratio_and_filled_posts

    # model_primary_service_rate_of_change_trendline - ascwds_rate_of_change_trendline_model

    # model_pir_filled_posts

    # merge_ascwds_and_pir_filled_post_submissions

    # model_imputation_with_extrapolation_and_interpolation - imputed_filled_post_model

    # model_imputation_with_extrapolation_and_interpolation - imputed_filled_posts_per_bed_ratio_model

    # model_calculate_rolling_average - posts_rolling_average_model
    lf = lf.with_columns(
        calculate_rolling_average(
            IndCQC.imputed_filled_post_model,
            NumericalValues.number_of_days_in_window,
            [IndCQC.primary_service_type],
        ).alias(IndCQC.posts_rolling_average_model)
    )

    # create_banded_bed_count_column

    # model_calculate_rolling_average - banded_bed_ratio_rolling_average_model
    #   Need to make banded bed count before calling rolling average.

    # convert_care_home_ratios_to_posts

    # combine_care_home_and_non_res_values_into_single_column - ct_combined_care_home_and_non_res

    # model_primary_service_rate_of_change_trendline - ct_combined_care_home_and_non_res_rate_of_change_trendline

    # model_imputation_with_extrapolation_and_interpolation - ct_care_home_total_employed_imputed

    # model_imputation_with_extrapolation_and_interpolation - ct_non_res_care_workers_employed_imputed

    # nullify_ct_values_previous_to_first_submission

    print(f"Exporting as parquet to {destination}")

    utils.sink_to_parquet(
        lf,
        destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )

    print("Completed imputing independent CQC ASCWDS and PIR")


def calculate_rolling_average(
    column_to_average: str,
    period: str,
    columns_to_partition_by: list,
) -> pl.Expr:
    """
    Calculate the rolling mean of the "column_to_average" over a given period
    and partition.

    This function calculates the rolling mean of a column based on a given
    number of days and a column to partition by. For example, a 3-day rolling
    average includes the current day plus the two preceding days.

    Args:
        column_to_average (str): The name of the column with the values to average.
        period (str): period (str): String language timedelta. See:
          https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.rolling.html
        columns_to_partition_by (list): The name of the column to partition the window by.

    Returns:
        pl.Expr: Expression for rolling mean of column_to_average.
    """
    return (
        pl.mean(column_to_average)
        .rolling(index_column=IndCQC.cqc_location_import_date, period=period)
        .over(columns_to_partition_by)
    )


if __name__ == "__main__":
    print("Running impute independent CQC ASCWDS and PIR job")

    args = utils.get_args(
        (
            "--cleaned_ind_cqc_source",
            "S3 URI to read cleaned independent CQC data from",
        ),
        (
            "--destination",
            "S3 URI to save imputed data to",
        ),
    )

    main(
        cleaned_ind_cqc_source=args.cleaned_ind_cqc_source,
        destination=args.destination,
    )

    print("Finished impute independent CQC ASCWDS and PIR job")
    print("Finished impute independent CQC ASCWDS and PIR job")
    print("Finished impute independent CQC ASCWDS and PIR job")
