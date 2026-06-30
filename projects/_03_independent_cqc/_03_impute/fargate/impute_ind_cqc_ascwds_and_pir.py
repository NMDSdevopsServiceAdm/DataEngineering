from dataclasses import dataclass

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.expressions import is_care_home
from projects._03_independent_cqc._03_impute.fargate.utils.convert_pir_people_to_filled_posts import (
    convert_pir_to_filled_posts,
)
from projects._03_independent_cqc._03_impute.fargate.utils.forward_fill_latest_known_value import (
    forward_fill_latest_known_value,
)
from projects._03_independent_cqc._03_impute.fargate.utils.primary_service_rate_of_change import (
    model_primary_service_rate_of_change_trendline,
)
from projects._03_independent_cqc.utils.imputation.imputation import model_imputation
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class NumericalValues:
    number_of_days_in_window: int = 95  # Note: using 95 as a proxy for 3 months
    max_number_of_days_to_interpolate_between: int = 185  # proxy for 6 months


def main(cleaned_ind_cqc_source: str, destination: str) -> None:
    """
    Impute values into ASC-WDS, PIR and Capacity Tracker data.

    Args:
        cleaned_ind_cqc_source (str): s3 path to the cleaned ind cqc data
        destination (str): s3 path to save the output data
    """
    lf = utils.scan_parquet(cleaned_ind_cqc_source)
    print("Cleaned IND CQC LazyFrame read in")

    lf = forward_fill_latest_known_value(lf, IndCQC.ascwds_filled_posts_dedup_clean)

    lf = forward_fill_latest_known_value(lf, IndCQC.pir_people_directly_employed_dedup)

    lf = cUtils.calculate_filled_posts_per_bed_ratio(
        lf,
        IndCQC.ascwds_filled_posts_dedup_clean,
        IndCQC.filled_posts_per_bed_ratio,
    )

    lf = lf.with_columns(
        pl.when(is_care_home())
        .then(pl.col(IndCQC.filled_posts_per_bed_ratio))
        .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
        .cast(pl.Float32)
        .alias(IndCQC.combined_ratio_and_filled_posts)
    )

    lf = model_primary_service_rate_of_change_trendline(
        lf,
        IndCQC.combined_ratio_and_filled_posts,
        NumericalValues.number_of_days_in_window,
        IndCQC.ascwds_rate_of_change_trendline_model,
        max_days_between_submissions=NumericalValues.max_number_of_days_to_interpolate_between,
    )

    lf = convert_pir_to_filled_posts(lf)

    # merge_ascwds_and_pir_filled_post_submissions

    # Uncomment this call when merge_ascwds_and_pir_filled_post_submissions is converted to polars.
    # lf = model_imputation(
    #     lf,
    #     IndCQC.ascwds_pir_merged,
    #     IndCQC.ascwds_rate_of_change_trendline_model,
    #     IndCQC.imputed_filled_post_model,
    #     care_home=False,
    #     extrapolation_method="ratio",
    # )

    lf = model_imputation(
        lf,
        IndCQC.filled_posts_per_bed_ratio,
        IndCQC.ascwds_rate_of_change_trendline_model,
        IndCQC.imputed_filled_posts_per_bed_ratio_model,
        care_home=True,
        extrapolation_method="ratio",
    )

    # model_calculate_rolling_average - posts_rolling_average_model

    # create_banded_bed_count_column

    # model_calculate_rolling_average - banded_bed_ratio_rolling_average_model

    # convert_care_home_ratios_to_posts - unhash after `model_calculate_rolling_average` converted
    # lf = lf.with_columns(
    #     pl.when(is_care_home())
    #     .then(
    #         pl.col(IndCQC.banded_bed_ratio_rolling_average_model)
    #         * pl.col(IndCQC.number_of_beds)
    #     )
    #     .otherwise(pl.col(IndCQC.posts_rolling_average_model))
    #     .cast(pl.Float32)
    #     .alias(IndCQC.posts_rolling_average_model)
    # )

    lf = lf.with_columns(
        pl.when(is_care_home())
        .then(pl.col(IndCQC.ct_care_home_total_employed_cleaned))
        .otherwise(pl.col(IndCQC.ct_non_res_care_workers_employed_cleaned))
        .cast(pl.Float32)
        .alias(IndCQC.ct_combined_care_home_and_non_res)
    )

    lf = model_primary_service_rate_of_change_trendline(
        lf,
        IndCQC.ct_combined_care_home_and_non_res,
        NumericalValues.number_of_days_in_window,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        max_days_between_submissions=NumericalValues.max_number_of_days_to_interpolate_between,
    )

    lf = model_imputation(
        lf,
        IndCQC.ct_care_home_total_employed_cleaned,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_care_home_total_employed_imputed,
        care_home=True,
        extrapolation_method="ratio",
    )

    lf = model_imputation(
        lf,
        IndCQC.ct_non_res_care_workers_employed_cleaned,
        IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
        IndCQC.ct_non_res_care_workers_employed_imputed,
        care_home=False,
        extrapolation_method="ratio",
    )

    lf = lf.with_columns(
        utils.nullify_ct_values_previous_to_first_submission(
            [
                IndCQC.ct_care_home_total_employed_imputed,
                IndCQC.ct_non_res_care_workers_employed_imputed,
            ],
        )
    )

    print(f"Exporting as parquet to {destination}")

    utils.sink_to_parquet(
        lf,
        destination,
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
