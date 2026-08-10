from dataclasses import dataclass

import polars as pl

from polars_utils import cleaning_utils as cUtils
from polars_utils import utils
from polars_utils.expressions import is_care_home
from projects._03_independent_cqc._03_impute.fargate.utils.combine_ascwds_and_pir import (
    merge_ascwds_and_pir_filled_post_submissions,
)
from projects._03_independent_cqc._03_impute.fargate.utils.convert_pir_people_to_filled_posts import (
    convert_pir_to_filled_posts,
)
from projects._03_independent_cqc._03_impute.fargate.utils.forward_fill_latest_known_value import (
    forward_fill_latest_known_value,
)
from projects._03_independent_cqc._03_impute.fargate.utils.memory_monitor import (
    checkpoint,
    profile_step,
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

    Every step is wrapped in `profile_step` for temporary memory-spike
    investigation: it logs elapsed time plus periodic/peak RSS. Each step also
    ends with `checkpoint(lf)`, which forces that step to materialize
    immediately instead of staying lazy - without this, every step fuses into
    one execution at `sink_to_parquet` and `profile_step` can only measure the
    fused whole, not which step actually caused a memory spike. Both are
    temporary diagnostic aids and should be removed once the investigation is
    done.

    Args:
        cleaned_ind_cqc_source (str): s3 path to the cleaned ind cqc data
        destination (str): s3 path to save the output data
    """
    with profile_step("scan_parquet"):
        lf = utils.scan_parquet(cleaned_ind_cqc_source)
        lf = checkpoint(lf)
    print("Cleaned IND CQC LazyFrame read in")

    with profile_step("forward_fill:ascwds_filled_posts_dedup_clean"):
        lf = forward_fill_latest_known_value(lf, IndCQC.ascwds_filled_posts_dedup_clean)
        lf = checkpoint(lf)

    with profile_step("forward_fill:pir_people_directly_employed_dedup"):
        lf = forward_fill_latest_known_value(
            lf, IndCQC.pir_people_directly_employed_dedup
        )
        lf = checkpoint(lf)

    with profile_step("calculate_filled_posts_per_bed_ratio"):
        lf = cUtils.calculate_filled_posts_per_bed_ratio(
            lf,
            IndCQC.ascwds_filled_posts_dedup_clean,
            IndCQC.filled_posts_per_bed_ratio,
        )
        lf = checkpoint(lf)

    with profile_step("with_columns: combined_ratio_and_filled_posts"):
        lf = lf.with_columns(
            pl.when(is_care_home())
            .then(pl.col(IndCQC.filled_posts_per_bed_ratio))
            .otherwise(pl.col(IndCQC.ascwds_filled_posts_dedup_clean))
            .cast(pl.Float32)
            .alias(IndCQC.combined_ratio_and_filled_posts)
        )
        lf = checkpoint(lf)

    with profile_step("model_roc_trendline:ascwds"):
        lf = model_primary_service_rate_of_change_trendline(
            lf,
            IndCQC.combined_ratio_and_filled_posts,
            NumericalValues.number_of_days_in_window,
            IndCQC.ascwds_rate_of_change_trendline_model,
            max_days_between_submissions=NumericalValues.max_number_of_days_to_interpolate_between,
        )
        lf = checkpoint(lf)

    with profile_step("convert_pir_to_filled_posts"):
        lf = convert_pir_to_filled_posts(lf)
        lf = checkpoint(lf)

    with profile_step("merge_ascwds_and_pir_filled_post_submissions"):
        lf = merge_ascwds_and_pir_filled_post_submissions(lf)
        lf = checkpoint(lf)

    with profile_step("model_imputation:ascwds_pir_merged"):
        lf = model_imputation(
            lf,
            IndCQC.ascwds_pir_merged,
            IndCQC.ascwds_rate_of_change_trendline_model,
            IndCQC.imputed_filled_post_model,
            care_home=False,
            extrapolation_method="ratio",
        )
        lf = checkpoint(lf)

    with profile_step("model_imputation:filled_posts_per_bed_ratio"):
        lf = model_imputation(
            lf,
            IndCQC.filled_posts_per_bed_ratio,
            IndCQC.ascwds_rate_of_change_trendline_model,
            IndCQC.imputed_filled_posts_per_bed_ratio_model,
            care_home=True,
            extrapolation_method="ratio",
        )
        lf = checkpoint(lf)

    with profile_step("calculate_rolling_average: imputed_filled_post_model"):
        lf = calculate_rolling_average(
            lf,
            IndCQC.imputed_filled_post_model,
            f"{NumericalValues.number_of_days_in_window}d",
            [IndCQC.primary_service_type],
            IndCQC.posts_rolling_average_model,
        )
        lf = checkpoint(lf)

    with profile_step("create_banded_bed_count_column"):
        lf = cUtils.create_banded_bed_count_column(
            lf,
            IndCQC.number_of_beds_banded_for_rolling_avg,
            [0, 1, 10, 15, 20, 25, 50, float("Inf")],
        )
        lf = checkpoint(lf)

    with profile_step(
        "calculate_rolling_average: imputed_filled_posts_per_bed_ratio_model"
    ):
        lf = calculate_rolling_average(
            lf,
            IndCQC.imputed_filled_posts_per_bed_ratio_model,
            f"{NumericalValues.number_of_days_in_window}d",
            [
                IndCQC.primary_service_type,
                IndCQC.number_of_beds_banded_for_rolling_avg,
            ],
            IndCQC.banded_bed_ratio_rolling_average_model,
        )
        lf = checkpoint(lf)

    with profile_step("with_columns: posts_rolling_average_model"):
        lf = lf.with_columns(
            pl.when(is_care_home())
            .then(
                pl.col(IndCQC.banded_bed_ratio_rolling_average_model)
                * pl.col(IndCQC.number_of_beds)
            )
            .otherwise(pl.col(IndCQC.posts_rolling_average_model))
            .cast(pl.Float32)
            .alias(IndCQC.posts_rolling_average_model)
        )
        lf = checkpoint(lf)

    with profile_step("with_columns: ct_combined_care_home_and_non_res"):
        lf = lf.with_columns(
            pl.when(is_care_home())
            .then(pl.col(IndCQC.ct_care_home_total_employed_cleaned))
            .otherwise(pl.col(IndCQC.ct_non_res_care_workers_employed_cleaned))
            .cast(pl.Float32)
            .alias(IndCQC.ct_combined_care_home_and_non_res)
        )
        lf = checkpoint(lf)

    with profile_step("model_roc_trendline:ct_combined_care_home_and_non_res"):
        lf = model_primary_service_rate_of_change_trendline(
            lf,
            IndCQC.ct_combined_care_home_and_non_res,
            NumericalValues.number_of_days_in_window,
            IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
            max_days_between_submissions=NumericalValues.max_number_of_days_to_interpolate_between,
        )
        lf = checkpoint(lf)

    with profile_step("model_imputation:ct_care_home_total_employed_cleaned"):
        lf = model_imputation(
            lf,
            IndCQC.ct_care_home_total_employed_cleaned,
            IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
            IndCQC.ct_care_home_total_employed_imputed,
            care_home=True,
            extrapolation_method="ratio",
        )
        lf = checkpoint(lf)

    with profile_step("model_imputation:ct_non_res_care_workers_employed_cleaned"):
        lf = model_imputation(
            lf,
            IndCQC.ct_non_res_care_workers_employed_cleaned,
            IndCQC.ct_combined_care_home_and_non_res_rate_of_change_trendline,
            IndCQC.ct_non_res_care_workers_employed_imputed,
            care_home=False,
            extrapolation_method="ratio",
        )
        lf = checkpoint(lf)

    with profile_step("with_columns: nullify_ct_values_previous_to_first_submission"):
        lf = lf.with_columns(
            utils.nullify_ct_values_previous_to_first_submission(
                [
                    IndCQC.ct_care_home_total_employed_imputed,
                    IndCQC.ct_non_res_care_workers_employed_imputed,
                ],
            )
        )
        lf = checkpoint(lf)

    print(f"Exporting as parquet to {destination}")

    with profile_step("sink_to_parquet"):
        utils.sink_to_parquet(
            lf,
            destination,
        )

    print("Completed imputing independent CQC ASCWDS and PIR")


def calculate_rolling_average(
    lf: pl.LazyFrame,
    column_to_average: str,
    period: str,
    columns_to_partition_by: list[str],
    new_column_name: str,
) -> pl.LazyFrame:
    """
    Add the rolling mean of "column_to_average" over a given period and
    partition. For example, a 3-day rolling average includes the current day
    plus the two preceding days.

    Rewritten from an `Expr.rolling().over()` implementation, which caused a
    production OOM: that combination keeps a per-window-function cache alive
    for the whole frame (pola-rs/polars#20783), and this pipeline's partition
    columns are low-cardinality (a handful of service types/bed bands), so
    each partition held millions of rows. This version narrows to only the
    columns the calculation needs, sorts (required for grouped rolling),
    computes the rolling mean via the native grouped-rolling API, then joins
    the small per-partition-per-date result back onto the full frame -
    joining a tiny lookup table is far cheaper than running the window
    function across every other column in the frame.

    Args:
        lf (pl.LazyFrame): The LazyFrame to add the rolling average to.
        column_to_average (str): The name of the column with the values to average.
        period (str): String language timedelta. See:
          https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.rolling.html
        columns_to_partition_by (list[str]): The columns to partition the window by.
        new_column_name (str): The name to give the new rolling average column.

    Returns:
        pl.LazyFrame: `lf` with `new_column_name` added.
    """
    join_columns = [*columns_to_partition_by, IndCQC.cqc_location_import_date]

    rolling_lf = (
        lf.select([*join_columns, column_to_average])
        .sort(join_columns)
        .rolling(
            index_column=IndCQC.cqc_location_import_date,
            period=period,
            group_by=columns_to_partition_by,
        )
        .agg(pl.col(column_to_average).mean().alias(new_column_name))
        .unique(subset=join_columns, keep="any")
    )

    return lf.join(rolling_lf, on=join_columns, how="left")


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
