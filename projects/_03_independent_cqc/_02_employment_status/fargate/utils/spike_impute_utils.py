import polars as pl

from polars_utils.column_types import CategoricalColumnTypes as CatColType
from polars_utils.expressions import percentage_share
from projects._03_independent_cqc._01_filled_posts.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc._01_filled_posts.utils.imputation.interpolation import (
    model_interpolation,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from projects._03_independent_cqc._02_employment_status.fargate.utils.spike_columns import (
    EmploymentStatusSpikeColumns as SpikeCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels

# Ticket 2110 spike, candidate L1 (fully long): ported from ticket 2000's
# `2000-a-long` (tag spike-2000-l1). Crosses published_job_role_label x
# employment_status into one long dimension (5 rows per input row).

EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS: dict[str, str] = {
    EmploymentStatusLabels.permanent: EmpStatus.permanent_percentage,
    EmploymentStatusLabels.temporary: EmpStatus.temporary_percentage,
    EmploymentStatusLabels.bank_or_pool: EmpStatus.bank_or_pool_percentage,
    EmploymentStatusLabels.agency: EmpStatus.agency_percentage,
    EmploymentStatusLabels.other: EmpStatus.other_percentage,
}

# One employment status time series: a job role's 5 statuses are imputed
# independently of each other.
JOB_ROLE_STATUS_GROUPS: list[str] = [
    IndCQC.location_id,
    IndCQC.published_job_role_label,
    SpikeCols.employment_status_label,
]

# Groups contributing to the rolling trendline average. As in ticket 2000, this
# drops the job role pipeline's estimate_filled_posts_size_group banding.
ROLLING_GROUPS: list[str] = [
    IndCQC.primary_service_type,
    IndCQC.published_job_role_label,
    SpikeCols.employment_status_label,
]

ROLLING_PERIOD = "6mo"


def reshape_employment_status_percentages_to_long_rows(
    lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Converts the 5 wide employment status percentage columns into one row per status.

    Every other column is carried through onto all 5 rows, so callers should select
    only the columns they need before calling this - each carried column is
    multiplied 5 times by the explode.

    Args:
        lf (pl.LazyFrame): Employment status clean output, one row per location,
            job role and import date, with the 5 `emplstat_*_percentage` columns.

    Returns:
        pl.LazyFrame: `lf` with the 5 percentage columns replaced by
            `employment_status_label` and `employment_status_rate`, one row per status.
    """
    struct_list_column = "_employment_status_struct_list"
    label_structs = [
        pl.struct(
            pl.lit(label).alias(SpikeCols.employment_status_label),
            pl.col(column).alias(SpikeCols.employment_status_rate),
        )
        for label, column in EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS.items()
    ]

    return (
        lf.select(
            pl.exclude(EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS.values()),
            pl.concat_list(label_structs).alias(struct_list_column),
        )
        .explode(struct_list_column, empty_as_null=True)
        .unnest(struct_list_column)
        .with_columns(
            pl.col(SpikeCols.employment_status_label).cast(
                CatColType.EmploymentStatusCatType
            ),
            pl.col(SpikeCols.employment_status_rate).cast(pl.Float32),
        )
    )


def add_fill_boundaries(long_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds the first/last known rate and date, and the nearest known date either side
    of every row, for each location, job role and employment status series.

    Args:
        long_lf (pl.LazyFrame): Long-format data with `employment_status_rate`.

    Returns:
        pl.LazyFrame: `long_lf` with the boundary columns added.
    """
    order_key = IndCQC.cqc_location_import_date

    is_known = pl.col(SpikeCols.employment_status_rate).is_not_null()
    known_date = pl.when(is_known).then(pl.col(order_key))

    long_lf = long_lf.with_columns(
        known_date.min().over(JOB_ROLE_STATUS_GROUPS).alias(SpikeCols.first_known_date),
        known_date.max().over(JOB_ROLE_STATUS_GROUPS).alias(SpikeCols.last_known_date),
        known_date.forward_fill()
        .over(JOB_ROLE_STATUS_GROUPS, order_by=order_key)
        .alias(SpikeCols.previous_known_date),
        known_date.backward_fill()
        .over(JOB_ROLE_STATUS_GROUPS, order_by=order_key)
        .alias(SpikeCols.next_known_date),
    )

    return long_lf.with_columns(
        pl.when(is_known & (pl.col(order_key) == pl.col(SpikeCols.first_known_date)))
        .then(pl.col(SpikeCols.employment_status_rate))
        .max()
        .over(JOB_ROLE_STATUS_GROUPS)
        .alias(SpikeCols.first_known_value),
        pl.when(is_known & (pl.col(order_key) == pl.col(SpikeCols.last_known_date)))
        .then(pl.col(SpikeCols.employment_status_rate))
        .max()
        .over(JOB_ROLE_STATUS_GROUPS)
        .alias(SpikeCols.last_known_value),
    )


def add_imputed_rates_for_trendline(
    long_lf: pl.LazyFrame,
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Imputes employment status rates within time limits, for the trendline only.

    Interpolates by date across gaps up to `interpolation_cap_period`, and carries
    the first/last known value up to `extrapolation_period` outside the known range.

    Args:
        long_lf (pl.LazyFrame): Long-format data with `employment_status_rate`.
        extrapolation_period (str): Polars offset string, e.g. "2y".
        interpolation_cap_period (str): Polars offset string, e.g. "5y".

    Returns:
        pl.LazyFrame: `long_lf` with the boundary columns and
            `imputed_rate_for_trendline` added.
    """
    order_key = IndCQC.cqc_location_import_date

    long_lf = add_fill_boundaries(long_lf)

    within_interpolation_cap = pl.col(SpikeCols.next_known_date) <= pl.col(
        SpikeCols.previous_known_date
    ).dt.offset_by(interpolation_cap_period)

    interpolated = (
        pl.col(SpikeCols.employment_status_rate)
        .interpolate_by(pl.col(order_key))
        .over(JOB_ROLE_STATUS_GROUPS, order_by=order_key)
    )

    within_forward_fill = (pl.col(order_key) > pl.col(SpikeCols.last_known_date)) & (
        pl.col(order_key)
        <= pl.col(SpikeCols.last_known_date).dt.offset_by(extrapolation_period)
    )
    within_backward_fill = (pl.col(order_key) < pl.col(SpikeCols.first_known_date)) & (
        pl.col(order_key)
        >= pl.col(SpikeCols.first_known_date).dt.offset_by(f"-{extrapolation_period}")
    )

    return long_lf.with_columns(
        pl.coalesce(
            pl.col(SpikeCols.employment_status_rate),
            pl.when(within_interpolation_cap).then(interpolated),
            pl.when(within_forward_fill)
            .then(pl.col(SpikeCols.last_known_value))
            .when(within_backward_fill)
            .then(pl.col(SpikeCols.first_known_value)),
        )
        .cast(pl.Float32)
        .alias(SpikeCols.imputed_rate_for_trendline)
    )


def add_rolling_employment_status_ratio(
    long_lf: pl.LazyFrame,
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Adds a rolling 6-month employment status rate trendline per ROLLING_GROUPS.

    Pre-aggregates by group and date, rolls, then joins back. This group_by +
    rolling + join is the cardinality-sensitive step the spike measures: the long
    shape runs it over 5x as many groups as the wide shape.

    Args:
        long_lf (pl.LazyFrame): Long-format data with `employment_status_rate`.
        extrapolation_period (str): Passed to `add_imputed_rates_for_trendline`.
        interpolation_cap_period (str): Passed to `add_imputed_rates_for_trendline`.

    Returns:
        pl.LazyFrame: `long_lf` with `employment_status_rolling_ratio` added.
    """
    long_lf = add_imputed_rates_for_trendline(
        long_lf,
        extrapolation_period=extrapolation_period,
        interpolation_cap_period=interpolation_cap_period,
    )

    order_key = IndCQC.cqc_location_import_date
    monthly_groups = ROLLING_GROUPS + [order_key]

    monthly_totals_lf = long_lf.group_by(monthly_groups).agg(
        pl.col(SpikeCols.imputed_rate_for_trendline).sum().alias(SpikeCols.ratio_total),
        pl.col(SpikeCols.imputed_rate_for_trendline)
        .is_not_null()
        .sum()
        .alias(SpikeCols.contributing_rows),
    )

    rolling_agg_lf = (
        monthly_totals_lf.sort(*ROLLING_GROUPS, order_key)
        .rolling(index_column=order_key, group_by=ROLLING_GROUPS, period=ROLLING_PERIOD)
        .agg(
            pl.col(SpikeCols.ratio_total).sum(),
            pl.col(SpikeCols.contributing_rows).sum(),
        )
    )

    rolling_agg_lf = rolling_agg_lf.with_columns(
        pl.when(pl.col(SpikeCols.contributing_rows) > 0)
        .then(pl.col(SpikeCols.ratio_total) / pl.col(SpikeCols.contributing_rows))
        .cast(pl.Float32)
        .alias(SpikeCols.employment_status_rolling_ratio)
    ).with_columns(
        pl.col(SpikeCols.employment_status_rolling_ratio)
        .forward_fill()
        .backward_fill()
        .over(ROLLING_GROUPS, order_by=order_key)
    )

    temp_columns_to_drop = [
        SpikeCols.first_known_date,
        SpikeCols.last_known_date,
        SpikeCols.first_known_value,
        SpikeCols.last_known_value,
        SpikeCols.previous_known_date,
        SpikeCols.next_known_date,
        SpikeCols.ratio_total,
        SpikeCols.contributing_rows,
        SpikeCols.imputed_rate_for_trendline,
    ]

    return long_lf.join(rolling_agg_lf, on=monthly_groups, how="left").drop(
        *temp_columns_to_drop, strict=False
    )


def normalise_employment_status_rates_within_job_role(
    long_lf: pl.LazyFrame, rate_column: str, output_column: str
) -> pl.LazyFrame:
    """
    Rescales `rate_column` so the 5 status rows sum to 1 per location, job role and date.

    Args:
        long_lf (pl.LazyFrame): Long-format data, one row per employment status per
            location, job role and date.
        rate_column (str): Column to normalise.
        output_column (str): Name for the normalised column.

    Returns:
        pl.LazyFrame: `long_lf` with `output_column` added.
    """
    groups = [
        IndCQC.location_id,
        IndCQC.published_job_role_label,
        IndCQC.cqc_location_import_date,
    ]
    return long_lf.with_columns(
        percentage_share(rate_column).cast(pl.Float32).over(groups).alias(output_column)
    )


def add_imputed_employment_status_rates(long_lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Imputes each status rate along the rolling trendline, then re-normalises.

    `model_extrapolation` and `model_interpolation` are reused unchanged. Known rates
    are coalesced back in after normalising.

    Known limitation carried over from ticket 2000: only the imputed rows are
    normalised, which is only correct while the 5 statuses are always known or null
    together (true for this data, as clean nulls them together).

    Args:
        long_lf (pl.LazyFrame): Long-format data with `employment_status_rate` and
            `employment_status_rolling_ratio`.

    Returns:
        pl.LazyFrame: `long_lf` with `imputed_employment_status_rate` added.
    """
    long_lf = model_extrapolation(
        long_lf,
        column_with_null_values=SpikeCols.employment_status_rate,
        model_to_extrapolate_from=SpikeCols.employment_status_rolling_ratio,
        extrapolation_method="nominal",
        group_columns=JOB_ROLE_STATUS_GROUPS,
    )

    long_lf = model_interpolation(
        long_lf,
        column_with_null_values=SpikeCols.employment_status_rate,
        method="trend",
        group_columns=JOB_ROLE_STATUS_GROUPS,
    )

    long_lf = long_lf.with_columns(
        pl.when(pl.col(SpikeCols.employment_status_rate).is_null())
        .then(
            pl.coalesce(IndCQC.extrapolation_model, IndCQC.interpolation_model).clip(
                lower_bound=0
            )
        )
        .cast(pl.Float32)
        .alias(SpikeCols.unnormalised_rate)
    ).drop(
        IndCQC.extrapolation_forwards,
        IndCQC.extrapolation_model,
        IndCQC.interpolation_model,
    )

    long_lf = normalise_employment_status_rates_within_job_role(
        long_lf,
        rate_column=SpikeCols.unnormalised_rate,
        output_column=SpikeCols.imputed_employment_status_rate,
    )

    long_lf = long_lf.with_columns(
        pl.coalesce(
            SpikeCols.employment_status_rate,
            SpikeCols.imputed_employment_status_rate,
        ).alias(SpikeCols.imputed_employment_status_rate)
    )

    return long_lf.drop(SpikeCols.unnormalised_rate)
