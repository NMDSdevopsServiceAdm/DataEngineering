import polars as pl

from projects._03_independent_cqc._01_filled_posts.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc._01_filled_posts.utils.imputation.interpolation import (
    model_interpolation,
)
from projects._03_independent_cqc._02_employment_status.fargate.utils.spike_columns import (
    EmploymentStatusSpikeColumns as SpikeCols,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels

# Ticket 2110 spike, candidate L2 (long job role, wide status): ported from ticket
# 2000's `2000-b-wide` (tag spike-2000-l2). Keeps one row per job role and imputes
# the 5 percentage columns side by side, running the same logic once per column.

# The clean output's percentage columns are the rates L2 imputes directly.
EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS: dict[str, str] = {
    EmploymentStatusLabels.permanent: EmpStatus.permanent_percentage,
    EmploymentStatusLabels.temporary: EmpStatus.temporary_percentage,
    EmploymentStatusLabels.bank_or_pool: EmpStatus.bank_or_pool_percentage,
    EmploymentStatusLabels.agency: EmpStatus.agency_percentage,
    EmploymentStatusLabels.other: EmpStatus.other_percentage,
}

# No employment status in the keys: a job role's 5 statuses share a row.
JOB_ROLE_GROUPS: list[str] = [IndCQC.location_id, IndCQC.published_job_role_label]

# As in ticket 2000, drops the job role pipeline's estimate_filled_posts_size_group
# banding from the trendline groups.
ROLLING_GROUPS: list[str] = [
    IndCQC.primary_service_type,
    IndCQC.published_job_role_label,
]

ROLLING_PERIOD = "6mo"


def status_column(base: str, label: str) -> str:
    """
    Returns the per-status name of a wide working column.

    Args:
        base (str): Base column name, e.g. `es_spike_unnormalised_rate`.
        label (str): Employment status label, e.g. `permanent`.

    Returns:
        str: The combined column name, e.g. `es_spike_unnormalised_rate_permanent`.
    """
    return f"{base}_{label}"


def add_fill_boundaries(
    wide_lf: pl.LazyFrame, percentage_columns: dict[str, str] | None = None
) -> pl.LazyFrame:
    """
    Adds the first/last known rate and date, and the nearest known date either side
    of every row, for each percentage column.

    Args:
        wide_lf (pl.LazyFrame): Data with one percentage column per status.
        percentage_columns (dict[str, str] | None): Status label to percentage
            column. Defaults to the 5 real statuses; the N=20 stress test passes
            extra dummy statuses.

    Returns:
        pl.LazyFrame: `wide_lf` with the boundary columns added, once per status.
    """
    if percentage_columns is None:
        percentage_columns = EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS

    order_key = IndCQC.cqc_location_import_date

    boundary_exprs = []
    for label, rate_col in percentage_columns.items():
        known_date = pl.when(pl.col(rate_col).is_not_null()).then(pl.col(order_key))
        boundary_exprs.extend(
            [
                known_date.min()
                .over(JOB_ROLE_GROUPS)
                .alias(status_column(SpikeCols.first_known_date, label)),
                known_date.max()
                .over(JOB_ROLE_GROUPS)
                .alias(status_column(SpikeCols.last_known_date, label)),
                known_date.forward_fill()
                .over(JOB_ROLE_GROUPS, order_by=order_key)
                .alias(status_column(SpikeCols.previous_known_date, label)),
                known_date.backward_fill()
                .over(JOB_ROLE_GROUPS, order_by=order_key)
                .alias(status_column(SpikeCols.next_known_date, label)),
            ]
        )
    wide_lf = wide_lf.with_columns(boundary_exprs)

    value_exprs = []
    for label, rate_col in percentage_columns.items():
        is_known = pl.col(rate_col).is_not_null()
        for date_base, value_base in [
            (SpikeCols.first_known_date, SpikeCols.first_known_value),
            (SpikeCols.last_known_date, SpikeCols.last_known_value),
        ]:
            is_boundary = pl.col(order_key) == pl.col(status_column(date_base, label))
            value_exprs.append(
                pl.when(is_known & is_boundary)
                .then(pl.col(rate_col))
                .max()
                .over(JOB_ROLE_GROUPS)
                .alias(status_column(value_base, label))
            )
    return wide_lf.with_columns(value_exprs)


def add_imputed_rates_for_trendline(
    wide_lf: pl.LazyFrame,
    extrapolation_period: str,
    interpolation_cap_period: str,
    percentage_columns: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """
    Imputes each percentage column within time limits, for the trendline only.

    Interpolates by date across gaps up to `interpolation_cap_period`, and carries
    the first/last known value up to `extrapolation_period` outside the known range.

    Args:
        wide_lf (pl.LazyFrame): Data with one percentage column per status.
        extrapolation_period (str): Polars offset string, e.g. "2y".
        interpolation_cap_period (str): Polars offset string, e.g. "5y".
        percentage_columns (dict[str, str] | None): Status label to percentage
            column. Defaults to the 5 real statuses; the N=20 stress test passes
            extra dummy statuses.

    Returns:
        pl.LazyFrame: `wide_lf` with the boundary columns and one
            `imputed_rate_for_trendline_*` column per status added.
    """
    if percentage_columns is None:
        percentage_columns = EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS
    order_key = IndCQC.cqc_location_import_date

    wide_lf = add_fill_boundaries(wide_lf, percentage_columns=percentage_columns)

    exprs = []
    for label, rate_col in percentage_columns.items():
        first_date = pl.col(status_column(SpikeCols.first_known_date, label))
        last_date = pl.col(status_column(SpikeCols.last_known_date, label))
        previous_date = pl.col(status_column(SpikeCols.previous_known_date, label))
        next_date = pl.col(status_column(SpikeCols.next_known_date, label))

        within_interpolation_cap = next_date <= previous_date.dt.offset_by(
            interpolation_cap_period
        )
        interpolated = (
            pl.col(rate_col)
            .interpolate_by(pl.col(order_key))
            .over(JOB_ROLE_GROUPS, order_by=order_key)
        )
        within_forward_fill = (pl.col(order_key) > last_date) & (
            pl.col(order_key) <= last_date.dt.offset_by(extrapolation_period)
        )
        within_backward_fill = (pl.col(order_key) < first_date) & (
            pl.col(order_key) >= first_date.dt.offset_by(f"-{extrapolation_period}")
        )

        exprs.append(
            pl.coalesce(
                pl.col(rate_col),
                pl.when(within_interpolation_cap).then(interpolated),
                pl.when(within_forward_fill)
                .then(pl.col(status_column(SpikeCols.last_known_value, label)))
                .when(within_backward_fill)
                .then(pl.col(status_column(SpikeCols.first_known_value, label))),
            )
            .cast(pl.Float32)
            .alias(status_column(SpikeCols.imputed_rate_for_trendline, label))
        )

    return wide_lf.with_columns(exprs)


def add_rolling_employment_status_ratios(
    wide_lf: pl.LazyFrame,
    extrapolation_period: str,
    interpolation_cap_period: str,
    percentage_columns: dict[str, str] | None = None,
) -> pl.LazyFrame:
    """
    Adds a rolling 6-month trendline for each percentage column.

    One group_by + rolling + join over ROLLING_GROUPS covers all 5 statuses at once,
    since they share the job role grouping. L1 has to key the same step by job role
    and status, so it runs over 5x as many groups.

    Args:
        wide_lf (pl.LazyFrame): Data with one percentage column per status.
        extrapolation_period (str): Passed to `add_imputed_rates_for_trendline`.
        interpolation_cap_period (str): Passed to `add_imputed_rates_for_trendline`.
        percentage_columns (dict[str, str] | None): Status label to percentage
            column. Defaults to the 5 real statuses; the N=20 stress test passes
            extra dummy statuses.

    Returns:
        pl.LazyFrame: `wide_lf` with one `employment_status_rolling_ratio_*` column
            per status.
    """
    if percentage_columns is None:
        percentage_columns = EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS

    wide_lf = add_imputed_rates_for_trendline(
        wide_lf,
        extrapolation_period=extrapolation_period,
        interpolation_cap_period=interpolation_cap_period,
        percentage_columns=percentage_columns,
    )

    order_key = IndCQC.cqc_location_import_date
    monthly_groups = ROLLING_GROUPS + [order_key]
    labels = list(percentage_columns)

    agg_exprs = []
    for label in labels:
        trendline_col = pl.col(
            status_column(SpikeCols.imputed_rate_for_trendline, label)
        )
        agg_exprs.extend(
            [
                trendline_col.sum().alias(status_column(SpikeCols.ratio_total, label)),
                trendline_col.is_not_null()
                .sum()
                .alias(status_column(SpikeCols.contributing_rows, label)),
            ]
        )
    monthly_totals_lf = wide_lf.group_by(monthly_groups).agg(agg_exprs)

    rolling_agg_lf = (
        monthly_totals_lf.sort(*ROLLING_GROUPS, order_key)
        .rolling(index_column=order_key, group_by=ROLLING_GROUPS, period=ROLLING_PERIOD)
        .agg(
            pl.col(status_column(base, label)).sum()
            for label in labels
            for base in [SpikeCols.ratio_total, SpikeCols.contributing_rows]
        )
    )

    rolling_agg_lf = rolling_agg_lf.with_columns(
        pl.when(pl.col(status_column(SpikeCols.contributing_rows, label)) > 0)
        .then(
            pl.col(status_column(SpikeCols.ratio_total, label))
            / pl.col(status_column(SpikeCols.contributing_rows, label))
        )
        .cast(pl.Float32)
        .alias(status_column(SpikeCols.employment_status_rolling_ratio, label))
        for label in labels
    ).with_columns(
        pl.col(status_column(SpikeCols.employment_status_rolling_ratio, label))
        .forward_fill()
        .backward_fill()
        .over(ROLLING_GROUPS, order_by=order_key)
        for label in labels
    )

    temp_columns_to_drop = [
        status_column(base, label)
        for label in labels
        for base in [
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
    ]

    return wide_lf.join(rolling_agg_lf, on=monthly_groups, how="left").drop(
        *temp_columns_to_drop, strict=False
    )


def normalise_employment_status_rates_row_wise(
    wide_lf: pl.LazyFrame, percentage_columns: dict[str, str] | None = None
) -> pl.LazyFrame:
    """
    Rescales the unnormalised rate columns to sum to 1 within each row.

    All 5 statuses share a row, so this is a plain row-wise sum and divide - no
    group_by or `.over()`, unlike L1. Known percentages are coalesced back in.

    Known limitation carried over from ticket 2000: only the imputed values are
    normalised, which is only correct while the 5 statuses are always known or null
    together (true for this data, as clean nulls them together).

    Args:
        wide_lf (pl.LazyFrame): Data with an `es_spike_unnormalised_rate_*` and a
            percentage column per status.
        percentage_columns (dict[str, str] | None): Status label to percentage
            column. Defaults to the 5 real statuses; the N=20 stress test passes
            extra dummy statuses.

    Returns:
        pl.LazyFrame: `wide_lf` with one `imputed_employment_status_rate_*` column
            per status, and the unnormalised columns dropped.
    """
    if percentage_columns is None:
        percentage_columns = EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS
    unnormalised_columns = [
        status_column(SpikeCols.unnormalised_rate, label)
        for label in percentage_columns
    ]
    total = pl.sum_horizontal(unnormalised_columns)

    wide_lf = wide_lf.with_columns(
        pl.coalesce(
            pl.col(rate_col),
            pl.col(status_column(SpikeCols.unnormalised_rate, label)) / total,
        )
        .cast(pl.Float32)
        .alias(status_column(SpikeCols.imputed_employment_status_rate, label))
        for label, rate_col in percentage_columns.items()
    )

    return wide_lf.drop(*unnormalised_columns)


def add_imputed_employment_status_rates(
    wide_lf: pl.LazyFrame, percentage_columns: dict[str, str] | None = None
) -> pl.LazyFrame:
    """
    Imputes each percentage column along its rolling trendline, then re-normalises.

    `model_extrapolation` and `model_interpolation` are reused unchanged, called
    once per status. `model_extrapolation`'s output column names can't be set, so
    each status's result is renamed before the next call overwrites it.

    Args:
        wide_lf (pl.LazyFrame): Data with a percentage and an
            `employment_status_rolling_ratio_*` column per status.
        percentage_columns (dict[str, str] | None): Status label to percentage
            column. Defaults to the 5 real statuses; the N=20 stress test passes
            extra dummy statuses.

    Returns:
        pl.LazyFrame: `wide_lf` with one `imputed_employment_status_rate_*` column
            per status.
    """
    if percentage_columns is None:
        percentage_columns = EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS
    for label, rate_col in percentage_columns.items():
        rolling_col = status_column(SpikeCols.employment_status_rolling_ratio, label)
        interpolation_col = status_column(IndCQC.interpolation_model, label)
        extrapolation_col = status_column(IndCQC.extrapolation_model, label)

        wide_lf = model_extrapolation(
            wide_lf,
            column_with_null_values=rate_col,
            model_to_extrapolate_from=rolling_col,
            extrapolation_method="nominal",
            group_columns=JOB_ROLE_GROUPS,
        )
        wide_lf = model_interpolation(
            wide_lf,
            column_with_null_values=rate_col,
            method="trend",
            new_column_name=interpolation_col,
            group_columns=JOB_ROLE_GROUPS,
        )
        wide_lf = wide_lf.rename({IndCQC.extrapolation_model: extrapolation_col}).drop(
            IndCQC.extrapolation_forwards
        )

        wide_lf = wide_lf.with_columns(
            pl.when(pl.col(rate_col).is_null())
            .then(pl.coalesce(extrapolation_col, interpolation_col).clip(lower_bound=0))
            .cast(pl.Float32)
            .alias(status_column(SpikeCols.unnormalised_rate, label))
        ).drop(extrapolation_col, interpolation_col)

    return normalise_employment_status_rates_row_wise(
        wide_lf, percentage_columns=percentage_columns
    )
