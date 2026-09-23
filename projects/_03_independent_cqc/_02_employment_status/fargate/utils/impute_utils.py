import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusImputeTempColumns as TempCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

LOCATION_JOB_ROLE_GROUPS: list[str] = [
    IndCQC.location_id,
    IndCQC.published_job_role_label,
]

PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage,
    EmpStatus.temporary_percentage,
    EmpStatus.bank_or_pool_percentage,
    EmpStatus.agency_percentage,
    EmpStatus.other_percentage,
]

IMPUTED_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage_imputed,
    EmpStatus.temporary_percentage_imputed,
    EmpStatus.bank_or_pool_percentage_imputed,
    EmpStatus.agency_percentage_imputed,
    EmpStatus.other_percentage_imputed,
]

ROLLING_AVERAGE_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage_rolling_avg,
    EmpStatus.temporary_percentage_rolling_avg,
    EmpStatus.bank_or_pool_percentage_rolling_avg,
    EmpStatus.agency_percentage_rolling_avg,
    EmpStatus.other_percentage_rolling_avg,
]

FILL_BOUNDARY_COLUMNS: list[str] = [
    TempCols.first_known_date,
    TempCols.last_known_date,
    TempCols.previous_known_date,
    TempCols.next_known_date,
    *[TempCols.first_known_value_prefix + col for col in PERCENTAGE_COLUMNS],
    *[TempCols.last_known_value_prefix + col for col in PERCENTAGE_COLUMNS],
]


def add_fill_boundaries(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add the first and last known date and percentages, and the nearest known date either side
    of every row, for each location and job role.

    The 5 percentages are either all populated or all null, so one known-indicator (the
    permanent share) sets the dates for all of them; only the first and last known values are
    needed per percentage column. Each known value is copied onto every row of its group by
    masking out the other rows, then taking `.max()` over the group. Split into two
    `.with_columns()` calls so the second can read the boundary dates back as columns, instead of
    repeating the expressions that built them and making Polars compute those windows twice.

    Args:
        lf (pl.LazyFrame): dataset containing the employment status percentage columns

    Returns:
        pl.LazyFrame: dataset with the boundary columns added
    """
    order_key = IndCQC.cqc_location_import_date

    is_known = pl.col(EmpStatus.permanent_percentage).is_not_null()
    known_date = pl.when(is_known).then(pl.col(order_key))

    lf = lf.with_columns(
        known_date.min()
        .over(LOCATION_JOB_ROLE_GROUPS)
        .alias(TempCols.first_known_date),
        known_date.max().over(LOCATION_JOB_ROLE_GROUPS).alias(TempCols.last_known_date),
        known_date.forward_fill()
        .over(LOCATION_JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.previous_known_date),
        known_date.backward_fill()
        .over(LOCATION_JOB_ROLE_GROUPS, order_by=order_key)
        .alias(TempCols.next_known_date),
    )

    is_first_known = is_known & (pl.col(order_key) == pl.col(TempCols.first_known_date))
    is_last_known = is_known & (pl.col(order_key) == pl.col(TempCols.last_known_date))

    return lf.with_columns(
        *[
            pl.when(is_first_known)
            .then(pl.col(col))
            .max()
            .over(LOCATION_JOB_ROLE_GROUPS)
            .alias(TempCols.first_known_value_prefix + col)
            for col in PERCENTAGE_COLUMNS
        ],
        *[
            pl.when(is_last_known)
            .then(pl.col(col))
            .max()
            .over(LOCATION_JOB_ROLE_GROUPS)
            .alias(TempCols.last_known_value_prefix + col)
            for col in PERCENTAGE_COLUMNS
        ],
    )


def add_short_term_imputed_percentages(
    lf: pl.LazyFrame,
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Impute the 5 employment status percentages within time limits, for each location and job
    role.

    Gaps are interpolated by date if they span no more than `interpolation_cap_period`, and the
    first and last known values are carried outside the known range for no more than
    `extrapolation_period`. Known values are kept as they are. Interpolating each percentage
    linearly between two splits that sum to 1 gives a split that also sums to 1.

    Args:
        lf (pl.LazyFrame): dataset containing the employment status percentage columns
        extrapolation_period (str): how far to carry the first/last known value outside the
            known range, as a Polars offset string (e.g. "2y")
        interpolation_cap_period (str): the widest gap to interpolate across, as a Polars
            offset string (e.g. "5y")

    Returns:
        pl.LazyFrame: dataset with the 5 "emplstat_<status>_percentage_imputed" columns added
    """
    order_key = IndCQC.cqc_location_import_date

    lf = add_fill_boundaries(lf)

    within_interpolation_cap = pl.col(TempCols.next_known_date) <= pl.col(
        TempCols.previous_known_date
    ).dt.offset_by(interpolation_cap_period)

    within_forward_fill = (pl.col(order_key) > pl.col(TempCols.last_known_date)) & (
        pl.col(order_key)
        <= pl.col(TempCols.last_known_date).dt.offset_by(extrapolation_period)
    )
    within_backward_fill = (pl.col(order_key) < pl.col(TempCols.first_known_date)) & (
        pl.col(order_key)
        >= pl.col(TempCols.first_known_date).dt.offset_by(f"-{extrapolation_period}")
    )

    return lf.with_columns(
        pl.coalesce(
            pl.col(col),
            pl.when(within_interpolation_cap).then(
                pl.col(col)
                .interpolate_by(pl.col(order_key))
                .over(LOCATION_JOB_ROLE_GROUPS, order_by=order_key)
            ),
            pl.when(within_forward_fill)
            .then(pl.col(TempCols.last_known_value_prefix + col))
            .when(within_backward_fill)
            .then(pl.col(TempCols.first_known_value_prefix + col)),
        )
        .cast(pl.Float32)
        .alias(imputed_col)
        for col, imputed_col in zip(PERCENTAGE_COLUMNS, IMPUTED_PERCENTAGE_COLUMNS)
    ).drop(FILL_BOUNDARY_COLUMNS)


def add_rolling_average_percentages(
    lf: pl.LazyFrame,
    rolling_period: str,
) -> pl.LazyFrame:
    """
    Add a rolling average of the short-term imputed percentages per primary service type,
    region and job role.

    The average is the mean imputed share across the locations contributing to a group, counting
    each location once regardless of size. Averages sum to 1 across the 5 statuses without extra
    normalisation, because a location has every imputed percentage populated or none of them.

    Steps:
        1. Total each imputed percentage and count the contributing locations per date,
           pre-aggregated so the calculation stays within the Polars streaming engine.
        2. Roll the totals over `rolling_period` on this small aggregated dataset.
        3. Divide each total by the count, carrying the nearest known average into any date with
           no contributing locations.
        4. Join the averages back onto the location-level dataset and drop the temporary columns.

    Args:
        lf (pl.LazyFrame): dataset containing the 5 "emplstat_<status>_percentage_imputed"
            columns
        rolling_period (str): the rolling window length, as a Polars offset string (e.g. "6mo")

    Returns:
        pl.LazyFrame: dataset with the 5 "emplstat_<status>_percentage_rolling_avg" columns added
    """
    rolling_groups = [
        IndCQC.primary_service_type,
        IndCQC.current_region,
        IndCQC.published_job_role_label,
    ]
    order_key = IndCQC.cqc_location_import_date
    date_groups = rolling_groups + [order_key]
    rolling_total_columns = [
        TempCols.rolling_total_prefix + col for col in IMPUTED_PERCENTAGE_COLUMNS
    ]

    # polars_streaming: groupby-agg pre-aggregation workaround; data reduction allows streaming but limits flexibility
    date_totals_lf = lf.group_by(date_groups).agg(
        *[
            pl.col(col).sum().alias(total_col)
            for col, total_col in zip(IMPUTED_PERCENTAGE_COLUMNS, rolling_total_columns)
        ],
        pl.col(EmpStatus.permanent_percentage_imputed)
        .is_not_null()
        .sum()
        .alias(TempCols.contributing_locations),
    )

    # polars_streaming: .rolling() with groupby requires pre-aggregation workaround; could use .over() for grouped rolling windows when streaming is supported
    rolling_agg_lf = (
        date_totals_lf.sort(*rolling_groups, order_key)
        .rolling(index_column=order_key, group_by=rolling_groups, period=rolling_period)
        .agg(
            *[pl.col(total_col).sum() for total_col in rolling_total_columns],
            pl.col(TempCols.contributing_locations).sum(),
        )
    )

    rolling_agg_lf = rolling_agg_lf.with_columns(
        pl.when(pl.col(TempCols.contributing_locations) > 0)
        .then(pl.col(total_col) / pl.col(TempCols.contributing_locations))
        .cast(pl.Float32)
        .alias(rolling_col)
        for total_col, rolling_col in zip(
            rolling_total_columns, ROLLING_AVERAGE_PERCENTAGE_COLUMNS
        )
    ).with_columns(
        pl.col(ROLLING_AVERAGE_PERCENTAGE_COLUMNS)
        .forward_fill()
        .backward_fill()
        .over(rolling_groups, order_by=order_key)
    )

    return lf.join(
        rolling_agg_lf.drop(*rolling_total_columns, TempCols.contributing_locations),
        on=date_groups,
        how="left",
    )
