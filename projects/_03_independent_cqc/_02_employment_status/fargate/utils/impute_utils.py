import polars as pl

from projects._03_independent_cqc.utils.imputation.extrapolation import (
    model_extrapolation,
)
from projects._03_independent_cqc.utils.imputation.interpolation import (
    model_interpolation,
)
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

TRENDLINE_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage_imputed_for_trendline,
    EmpStatus.temporary_percentage_imputed_for_trendline,
    EmpStatus.bank_or_pool_percentage_imputed_for_trendline,
    EmpStatus.agency_percentage_imputed_for_trendline,
    EmpStatus.other_percentage_imputed_for_trendline,
]

ROLLING_AVERAGE_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage_rolling_avg,
    EmpStatus.temporary_percentage_rolling_avg,
    EmpStatus.bank_or_pool_percentage_rolling_avg,
    EmpStatus.agency_percentage_rolling_avg,
    EmpStatus.other_percentage_rolling_avg,
]

FULL_IMPUTED_PERCENTAGE_COLUMNS: list[str] = [
    EmpStatus.permanent_percentage_full_imputed,
    EmpStatus.temporary_percentage_full_imputed,
    EmpStatus.bank_or_pool_percentage_full_imputed,
    EmpStatus.agency_percentage_full_imputed,
    EmpStatus.other_percentage_full_imputed,
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
    Fill short gaps in the 5 employment status percentages, for each location and job role.

    Gaps spanning no more than `interpolation_cap_period` are interpolated by date, and the first
    and last known values are carried outside the known range for no more than
    `extrapolation_period`. Known values are kept. Linear interpolation between two splits that
    sum to 1 gives a split that also sums to 1.

    Args:
        lf (pl.LazyFrame): dataset containing the employment status percentage columns
        extrapolation_period (str): how far to carry the first/last known value outside the
            known range, as a Polars offset string (e.g. "2y")
        interpolation_cap_period (str): the widest gap to interpolate across, as a Polars
            offset string (e.g. "5y")

    Returns:
        pl.LazyFrame: dataset with the 5 imputed-for-trendline percentage columns added
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
        .alias(trendline_col)
        for col, trendline_col in zip(PERCENTAGE_COLUMNS, TRENDLINE_PERCENTAGE_COLUMNS)
    ).drop(FILL_BOUNDARY_COLUMNS)


def add_rolling_average_percentages(
    lf: pl.LazyFrame,
    rolling_period: str,
) -> pl.LazyFrame:
    """
    Add a rolling average of the imputed-for-trendline percentages per primary service type,
    region and job role.

    Each location counts once, regardless of size. Averages sum to 1 across the 5 statuses
    without normalisation, because a location has every percentage populated or none of them.

    Steps:
        1. Total each percentage and count the contributing locations per date, pre-aggregated
           to stay within the Polars streaming engine.
        2. Roll the totals over `rolling_period` on this small dataset.
        3. Divide each total by the count, carrying the nearest average into any date with no
           contributing locations.
        4. Join the averages back on and drop the temporary columns.

    Args:
        lf (pl.LazyFrame): dataset containing the 5 imputed-for-trendline percentage columns
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
        TempCols.rolling_total_prefix + col for col in TRENDLINE_PERCENTAGE_COLUMNS
    ]

    # Totals are Float64: a Float32 sliding-window sum leaves a residual as values leave the
    # window, pushing averages just outside 0 to 1. The aggregate is small, so this is cheap.
    # polars_streaming: groupby-agg pre-aggregation workaround; data reduction allows streaming but limits flexibility
    date_totals_lf = lf.group_by(date_groups).agg(
        *[
            pl.col(col).cast(pl.Float64).sum().alias(total_col)
            for col, total_col in zip(
                TRENDLINE_PERCENTAGE_COLUMNS, rolling_total_columns
            )
        ],
        pl.col(EmpStatus.permanent_percentage_imputed_for_trendline)
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
        pl.when(pl.col(TempCols.contributing_locations) > 0).then(
            pl.col(total_col) / pl.col(TempCols.contributing_locations)
        )
        # a mean of shares is within 0 to 1, so clipping only removes rounding error
        .clip(0, 1).cast(pl.Float32).alias(rolling_col)
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


def add_full_imputed_percentages(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add the 5 employment status percentages with every gap filled, for each location and job
    role.

    Known values are kept. Each status's gaps are filled along the change in its rolling average:
    extrapolated outside the known range and interpolated by trend between known values, with no
    time limit. Filled values are floored at zero and re-shared to sum to 1, and a location and
    job role with no known values stays null.

    The floored total is at least 1 because the unfloored shares sum to 1, so the re-share needs
    no zero guard. Statuses run one at a time because the shared helpers return fixed column
    names, which are dropped before the next status.

    Args:
        lf (pl.LazyFrame): dataset containing the 5 percentage columns and their rolling averages

    Returns:
        pl.LazyFrame: dataset with the 5 "emplstat_<status>_percentage_full_imputed" columns added
    """
    unnormalised_columns = [
        TempCols.unnormalised_prefix + col for col in PERCENTAGE_COLUMNS
    ]

    for col, rolling_col, unnormalised_col in zip(
        PERCENTAGE_COLUMNS, ROLLING_AVERAGE_PERCENTAGE_COLUMNS, unnormalised_columns
    ):
        lf = model_extrapolation(
            lf, col, rolling_col, "nominal", group_columns=LOCATION_JOB_ROLE_GROUPS
        )
        lf = model_interpolation(
            lf, col, method="trend", group_columns=LOCATION_JOB_ROLE_GROUPS
        )
        lf = lf.with_columns(
            pl.when(pl.col(col).is_null())
            .then(
                pl.coalesce(
                    IndCQC.extrapolation_model, IndCQC.interpolation_model
                ).clip(lower_bound=0)
            )
            .alias(unnormalised_col)
        ).drop(
            IndCQC.extrapolation_forwards,
            IndCQC.extrapolation_model,
            IndCQC.interpolation_model,
        )

    unnormalised_total = pl.sum_horizontal(unnormalised_columns)

    return lf.with_columns(
        pl.coalesce(col, pl.col(unnormalised_col) / unnormalised_total)
        .cast(pl.Float32)
        .alias(full_col)
        for col, unnormalised_col, full_col in zip(
            PERCENTAGE_COLUMNS, unnormalised_columns, FULL_IMPUTED_PERCENTAGE_COLUMNS
        )
    ).drop(unnormalised_columns)
