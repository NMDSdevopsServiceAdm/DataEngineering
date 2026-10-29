import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    TrendlineFillTempColumns as TempCols,
)

BOUNDARY_COLUMNS_DATES: list[str] = [
    TempCols.first_known_date,
    TempCols.last_known_date,
    TempCols.previous_known_date,
    TempCols.next_known_date,
]


def add_imputed_trendline(
    lf: pl.LazyFrame,
    known_columns: list[str],
    trendline_columns: list[str],
    group_columns: list[str],
    extrapolation_period: str,
    interpolation_cap_period: str,
) -> pl.LazyFrame:
    """
    Fill short gaps in each known column, within each group.

    Gaps spanning no more than `interpolation_cap_period` are interpolated by date, and the
    first and last known values are carried outside the known range for no more than
    `extrapolation_period`. Known values are kept. Results are Float32.

    The known columns must be populated or null together, so the first one sets the known
    dates for all of them. Each known value is copied onto every row of its group by masking
    out the other rows and taking `.max()` over the group. The boundary dates are added in a
    separate `.with_columns()` so the value expressions can read them back as columns.

    Args:
        lf (pl.LazyFrame): dataset containing the known columns and import date
        known_columns (list[str]): the columns to fill
        trendline_columns (list[str]): the new filled columns, in the order of `known_columns`
        group_columns (list[str]): the columns that define each series, such as location
            and job role
        extrapolation_period (str): how far to carry the first/last known value outside the
            known range, as a Polars offset string (e.g. "12mo")
        interpolation_cap_period (str): the widest gap to interpolate across, as a Polars
            offset string (e.g. "5y")

    Returns:
        pl.LazyFrame: dataset with the trendline columns added
    """
    order_key = IndCQC.cqc_location_import_date

    is_known = pl.col(known_columns[0]).is_not_null()
    known_date = pl.when(is_known).then(pl.col(order_key))

    lf = lf.with_columns(
        known_date.min().over(group_columns).alias(TempCols.first_known_date),
        known_date.max().over(group_columns).alias(TempCols.last_known_date),
        known_date.forward_fill()
        .over(group_columns, order_by=order_key)
        .alias(TempCols.previous_known_date),
        known_date.backward_fill()
        .over(group_columns, order_by=order_key)
        .alias(TempCols.next_known_date),
    )

    is_first_known = is_known & (pl.col(order_key) == pl.col(TempCols.first_known_date))
    is_last_known = is_known & (pl.col(order_key) == pl.col(TempCols.last_known_date))
    first_value_columns = [
        TempCols.first_known_value_prefix + col for col in known_columns
    ]
    last_value_columns = [
        TempCols.last_known_value_prefix + col for col in known_columns
    ]

    lf = lf.with_columns(
        *[
            pl.when(is_first_known)
            .then(pl.col(col))
            .max()
            .over(group_columns)
            .alias(first_col)
            for col, first_col in zip(known_columns, first_value_columns)
        ],
        *[
            pl.when(is_last_known)
            .then(pl.col(col))
            .max()
            .over(group_columns)
            .alias(last_col)
            for col, last_col in zip(known_columns, last_value_columns)
        ],
    )

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
                .over(group_columns, order_by=order_key)
            ),
            pl.when(within_forward_fill)
            .then(pl.col(last_col))
            .when(within_backward_fill)
            .then(pl.col(first_col)),
        )
        .cast(pl.Float32)
        .alias(trendline_col)
        for col, trendline_col, first_col, last_col in zip(
            known_columns, trendline_columns, first_value_columns, last_value_columns
        )
    ).drop(*BOUNDARY_COLUMNS_DATES, *first_value_columns, *last_value_columns)
