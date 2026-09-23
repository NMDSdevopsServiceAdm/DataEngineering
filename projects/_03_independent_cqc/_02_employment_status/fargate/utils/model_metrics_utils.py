import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)


def aggregate_shares_by_cell(
    lf: pl.LazyFrame,
    predicted_columns: list[str],
    actual_columns: list[str],
    weight_column: str,
    cell_columns: list[str],
) -> pl.LazyFrame:
    """
    Aggregate predicted and actual shares into cells, such as service, CSSR, job role and year.

    Only rows with known actual shares are used, so predictions are compared with actual shares
    for the same workers. Each cell's share is the average of its rows' shares weighted by their
    workers, Σ(share × weight) / Σ weight, so it matches the share of all the cell's workers. The
    sums are in Float64, since a cell can sum a lot of rows and the aggregated result is small.

    Args:
        lf (pl.LazyFrame): dataset containing the share, weight and cell columns
        predicted_columns (list[str]): the predicted share columns
        actual_columns (list[str]): the actual share columns
        weight_column (str): the number of workers each row's shares apply to
        cell_columns (list[str]): the columns that define each cell

    Returns:
        pl.LazyFrame: one row per cell with any known actual shares, with its weighted shares
            (keeping their column names) and its total weight in "cell_weight"
    """
    weight = pl.col(weight_column).cast(pl.Float64)

    lf = lf.filter(pl.all_horizontal(pl.col(actual_columns).is_not_null()))

    return lf.group_by(cell_columns).agg(
        *[
            ((pl.col(column).cast(pl.Float64) * weight).sum() / weight.sum()).alias(
                column
            )
            for column in [*predicted_columns, *actual_columns]
        ],
        weight.sum().alias(ShareModel.cell_weight),
    )


def score_cell_shares(
    cells_lf: pl.LazyFrame,
    predicted_columns: list[str],
    actual_columns: list[str],
    weight_column: str,
    by_columns: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Score each share's predictions against the actual shares across cells.

    R² and mean absolute error are both weighted by each cell's weight, so bigger cells count
    for more: R² = 1 - Σ w(actual - predicted)² / Σ w(actual - weighted mean actual)², and the
    error is Σ w|actual - predicted| / Σ w, in percentage points.

    Args:
        cells_lf (pl.LazyFrame): one row per cell, such as from `aggregate_shares_by_cell`
        predicted_columns (list[str]): the predicted share columns
        actual_columns (list[str]): the actual share columns, paired in order with
            `predicted_columns`
        weight_column (str): each cell's weight
        by_columns (list[str] | None): columns to score each group of separately, such as the
            fold. Defaults to scoring all cells together.

    Returns:
        pl.LazyFrame: one row per share (and per group of `by_columns`), naming the share by its
            actual column in "share", with its "r2" and its "mean_absolute_error" in percentage
            points
    """
    by_columns = by_columns or []
    weight = pl.col(weight_column).cast(pl.Float64)

    share_scores_lfs = []
    for predicted_column, actual_column in zip(
        predicted_columns, actual_columns, strict=True
    ):
        predicted = pl.col(predicted_column).cast(pl.Float64)
        actual = pl.col(actual_column).cast(pl.Float64)
        error = actual - predicted
        weighted_mean_actual = (weight * actual).sum() / weight.sum()

        scores = [
            (
                1
                - (weight * error**2).sum()
                / (weight * (actual - weighted_mean_actual) ** 2).sum()
            ).alias(IndCQC.r2),
            (100 * (weight * error.abs()).sum() / weight.sum()).alias(
                ShareModel.mean_absolute_error
            ),
        ]
        scores_lf = (
            cells_lf.group_by(by_columns).agg(scores)
            if by_columns
            else cells_lf.select(scores)
        )

        share_scores_lfs.append(
            scores_lf.select(
                *by_columns,
                pl.lit(actual_column).alias(ShareModel.share),
                IndCQC.r2,
                ShareModel.mean_absolute_error,
            )
        )

    return pl.concat(share_scores_lfs)


def mean_period_to_period_change(
    lf: pl.LazyFrame,
    share_columns: list[str],
    partition_columns: list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Measure how jumpy each share is: its mean absolute change from one period to the next.

    Changes are only taken between consecutive dates within a partition (such as a location and
    job role), never from one partition's last date to the next partition's first.

    Args:
        lf (pl.LazyFrame): dataset containing the share, partition and date columns
        share_columns (list[str]): the share columns to measure, such as predictions
        partition_columns (list[str]): the columns that identify each timeline
        date_column (str): the date that orders each timeline

    Returns:
        pl.LazyFrame: one row per share, named by its column in "share", with its
            "mean_period_to_period_change" in percentage points
    """
    return lf.select(
        (
            100
            * pl.col(column)
            .cast(pl.Float64)
            .diff()
            .over(partition_columns, order_by=date_column)
            .abs()
            .mean()
        ).alias(column)
        for column in share_columns
    ).unpivot(
        variable_name=ShareModel.share,
        value_name=ShareModel.mean_period_to_period_change,
    )
