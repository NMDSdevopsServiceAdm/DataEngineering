from datetime import date

import polars as pl

from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat


def filter_worker_turnover_data(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Keeps permanent and temporary workers from each month's earliest file.

    Only files from 1 January 2020 onwards are kept. The earliest file is
    chosen after the other filters, so it is the earliest with permanent or
    temporary workers. This month filter uses `.over()`, which the streaming
    engine does not cover, so the sink may fall back to the in-memory engine.

    Args:
        worker_lf (pl.LazyFrame): Cleaned ASC-WDS worker data.

    Returns:
        pl.LazyFrame: The filtered worker data with all input columns.
    """
    start_date = date(2020, 1, 1)

    return worker_lf.filter(
        pl.col(AWKClean.ascwds_worker_import_date) >= start_date,
        pl.col(AWKClean.employment_status_clean_labelled).is_in(
            [EmpStat.permanent, EmpStat.temporary]
        ),
    ).filter(earliest_file_per_month_filter_expr(AWKClean.ascwds_worker_import_date))


def get_latest_save_date_per_establishment(
    worker_lf: pl.LazyFrame, snapshot_worker_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Finds the latest worker save date per establishment for each snapshot.

    Covers workers of every employment status, so `worker_lf` is the
    unfiltered data. Only snapshot import dates are kept, and they are
    semi-joined before aggregating so the aggregate stays small. Only three
    columns are read from `worker_lf` and one from `snapshot_worker_lf`.

    Args:
        worker_lf (pl.LazyFrame): Cleaned ASC-WDS worker data.
        snapshot_worker_lf (pl.LazyFrame): Output of
            `filter_worker_turnover_data`, which defines the snapshot import
            dates.

    Returns:
        pl.LazyFrame: Establishment ID, import date and `latest_save_date`
            (null where every worker save date is null), one row per
            establishment and snapshot.
    """
    snapshot_dates_lf = snapshot_worker_lf.select(
        AWKClean.ascwds_worker_import_date
    ).unique()

    return (
        worker_lf.join(
            snapshot_dates_lf, on=AWKClean.ascwds_worker_import_date, how="semi"
        )
        .group_by(AWKClean.establishment_id, AWKClean.ascwds_worker_import_date)
        .agg(
            pl.col(AWKClean.ascwds_worker_save_date)
            .max()
            .alias(HackCols.latest_save_date)
        )
    )


def add_establishment_active_flag(
    worker_lf: pl.LazyFrame, save_dates_lf: pl.LazyFrame, n_months: int = 6
) -> pl.LazyFrame:
    """Flags workers whose establishment saved data recently at both snapshots.

    The gap is whole calendar months from the establishment's latest save date
    to the import date, at the start snapshot and the snapshot 12 months
    later. It is negative if the save date is after the import date, and null
    with no save date. `establishment_active` is true when both gaps are at
    most `n_months`, false when either gap is null, and null where there is no
    snapshot 12 months later (`is_leaver` is null). Expects one snapshot per
    month. Gaps are built on the small save date frame so only one join
    reaches the worker rows, which are not re-evaluated to find snapshots.

    Args:
        worker_lf (pl.LazyFrame): Worker data with `is_leaver`, one snapshot
            per month.
        save_dates_lf (pl.LazyFrame): Latest save date per establishment and
            snapshot import date, from `get_latest_save_date_per_establishment`.
        n_months (int): Most months since the latest save date for an
            establishment to count as active.

    Returns:
        pl.LazyFrame: The input rows with `months_since_save_start`,
            `months_since_save_end` and `establishment_active` added.
    """
    keys = [AWKClean.establishment_id, AWKClean.ascwds_worker_import_date]
    import_date = pl.col(AWKClean.ascwds_worker_import_date)
    save_date = pl.col(HackCols.latest_save_date)

    save_gap_lf = save_dates_lf.with_columns(
        import_date.dt.truncate("1mo").alias(HackCols.import_month),
        (
            (import_date.dt.year() - save_date.dt.year()) * 12
            + (import_date.dt.month() - save_date.dt.month())
        ).alias(HackCols.months_since_save_start),
    )

    start_gap_lf = save_gap_lf.select(
        *keys,
        pl.col(HackCols.import_month).dt.offset_by("12mo").alias(HackCols.end_month),
        HackCols.months_since_save_start,
    )
    end_gap_lf = save_gap_lf.select(
        AWKClean.establishment_id,
        pl.col(HackCols.import_month).alias(HackCols.end_month),
        pl.col(HackCols.months_since_save_start).alias(HackCols.months_since_save_end),
    )
    gap_lf = start_gap_lf.join(
        end_gap_lf, on=[AWKClean.establishment_id, HackCols.end_month], how="left"
    ).drop(HackCols.end_month)

    worker_lf = worker_lf.join(gap_lf, on=keys, how="left")

    both_gaps_within_limit = (
        (pl.col(HackCols.months_since_save_start) <= n_months)
        & (pl.col(HackCols.months_since_save_end) <= n_months)
    ).fill_null(False)

    return worker_lf.with_columns(
        pl.when(pl.col(HackCols.is_leaver).is_null())
        .then(None)
        .otherwise(both_gaps_within_limit)
        .alias(HackCols.establishment_active)
    )


def add_is_leaver_flag(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Flags workers absent from the snapshot 12 months later.

    Months are matched on month and year. A worker is an establishment ID and
    worker ID pair. `is_leaver` is null where no snapshot exists 12 months later.
    Semi and anti joins keep each input row once, including duplicates;
    `pl.concat` does not preserve row order.

    Args:
        worker_lf (pl.LazyFrame): Filtered worker data, one snapshot per month.

    Returns:
        pl.LazyFrame: The input rows with all columns plus `is_leaver`.
    """
    worker_keys = [AWKClean.establishment_id, AWKClean.worker_id]

    worker_lf = worker_lf.with_columns(
        pl.col(AWKClean.ascwds_worker_import_date)
        .dt.truncate("1mo")
        .alias(HackCols.import_month)
    )
    end_lf = worker_lf.select(worker_keys + [HackCols.import_month])
    end_months_lf = end_lf.select(HackCols.import_month).unique()

    start_lf = worker_lf.with_columns(
        pl.col(HackCols.import_month).dt.offset_by("12mo").alias(HackCols.end_month)
    )

    no_end_snapshot_lf = start_lf.join(
        end_months_lf,
        left_on=HackCols.end_month,
        right_on=HackCols.import_month,
        how="anti",
    ).with_columns(pl.lit(None, dtype=pl.Boolean).alias(HackCols.is_leaver))
    start_lf = start_lf.join(
        end_months_lf,
        left_on=HackCols.end_month,
        right_on=HackCols.import_month,
        how="semi",
    )

    start_keys = worker_keys + [HackCols.end_month]
    end_keys = worker_keys + [HackCols.import_month]

    stayers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="semi"
    ).with_columns(pl.lit(False).alias(HackCols.is_leaver))
    leavers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="anti"
    ).with_columns(pl.lit(True).alias(HackCols.is_leaver))

    return pl.concat([stayers_lf, leavers_lf, no_end_snapshot_lf]).drop(
        HackCols.import_month, HackCols.end_month
    )
