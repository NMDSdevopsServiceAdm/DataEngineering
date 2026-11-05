from datetime import date

import polars as pl

from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import PrimaryServiceType


def filter_worker_turnover_data(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Filters worker data to the rows kept for turnover analysis.

    Kept rows:
    - Import date on or after 1 January 2020.
    - Location ID is not null.
    - Employment status is permanent or temporary.
    - Import date is the earliest in its month, after the other filters. This
      uses `.over()`, which the streaming engine does not cover, so the sink may
      fall back to the in-memory engine.

    Args:
        worker_lf (pl.LazyFrame): Cleaned ASC-WDS worker data.

    Returns:
        pl.LazyFrame: The filtered worker data with all input columns.
    """
    start_date = date(2020, 1, 1)

    return worker_lf.filter(
        pl.col(AWKClean.ascwds_worker_import_date) >= start_date,
        pl.col(AWKClean.location_id).is_not_null(),
        pl.col(AWKClean.employment_status_clean_labelled).is_in(
            [EmpStat.permanent, EmpStat.temporary]
        ),
    ).filter(earliest_file_per_month_filter_expr(AWKClean.ascwds_worker_import_date))


def add_main_service_hackathon(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds `main_service_hackathon`, a three-way recode of main service ID.

    Values:
    - "Care home with nursing" for code 1 (CHN).
    - "Care home without nursing" for code 2 (CHS).
    - "non-residential" for any other main service code.
    - Null where the main service code is null.

    Args:
        worker_lf (pl.LazyFrame): Cleaned worker data with main service type ID.

    Returns:
        pl.LazyFrame: The input rows with all columns plus `main_service_hackathon`.
    """
    main_service = pl.col(AWKClean.main_service_type_id)
    with_nursing_code = "1"
    without_nursing_code = "2"

    return worker_lf.with_columns(
        pl.when(main_service == with_nursing_code)
        .then(pl.lit(PrimaryServiceType.care_home_with_nursing))
        .when(main_service == without_nursing_code)
        .then(pl.lit(PrimaryServiceType.care_home_only))
        .when(main_service.is_not_null())
        .then(pl.lit(PrimaryServiceType.non_residential))
        .cast(CategoricalColumnTypes.PrimaryServiceEnumType)
        .alias(HackCols.main_service_hackathon)
    )


def add_is_leaver_flag(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Flags workers absent from the snapshot 12 months later.

    Months are matched on month and year. A worker is an establishment ID and
    worker ID pair. `establishment_in_both_snapshots` is true when the
    establishment has a worker in the snapshot 12 months later. `is_leaver` is
    null where that is false, so a worker at a vanished establishment is not
    counted as a leaver. Both flags are null where no snapshot exists 12 months
    later. Semi and anti joins keep each input row once, including duplicates;
    `pl.concat` does not preserve row order.

    Args:
        worker_lf (pl.LazyFrame): Filtered worker data, one snapshot per month.

    Returns:
        pl.LazyFrame: The input rows with all columns plus
            `establishment_in_both_snapshots` and `is_leaver`.
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
    ).with_columns(
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.is_leaver),
    )
    start_lf = start_lf.join(
        end_months_lf,
        left_on=HackCols.end_month,
        right_on=HackCols.import_month,
        how="semi",
    )

    end_establishments_lf = end_lf.select(
        AWKClean.establishment_id, HackCols.import_month
    ).unique()
    establishment_start_keys = [AWKClean.establishment_id, HackCols.end_month]
    establishment_end_keys = [AWKClean.establishment_id, HackCols.import_month]

    establishment_missing_lf = start_lf.join(
        end_establishments_lf,
        left_on=establishment_start_keys,
        right_on=establishment_end_keys,
        how="anti",
    ).with_columns(
        pl.lit(False).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(None, dtype=pl.Boolean).alias(HackCols.is_leaver),
    )
    start_lf = start_lf.join(
        end_establishments_lf,
        left_on=establishment_start_keys,
        right_on=establishment_end_keys,
        how="semi",
    )

    start_keys = worker_keys + [HackCols.end_month]
    end_keys = worker_keys + [HackCols.import_month]

    stayers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="semi"
    ).with_columns(
        pl.lit(True).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(False).alias(HackCols.is_leaver),
    )
    leavers_lf = start_lf.join(
        end_lf, left_on=start_keys, right_on=end_keys, how="anti"
    ).with_columns(
        pl.lit(True).alias(HackCols.establishment_in_both_snapshots),
        pl.lit(True).alias(HackCols.is_leaver),
    )

    return pl.concat(
        [stayers_lf, leavers_lf, establishment_missing_lf, no_end_snapshot_lf]
    ).drop(HackCols.import_month, HackCols.end_month)
