from datetime import date

import polars as pl

from polars_utils.cleaning_utils import column_to_date
from polars_utils.column_types import CategoricalColumnTypes
from polars_utils.filtering_utils import earliest_file_per_month_filter_expr
from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols
from utils.column_values.ascwds_labelled_vocab import EmploymentStatusLabels as EmpStat
from utils.column_values.categorical_column_values import PrimaryServiceType


WORKER_CATEGORICAL_COLUMNS = [
    AWKClean.gender,
    AWKClean.ethnicity,
    AWKClean.nationality,
    AWKClean.country_of_birth,
    AWKClean.disabled,
    AWKClean.hourly_or_annual_pay,
    AWKClean.highest_qual_level,
    AWKClean.level_social_care_qualification_held,
    AWKClean.care_certificate_status,
    AWKClean.apprentice,
    AWKClean.zero_hours,
    AWKClean.source_of_recruitment,
]

WORKER_NUMERIC_COLUMNS = [
    AWKClean.age,
    AWKClean.hourly_rate,
    AWKClean.salary,
    AWKClean.contracted_hours,
    AWKClean.average_hours,
    AWKClean.distance_to_work_in_miles,
    AWKClean.start_age,
]

# Kept as raw strings: the start date and the change/save date of each field.
WORKER_STRING_COLUMNS = [
    AWKClean.start_date,
    AWKClean.gender_change_date,
    AWKClean.gender_save_date,
    AWKClean.age_change_date,
    AWKClean.age_save_date,
    AWKClean.ethnicity_change_date,
    AWKClean.ethnicity_save_date,
    AWKClean.is_british_change_date,  # nationality
    AWKClean.is_british_save_date,
    AWKClean.born_in_uk_change_date,  # country of birth
    AWKClean.born_in_uk_save_date,
    AWKClean.disabled_change_date,
    AWKClean.disabled_save_date,
    AWKClean.pay_change_date,  # hourly or annual pay
    AWKClean.pay_save_date,
    AWKClean.highest_qual_level_change_date,
    AWKClean.highest_qual_level_save_date,
    AWKClean.level_social_care_qualification_held_change_date,
    AWKClean.level_social_care_qualification_held_save_date,
    AWKClean.care_certificate_status_change_date,
    AWKClean.care_certificate_status_save_date,
    AWKClean.apprentice_change_date,
    AWKClean.apprentice_save_date,
    AWKClean.contracted_hours_change_date,
    AWKClean.contracted_hours_save_date,
    AWKClean.average_hours_change_date,
    AWKClean.average_hours_save_date,
    AWKClean.zero_hours_change_date,
    AWKClean.zero_hours_save_date,
    AWKClean.start_date_change_date,
    AWKClean.start_date_save_date,
    AWKClean.source_of_recruitment_change_date,
    AWKClean.source_of_recruitment_save_date,
    AWKClean.start_in_sector_change_date,  # start age
    AWKClean.start_in_sector_save_date,
]


def add_raw_worker_columns(
    worker_lf: pl.LazyFrame, raw_worker_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """Adds raw worker modelling columns from the worker's own snapshot.

    Left joins on establishment ID, worker ID and import date. Only the needed
    raw columns are kept, selected before the join, to limit memory. Raw values
    are not cleaned: code columns become categorical, numeric columns are cast to
    `Float32` (values that do not parse become null), and the rest stay strings.
    A raw worker with duplicate keys would duplicate rows.

    Args:
        worker_lf (pl.LazyFrame): Flagged worker data with establishment ID,
            worker ID and `ascwds_worker_import_date`.
        raw_worker_lf (pl.LazyFrame): Raw ASC-WDS worker data, all strings.

    Returns:
        pl.LazyFrame: The input rows with all columns plus the raw worker columns.
    """
    keys = [
        AWKClean.establishment_id,
        AWKClean.worker_id,
        AWKClean.ascwds_worker_import_date,
    ]

    raw_worker_lf = column_to_date(
        raw_worker_lf.select(
            AWKClean.establishment_id,
            AWKClean.worker_id,
            AWKClean.import_date,
            *WORKER_CATEGORICAL_COLUMNS,
            *WORKER_NUMERIC_COLUMNS,
            *WORKER_STRING_COLUMNS,
        ),
        AWKClean.import_date,
        AWKClean.ascwds_worker_import_date,
    ).drop(AWKClean.import_date)

    raw_worker_lf = raw_worker_lf.with_columns(
        pl.col(AWKClean.establishment_id).cast(
            CategoricalColumnTypes.EstablishmentCatType
        ),
        pl.col(WORKER_CATEGORICAL_COLUMNS).cast(pl.Categorical),
        pl.col(WORKER_NUMERIC_COLUMNS).cast(pl.Float32, strict=False),
    )

    return worker_lf.join(raw_worker_lf, on=keys, how="left")


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


def add_main_service_type(worker_lf: pl.LazyFrame) -> pl.LazyFrame:
    """Adds `main_service_type`, a three-way recode of main service ID.

    Values:
    - "Care home with nursing" for code 1 (CHN).
    - "Care home without nursing" for code 2 (CHS).
    - "non-residential" for any other main service code.

    Args:
        worker_lf (pl.LazyFrame): Cleaned worker data with main service type ID.

    Returns:
        pl.LazyFrame: The input rows with all columns plus `main_service_type`.
    """
    main_service = pl.col(AWKClean.main_service_type_id)
    with_nursing_code = "1"
    without_nursing_code = "2"

    return worker_lf.with_columns(
        pl.when(main_service == with_nursing_code)
        .then(pl.lit(PrimaryServiceType.care_home_with_nursing))
        .when(main_service == without_nursing_code)
        .then(pl.lit(PrimaryServiceType.care_home_only))
        .otherwise(pl.lit(PrimaryServiceType.non_residential))
        .cast(CategoricalColumnTypes.PrimaryServiceEnumType)
        .alias(HackCols.main_service_type)
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
