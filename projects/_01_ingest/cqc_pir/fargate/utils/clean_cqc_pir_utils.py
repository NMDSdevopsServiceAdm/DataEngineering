import polars as pl

from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPIRCleanedColumns as PIRClean,
)
from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns as PIRCols
from utils.column_values.categorical_column_values import CareHome, PIRType

TWO_SUBMISSIONS = 2
LARGE_LOCATION_IDENTIFIER = 100


def add_care_home_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Adds a `care_home` column derived from `pir_type`.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing `pir_type`.

    Returns:
        pl.LazyFrame: The input LazyFrame with an added `care_home` column.
    """
    return lf.with_columns(
        pl.when(pl.col(PIRCols.pir_type) == PIRType.residential)
        .then(pl.lit(CareHome.care_home))
        .when(pl.col(PIRCols.pir_type) == PIRType.community)
        .then(pl.lit(CareHome.not_care_home))
        .otherwise(None)
        .alias(PIRClean.care_home)
    )


def filter_latest_submission_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Filters to the latest submission date per grouping of location, import date and care home status.

    Uses a window function rather than a join, since we only need the max
    submission date per group to filter the existing rows - there's no
    foreign value being broadcast onto every row.

    Args:
        lf (pl.LazyFrame): A LazyFrame that must contain at least `location_id`,
            `cqc_pir_import_date`, `care_home` and `pir_submission_date_as_date`.

    Returns:
        pl.LazyFrame: A filtered form of the input LazyFrame, where there is now
            only the latest submission date per grouping of the other 3 fields.
    """
    latest_submission_date = (
        pl.col(PIRClean.pir_submission_date_as_date)
        .max()
        .over(PIRClean.location_id, PIRClean.cqc_pir_import_date, PIRClean.care_home)
    )

    return lf.filter(
        pl.col(PIRClean.pir_submission_date_as_date) == latest_submission_date
    ).unique(
        subset=[
            PIRClean.location_id,
            PIRClean.cqc_pir_import_date,
            PIRClean.care_home,
            PIRClean.pir_submission_date_as_date,
        ]
    )


def null_people_directly_employed_outliers(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Creates a clean version of the `pir_people_directly_employed` column.

    Duplicates `pir_people_directly_employed` as
    `pir_people_directly_employed_cleaned` and then runs outlier removal on it.

    Args:
        lf (pl.LazyFrame): A LazyFrame containing `pir_people_directly_employed`.

    Returns:
        pl.LazyFrame: A LazyFrame containing `pir_people_directly_employed_cleaned`.
    """
    lf = lf.with_columns(
        pl.col(PIRCols.pir_people_directly_employed).alias(
            PIRClean.pir_people_directly_employed_cleaned
        )
    )

    return null_large_single_submission_locations(lf)


def null_large_single_submission_locations(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Excludes CQC PIR data for locations who submitted a large employee figure and only submitted once.

    Large locations can have a big impact in localised data. In order to help
    identify data quality issues, we only want to keep data for larger locations
    who have submitted more than once. This is so we have multiple submissions
    to verify that the location are consistently large as opposed to a data
    input error.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.

    Returns:
        pl.LazyFrame: The input LazyFrame with large, single submission values
            nulled out.
    """
    submission_count = (
        pl.col(PIRClean.pir_people_directly_employed_cleaned)
        .count()
        .over(PIRClean.location_id)
    )
    max_people_employed = (
        pl.col(PIRClean.pir_people_directly_employed_cleaned)
        .max()
        .over(PIRClean.location_id)
    )

    return lf.with_columns(
        pl.when(
            (max_people_employed >= LARGE_LOCATION_IDENTIFIER)
            & (submission_count < TWO_SUBMISSIONS)
        )
        .then(None)
        .otherwise(pl.col(PIRClean.pir_people_directly_employed_cleaned))
        .alias(PIRClean.pir_people_directly_employed_cleaned)
    )
