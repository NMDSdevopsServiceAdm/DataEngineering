import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys


def impute_missing_struct_columns(
    lf: pl.LazyFrame, column_names: list[str]
) -> pl.LazyFrame:
    """
    Imputes missing (None or empty struct) entries in multiple struct columns in a LazyFrame.

    Steps:
    1. Sort the LazyFrame by date for each location ID.
    2. For each column,
        - Creates a new column with the prefix 'imputed_
        - Copies over non-missing struct values (treating empty structs as null)
        - Forward-fills (populate with last known value) missing entries.
        - Backward-fills (next known) missing entries.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing the struct column.
        column_names (list[str]): Names of struct columns to impute.

    Returns:
        pl.LazyFrame: LazyFrame with a new set of columns called `imputed_<column_name>` containing imputed values.
    """
    for column_name in column_names:
        new_col = f"imputed_{column_name}"

        lf = lf.with_columns(
            pl.when((pl.col(column_name).list.len().fill_null(0) > 0))
            .then(pl.col(column_name))
            .otherwise(None)
            .alias(new_col)
        )

        lf = lf.with_columns(
            pl.col(new_col)
            .forward_fill()
            .backward_fill()
            .over(
                partition_by=CQCLClean.location_id,
                order_by=CQCLClean.import_date,
            )
            .alias(new_col)
        )

    return lf


def clean_and_impute_registration_date(
    cqc_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """
    Cleans registration dates into YYYY-MM-DD format, removing invalid dates and then imputing missing values.

    1. Copy all existing registration dates into the imputed column
    2. Remove any time elements from the (imputed) registration date
    3. Replace registration dates that are after the import date with null
    4. Replace registration dates with the minimum registration date for that location id
    5. Replace registration dates with the minimum import date if there are no registration dates for that location

    Args:
        cqc_lf (pl.LazyFrame): Dataframe with registration and import date columns

    Returns:
        pl.LazyFrame: Dataframe with imputed registration date column

    """
    # 1. Copy all existing registration dates into the imputed column
    cqc_lf = cqc_lf.with_columns(
        pl.col(CQCLClean.registration_date).alias(CQCLClean.imputed_registration_date)
    )

    # 2. Remove any time elements from the (imputed) registration date
    cqc_lf = cqc_lf.with_columns(pl.col(CQCLClean.imputed_registration_date).dt.date())

    # 3. Replace registration dates that are after the import date with null
    cqc_lf = cqc_lf.with_columns(
        pl.when(
            pl.col(CQCLClean.imputed_registration_date)
            <= pl.col(Keys.import_date).str.to_date(format="%Y%m%d")
        )
        .then(pl.col(CQCLClean.imputed_registration_date))
        .otherwise(None)
    )

    # 4. Replace registration dates with the minimum registration date for that location id
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(CQCLClean.imputed_registration_date)
            .min()
            .over(CQCLClean.location_id)
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
    )

    # 5. Replace registration dates with the minimum import date if there are no registration dates for that location
    cqc_lf = cqc_lf.with_columns(
        pl.when(pl.col(CQCLClean.imputed_registration_date).is_null())
        .then(
            pl.col(Keys.import_date)
            .min()
            .over(CQCLClean.location_id)
            .str.strptime(pl.Date, format="%Y%m%d")
        )
        .otherwise(pl.col(CQCLClean.imputed_registration_date))
        .alias(CQCLClean.imputed_registration_date)
    )

    return cqc_lf
