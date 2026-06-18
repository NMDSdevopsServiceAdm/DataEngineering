from datetime import date

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsByJobRoleCategoricalValues as CatVals,
)

job_role_labels = IndCQC.main_job_role_clean_labelled
# Cache the stable, ordered list of job role labels for reuse and determinism.
MAIN_JOB_ROLE_VALUES = CatVals.main_job_role_labels_column_values.categorical_values


def join_estimates_to_ascwds(
    estimates_lf: pl.LazyFrame,
    ascwds_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Join job role estimates to ASCWDS counts ensuring a row for every job role.

    Performs a cross join on the estimates join keys first to ensure there is a row for
    every job role across all time periods for each location. This is then joined with
    the ASCWDS data.

    Args:
        estimates_lf (pl.LazyFrame): Input LazyFrame with Estimates data.
        ascwds_lf (pl.LazyFrame): Input LazyFrame with ASC-WDS job role counts.

    Returns:
        pl.LazyFrame: Joined LazyFrame with a row for every job role.

    """
    join_keys = [
        IndCQC.ascwds_workplace_import_date,
        IndCQC.establishment_id,
    ]

    narrow_keys_lf = estimates_lf.select(
        [IndCQC.id_per_locationid_import_date] + join_keys
    )

    roles_lf = create_job_role_lazyframe()

    expanded_keys_lf = narrow_keys_lf.join(roles_lf, how="cross")

    expanded_counts_lf = expanded_keys_lf.join(
        other=ascwds_lf,
        on=join_keys + [job_role_labels],
        how="left",
    )

    return estimates_lf.join(
        expanded_counts_lf.drop(join_keys),
        on=IndCQC.id_per_locationid_import_date,
        how="right",
    ).drop(join_keys)


def create_job_role_lazyframe() -> pl.LazyFrame:
    """
    Creates a LazyFrame with one column containing a row per job role label.

    Returns:
        pl.LazyFrame: A LazyFrame of job role labels.
    """
    values = MAIN_JOB_ROLE_VALUES
    # Build explicit 1-tuple rows so Polars interprets each string as a single column value.
    rows = [(v,) for v in values]
    return pl.LazyFrame(
        data=rows,
        schema={job_role_labels: pl.Enum(values)},
        orient="row",
    )


def reduced_data_filter_expr(
    today: date | None = None,
    fy_start_month: int = 4,
    lookback_fy_years: int = 2,
    quarter_months: tuple[int, ...] = (1, 4, 7, 10),
    date_col: str = "cqc_location_import_date",
) -> pl.Expr:
    """
    Build a Polars expression for filtering a reduced dataset using financial-year
    windowing with quarterly sampling for older data.

    The filter implements a two-tier retention strategy:

    1. Full retention window:
       Rows with dates greater than or equal to the start of the current financial
       year minus `lookback_fy_years` are always included.

    2. Historical sampling window:
       Rows older than the full retention window are only included if their month
       falls within `quarter_months` (e.g. quarterly snapshots).

    This allows recent data to be fully retained while reducing storage and
    processing cost for older data via periodic sampling.

    Args:
        today (date | None): Reference date used to compute financial year boundaries.
            If None, defaults to the current system date.

        fy_start_month (int): Month in which the financial year starts
            (default is 4 for April).

        lookback_fy_years (int): Number of financial years to retain in full before
            applying sampling.

        quarter_months (tuple[int, ...]): Months considered valid for quarterly sampling
            of historical data (Defaults to Jan, Apr, Jul, and Oct).

        date_col (str): Name of the date column the filter is applied to.

    Returns:
        pl.Expr: A Polars boolean expression that can be used inside `.filter()` or
            `.with_columns()` to select rows based on the reduced data strategy.
    """
    today: date = today or date.today()

    fy_year = today.year if today.month >= fy_start_month else today.year - 1

    monthly_start = date(fy_year - lookback_fy_years, fy_start_month, 1)

    dt = pl.col(date_col)

    return (dt >= monthly_start) | (
        (dt < monthly_start) & (dt.dt.month().is_in(quarter_months))
    )
