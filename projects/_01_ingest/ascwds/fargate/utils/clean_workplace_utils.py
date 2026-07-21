import re
from typing import Generator

import polars as pl
import polars.selectors as cs

import polars_utils.expressions as expr
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE = 24

# Organisation IDs used internally by Skills for Care for testing purposes.
# These organisations do not represent real workplaces and are excluded from
# downstream processing.
TEST_ACCOUNTS: set[str] = {
    "305",
    "307",
    "308",
    "309",
    "310",
    "2452",
    "28470",
    "26792",
    "31657",
    "31138",
    "51818",
}

# Establishment IDs known to contain duplicated ASC-WDS submissions.
#
# These records represent the same workplace data uploaded against multiple
# accounts. The issue was raised with the support team but there is no way of
# automatically blocking this going forwards.
#
# There are two sets of workplaces here who submitted the exact same ASCWDS
# files on the same day.
# - Four locations ("48904" to "49968")
# - 18 locations ("50538" to "50870").
DUPLICATE_ESTABLISHMENT_IDS: set[str] = {
    "48904",
    "49966",
    "49967",
    "49968",
    "50538",
    "50561",
    "50590",
    "50596",
    "50598",
    "50621",
    "50623",
    "50624",
    "50627",
    "50629",
    "50639",
    "50640",
    "50767",
    "50769",
    "50770",
    "50771",
    "50869",
    "50870",
}


def valid_workplace_filter() -> pl.Expr:
    """
    Return a filter expression that excludes known invalid workplace records.

    Removes:
        - Internal Skills for Care test organisations.
        - Known duplicate workplace submissions.

    Returns:
        pl.Expr: A Polars expression that can be used to filter a LazyFrame.
    """
    return (
        # exclude test accounts
        ~pl.col(AWPClean.organisation_id).is_in(TEST_ACCOUNTS)
        &
        # exclude duplicate establishments
        ~pl.col(AWPClean.establishment_id).is_in(DUPLICATE_ESTABLISHMENT_IDS)
    )


def remove_rows_with_duplicate_location_ids(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Remove rows where a non-null location_id appears more than once within
    the same ascwds_workplace_import_date.

    polars_streaming:
    Identify duplicate keys separately rather than using a window expression.
    This allows the aggregation to stream over only the key columns, avoiding
    the memory overhead of `.over()` on wide datasets.

    Args:
        lf (pl.LazyFrame): A LazyFrame with duplicate location_id's per ascwds_workplace_import_date.

    Returns:
        pl.LazyFrame: The input LazyFrame without rows containing duplicate location_id's.
    """
    group_cols = [AWPClean.location_id, AWPClean.ascwds_workplace_import_date]
    duplicate_keys = (
        lf.select(group_cols)
        .filter(pl.col(AWPClean.location_id).is_not_null())
        .group_by(group_cols)
        .len()
        .filter(pl.col("len") > 1)
        .select(group_cols)
    )

    return lf.join(
        duplicate_keys,
        on=group_cols,
        how="anti",
    )


class PurgeWorkplaceDataExpressions:
    """
    Polars expressions for purging ASCWDS workplace data.

    Attributes:
        data_last_amended_date (pl.Expr): Expression to compute the most
            recent update date for the workplace, using the org-level max for
            parent workplaces and establishment-level dates for others.
        workplace_last_active_date (pl.Expr): Expression to compute the most recent
            date the workplace was active, using the maximum of data_last_amended_date
            and last_logged_in_date.
        purge_date (pl.Expr): Expression to compute the purge date based on the given purge window.
    """

    data_last_amended_date: pl.Expr
    workplace_last_active_date: pl.Expr
    purge_date: pl.Expr

    def __init__(self):
        # Parent workplaces use the org-level max; others use their own date
        self.data_last_amended_date = (
            pl.when(pl.col(AWPClean.is_parent) == "Yes")
            .then(pl.col(AWPClean.master_update_date_org))
            .otherwise(pl.col(AWPClean.master_update_date))
            .alias(AWPClean.data_last_amended_date)
        )

        # Most recent of data_last_amended_date and last_logged_in_date.
        self.workplace_last_active_date = pl.max_horizontal(
            pl.col(AWPClean.data_last_amended_date),
            pl.col(AWPClean.last_logged_in_date),
        ).alias(AWPClean.workplace_last_active_date)

        # Import date minus the configured purge window in months.
        self.purge_date = (
            pl.col(AWPClean.ascwds_workplace_import_date)
            .dt.offset_by(f"-{MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE}mo")
            .alias(AWPClean.purge_date)
        )


def add_master_update_date_org(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Join the org-level max master_update_date onto the input frame.

    Implemented as a group_by + join rather than a window expression to
    preserve compatibility with Polars streaming execution.

    Args:
        lf (pl.LazyFrame): Input lazy frame containing workplace records.

    Returns:
        pl.LazyFrame: Input frame with ``master_update_date_org`` column added.
    """
    # polars_streaming: groupby+join workaround; could be .max().over() when window functions support streaming
    org_max = lf.group_by(
        AWPClean.organisation_id, AWPClean.ascwds_workplace_import_date
    ).agg(
        pl.col(AWPClean.master_update_date).max().alias(AWPClean.master_update_date_org)
    )

    return lf.join(
        org_max,
        on=[AWPClean.organisation_id, AWPClean.ascwds_workplace_import_date],
        how="left",
    )


def create_purge_date_columns(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Ochestrator function to create purge date columns for the input LazyFrame.

    Args:
        lf (pl.LazyFrame): The input LazyFrame.

    Returns:
        pl.LazyFrame: The LazyFrame with purge date columns added.
    """
    expr = PurgeWorkplaceDataExpressions()
    lf = add_master_update_date_org(lf)
    lf = lf.with_columns(
        expr.purge_date,
        expr.data_last_amended_date,
    ).with_columns(
        expr.workplace_last_active_date,
    )

    return lf


def apply_data_corrections(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Apply legacy data corrections to an ASC-WDS workplace LazyFrame.

    The following corrections are applied:
        - Convert empty and whitespace-only string values to NULL across all
          string columns.
        - Set `parent_permission` to NULL where its value is `3`, as this is a
          legacy value present in older data.

    Args:
        lf (pl.LazyFrame): Input LazyFrame containing ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: A LazyFrame with the data corrections applied.
    """
    # Treat blank strings as missing values.
    lf = lf.with_columns(cs.string().str.strip_chars().replace("", None))

    # legacy parent permission temporarily contained invalid "3" codes
    lf = lf.with_columns(pl.col(AWPClean.parent_permission).replace(3, None))

    return lf


class BoundingExpressions:
    """Create Polars expressions that bound workplace metrics to valid ranges.

    The class defines expressions for constraining filled-posts values and
    starters, leavers, vacancies (SLV) job-role values to acceptable ranges.
    These expressions are designed for use in lazy Polars pipelines and keep
    the transformation logic declarative and readable.

    Attributes:
        filled_posts_bounding_cols (list[str]): Columns used in filled-posts
            estimates needing bounding.
        filled_posts_lower_bound (int): Minimum accepted value for filled-posts
            columns.
        slv_bounding_cols (pl.selectors.Selector): Selector for SLV job-role
            columns.
        slv_lower_bound (int): Minimum accepted value for SLV job-role columns.
        slv_upper_bound (int): Maximum accepted value for SLV job-role columns.
        filled_posts_expr (pl.Expr): Expression that bounds columns needed in
            filled-posts estimates to the configured valid range and renaming.
        slv_expr (pl.Expr): Expression that bounds SLV job-role columns to the
            configured valid range while preserving the original column names.
    """

    filled_posts_bounding_cols: list[str] = [
        AWPClean.total_staff,
        AWPClean.worker_records,
    ]
    filled_posts_lower_bound: int = 1

    slv_bounding_cols: pl.selectors.Selector = expr.is_slv_job_role_column()
    slv_lower_bound: int = 1
    slv_upper_bound: int = 998  # 999 has been used as code for not known

    filled_posts_expr: pl.Expr = (
        pl.when(pl.col(*filled_posts_bounding_cols) >= filled_posts_lower_bound)
        .then(pl.col(*filled_posts_bounding_cols))
        .otherwise(None)
        .name.suffix("_bounded")
    )

    slv_expr: pl.Expr = (
        (
            pl.when(
                (slv_bounding_cols.as_expr() < slv_lower_bound)
                | (slv_bounding_cols.as_expr() > slv_upper_bound)
            )
        )
        .then(None)
        .otherwise(slv_bounding_cols)
        .name.keep()
    )


def fix_legacy_job_roles(
    lf: pl.LazyFrame, legacy_job_roles_dict: dict[str, list[str]]
) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns.

    The ASC-WDS workplace job role columns corresponding to each key
    and all its values from given dict are summed, then all value columns
    are dropped.

    Job role 33 (personal assistant) is hard coded to be dropped without
    merging their values.

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.
        legacy_job_roles_dict (dict[str, list[str]]): A mapping of job roles. E.g.
            {new: [old_1, old_2...]}

    Returns:
        pl.LazyFrame: Input LazyFrame in which columns have been merged and
            removed.
    """
    job_role_cols = lf.collect_schema().names()
    job_role_suffixes = list(
        {re.sub(r"^jr\d+", "", col) for col in job_role_cols if col.startswith("jr")}
    )

    lf = lf.with_columns(
        legacy_replacement_expressions(legacy_job_roles_dict, job_role_suffixes),
    )

    old_roles = [old for olds in legacy_job_roles_dict.values() for old in olds]
    cols_to_drop = [
        f"jr{role}{suffix}"
        for role in old_roles + ["33"]
        for suffix in job_role_suffixes
    ]
    lf = lf.drop(cols_to_drop)

    return lf


def legacy_replacement_expressions(
    legacy_job_roles_dict: dict[str, list[str]], slv_suffixes: list[str]
) -> Generator[pl.Expr, None, None]:
    """
    A generator function that yields Polars expressions that sum
    ASC-WDS workplace job role columns in the given legacy_job_roles_dict
    that have the given slv_suffixes.

    Args:
        legacy_job_roles_dict (dict[str, list[str]]): A mapping of job roles.
            E.g. {new: [old_1, old_2...]}
        slv_suffixes (list[str]): A list of ASC-WDS workplace job role column suffixes.
            E.g. ["flag", "emp", "work"]

    Yields:
        pl.Expr: Polars expressions for summing columns.

    """
    for new, olds in legacy_job_roles_dict.items():
        for suffix in slv_suffixes:
            prefixes = [f"jr{new}"] + [f"jr{old}" for old in olds]
            yield pl.sum_horizontal(
                (cs.starts_with(*prefixes) & cs.ends_with(suffix))
            ).alias(f"jr{new}{suffix}")
