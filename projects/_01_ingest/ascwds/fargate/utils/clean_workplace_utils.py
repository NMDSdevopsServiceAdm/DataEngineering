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

# Groups of establishment IDs known to have submitted identical ASC-WDS data
# under different accounts. Whether a group is still identical is re-checked
# at runtime (recheck_duplicate_establishments), not assumed.
DUPLICATE_ESTABLISHMENT_GROUPS: list[frozenset[str]] = [
    frozenset({"48904", "49966", "49967", "49968"}),
    frozenset(
        {
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
    ),
]


def exclude_test_accounts_filter() -> pl.Expr:
    """
    Return a filter expression that excludes internal Skills for Care test organisations.

    Returns:
        pl.Expr: A Polars expression that can be used to filter a LazyFrame.
    """
    return ~pl.col(AWPClean.organisation_id).is_in(TEST_ACCOUNTS)


NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES: list[str] = [
    AWPClean.total_staff,
    AWPClean.worker_records,
    AWPClean.total_starters,
    AWPClean.total_leavers,
    AWPClean.total_vacancies,
]


def recheck_duplicate_establishments(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Recheck the known duplicate establishment groups and null their numeric
    data if still matching each other.

    DUPLICATE_ESTABLISHMENT_GROUPS are groups proven to have unreliable
    numeric data at certain import dates. Since identifying them by
    observing NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES, we
    have wiped their data, but they may have re-submitted since then.

    Establishments within a group that still have the same values for all
    NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES per import data are returned.
    An all-null/0 match still counts as identical.

    polars_streaming:
    Each group filters down to a handful of rows before comparison, so
    looping over the groups and concatenating is negligible regardless of
    streaming support.

    Args:
        lf (pl.LazyFrame): Raw ASC-WDS workplace LazyFrame containing
            establishment_id, import_date and
            NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES.

    Returns:
        pl.LazyFrame: establishment_id and import_date of establishments
            with duplicate numeric data.
    """
    numeric_data = "numeric_data"
    duplicate_count = "duplicate_count"
    still_matching_lfs = []
    for group in DUPLICATE_ESTABLISHMENT_GROUPS:
        group_lf = lf.filter(pl.col(AWPClean.establishment_id).is_in(group)).select(
            AWPClean.establishment_id,
            AWPClean.import_date,
            pl.struct(NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES).alias(numeric_data),
        )
        still_matching_lfs.append(
            group_lf.group_by(AWPClean.import_date, numeric_data)
            .agg(AWPClean.establishment_id, pl.len().alias(duplicate_count))
            .filter(pl.col(duplicate_count) > 1)
            .select(AWPClean.establishment_id, AWPClean.import_date)
            .explode(AWPClean.establishment_id)
        )

    return pl.concat(still_matching_lfs)


def null_duplicate_establishment_numeric_data(
    lf: pl.LazyFrame, still_matching_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Null numeric submission data for rows matching still_matching_lf.

    Columns in NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES and columns selected
    by is_slv_job_role_column() are nulled for rows matched to still_matching_lf.
    Tolerates a frame without job role columns yet (require_all=False),
    since this runs both before and after they're joined on in clean_ascwds_workplace.py.

    Args:
        lf (pl.LazyFrame): Workplace LazyFrame containing establishment_id
            and import_date.
        still_matching_lf (pl.LazyFrame): Output of
            recheck_duplicate_establishments.

    Returns:
        pl.LazyFrame: lf with target numeric columns nulled for matching rows.
    """
    should_null_col = "should_null_duplicate"
    lf = lf.join(
        still_matching_lf.with_columns(pl.lit(True).alias(should_null_col)),
        on=[AWPClean.establishment_id, AWPClean.import_date],
        how="left",
    )

    columns_to_null = (
        cs.by_name(*NUMERIC_COLUMNS_TO_NULL_FOR_DUPLICATES, require_all=False)
        | expr.is_slv_job_role_column()
    )
    return lf.with_columns(
        pl.when(pl.col(should_null_col).is_not_null())
        .then(None)
        .otherwise(columns_to_null)
        .name.keep()
    ).drop(should_null_col)


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
    """Add the org-level max master_update_date onto the input frame.

    Args:
        lf (pl.LazyFrame): Input lazy frame containing workplace records.

    Returns:
        pl.LazyFrame: Input frame with ``master_update_date_org`` column added.
    """
    return lf.with_columns(
        pl.col(AWPClean.master_update_date)
        .max()
        .over(AWPClean.organisation_id, AWPClean.ascwds_workplace_import_date)
        .alias(AWPClean.master_update_date_org)
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


def merge_legacy_job_role_columns(
    lf: pl.LazyFrame, job_role_mapping: dict[str, list[str]]
) -> pl.LazyFrame:
    """
    Merge ASC-WDS workplace job role columns using the legacy job role mapping.

    For each key job role code in job_role_mapping, sums the key job role
    together with all the listed job role columns. This sum then replaces the
    key job roles value. The listed job role columns are then dropped.

    Args:
        lf (pl.LazyFrame): ASC-WDS workplace LazyFrame.
        job_role_mapping (dict[str, list[str]]): A mapping of job roles.
            E.g. {role_to_merge_and_keep: [role_1_to_merge_and_drop, role_2_to_merge_and_drop...]}

    Returns:
        pl.LazyFrame: Input LazyFrame in which columns have been merged and
            removed.
    """
    job_role_cols = lf.collect_schema().names()
    job_role_suffixes = list(
        {re.sub(r"^jr\d+", "", col) for col in job_role_cols if col.startswith("jr")}
    )

    lf = lf.with_columns(
        merge_legacy_job_roles_expressions(job_role_mapping, job_role_suffixes),
    )

    # Flatten job role lists from job_role_mapping into single list, format them
    # to match column names, then drop those columns.
    old_roles = [old for olds in job_role_mapping.values() for old in olds]
    roles_to_drop = [
        f"jr{role}{suffix}" for role in old_roles for suffix in job_role_suffixes
    ]
    lf = lf.drop(cs.by_name(*roles_to_drop, require_all=False))

    return lf


def merge_legacy_job_roles_expressions(
    job_role_mapping: dict[str, list[str]], slv_suffixes: list[str]
) -> Generator[pl.Expr, None, None]:
    """
    A generator function that yields Polars expressions that sum
    ASC-WDS workplace job role columns in the given mapping dictionary
    that have the given slv_suffixes.

    When all columns to sum are null then expression produces null.

    Args:
        job_role_mapping (dict[str, list[str]]): A mapping of job roles.
            E.g. {role_to_merge_and_keep: [role_1_to_merge_and_drop, role_2_to_merge_and_drop...]}
        slv_suffixes (list[str]): A list of ASC-WDS workplace job role column suffixes.
            E.g. ["flag", "emp", "work"]

    Yields:
        pl.Expr: Polars expressions for summing columns.

    """
    for role_to_keep, roles_to_merge in job_role_mapping.items():
        for suffix in slv_suffixes:
            prefixes = [f"jr{role_to_keep}"] + [f"jr{old}" for old in roles_to_merge]
            cols = cs.starts_with(*prefixes) & cs.ends_with(suffix)
            yield (
                pl.when(pl.all_horizontal(cols.is_null()))
                .then(pl.lit(None))
                .otherwise(pl.sum_horizontal(cols))
                .alias(f"jr{role_to_keep}{suffix}")
            )
