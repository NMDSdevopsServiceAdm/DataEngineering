import polars as pl
import polars.selectors as cs

from polars_utils import utils
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWKRaw,
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

jr_in_wp_but_not_wrkr = [12, 13, 14, 18, 19, 20, 21, 28, 29, 30, 31, 32, 33]


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


def create_slv_schema(jr_num_list: list[int], incl_index: bool = False) -> pl.Schema:
    """
    Creates a Polars schema for employees, starters, leavers and vacancy columns.

    If incl_index is True then schema will also include establishment_id and import_date,
    and all datatypes will be pl.String.
    If False, then only slv columns will be in schema and all datatypes will be pl.Int32.

    Single digit job role codes are padded with 0.

    Args:
        jr_num_list (list[int]): A list of job role codes for the roles
            you want in the polars schema.
        incl_index (bool): True to include establishment_id and import_date
          in the polars schema. Defaults to False.

    Returns:
        pl.Schema: A Polars schema of ascwds workplace job role columns.

    Raises:
        ValueError: If given job role list is empty.
    """
    if jr_num_list == []:
        raise ValueError(f"Given job role list must be populated. Got {jr_num_list}")
    if len(jr_num_list) != len(set(jr_num_list)):
        raise ValueError(f"Values in job role list must be unique. Got {jr_num_list}")

    jr_numbers = [f"{i:02d}" for i in jr_num_list]
    suffixes = ["emp", "strt", "stop", "vacy"]

    if incl_index:
        schema = {AWPClean.establishment_id: pl.String, AWPClean.import_date: pl.String}
        schema.update(
            {f"jr{num}{suffix}": pl.String for num in jr_numbers for suffix in suffixes}
        )
    else:
        schema = {
            f"jr{num}{suffix}": pl.Int32 for num in jr_numbers for suffix in suffixes
        }

    return pl.Schema(schema)


def check_job_roles_list(worker_source: pl.LazyFrame, jr_num_list: list[int]) -> None:
    """
    Compares a list of job role codes against codes in raw worker data.

    Some job roles are known to exist in raw workplace but not in
    raw worker data, see jr_in_wp_but_not_wrkr, therefore these are
    removed from the given jr_num_list before checking.

    Args:
        worker_source (pl.LazyFrame): Raw worker LazyFrame.
        jr_num_list (list[int]): A list of job role codes to check.

    Raises:
        ValueError: When given list differs from worker data list.
    """

    worker_job_roles = (
        worker_source.filter(
            pl.col(AWKRaw.main_job_role_id) != "-1"
        )  # -1 == not recorded
        .select(pl.col(AWKRaw.main_job_role_id).cast(pl.Int32))
        .unique()
        .collect()
        .get_column(AWKRaw.main_job_role_id)
        .to_list()
    )

    worker_job_roles = sorted(worker_job_roles)

    # These roles exist in raw workplace but not raw worker data.
    jr_num_list = [role for role in jr_num_list if role not in jr_in_wp_but_not_wrkr]

    if set(jr_num_list) != set(worker_job_roles):
        raise ValueError(
            f"Given job role list ({jr_num_list}) must match equivalent from raw worker data ({worker_job_roles})."
        )
