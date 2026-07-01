import polars as pl

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


class CleanWorkplaceDataExpressions:
    """
    Polars expressions for clean ASCWDS workplace data.
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


def create_purged_lfs_for_reconciliation_and_data(
    lf: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    """
    This process is designed to purge/remove data which has not been updated for
    an extended period of time.

    If the worplace is a parent account, the mupddate used to purge is the
    maximum of any account within that organisation.

    The purge rules for workplace_last_active_date also takes last_logged_in
    date into account.

    Args:
        lf (pl.LazyFrame): The ascwds_workplace_lf to be purged

    Returns:
        tuple[pl.LazyFrame, pl.LazyFrame]: A tuple of two LazyFrames:
        - ascwds_workplace_lf where old data has been removed based on mupddate
            date
        - reconciliation_lf where old data has been removed based on the maximum
            of mupddate and lastloggedin date
    """
    expr = CleanWorkplaceDataExpressions()
    lf = add_master_update_date_org(lf)
    lf = lf.with_columns(
        expr.purge_date,
        expr.data_last_amended_date,
    ).with_columns(
        expr.workplace_last_active_date,
    )

    ascwds_workplace_lf = lf.filter(
        pl.col(AWPClean.data_last_amended_date) >= pl.col(AWPClean.purge_date)
    )
    reconciliation_lf = lf.filter(
        pl.col(AWPClean.workplace_last_active_date) >= pl.col(AWPClean.purge_date)
    )

    return ascwds_workplace_lf, reconciliation_lf
