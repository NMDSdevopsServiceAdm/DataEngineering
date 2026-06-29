import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE = 24


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
        lf: Input lazy frame containing workplace records.

    Returns:
        Input frame with ``master_update_date_org`` column added.
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
    """Create purged DataFrames for data reconciliation and workplace activity.

    Removes stale records based on two different staleness definitions:

    - ``ascwds_workplace_df``: filtered by ``data_last_amended_date``, which
      uses the org-level max update date for parent accounts and the record's
      own ``master_update_date`` for others.
    - ``reconciliation_df``: filtered by ``workplace_last_active_date``, which
      additionally accounts for ``last_logged_in_date``.

    Both filters compare against ``purge_date``, derived by subtracting
    ``MONTHS_BEFORE_COMPARISON_DATE_TO_PURGE`` months from the import date.

    Args:
        lf: Lazy ASCWDS workplace frame. Must contain columns defined in
            ``AWPClean``: ``organisation_id``, ``ascwds_workplace_import_date``,
            ``master_update_date``, ``is_parent``, ``last_logged_in_date``.

    Returns:
        Tuple of two ``LazyFrame``s:
            - ``ascwds_workplace_df``: records whose ``data_last_amended_date``
              is on or after ``purge_date``.
            - ``reconciliation_df``: records whose ``workplace_last_active_date``
              is on or after ``purge_date``.

    Note:
        Both returned frames share the same intermediate computation up to the
        filter step. Polars' query optimiser will deduplicate the shared plan
        when both are collected together via ``pl.collect_all``.
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
