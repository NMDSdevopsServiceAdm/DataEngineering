import polars as pl

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)
from utils.column_values.ascwds_labelled_vocab import IsParent, ParentPermission
from utils.column_values.categorical_column_values import ParentsOrSinglesAndSubs


def add_parents_or_singles_and_subs_column(
    ascwds_workplace_lf: pl.LazyFrame,
) -> pl.LazyFrame:
    """Classifies each ASC-WDS workplace as a parent account, or a single/sub account.

    Args:
        ascwds_workplace_lf (pl.LazyFrame): ASC-WDS workplace data.

    Returns:
        pl.LazyFrame: The same data with `parents_or_singles_and_subs` added.
    """
    is_parent_account = (pl.col(AWPClean.is_parent) == IsParent.is_parent) | (
        (pl.col(AWPClean.is_parent) == IsParent.is_not_parent)
        & (pl.col(AWPClean.parent_permission) == ParentPermission.parent_has_ownership)
    )
    return ascwds_workplace_lf.with_columns(
        pl.when(is_parent_account)
        .then(pl.lit(ParentsOrSinglesAndSubs.parents))
        .otherwise(pl.lit(ParentsOrSinglesAndSubs.singles_and_subs))
        .alias(ReconColumn.parents_or_singles_and_subs)
    )
