import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import MainJobRoleLabels
from utils.value_labels.ascwds_worker.ascwds_worker_jobgroup_dictionary import (
    AscwdsWorkerValueLabelsJobGroup,
)


def percentage_share_handling_zero_sum(column: str | pl.Expr) -> pl.Expr:
    """Calculate the percentage share of a column handling zero sum case.

    If the sum of all non-null values is zero, dividing by zero leads to a NaN.
    In this case we want to assume an even distribution across all non-null
    rows.

    Can be used in conjunction with `.group_by` and `.over` methods to get
    proportions within groups.
    """
    col = pl.col(column) if isinstance(column, str) else column
    total = col.sum()
    return (
        pl.when((total == 0) & (col == 0))
        .then(1 / col.is_not_null().sum())
        .otherwise(col / total)
    )


class ManagerialFilledPostAdjustmentExpr:
    """Polars expression factory for redistributing managerial filled posts.

    This class provides a method to adjust "Registered Manager" (RM) estimates based
    on the counts of names given in the CQC data, and proportionally redistribute the
    difference across other managerial roles.

    The expression is returned via the class method `.build()`.

    Expects DataFrame to contain the following columns:
        - IndCQC.main_job_role_clean_labelled
        - IndCQC.estimate_filled_posts_by_job_role
        - IndCQC.registered_manager_names

    It's expected that we'd execute this expression over each location and import date,
    as shown in the example usage.

    Example Usage:
        >>> adjustment_expr = ManagerialFilledPostAdjustmentExpression.build()
        >>> groups = [IndCQC.location_id, IndCQC.cqc_location_import_date]
        >>> lf.with_columns(adjustment_expr.over(groups).alias("new_col"))
    """

    job_roles: pl.Expr = pl.col(IndCQC.main_job_role_clean_labelled)
    filled_post_estimates: pl.Expr = pl.col(IndCQC.estimate_filled_posts_by_job_role)
    _is_registered_manager: pl.Expr = job_roles == MainJobRoleLabels.registered_manager

    @classmethod
    def build(cls) -> pl.Expr:
        """Build adjust managerial filled post estimates expression."""
        return (
            pl.when(cls._is_non_rm_manager())
            .then(cls._adjusted_non_rm_manager_estimates())
            .when(cls._is_registered_manager)
            .then(cls._clip_rm_count())
            .otherwise(cls.filled_post_estimates)
        )

    @classmethod
    def _is_non_rm_manager(cls) -> pl.Expr:
        """Return DataFrame mask for non-RM manager roles."""
        manager_roles = AscwdsWorkerValueLabelsJobGroup.manager_roles()
        non_rm_manager_roles = [
            r for r in manager_roles if r != MainJobRoleLabels.registered_manager
        ]
        return cls.job_roles.is_in(non_rm_manager_roles)

    @classmethod
    def _clip_rm_count(cls) -> pl.Expr:
        """Return 1 if there is one or more registered manager names listed, 0 if not."""
        return (
            pl.col(IndCQC.registered_manager_names)
            .list.len()
            .clip(upper_bound=1)
            .fill_null(0)
        )

    @classmethod
    def _rm_manager_diff(cls) -> pl.Expr:
        """Subtract capped estimate of registered managers from CQC count to get diff.

        The "_is_registered_manager" mask should only equate to a single row
        (for each location and import date), and so summing with all other
        values as 0 results in the value at that "registered_manager" row
        broadcast to all other rows within the group.
        """
        diff = cls.filled_post_estimates.sub(cls._clip_rm_count())
        return pl.when(cls._is_registered_manager).then(diff).otherwise(0).sum()

    @classmethod
    def _non_rm_manager_proportions(cls) -> pl.Expr:
        """Get proportion of non-RM managerial role estimates as a percentage of total.

        The total in this case is the sum of all non-RM managerial roles. If this
        totals to 0, then an even distribution is assumed.
        """
        return percentage_share_handling_zero_sum(
            pl.when(cls._is_non_rm_manager()).then(cls.filled_post_estimates)
        )

    @classmethod
    def _adjusted_non_rm_manager_estimates(cls) -> pl.Expr:
        """Proportionally redistribute difference across remaining managerial roles.

        Ensure non-negative values following redistribution of RMs.
        """
        return cls.filled_post_estimates.add(
            cls._rm_manager_diff().mul(cls._non_rm_manager_proportions())
        ).clip(lower_bound=0)
