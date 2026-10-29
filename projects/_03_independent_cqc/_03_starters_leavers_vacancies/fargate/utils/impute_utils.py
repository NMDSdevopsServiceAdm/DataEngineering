import polars as pl

from projects._03_independent_cqc.utils.imputation.imputation import model_imputation

from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    SlvImputeTempColumns as TempCols,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)


def add_rolling_avg_rate(
    lf: pl.LazyFrame,
    rolling_period: str,
    group_levels: list[list[str]],
    min_contributors: int,
) -> pl.LazyFrame:
    """
    Add the rolling turnover rate: total leavers over total employees in each group.

    Each row uses the finest group with at least `min_contributors` contributing workplaces
    on its date, else the next coarser group, and the last group whatever its size. Rates are
    weighted by employees, so a workplace's rate times its employees gives its leavers.
    Workplaces with null or zero employees contribute to neither total.

    Steps, for each group level:
        1. Total the leavers and employees and count contributors per date, pre-aggregated
           to stay within the Polars streaming engine.
        2. Roll the totals over `rolling_period` on this small dataset and divide.
        3. Join the rolling rate and contributor count back on.

    Totals are Float64: a Float32 sliding-window sum leaves a residual as values leave the
    window. The aggregates are small, so this is cheap. The rate is not capped above,
    because leavers can exceed employees in a small workplace.

    Args:
        lf (pl.LazyFrame): dataset containing the trendline rate, estimated employees and
            every group column
        rolling_period (str): the rolling window length, as a Polars offset string (e.g. "6mo")
        group_levels (list[list[str]]): group columns, finest first
        min_contributors (int): workplaces needed on the date to use a group level

    Returns:
        pl.LazyFrame: dataset with the rolling average turnover rate added
    """
    order_key = IndCQC.cqc_location_import_date
    rate = pl.col(SLVCols.ascwds_turnover_rate_imputed_trendline)
    employees = pl.col(EmpStatus.estimated_employees)
    is_contributor = rate.is_not_null() & (employees >= 1)

    rolling_avg_columns = []
    contributor_columns = []
    for level, group_columns in enumerate(group_levels):
        date_groups = group_columns + [order_key]
        rolling_avg_col = f"{TempCols.rolling_avg_level_prefix}{level}"
        contributors_col = f"{TempCols.contributors_level_prefix}{level}"
        rolling_avg_columns.append(rolling_avg_col)
        contributor_columns.append(contributors_col)

        # polars_streaming: groupby-agg pre-aggregation workaround; data reduction allows streaming but limits flexibility
        date_totals_lf = lf.group_by(date_groups).agg(
            pl.when(is_contributor)
            .then(rate.cast(pl.Float64) * employees.cast(pl.Float64))
            .sum()
            .alias(TempCols.weighted_rate_total),
            pl.when(is_contributor)
            .then(employees.cast(pl.Float64))
            .sum()
            .alias(TempCols.employee_total),
            is_contributor.sum().alias(contributors_col),
        )

        # polars_streaming: .rolling() with groupby requires pre-aggregation workaround
        rolling_lf = (
            date_totals_lf.sort(*group_columns, order_key)
            .rolling(
                index_column=order_key, group_by=group_columns, period=rolling_period
            )
            .agg(
                pl.col(TempCols.weighted_rate_total).sum(),
                pl.col(TempCols.employee_total).sum(),
                pl.col(contributors_col).last(),
            )
            .select(
                *date_groups,
                pl.when(pl.col(TempCols.employee_total) > 0)
                .then(
                    pl.col(TempCols.weighted_rate_total)
                    / pl.col(TempCols.employee_total)
                )
                .clip(lower_bound=0)
                .cast(pl.Float32)
                .alias(rolling_avg_col),
                contributors_col,
            )
        )

        lf = lf.join(rolling_lf, on=date_groups, how="left")

    # Start from the coarsest level, then override with each finer level that has enough
    # contributors.
    rolling_avg = pl.col(rolling_avg_columns[-1])
    for rolling_avg_col, contributors_col in zip(
        reversed(rolling_avg_columns[:-1]), reversed(contributor_columns[:-1])
    ):
        rolling_avg = (
            pl.when(pl.col(contributors_col) >= min_contributors)
            .then(pl.col(rolling_avg_col))
            .otherwise(rolling_avg)
        )

    return lf.with_columns(
        rolling_avg.alias(SLVCols.ascwds_turnover_rate_rolling_avg)
    ).drop(*rolling_avg_columns, *contributor_columns)


def add_imputed_rate(lf: pl.LazyFrame, extrapolation_method: str) -> pl.LazyFrame:
    """
    Add the turnover rate with every gap filled, for each location and job role.

    Known rates are kept. Gaps are filled by following the change in the rolling average,
    either as a ratio or as a nominal difference, extrapolated outside the known range and interpolated
    by trend between known rates, with no time limit. A location and job role with no known
    rate stays null, as does a ratio fill where the rolling average at the known rate is 0,
    since the ratio is undefined. Filled rates are floored at 0.

    Args:
        lf (pl.LazyFrame): dataset containing the turnover rate and its rolling average
        extrapolation_method (str): "ratio" or "nominal"

    Returns:
        pl.LazyFrame: dataset with the imputed turnover rate added
    """
    imputed_col = SLVCols.ascwds_turnover_rate_imputed

    lf = model_imputation(
        lf,
        SLVCols.ascwds_turnover_rate,
        SLVCols.ascwds_turnover_rate_rolling_avg,
        imputed_col,
        care_home=None,
        extrapolation_method=extrapolation_method,
        group_columns=[IndCQC.location_id, IndCQC.published_job_role_label],
    )

    return lf.with_columns(
        pl.when(pl.col(imputed_col).is_finite())
        .then(pl.col(imputed_col).clip(lower_bound=0))
        .alias(imputed_col)
    )
