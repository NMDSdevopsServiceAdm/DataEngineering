import polars as pl

import projects._03_independent_cqc.utils.model_evaluation_utils as evaluationUtils
import projects._03_independent_cqc.utils.model_utils as modelUtils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_values.categorical_column_values import ImputationRowKind


def add_imputation_row_kind(
    lf: pl.LazyFrame,
    known_column: str,
    imputed_column: str,
    partition_columns: list[str],
    date_column: str,
) -> pl.LazyFrame:
    """
    Label each row "known", "interpolated" (between known dates) or "carried" (before the
    first or after the last known date), per partition. Rows with no imputed share are null.

    One share column of each is enough, as a breakdown's shares are all populated or all null.

    Args:
        lf (pl.LazyFrame): dataset containing the share columns
        known_column (str): a known share column
        imputed_column (str): the matching imputed share column
        partition_columns (list[str]): the columns identifying each timeline
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "imputation_row_kind" added
    """
    is_known = pl.col(known_column).is_not_null()
    is_imputed = pl.col(imputed_column).is_not_null()
    known_date = pl.when(is_known).then(pl.col(date_column))
    is_between_known_dates = pl.col(date_column).is_between(
        known_date.min().over(partition_columns),
        known_date.max().over(partition_columns),
    )
    row_kind_type = pl.Enum(
        ImputationRowKind(ShareModel.imputation_row_kind).categorical_values
    )

    return lf.with_columns(
        pl.when(is_known)
        .then(pl.lit(ImputationRowKind.known))
        .when(is_imputed & is_between_known_dates)
        .then(pl.lit(ImputationRowKind.interpolated))
        .when(is_imputed)
        .then(pl.lit(ImputationRowKind.carried))
        .cast(row_kind_type)
        .alias(ShareModel.imputation_row_kind)
    )


def build_modelling_dataset(
    shares_lf: pl.LazyFrame,
    estimates_lf: pl.LazyFrame,
    known_share_columns: list[str],
    imputed_share_columns: list[str],
    rolling_average_columns: list[str],
) -> pl.LazyFrame:
    """
    Build the dataset for modelling a share breakdown, such as employment status.

    Only the needed columns are selected, as the sources are wide. The join doesn't duplicate
    rows, as estimates are unique per location and date. Build before filtering rows, so each
    location's flags use all its data. Flags use the first known and imputed share columns.

    Args:
        shares_lf (pl.LazyFrame): the breakdown's imputed dataset, a row per location, job role
            and date
        estimates_lf (pl.LazyFrame): the filled posts estimates dataset
        known_share_columns (list[str]): the known share columns
        imputed_share_columns (list[str]): the imputed share columns
        rolling_average_columns (list[str]): the rolling average share columns

    Returns:
        pl.LazyFrame: the modelling dataset, a row per location, job role and date
    """
    location_role_columns = [IndCQC.location_id, IndCQC.published_job_role_label]
    date_column = IndCQC.cqc_location_import_date

    lf = shares_lf.select(
        *location_role_columns,
        date_column,
        IndCQC.primary_service_type,
        IndCQC.care_home,
        IndCQC.current_region,
        IndCQC.services_offered,
        IndCQC.estimate_filled_posts_by_job_role,
        *known_share_columns,
        *imputed_share_columns,
        *rolling_average_columns,
    )

    estimates_lf = estimates_lf.select(
        IndCQC.location_id,
        date_column,
        IndCQC.specialisms_offered,
        IndCQC.regulated_activities_offered,
        IndCQC.current_rural_urban_indicator_2011,
        IndCQC.current_cssr,
        IndCQC.time_registered,
        IndCQC.estimate_filled_posts,
    )
    lf = lf.join(estimates_lf, on=[IndCQC.location_id, date_column], how="left")

    lf = modelUtils.add_date_index(
        lf, partition_columns=location_role_columns, date_column=date_column
    )
    lf = add_imputation_row_kind(
        lf,
        known_column=known_share_columns[0],
        imputed_column=imputed_share_columns[0],
        partition_columns=location_role_columns,
        date_column=date_column,
    )
    lf = evaluationUtils.add_never_submitted_flag(
        lf, known_column=known_share_columns[0], location_column=IndCQC.location_id
    )

    return lf
