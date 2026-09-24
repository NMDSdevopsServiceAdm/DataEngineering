from typing import Callable

import polars as pl

import projects._03_independent_cqc.utils.model_evaluation_utils as evaluationUtils
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ShareModelColumns as ShareModel,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCRatingsValues,
    ImputationRowKind,
)


def add_elapsed_months(lf: pl.LazyFrame, date_column: str) -> pl.LazyFrame:
    """
    Add calendar months since the dataset's earliest date, so a quarterly step counts as 3.

    Args:
        lf (pl.LazyFrame): dataset containing `date_column`
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "elapsed_months" added
    """
    month_number = pl.col(date_column).dt.year() * 12 + pl.col(date_column).dt.month()

    return lf.with_columns(
        (month_number - month_number.min()).alias(ShareModel.elapsed_months)
    )


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


def add_provider_location_count(
    lf: pl.LazyFrame, provider_column: str, location_column: str, date_column: str
) -> pl.LazyFrame:
    """
    Add each provider's number of distinct locations per date, counting locations, not rows.

    Args:
        lf (pl.LazyFrame): dataset containing the provider, location and date columns
        provider_column (str): the provider ID
        location_column (str): the location ID
        date_column (str): the date column

    Returns:
        pl.LazyFrame: dataset with "provider_location_count" added
    """
    return lf.with_columns(
        pl.col(location_column)
        .n_unique()
        .over(provider_column, date_column)
        .alias(ShareModel.provider_location_count)
    )


def add_latest_overall_rating(
    lf: pl.LazyFrame, ratings_lf: pl.LazyFrame, location_column: str, date_column: str
) -> pl.LazyFrame:
    """
    Add each location's latest real overall CQC rating as of each row's date.

    Blank (e.g. "Inspected but not rated") and undated ratings are skipped, and locations
    with no rating yet get "Not yet rated". Same-date ratings are ordered by assessment date,
    then the ratings job's latest rating flag, then the best rating. Found once per location
    and date, then joined on. Replaces any existing rating.

    Args:
        lf (pl.LazyFrame): dataset containing the location and date columns
        ratings_lf (pl.LazyFrame): CQC ratings dataset, with the same location column name
        location_column (str): the location ID
        date_column (str): the date each rating must be in place by

    Returns:
        pl.LazyFrame: dataset with categorical "latest_overall_rating" added
    """
    # Ratings store location IDs as strings, so match this dataset's type for the as-of join.
    location_type = lf.collect_schema()[location_column]

    # The ratings job's numerical values, so the best rating wins any remaining tie.
    rating_value = (
        pl.col(CQCRatings.overall_rating)
        .str.to_lowercase()
        .replace_strict(
            {
                CQCRatingsValues.outstanding.lower(): 4,
                CQCRatingsValues.good.lower(): 3,
                CQCRatingsValues.requires_improvement.lower(): 2,
                CQCRatingsValues.inadequate.lower(): 1,
            },
            default=0,
        )
    )

    ratings_by_date_lf = (
        ratings_lf.filter(
            pl.col(CQCRatings.overall_rating).is_not_null()
            & pl.col(CQCRatings.date).is_not_null()
        )
        .group_by(location_column, CQCRatings.date)
        .agg(
            pl.col(CQCRatings.overall_rating)
            .sort_by(
                [CQCL.assessment_date, CQCRatings.latest_rating_flag, rating_value],
                descending=True,
                nulls_last=True,
            )
            .first()
            .cast(pl.Categorical)
            .alias(ShareModel.latest_overall_rating)
        )
        .with_columns(
            pl.col(location_column).cast(location_type),
            pl.col(CQCRatings.date).str.to_date("%Y-%m-%d"),
        )
        .sort(CQCRatings.date)
    )

    # join_asof needs both sides sorted by date, which Polars can't check when using `by`.
    location_date_ratings_lf = (
        lf.select(location_column, date_column)
        .unique()
        .sort(date_column)
        .join_asof(
            ratings_by_date_lf,
            left_on=date_column,
            right_on=CQCRatings.date,
            by=location_column,
            strategy="backward",
            check_sortedness=False,
        )
        .drop(CQCRatings.date)
    )

    lf = lf.drop(ShareModel.latest_overall_rating, strict=False).join(
        location_date_ratings_lf, on=[location_column, date_column], how="left"
    )

    return lf.with_columns(
        pl.col(ShareModel.latest_overall_rating).fill_null(
            CQCRatingsValues.not_yet_rated
        )
    )


def build_modelling_dataset(
    shares_lf: pl.LazyFrame,
    estimates_lf: pl.LazyFrame,
    ratings_lf: pl.LazyFrame,
    known_share_columns: list[str],
    imputed_share_columns: list[str],
    rolling_average_columns: list[str],
) -> pl.LazyFrame:
    """
    Build the dataset for modelling a share breakdown, such as employment status.

    Only the needed columns are selected, as the sources are wide. Neither join duplicates
    rows: estimates are unique per location and date, and ratings are reduced to one per
    location and date. Build before filtering rows, so each location's flags use all its data.

    Args:
        shares_lf (pl.LazyFrame): the breakdown's imputed dataset, a row per location, job role
            and date
        estimates_lf (pl.LazyFrame): the filled posts estimates dataset
        ratings_lf (pl.LazyFrame): the CQC ratings dataset
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
        IndCQC.provider_id,
        IndCQC.primary_service_type,
        IndCQC.care_home,
        IndCQC.current_region,
        IndCQC.services_offered,
        IndCQC.estimate_filled_posts_by_job_role,
        *known_share_columns,
        *imputed_share_columns,
        *rolling_average_columns,
    )

    # Match the estimates' location ID type to this dataset's for the join.
    location_type = lf.collect_schema()[IndCQC.location_id]
    estimates_lf = estimates_lf.select(
        pl.col(IndCQC.location_id).cast(location_type),
        date_column,
        IndCQC.specialisms_offered,
        IndCQC.regulated_activities_offered,
        IndCQC.current_rural_urban_indicator_2011,
        IndCQC.current_cssr,
        IndCQC.time_registered,
        IndCQC.estimate_filled_posts,
    )
    lf = lf.join(estimates_lf, on=[IndCQC.location_id, date_column], how="left")

    lf = add_elapsed_months(lf, date_column)
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
    lf = add_provider_location_count(
        lf,
        provider_column=IndCQC.provider_id,
        location_column=IndCQC.location_id,
        date_column=date_column,
    )
    lf = add_latest_overall_rating(
        lf, ratings_lf, location_column=IndCQC.location_id, date_column=date_column
    )

    return lf


def add_fold_safe_rolling_average(
    lf: pl.LazyFrame,
    rolling_average_function: Callable[[pl.LazyFrame], pl.LazyFrame],
    input_columns: list[str],
    output_columns: list[str],
    fold_column: str,
    n_folds: int,
    key_columns: list[str],
) -> pl.LazyFrame:
    """
    Add a rolling average where each fold's rows only get averages from the other folds.

    Each fold's inputs are blanked, the function run and that fold's rows kept, then joined
    back so every row keeps its inputs. Existing `output_columns` are replaced. Folds run one
    at a time to limit memory, so pass a collected frame (`df.lazy()`) to avoid recomputing.

    Args:
        lf (pl.LazyFrame): dataset containing the input, fold and key columns
        rolling_average_function (Callable[[pl.LazyFrame], pl.LazyFrame]): adds
            `output_columns` from `input_columns`, keeping every row
        input_columns (list[str]): the columns to average
        output_columns (list[str]): the columns the function adds
        fold_column (str): each row's fold, numbered from 0
        n_folds (int): the number of folds
        key_columns (list[str]): the columns identifying each row

    Returns:
        pl.LazyFrame: dataset with fold-safe `output_columns`
    """
    lf = lf.drop(output_columns, strict=False)

    fold_output_lfs = []
    for fold in range(n_folds):
        blanked_lf = lf.with_columns(
            pl.when(pl.col(fold_column) != fold).then(pl.col(column)).alias(column)
            for column in input_columns
        )
        fold_output_lfs.append(
            rolling_average_function(blanked_lf)
            .filter(pl.col(fold_column) == fold)
            .select(*key_columns, *output_columns)
        )

    return lf.join(
        pl.concat(fold_output_lfs, parallel=False), on=key_columns, how="left"
    )
