import polars as pl

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
    Add the number of calendar months between each row's date and the earliest date in the
    dataset.

    Months follow the calendar rather than counting distinct dates, so a step between quarterly
    dates counts as 3 months.

    Args:
        lf (pl.LazyFrame): dataset containing `date_column`
        date_column (str): the date to count months from

    Returns:
        pl.LazyFrame: dataset with the "elapsed_months" column added
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
    Label where each row's imputed share comes from, within each partition (such as each
    location and job role).

    Rows with a known share are "known". Rows with only an imputed share are "interpolated" when
    they fall between the partition's first and last known dates, and "carried" when they fall
    before the first or after the last. Rows without an imputed share are left null.

    The shares in a breakdown are either all populated or all null, so one known share column
    and its imputed column are enough to label every row.

    Args:
        lf (pl.LazyFrame): dataset containing the known and imputed share columns
        known_column (str): one of the known share columns
        imputed_column (str): the imputed column for the same share
        partition_columns (list[str]): the columns that identify each timeline
        date_column (str): the date that orders each timeline

    Returns:
        pl.LazyFrame: dataset with the "imputation_row_kind" column added
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


def add_never_submitted_flag(
    lf: pl.LazyFrame, known_column: str, location_column: str
) -> pl.LazyFrame:
    """
    Flag the locations that never have a known share, for any job role or date.

    Args:
        lf (pl.LazyFrame): dataset containing a known share column
        known_column (str): one of the known share columns
        location_column (str): the location ID

    Returns:
        pl.LazyFrame: dataset with the boolean "never_submitted" column added
    """
    return lf.with_columns(
        pl.col(known_column)
        .is_not_null()
        .any()
        .over(location_column)
        .not_()
        .alias(ShareModel.never_submitted)
    )


def add_provider_location_count(
    lf: pl.LazyFrame, provider_column: str, location_column: str, date_column: str
) -> pl.LazyFrame:
    """
    Add the number of distinct locations each provider has on each date.

    Each location has a row per job role, so locations are counted once each rather than
    counting rows.

    Args:
        lf (pl.LazyFrame): dataset containing the provider, location and date columns
        provider_column (str): the provider ID
        location_column (str): the location ID
        date_column (str): the date

    Returns:
        pl.LazyFrame: dataset with the "provider_location_count" column added
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
    Add each location's latest real overall CQC rating as of each row's date, or "Not yet rated"
    when it has none by then.

    A rating counts from its rating date, so each row only gets a rating from on or before its
    own date. Ratings with no rating date can't be placed in time, so they're left out. Blank
    ratings (such as "Inspected but not rated", which the ratings job blanks) are skipped, so a
    location whose latest rating is blank gets its most recent real one. Ratings on the same date
    are ordered the way the ratings job picks its latest rating: by assessment date, with its
    latest rating flag breaking any tie.

    The rating is found once per location and date, rather than for every row (such as every job
    role), then joined back on. Any existing "latest_overall_rating" column is replaced.

    Args:
        lf (pl.LazyFrame): dataset containing the location ID and date
        ratings_lf (pl.LazyFrame): CQC ratings dataset, with the location ID in a column of the
            same name
        location_column (str): the location ID
        date_column (str): the date each row's rating must be in place by

    Returns:
        pl.LazyFrame: dataset with the categorical "latest_overall_rating" column added
    """
    # The ratings dataset stores location IDs as plain strings, so they're cast to this
    # dataset's type (categorical in the pipeline) to allow the joins.
    location_type = lf.collect_schema()[location_column]

    ratings_by_date_lf = (
        ratings_lf.filter(
            pl.col(CQCRatings.overall_rating).is_not_null()
            & pl.col(CQCRatings.date).is_not_null()
        )
        .group_by(location_column, CQCRatings.date)
        .agg(
            pl.col(CQCRatings.overall_rating)
            .sort_by(
                [CQCL.assessment_date, CQCRatings.latest_rating_flag],
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

    # join_asof needs both sides sorted by date (the ratings are sorted above). Polars can't
    # check that when also joining by location, so its check is switched off.
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
    Build the dataset for modelling a percentage-share breakdown, such as employment status.

    Both source datasets are wide, so only the columns the models need are selected, straight
    away. The estimates columns are joined on location and import date, which the estimates
    dataset has one row for each of, and the ratings are reduced to one per location and import
    date, so neither join duplicates rows.

    Build this before filtering out any rows, so each location's flags and counts reflect all of
    its data.

    Args:
        shares_lf (pl.LazyFrame): the breakdown's imputed dataset, with a row per location, job
            role and import date
        estimates_lf (pl.LazyFrame): the filled posts estimates dataset
        ratings_lf (pl.LazyFrame): the CQC ratings dataset
        known_share_columns (list[str]): the known share columns
        imputed_share_columns (list[str]): the imputed share columns
        rolling_average_columns (list[str]): the rolling average share columns

    Returns:
        pl.LazyFrame: the modelling dataset, with a row per location, job role and import date
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

    # The estimates dataset may store location IDs as plain strings, so they're cast to this
    # dataset's type (categorical in the pipeline) to allow the join.
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
    lf = add_never_submitted_flag(
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
